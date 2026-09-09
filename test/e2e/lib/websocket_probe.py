#!/usr/bin/env python3
"""Dependency-free guest WebSocket fixture and real data-plane probe."""

import argparse
import base64
import hashlib
import http.client
import http.server
import os
from pathlib import Path
import shlex
import socket
import struct
import time


GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def exact(reader, count):
    data = reader.read(count)
    if len(data) != count:
        raise RuntimeError("truncated WebSocket frame")
    return data


def frame(opcode, payload, masked=False):
    assert len(payload) < 126
    prefix = bytes([0x80 | opcode, len(payload) | (0x80 if masked else 0)])
    if not masked:
        return prefix + payload
    mask = os.urandom(4)
    return prefix + mask + bytes(value ^ mask[i % 4] for i, value in enumerate(payload))


def read_frame(reader, masked=False):
    first, second = exact(reader, 2)
    if first & 0xF0 != 0x80 or bool(second & 0x80) != masked or second & 0x7F >= 126:
        raise RuntimeError("unexpected WebSocket frame framing")
    mask = exact(reader, 4) if masked else bytes(4)
    payload = exact(reader, second & 0x7F)
    return first & 0x0F, bytes(value ^ mask[i % 4] for i, value in enumerate(payload))


class Guest(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_GET(self):
        self.connection.settimeout(10)
        self.close_connection = True
        if self.path == "/health":
            self.send_response(204)
            self.end_headers()
            return
        if self.path != "/ws" or self.headers.get("Upgrade", "").lower() != "websocket":
            self.send_error(400)
            return
        key = self.headers.get("Sec-WebSocket-Key", "")
        accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
        reply = ("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\n"
                 "Upgrade: websocket\r\nSec-WebSocket-Protocol: test\r\n"
                 f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode()
        # Exercise response read-ahead with the handshake and two frames together.
        self.connection.sendall(reply + frame(1, b"ready") + frame(2, b"\x00\xff\x7f"))
        while True:
            opcode, payload = read_frame(self.rfile, masked=True)
            if opcode == 8:
                if self.rfile.read() != b"":
                    raise RuntimeError("unexpected bytes after close frame")
                # This tail must survive the client's TCP write half-close.
                self.connection.sendall(frame(8, payload))
                return
            if opcode not in (1, 2, 9):
                raise RuntimeError("unexpected client opcode")
            self.connection.sendall(frame(10 if opcode == 9 else opcode, payload))


def probe(args):
    key = base64.b64encode(os.urandom(16)).decode()
    headers = [f"GET {args.path} HTTP/1.1", f"Host: {args.authority}",
               "Connection: keep-alive, Upgrade", "Upgrade: websocket",
               f"Sec-WebSocket-Key: {key}", "Sec-WebSocket-Version: 13",
               "Sec-WebSocket-Protocol: test"]
    if args.token:
        headers.append(f"X-Access-Token: {args.token}")
    headers.extend(args.header)
    with socket.create_connection(("127.0.0.1", args.port), timeout=10) as conn:
        conn.sendall(("\r\n".join(headers) + "\r\n\r\n").encode() + frame(1, b"client-text", True))
        with conn.makefile("rb") as reader:
            status = reader.readline().split()
            if len(status) < 2 or status[1] != b"101":
                code = status[1].decode() if len(status) > 1 and status[1].isdigit() else "malformed status"
                raise RuntimeError(f"WebSocket handshake returned {code}, expected 101")
            response = http.client.parse_headers(reader)
            expected = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
            if (response.get("Sec-WebSocket-Accept") != expected or
                    response.get("Upgrade", "").lower() != "websocket" or
                    response.get("Sec-WebSocket-Protocol") != "test"):
                raise RuntimeError("WebSocket handshake headers changed")
            for expected_frame in [(1, b"ready"), (2, b"\x00\xff\x7f"), (1, b"client-text")]:
                if read_frame(reader) != expected_frame:
                    raise RuntimeError("buffered WebSocket payload changed")
            conn.sendall(frame(2, b"\x00binary\xff", True) + frame(9, b"ping", True))
            for expected_frame in [(2, b"\x00binary\xff"), (10, b"ping")]:
                if read_frame(reader) != expected_frame:
                    raise RuntimeError("WebSocket duplex payload changed")
            close = struct.pack("!H", 1000)
            conn.sendall(frame(8, close, True))
            conn.shutdown(socket.SHUT_WR)
            if read_frame(reader) != (8, close) or reader.read() != b"":
                raise RuntimeError("WebSocket close tail changed")
    print("PASS: WebSocket handshake, buffered text/binary, ping/pong and half-close tail")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["serve", "wait", "probe", "guest-command"])
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--authority", default="guest")
    parser.add_argument("--path", default="/ws")
    parser.add_argument("--token", default="")
    parser.add_argument("--header", action="append", default=[])
    args = parser.parse_args()
    if args.mode == "serve":
        http.server.ThreadingHTTPServer(("0.0.0.0", args.port), Guest).serve_forever()
    elif args.mode == "probe":
        probe(args)
    elif args.mode == "guest-command":
        encoded = base64.b64encode(Path(__file__).read_bytes()).decode()
        path = "/tmp/kuasar-websocket.py"
        install = f"import base64;open({path!r},'wb').write(base64.b64decode({encoded!r}))"
        print(f"python3 -c {shlex.quote(install)}\n"
              f"python3 {path} serve --port {args.port} </dev/null >/tmp/kuasar-websocket.log 2>&1 &\n"
              f"python3 {path} wait --port {args.port}")
    else:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            conn = http.client.HTTPConnection("127.0.0.1", args.port, timeout=1)
            try:
                conn.request("GET", "/health")
                if conn.getresponse().status == 204:
                    return
            except OSError:
                pass
            finally:
                conn.close()
            time.sleep(0.1)
        raise RuntimeError("guest WebSocket fixture did not become ready")


if __name__ == "__main__":
    main()
