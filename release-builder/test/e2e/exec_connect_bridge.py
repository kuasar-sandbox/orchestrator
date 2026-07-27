#!/usr/bin/env python3
"""One-shot ctl.sock to HTTP/1 CONNECT bridge for the real exec E2E tests.

The bridge deliberately implements only the test transport needed by #64.  It
accepts one local Unix-stream client, opens one HTTP/1.1 CONNECT tunnel to a
node or cluster router, then relays the opaque sandbox-ctl stream in both
directions.  The access token is read from an inherited environment variable
and is never written to diagnostics or files by this helper.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


MAX_RESPONSE_HEADER = 64 * 1024


@dataclass(frozen=True)
class BridgeConfig:
    listen: Path
    ready_file: Path
    upstream_host: str
    upstream_port: int
    sandbox_id: str
    token: str
    group: str = ""
    route_key: str = ""


def _safe_header_value(name: str, value: str, *, required: bool = True) -> str:
    if required and not value:
        raise ValueError(f"{name} is required")
    if "\r" in value or "\n" in value:
        raise ValueError(f"{name} contains a line break")
    return value


def _connect_request(config: BridgeConfig) -> bytes:
    sandbox_id = _safe_header_value("sandbox id", config.sandbox_id)
    token = _safe_header_value("access token", config.token)
    group = _safe_header_value("group", config.group, required=False)
    route_key = _safe_header_value("route key", config.route_key, required=False)
    if bool(group) != bool(route_key):
        raise ValueError("group and route key must be supplied together")

    lines = [
        "CONNECT sandbox:443 HTTP/1.1",
        "Host: sandbox:443",
        f"E2b-Sandbox-Id: {sandbox_id}",
        "E2b-Sandbox-Service: exec",
        f"X-Access-Token: {token}",
    ]
    if group:
        lines.extend(
            (
                f"X-Kuasar-Sandbox-Group: {group}",
                f"X-Kuasar-Route-Key: {route_key}",
            )
        )
    return ("\r\n".join(lines) + "\r\n\r\n").encode("ascii")


def _read_connect_response(upstream: socket.socket) -> bytes:
    """Validate the CONNECT response and return bytes read past its headers."""

    response = bytearray()
    delimiter = b"\r\n\r\n"
    while True:
        end = response.find(delimiter)
        if end >= 0:
            header_end = end + len(delimiter)
            if header_end > MAX_RESPONSE_HEADER:
                raise RuntimeError("CONNECT response headers exceed 64 KiB")
            break
        if len(response) >= MAX_RESPONSE_HEADER:
            raise RuntimeError("CONNECT response headers exceed 64 KiB")
        chunk = upstream.recv(min(4096, MAX_RESPONSE_HEADER + 1 - len(response)))
        if not chunk:
            raise RuntimeError("CONNECT peer closed before completing response headers")
        response.extend(chunk)

    first_line = bytes(response[:end]).split(b"\r\n", 1)[0]
    fields = first_line.split(b" ", 2)
    if len(fields) < 2 or fields[0] not in (b"HTTP/1.0", b"HTTP/1.1"):
        raise RuntimeError("CONNECT peer returned a malformed status line")
    try:
        status = int(fields[1])
    except ValueError as exc:
        raise RuntimeError("CONNECT peer returned a malformed status code") from exc
    if status != 200:
        raise RuntimeError(f"CONNECT peer returned HTTP {status}")
    return bytes(response[header_end:])


def _shutdown_write(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_WR)
    except OSError:
        pass


def _shutdown_all(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def _relay(left: socket.socket, right: socket.socket) -> None:
    """Relay until both read halves close, preserving TCP/UDS half-close."""

    failures: list[BaseException] = []
    failure_lock = threading.Lock()

    def pump(source: socket.socket, destination: socket.socket) -> None:
        try:
            while True:
                data = source.recv(64 * 1024)
                if not data:
                    _shutdown_write(destination)
                    return
                destination.sendall(data)
        except (BrokenPipeError, ConnectionResetError):
            _shutdown_write(destination)
        except BaseException as exc:  # propagate unexpected relay failures
            with failure_lock:
                failures.append(exc)
            _shutdown_all(source)
            _shutdown_all(destination)

    left_to_right = threading.Thread(target=pump, args=(left, right), daemon=True)
    right_to_left = threading.Thread(target=pump, args=(right, left), daemon=True)
    left_to_right.start()
    right_to_left.start()
    left_to_right.join()
    right_to_left.join()
    if failures:
        raise RuntimeError("CONNECT tunnel relay failed") from failures[0]


def run_bridge(config: BridgeConfig) -> None:
    config.listen.parent.mkdir(parents=True, exist_ok=True)
    config.ready_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        config.listen.unlink()
    except FileNotFoundError:
        pass
    try:
        config.ready_file.unlink()
    except FileNotFoundError:
        pass

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(str(config.listen))
        listener.listen(1)
        config.ready_file.touch(mode=0o600, exist_ok=False)
        local, _ = listener.accept()
        with local:
            upstream = socket.create_connection(
                (config.upstream_host, config.upstream_port), timeout=10
            )
            with upstream:
                upstream.sendall(_connect_request(config))
                prefetched = _read_connect_response(upstream)
                upstream.settimeout(None)
                if prefetched:
                    local.sendall(prefetched)
                _relay(local, upstream)
    finally:
        listener.close()
        try:
            config.ready_file.unlink()
        except FileNotFoundError:
            pass
        try:
            config.listen.unlink()
        except FileNotFoundError:
            pass


def _parse_upstream(value: str) -> tuple[str, int]:
    host, separator, port_text = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError("upstream must be HOST:PORT")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        port = int(port_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("upstream port must be an integer") from exc
    if port < 1 or port > 65535:
        raise argparse.ArgumentTypeError("upstream port is outside 1..65535")
    return host, port


def _self_test() -> None:
    """Exercise readiness, response pre-read and both half-close directions."""

    with tempfile.TemporaryDirectory(prefix="exec-connect-bridge-") as temp_dir:
        root = Path(temp_dir)
        upstream_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_listener.bind(("127.0.0.1", 0))
        upstream_listener.listen(1)
        upstream_port = upstream_listener.getsockname()[1]
        upstream_input: list[bytes] = []
        upstream_header: list[bytes] = []

        def serve_upstream() -> None:
            conn, _ = upstream_listener.accept()
            with conn:
                request = bytearray()
                while b"\r\n\r\n" not in request:
                    data = conn.recv(4096)
                    if not data:
                        raise AssertionError("bridge closed during CONNECT request")
                    request.extend(data)
                header, prefetched = bytes(request).split(b"\r\n\r\n", 1)
                upstream_header.append(header)
                conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\nserver-prefetch")
                received = bytearray(prefetched)
                while True:
                    data = conn.recv(4096)
                    if not data:
                        break
                    received.extend(data)
                upstream_input.append(bytes(received))
                conn.sendall(b"-after-client-eof")
                _shutdown_write(conn)

        server_thread = threading.Thread(target=serve_upstream, daemon=True)
        server_thread.start()
        config = BridgeConfig(
            listen=root / "run" / "stable" / "ctl.sock",
            ready_file=root / "bridge.ready",
            upstream_host="127.0.0.1",
            upstream_port=upstream_port,
            sandbox_id="stable",
            token="self-test-token",
            group="/e2e/group",
            route_key="rk",
        )
        bridge_error: list[BaseException] = []

        def serve_bridge() -> None:
            try:
                run_bridge(config)
            except BaseException as exc:
                bridge_error.append(exc)

        bridge_thread = threading.Thread(target=serve_bridge, daemon=True)
        bridge_thread.start()
        for _ in range(200):
            if config.ready_file.exists() and config.listen.exists():
                break
            threading.Event().wait(0.01)
        else:
            raise AssertionError("bridge did not publish readiness")

        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(str(config.listen))
        with client:
            client.sendall(b"client-input")
            _shutdown_write(client)
            response = bytearray()
            while True:
                data = client.recv(4096)
                if not data:
                    break
                response.extend(data)

        bridge_thread.join(timeout=5)
        server_thread.join(timeout=5)
        upstream_listener.close()
        if bridge_thread.is_alive() or server_thread.is_alive():
            raise AssertionError("bridge self-test did not terminate")
        if bridge_error:
            raise AssertionError("bridge self-test raised an exception") from bridge_error[0]
        if upstream_input != [b"client-input"]:
            raise AssertionError("client-to-upstream relay or half-close failed")
        if response != b"server-prefetch-after-client-eof":
            raise AssertionError("upstream-to-client pre-read or half-close failed")
        header = upstream_header[0]
        for expected in (
            b"E2b-Sandbox-Id: stable",
            b"E2b-Sandbox-Service: exec",
            b"X-Kuasar-Sandbox-Group: /e2e/group",
            b"X-Kuasar-Route-Key: rk",
        ):
            if expected not in header:
                raise AssertionError(f"CONNECT request omitted {expected!r}")

        class OversizedHeader:
            def __init__(self) -> None:
                self.done = False

            def recv(self, size: int) -> bytes:
                if self.done:
                    return b""
                self.done = True
                return b"H" * (MAX_RESPONSE_HEADER + 1)

        try:
            _read_connect_response(OversizedHeader())  # type: ignore[arg-type]
        except RuntimeError as exc:
            if "64 KiB" not in str(exc):
                raise
        else:
            raise AssertionError("oversized CONNECT response header was accepted")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen", type=Path, help="one-shot Unix socket path")
    parser.add_argument("--ready-file", type=Path, help="created after listen()")
    parser.add_argument("--upstream", type=_parse_upstream, help="node/router HOST:PORT")
    parser.add_argument("--sandbox-id", help="stable SID sent in the CONNECT header")
    parser.add_argument(
        "--token-env",
        default="EXEC_CONNECT_TOKEN",
        help="environment variable containing the exec token",
    )
    parser.add_argument("--group", default="", help="cluster sandbox group")
    parser.add_argument("--route-key", default="", help="cluster route key")
    parser.add_argument("--self-test", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.self_test:
        _self_test()
        return 0
    missing = [
        name
        for name, value in (
            ("--listen", args.listen),
            ("--ready-file", args.ready_file),
            ("--upstream", args.upstream),
            ("--sandbox-id", args.sandbox_id),
        )
        if value is None
    ]
    if missing:
        parser.error("required arguments: " + ", ".join(missing))
    token = os.environ.get(args.token_env, "")
    if not token:
        parser.error(f"environment variable {args.token_env!r} is empty")
    host, port = args.upstream
    config = BridgeConfig(
        listen=args.listen,
        ready_file=args.ready_file,
        upstream_host=host,
        upstream_port=port,
        sandbox_id=args.sandbox_id,
        token=token,
        group=args.group,
        route_key=args.route_key,
    )
    try:
        run_bridge(config)
    except Exception as exc:
        print(f"exec CONNECT bridge: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
