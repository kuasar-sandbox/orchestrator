#!/usr/bin/env python3
"""Finite streaming Guest requests and separate MMDS checks during takeover."""
import argparse
import base64
import http.client
import http.server
import json
from pathlib import Path
import shlex
import threading
import time
from urllib.parse import parse_qs, urlsplit

events = {}
lock = threading.Lock()
expected_sid = ''


def event(key):
    with lock:
        return events.setdefault(key, threading.Event())


def metadata_connection():
    connection = http.client.HTTPConnection('169.254.169.254', 80, timeout=5)
    try:
        connection.request('PUT', '/latest/api/token', headers={'X-metadata-token-ttl-seconds': '60'})
        response = connection.getresponse()
        token = response.read().decode()
        assert response.status == 200, 'MMDS token request failed'
        return connection, token
    except BaseException:
        connection.close()
        raise


def metadata(connection, token):
    try:
        connection.request('GET', '/', headers={'X-metadata-token': token})
        response = connection.getresponse()
        body = json.loads(response.read())
        assert response.status == 200 and body['instanceID'] == expected_sid, 'MMDS identity mismatch'
    finally:
        connection.close()


class Guest(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_):
        pass

    def do_GET(self):
        route = urlsplit(self.path)
        if route.path in ('/health', '/metadata'):
            if route.path == '/metadata':
                metadata(*metadata_connection())
            self.send_response(200)
            self.send_header('Content-Length', '8')
            self.end_headers()
            self.wfile.write(b'healthy\n')
            return
        key = parse_qs(route.query).get('id', [''])[0]
        if not key or route.path not in ('/finite', '/release'):
            self.send_error(400)
            return
        if route.path == '/release':
            event(key).set()
            self.send_response(204)
            self.end_headers()
            return
        expected = b'begin\ncomplete\n'
        self.send_response(200)
        self.send_header('Content-Length', str(len(expected)))
        self.end_headers()
        self.wfile.write(b'begin\n')
        self.wfile.flush()
        if not event(key).wait(45):
            self.close_connection = True
            return
        try:
            self.wfile.write(b'complete\n')
        except Exception:
            self.close_connection = True


def main():
    global expected_sid
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['serve', 'wait', 'guest-command'])
    parser.add_argument('--sid', required=True)
    args = parser.parse_args()
    expected_sid = args.sid
    if args.mode == 'serve':
        http.server.ThreadingHTTPServer(('0.0.0.0', 8002), Guest).serve_forever()
    elif args.mode == 'guest-command':
        path = '/tmp/kuasar-proxy-lifecycle.py'
        encoded = base64.b64encode(Path(__file__).read_bytes()).decode()
        install = f"import base64;open({path!r},'wb').write(base64.b64decode({encoded!r}))"
        print(f'python3 -c {shlex.quote(install)}\n'
              f'python3 {path} serve --sid {shlex.quote(args.sid)} </dev/null >/tmp/kuasar-proxy-lifecycle.log 2>&1 &\n'
              f'python3 {path} wait --sid {shlex.quote(args.sid)}')
    else:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            connection = http.client.HTTPConnection('127.0.0.1', 8002, timeout=1)
            try:
                connection.request('GET', '/health')
                response = connection.getresponse()
                if response.status == 200 and response.read() == b'healthy\n':
                    return
            except OSError:
                pass
            finally:
                connection.close()
            time.sleep(.05)
        raise RuntimeError('Guest lifecycle server did not become ready')


if __name__ == '__main__':
    main()
