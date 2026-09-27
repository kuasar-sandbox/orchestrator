#!/usr/bin/env python3
import base64, http.client, json, socket, struct, sys

sock_path, token, cmd = sys.argv[1], sys.argv[2], sys.argv[3]
class UDS(http.client.HTTPConnection):
    def __init__(s): super().__init__("envd")
    def connect(s):
        s.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); s.sock.connect(sock_path)

req = {"process": {"cmd": "/bin/sh", "args": ["-c", "exec " + cmd], "cwd": "/home/user"}}
body = json.dumps(req).encode()
env = b"\x00" + struct.pack(">I", len(body)) + body
c = UDS()
c.request("POST", "/process.Process/Start", body=env, headers={
    "Content-Type": "application/connect+json", "Connect-Protocol-Version": "1",
    "Authorization": "Basic " + base64.b64encode(b"user:").decode(),
    "X-Access-Token": token})
r = c.getresponse()
if r.status != 200:
    raise SystemExit(f"envd start HTTP {r.status}: {r.read()!r}")
while True:
    header = r.read(5)
    if len(header) != 5:
        raise SystemExit("envd stream ended before start event")
    flag, length = header[0], struct.unpack(">I", header[1:])[0]
    message = r.read(length)
    event = json.loads(message) if message else {}
    if flag & 2:
        raise SystemExit(f"envd stream error before start: {event!r}")
    start = event.get("event", {}).get("start")
    if start is not None:
        print(start["pid"])
        r.close()
        c.close()
        break
