"""HTTP, process and read-only ownership observations for real Builder cases."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request

from placer_readiness import wait_for_placer


def command(*args):
    return subprocess.check_output(args, text=True, timeout=30).strip()


def wait_for(label, check, seconds=180):
    deadline = time.monotonic() + seconds
    while True:
        value = check()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"timeout: {label}")
        time.sleep(0.1)


def configure():
    global args, key, opener
    parser = argparse.ArgumentParser(description="Prepared Builder fixture endpoints")
    for name in ("url", "host", "db", "run-root", "base-root", "socket", "bin", "switch", "source", "evidence"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--conductor-pid", type=int, required=True)
    parser.add_argument("--group", default="")
    parser.add_argument("--cpu", type=int, default=2)
    parser.add_argument("--timeout-only", type=int, default=0, metavar="SECONDS")
    parser.add_argument("--restart-request")
    parser.add_argument("--restart-ready")
    parser.add_argument("--placer-url")
    parser.add_argument("--registry-url", action="append", default=[])
    parser.add_argument("--expected-node")
    args = parser.parse_args()
    key = os.environ["BUILD_ACTION_API_KEY"]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    args.post_restart = False
    return args

def request(method, path, body=None, header=None):
    headers = {"Host": args.host, "X-API-KEY": key}
    if args.group:
        headers["X-Kuasar-Sandbox-Group"] = args.group
    if header is not None:
        headers["X-Kuasar-Sandbox-Builder"] = header
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    req = urllib.request.Request(args.url + path, data=data, headers=headers, method=method)
    try:
        response = opener.open(req, timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        try:
            value = json.loads(raw) if raw else None
        except ValueError:
            value = raw.decode(errors="replace")
        return response.status, value, response.headers


def require(method, path, want, body=None, header=None):
    code, value, headers = request(method, path, body, header)
    assert code == want, (method, path, code, want, value)
    return value, headers


def row(build_id):
    with sqlite3.connect("file:" + args.db + "?mode=ro", uri=True, timeout=5) as db:
        db.row_factory = sqlite3.Row
        value = db.execute("SELECT * FROM builds WHERE build_id=?", (build_id,)).fetchone()
        return dict(value) if value else None


def register(label, target=None):
    if args.post_restart and args.placer_url:
        # Watch failover can invalidate a previously ready view between
        # Build actions. Recheck before each new registration, not the write.
        assert args.expected_node, "--placer-url requires --expected-node"
        wait_for_placer(args.placer_url, args.group, args.expected_node,
                        registry_urls=args.registry_url, api_key=key)
    value, _ = require("POST", "/v3/templates", 202,
                       {"name": "issue372-" + label, "cpuCount": args.cpu, "memoryMB": 6144},
                       header=json.dumps({"target": target}) if target else None)
    assert value["templateID"].startswith("transient-"), value
    return value["templateID"], value["buildID"]


def trigger(tid, bid, script):
    require("POST", f"/v2/templates/{tid}/builds/{bid}", 202,
            {"fromTemplate": args.source, "steps": [{"type": "RUN", "args": [script]}]})


def status_path(tid, bid):
    return f"/templates/{tid}/builds/{bid}/status"


def check_terminal(tid, bid, expected):
    value, _ = require("GET", status_path(tid, bid), 200)
    assert value["status"] in ("building", expected), value
    return value if value["status"] == expected else None


def unit_empty(unit):
    # list-units also covers collected instances without reloading them.
    units = json.loads(command("systemctl", "list-units", "--all", "--output=json", unit))
    return not units or all(u["active"] in ("inactive", "failed") for u in units)


def snapshot_processes(unit):
    cg = command("systemctl", "show", "--property=ControlGroup", "--value", unit)
    assert cg and cg.startswith("/"), (unit, cg)
    root = Path("/sys/fs/cgroup" + cg)
    processes = {}
    for procs in root.rglob("cgroup.procs"):
        for pid in procs.read_text().split():
            path = Path("/proc") / pid
            try:
                processes[pid] = {"stat": (path / "stat").read_text().split(")", 1)[1].split()[19],
                                  "command": (path / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")}
            except FileNotFoundError:
                pass
    assert any("cloud-hypervisor" in p["command"] for p in processes.values()), (unit, processes)
    return root, processes


def assert_reclaimed(bid, unit, cg, processes):
    assert unit_empty(unit), f"unit still live: {unit}"
    if cg.exists():
        assert "populated 0" in (cg / "cgroup.events").read_text(), str(cg)
    for pid, old in processes.items():
        path = Path("/proc") / pid / "stat"
        if path.exists():
            assert path.read_text().split(")", 1)[1].split()[19] != old["stat"], f"old process {pid} still alive"
    for root in (args.run_root, args.base_root):
        assert not (Path(root) / "builds" / bid).exists(), (root, bid)


def conductor_fds():
    targets = []
    for fd in (Path("/proc") / str(args.conductor_pid) / "fd").iterdir():
        try:
            target = os.readlink(fd)
            targets.append(target.removesuffix(" (deleted)"))
        except FileNotFoundError:
            pass
    return targets


def exec_sandbox(sid):
    command(str(Path(args.bin) / "sandbox-ctl"), "exec", "--run-root",
            str(Path(args.run_root) / "sandboxes"), "--sandbox-id", sid, "--", "/bin/true")


def create_sandbox(canonical):
    created, _ = require("POST", "/sandboxes", 201, {"templateID": canonical, "timeout": 120})
    sid = created["sandboxID"]

    def running():
        sandbox, _ = require("GET", "/sandboxes/" + sid, 200)
        assert sandbox["state"] != "dead", sandbox
        return sandbox["state"] == "running"

    wait_for("canonical Create guest ready", running)
    with sqlite3.connect("file:" + args.db + "?mode=ro", uri=True, timeout=5) as db:
        actual = db.execute("SELECT template_id FROM sandboxes WHERE id=?", (sid,)).fetchone()
    assert actual == (canonical,), (sid, actual, canonical)
    exec_sandbox(sid)
    return sid


def delete_sandbox(sid):
    require("DELETE", "/sandboxes/" + sid, 204)

    def cleaned():
        with sqlite3.connect("file:" + args.db + "?mode=ro", uri=True, timeout=5) as db:
            retained = db.execute("SELECT 1 FROM sandboxes WHERE id=?", (sid,)).fetchone()
        return retained is None and all(not (Path(root) / "sandboxes" / sid).exists()
                                        for root in (args.run_root, args.base_root))

    # Sandbox DELETE accepts asynchronous cleanup. Do not overlap the next
    # Create/Build with this guest's remaining resource ownership.
    wait_for("exact sandbox cleanup after DELETE", cleaned)
