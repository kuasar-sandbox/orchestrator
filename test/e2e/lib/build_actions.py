#!/usr/bin/env python3
"""Real Builder cancellation under a one-Build execution limit.

Invoked by the standalone and cluster fixtures, which own all supplied paths,
units, credentials, switch and registry artifacts. Database access is read-only.
No test-side Stop, DELETE SQL or directory cleanup can satisfy an assertion.
"""

import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "host", "db", "run-root", "base-root", "socket", "bin", "switch", "source", "evidence"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--group", default="")
    parser.add_argument("--cpu", type=int, default=2)
    parser.add_argument("--timeout-only", type=int, default=0, metavar="SECONDS")
    parser.add_argument("--restart-request")
    parser.add_argument("--restart-ready")
    args = parser.parse_args()
    key = os.environ["BUILD_ACTION_API_KEY"]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    evidence = []

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

    for mode in (("timeout",) if args.timeout_only else ("cancel", "query", "header")):
        tid, bid = register(mode + "-hang")
        marker = "ISSUE372_HANG_" + bid
        trigger(tid, bid, "echo " + marker + "; while :; do sleep 1; done")

        def live_hang():
            current = row(bid)
            assert current and current["status"] != "error", current and current["reason"]
            if not current["run_id"] or not current["phase_sandbox_id"]:
                return None
            log = command("journalctl", "KUASAR_BUILD_ID=" + bid, "--no-pager", "--output=cat")
            # The marker occurs in the submitted spec too; require the actual
            # guest output line rather than merely finding a command argument.
            return current if any(line.strip() == marker for line in log.splitlines()) else None

        current = wait_for("real guest hang " + mode, live_hang)
        unit = "sandbox-builder@" + current["run_id"] + ".service"
        cg, processes = snapshot_processes(unit)
        assert current["execution_claimed"] == 1 and current["runtime_vswitch_port"], current
        target = None
        if not args.group and mode in ("query", "header"):
            target = {"kind": "sandbox", "memory": mode == "header"}
        wt, wb = register(mode + "-waiter", target)
        trigger(wt, wb, "echo ISSUE372_NEXT_" + wb + "; sleep 3")
        waiting = row(wb)
        assert waiting and waiting["status"] == "waiting" and waiting["execution_claimed"] == 0, waiting
        before = row(bid)
        admin = json.loads(command(str(Path(args.bin) / "node-ctl"), "builder", "status", "--socket", args.socket))
        owned = [b for b in admin["builds"] if b["buildID"] == bid]
        assert len(owned) == 1 and owned[0]["templateID"] == tid and owned[0]["executionClaimed"], admin
        require("DELETE", "/templates/" + tid, 409)
        require("DELETE", "/templates/" + tid + "?cancel=true", 409, header='{"cancel":false}')
        assert row(bid)["cancel_requested_unix"] == 0 and not unit_empty(unit), "ordinary DELETE interrupted the hang"
        started = time.monotonic()
        if mode == "cancel":
            require("POST", f"/templates/{tid}/builds/{bid}/cancel", 202)
        elif mode == "query":
            _, headers = require("DELETE", "/templates/" + tid + "?cancel=true", 202)
            assert headers["Location"] == status_path(tid, bid), headers
        elif mode == "header":
            _, headers = require("DELETE", "/templates/" + tid + "?cancel=false", 202, header='{"cancel":true}')
            assert headers["Location"] == status_path(tid, bid), headers

        if mode == "timeout":
            cancelled = wait_for("normal total timeout cleans automatically",
                                 lambda: check_terminal(tid, bid, "error"), args.timeout_only + 60)
            assert not cancelled["cancelRequested"] and not cancelled["deleteRequested"], cancelled
            assert row(bid)["execution_claimed"] == 0
            age = time.time() - before["execution_claimed_unix"]
            assert args.timeout_only - 10 <= age <= args.timeout_only + 60, age
        elif mode == "cancel":
            cancelled = wait_for("cancel completes", lambda: check_terminal(tid, bid, "error"), 60)
            assert cancelled["cancelRequested"] and not cancelled["deleteRequested"], cancelled
            require("POST", f"/templates/{tid}/builds/{bid}/cancel", 204)
            assert row(bid)["execution_claimed"] == 0
        else:
            def deleted():
                code, value, _ = request("GET", status_path(tid, bid))
                assert code in (200, 404), (code, value)  # 5xx/network errors never mean completion.
                if code == 200:
                    assert value["cancelRequested"] and value["deleteRequested"], value
                return code == 404
            wait_for("delete completes", deleted, 60)
            assert row(bid) is None
            require("DELETE", "/templates/" + tid, 404)
        assert_reclaimed(bid, unit, cg, processes)
        elapsed = time.monotonic() - started
        if mode != "timeout":
            assert elapsed < 60, "cancellation fell back to execution/step timeout"
        ready = wait_for("next normal Build completes", lambda: check_terminal(wt, wb, "ready"))
        assert not ready["cancelRequested"] and not ready["deleteRequested"], ready
        next_log = command("journalctl", "KUASAR_BUILD_ID=" + wb, "--no-pager", "--output=cat")
        assert any(line.strip() == "ISSUE372_NEXT_" + wb for line in next_log.splitlines()), "waiting Build never ran its guest step"
        slots = json.loads(command(str(Path(args.bin) / "connector-ctl"), "vswitch", "show", "slots", args.switch))
        slot = [s for s in slots if str(s["port"]) == before["runtime_vswitch_port"]]
        assert len(slot) == 1 and not slot[0]["allocated"], (before["runtime_vswitch_port"], slot)
        assert row(wb)["execution_claimed"] == 0
        # A successful Build's original transient ID still routes after its
        # result has changed to a canonical reference; deletion cannot touch it.
        canonical = ready["templateID"]
        if args.restart_request and mode == "cancel":
            Path(args.restart_request).touch()
            wait_for("fixture Router/Registry restart", lambda: Path(args.restart_ready).exists(), 60)
            value, _ = require("GET", status_path(wt, wb), 200)
            assert value["status"] == "ready" and value["templateID"] == canonical, value
        require("DELETE", "/templates/" + canonical, 400)
        require("DELETE", "/templates/" + wt, 204)
        assert row(wb) is None
        if not args.group:
            created, _ = require("POST", "/sandboxes", 201, {"templateID": canonical, "timeout": 120})
            sid = created["sandboxID"]
            def running():
                sandbox, _ = require("GET", "/sandboxes/" + sid, 200)
                assert sandbox["state"] != "dead", sandbox
                return sandbox["state"] == "running"
            wait_for("canonical Create after explicit Build deletion", running)
            command(str(Path(args.bin) / "sandbox-ctl"), "exec", "--uds",
                    str(Path(args.run_root) / "sandboxes" / sid / "ctl.sock"), "--", "/bin/true")
            require("DELETE", "/sandboxes/" + sid, 204)
        rt, rb = register(mode + "-canonical-reuse", target)
        require("POST", f"/v2/templates/{rt}/builds/{rb}", 202, {"fromTemplate": canonical})
        reused = wait_for("canonical fromTemplate survives Build deletion", lambda: check_terminal(rt, rb, "ready"))
        assert reused["templateID"].split("-", 2)[:2] == canonical.split("-", 2)[:2], (reused, canonical)
        if target is None:
            assert reused["templateID"] == canonical, (reused, canonical)
        require("DELETE", "/templates/" + rt, 204)
        if mode in ("cancel", "timeout"):
            assert row(bid) is not None, "diagnostic row disappeared"
            require("DELETE", "/templates/" + tid, 204)
        item = {"mode": mode, "hang_build": bid, "transient_id": tid, "unit": unit,
                "phase_sandbox": before["phase_sandbox_id"], "port": before["runtime_vswitch_port"],
                "cgroup": str(cg), "stopped_pids": list(processes), "seconds": round(elapsed, 3),
                "next_build": wb, "next_status": "ready", "canonical_reused": canonical, "create_exec_verified": not bool(args.group)}
        evidence.append(item)
        Path(args.evidence).write_text(json.dumps(evidence, indent=2) + "\n")
        print("==> PASS: issue372 " + json.dumps(item), flush=True)


if __name__ == "__main__":
    main()
