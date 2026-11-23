#!/usr/bin/env python3
"""Real Node/SQLite/VM checks through the test-only NodeLink ordering relay."""
import concurrent.futures
import json
import os
import pathlib
import sqlite3
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    work = pathlib.Path(os.environ["FAULT_WORK"])
    out = pathlib.Path(os.environ["FAULT_OUT"])
    group = os.environ["FAULT_GROUP"]
    host = "api." + os.environ["FAULT_DOMAIN"]
    api_key = os.environ["FAULT_API_KEY"]
    router = os.environ["FAULT_ROUTER"]
    node = os.environ["FAULT_NODE"]
    mode_path = work / "fault-mode"
    receipts = []
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def mode(value):
        temporary = mode_path.with_suffix(".next")
        temporary.write_text(value + "\n")
        temporary.replace(mode_path)

    def request(base, method, path, route="", body=None):
        headers = {"Host": host, "X-API-KEY": api_key,
                   "X-Kuasar-Sandbox-Group": group}
        if route:
            headers["X-Kuasar-Route-Key"] = route
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
        try:
            response = urllib.request.urlopen(req, timeout=240)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read()
            try:
                body = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                # RouteLink errors use ordinary HTTP text; Node API errors
                # use JSON. Preserve the real status across either surface.
                body = {"error": raw.decode("utf-8", errors="replace")}
            return response.code, body

    def record(receipt):
        receipts.append(receipt)
        (out / "fault-results.json").write_text(json.dumps(receipts, indent=2) + "\n")

    def events():
        return [json.loads(line) for line in (out / "relay-events.jsonl").read_text().splitlines()]

    def until(check, label, timeout=40):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = check()
            if result:
                return result
            time.sleep(0.05)
        raise AssertionError(label)

    def row(sid):
        with sqlite3.connect(work / "cl/node-ctl.db") as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("select id, stable_id, run_id, state from sandboxes where stable_id=?", (sid,)).fetchall()
        assert len(rows) <= 1, "multiple current Node rows for one logical sandbox"
        return dict(rows[0]) if rows else None

    def assert_live(created, route):
        sid = created["sandboxID"]
        def ready():
            status, body = request(router, "GET", "/sandboxes/" + sid, route)
            return status == 200 and body.get("state") == "running"
        until(ready, "public object did not converge to running")
        local = row(sid)
        assert local and local["run_id"], "missing actual Node row/run"
        subprocess.run(["systemctl", "is-active", "--quiet", "sandbox-runner@" + local["run_id"] + ".service"], check=True)
        status, capability = request(router, "POST", "/sandboxes/" + sid + "/exec-sessions", route, {})
        assert status == 201, "existing object lost exec authorization"
        marker = "fault-acceptance-" + sid
        command = [os.environ["FAULT_SANDBOX_CTL"], "exec", "--proxy", router,
                   "--proxy-header", "E2b-Sandbox-Id: " + sid,
                   "--proxy-header", "E2b-Sandbox-Service: exec",
                   "--proxy-header", "X-Access-Token: " + capability["execAccessToken"],
                   "--proxy-header", "X-Kuasar-Sandbox-Group: " + group,
                   "--proxy-header", "X-Kuasar-Route-Key: " + route,
                   "--", "/bin/echo", marker]
        result = subprocess.run(command, capture_output=True, timeout=40)
        assert result.returncode == 0 and result.stdout.strip() == marker.encode(), "actual guest exec failed"
        return local

    def delete(created, route):
        sid = created["sandboxID"]
        status, _ = request(router, "DELETE", "/sandboxes/" + sid, route)
        assert status == 204, "Delete rejected"
        until(lambda: row(sid) is None, "Node deletion did not finalize")

    group_path = "/groups?" + urllib.parse.urlencode({"group": group})
    code, original = request(node, "GET", group_path)
    assert code == 200, "Group not ready"

    for label, fault in (("F03", "drop-create-ack"), ("F04", "error-after-ready")):
        route = "fault/" + label
        start = len(events())
        mode(fault)
        status, created = request(router, "POST", "/sandboxes", route, {})
        assert status == 201, label + " did not converge from real route facts: " + str(status)
        local = assert_live(created, route)
        observed = events()[start:]
        commands = [e for e in observed if e["action"] == "create-received"]
        assert len(commands) == 1 and commands[0]["sid"] == local["id"], "unexpected second Create delivery"
        action = "drop" if label == "F03" else "forward-late-error"
        assert any(e["action"] == action and e["type"] == "cmd_ack" for e in observed), "fault was not actually injected"
        if label == "F04":
            ready = next(i for i, e in enumerate(observed) if e["action"] == "forward-up" and e.get("state") == "running")
            error = next(i for i, e in enumerate(observed) if e["action"] == "forward-late-error")
            assert ready < error, "late error preceded the accepted fact"
        record({"case": label, "public_status": status, "node": local, "create_deliveries": len(commands), "fault_observed": action})
        mode("normal")
        status, again = request(router, "POST", "/sandboxes", route, {})
        assert status == 201 and again["sandboxID"] == created["sandboxID"], "existing request changed logical identity"
        for key in ("envdAccessToken", "trafficAccessToken", "forwardAccessToken"):
            assert again[key] == created[key], "existing request changed public credentials"
        delete(created, route)

    route = "fault/F06"
    start = len(events())
    mode("pause-create")
    pending = pool.submit(request, router, "POST", "/sandboxes", route, {})
    command = until(lambda: next((e for e in events()[start:] if e["action"] == "create-received"), None), "no paused initial delivery")
    status, detail = request(node, "GET", group_path)
    assert status == 200
    updated = dict(detail["context"])
    updated["metadata"] = {"fault-window": "before-node-admission"}
    status, changed = request(node, "PUT", group_path, body={"expected_version": detail["version"], "context": updated})
    assert status == 200 and changed["version"] > detail["version"], "Group update did not commit"
    mode("normal")
    status, _ = pending.result(timeout=50)
    assert status == 409, "stale Group command was not rejected: " + str(status)
    with sqlite3.connect(work / "cl/node-ctl.db") as db:
        assert db.execute("select count(*) from sandboxes where id=?", (command["sid"],)).fetchone()[0] == 0, "rejected delivery created a Node row"
    assert not (work / "cr/sandboxes" / command["sid"]).exists(), "rejected delivery created a RunDir"
    time.sleep(1)  # Allow the already committed Group report to publish; no write retry.
    status, created = request(router, "POST", "/sandboxes", route, {})
    assert status == 201, "fresh request after exact rejection failed: " + str(status)
    local = assert_live(created, route)
    assert local["id"] != command["sid"], "retired execution identity was reused"
    record({"case": "F06", "rejected_node_sid": command["sid"], "public_status": 409, "replacement": local})

    # Delete is later than copied/accepted input; retained object authorization
    # must survive, and the importer must not revive this explicit tombstone.
    status, current = request(node, "GET", group_path)
    assert status == 200
    status, tombstone = request(node, "DELETE", group_path + "&expected_version=" + str(current["version"]))
    assert status == 200
    assert_live(created, route)
    time.sleep(1.5)
    status, missing = request(node, "GET", group_path)
    assert status == 404 and missing["version"] == tombstone["version"], "background importer revived a tombstone"
    status, _ = request(router, "POST", "/sandboxes", "fault/after-delete", {})
    assert status != 201, "new creation admitted a deleted Group"
    record({"case": "F11-after-acceptance", "node": local, "tombstone_version": tombstone["version"], "new_create_status": status})
    delete(created, route)
    pool.shutdown()
    (out / "fault-results.json").write_text(json.dumps(receipts, indent=2) + "\n")
    print("PASS: actual NodeLink lost ACK, report-before-error, initial Group conflict and retained-object authorization")


if __name__ == "__main__":
    main()
