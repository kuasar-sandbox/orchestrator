#!/usr/bin/env python3
"""Real Node/SQLite/VM checks through the test-only NodeLink ordering relay."""
import concurrent.futures
import atexit
import http.client
import json
import os
import pathlib
import signal
import sqlite3
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

from group_readiness import wait_for_group


def _report_failure(error):
    # stdout is public: report only code-owned phases, exception categories and
    # this helper's line number. The full traceback stays in the private log.
    phase, line = "setup", 0
    frame = error.__traceback__
    while frame is not None:
        if (frame.tb_frame.f_code.co_filename == __file__
                and frame.tb_frame.f_code.co_name == "main"):
            phase = frame.tb_frame.f_locals.get("phase", "setup")
            line = frame.tb_lineno
            break
        frame = frame.tb_next
    kind = type(error).__name__
    if kind not in {
            "AssertionError", "TimeoutError", "TimeoutExpired", "CalledProcessError",
            "HTTPError", "URLError", "RemoteDisconnected", "ConnectionError",
            "ConnectionResetError", "BrokenPipeError", "OSError", "FileNotFoundError",
            "PermissionError", "ProcessLookupError", "JSONDecodeError",
            "OperationalError", "ValueError", "KeyError"}:
        kind = "Exception"
    print(f"FAIL: cluster fault phase={phase} exception={kind} "
          f"source=cluster_faults.py line={line}", flush=True)


def main():
    phase = "setup"
    work = pathlib.Path(os.environ["FAULT_WORK"])
    out = pathlib.Path(os.environ["FAULT_OUT"])
    group = os.environ["FAULT_GROUP"]
    host = "api." + os.environ["FAULT_DOMAIN"]
    api_key = os.environ["FAULT_API_KEY"]
    router = os.environ["FAULT_ROUTER"]
    node = os.environ["FAULT_NODE"]
    mode_path = work / "fault-mode"
    receipts = []
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)

    def mode(value):
        temporary = mode_path.with_suffix(".next")
        temporary.write_text(value + "\n")
        temporary.replace(mode_path)

    def request(base, method, path, route="", body=None, extra_headers=None):
        headers = {"Host": host, "X-API-KEY": api_key,
                   "X-Kuasar-Sandbox-Group": group}
        if route:
            headers["X-Kuasar-Route-Key"] = route
        if extra_headers:
            headers.update(extra_headers)
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
    phase = "group-read"
    code, original = request(node, "GET", group_path)
    assert code == 200, "Group not ready"

    for label, fault in (("F03", "drop-create-ack"), ("F04", "error-after-ready")):
        route = "fault/" + label
        start = len(events())
        mode(fault)
        phase = label + ".create"
        status, created = request(router, "POST", "/sandboxes", route, {})
        assert status == 201, label + " did not converge from real route facts: " + str(status)
        phase = label + ".live"
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
        phase = label + ".replay"
        status, again = request(router, "POST", "/sandboxes", route, {})
        assert status == 201 and again["sandboxID"] == created["sandboxID"], "existing request changed logical identity"
        for key in ("envdAccessToken", "trafficAccessToken", "forwardAccessToken"):
            assert again[key] == created[key], "existing request changed public credentials"
        phase = label + ".delete"
        delete(created, route)

    # The real Node has accepted a VM, while Registry has received neither its
    # ACK nor its route facts. Restart the actual Registry process on the same
    # FDB namespace, then recover through the real NodeLink full snapshot.
    phase = "F08.wait-delivery"
    route = "fault/F08"
    start = len(events())
    mode("drop-create-results")
    pending = pool.submit(request, router, "POST", "/sandboxes", route, {})
    command = until(lambda: next((e for e in events()[start:] if e["action"] == "create-received"), None), "no interrupted Create delivery")
    def accepted():
        with sqlite3.connect(work / "cl/node-ctl.db") as db:
            found = db.execute("select stable_id from sandboxes where id=? and state='running'", (command["sid"],)).fetchone()
        return found[0] if found else None
    phase = "F08.wait-node"
    stable = until(accepted, "Node did not accept the interrupted VM")
    # A direct Connect to the already running Node object exposes its original
    # public tokens; ordinary management GET intentionally omits those tokens.
    status, node_object = request(node, "POST", "/sandboxes/" + command["sid"] + "/connect", body={})
    assert status == 200, "accepted Node object is not independently observable"
    phase = "F08.before-restart"
    before_restart_fence = wait_for_group(
        node, group, registry_urls=[os.environ["FAULT_REGISTRY"]],
        api_key=api_key, node_host=host, node_id=os.environ["FAULT_NODE_ID"]
    )["registration"]["fence"]
    pid = int(os.environ["FAULT_REGISTRY_PID"])
    config = work / "registry-1.yaml"
    assert str(config).encode() in pathlib.Path("/proc/" + str(pid) + "/cmdline").read_bytes(), "unexpected Registry PID"
    phase = "F08.stop-registry"
    os.kill(pid, signal.SIGKILL)
    def exited():
        try:
            return pathlib.Path("/proc/" + str(pid) + "/stat").read_text().rsplit(") ", 1)[1].split()[0] == "Z"
        except FileNotFoundError:
            return True
    until(exited, "SIGKILL Registry did not exit")
    # Keep the owner unavailable until the original Router invocation ends;
    # otherwise its existing retry may legitimately recover through the new owner.
    phase = "F08.wait-interrupted-request"
    status, _ = pending.result(timeout=50)
    assert status != 201, "interrupted Create unexpectedly completed without a delivered fact"
    phase = "F08.restart-registry"
    mode("normal")
    with (work / "registry-restarted.log").open("w") as log:
        restarted = subprocess.Popen([os.environ["FAULT_CLUSTER_CTL"], "registry", "--config", str(config)], stdout=log, stderr=subprocess.STDOUT)
    def stop_registry():
        restarted.terminate()
        try:
            restarted.wait(timeout=10)
        except subprocess.TimeoutExpired:
            restarted.kill()
            restarted.wait(timeout=10)
    atexit.register(stop_registry)
    phase = "F08.wait-registration"
    until(lambda: any(e["type"] == "node_register" for e in events()[start:]), "actual Node did not reconnect")
    phase = "F08.wait-full-snapshot"
    until(lambda: request(router, "GET", "/sandboxes/" + stable, route)[0] == 200, "reconnected full snapshot did not converge")
    phase = "F08.wait-group"
    wait_for_group(node, group, registry_urls=[os.environ["FAULT_REGISTRY"]],
                   api_key=api_key, node_host=host, node_id=os.environ["FAULT_NODE_ID"],
                   after_fence=before_restart_fence)
    phase = "F08.recover"
    status, recovered = request(router, "POST", "/sandboxes", route, {})
    assert status == 201 and recovered["sandboxID"] == stable, "owner restart rebound the logical object"
    local = assert_live(recovered, route)
    assert local["id"] == command["sid"], "owner restart replaced the accepted execution"
    for key in ("envdAccessToken", "trafficAccessToken", "forwardAccessToken"):
        assert recovered[key] == node_object[key], "owner restart changed public credentials"
    commands = [e for e in events()[start:] if e["action"] == "create-received"]
    assert len(commands) == 1, "owner restart blindly redelivered Create"
    record({"case": "F08-after-Node-acceptance", "node": local, "create_deliveries": 1, "registry_signal": "SIGKILL", "full_snapshot_recovered": True})
    phase = "F08.delete"
    delete(recovered, route)

    # The intent is committed and bytes have reached the relay, but the actual
    # Node has not received this command. Kill the owner, discard that dead
    # stream, and recover the same logical slot with a fresh local generation.
    phase = "F02.wait-delivery"
    route = "fault/F02"
    start = len(events())
    mode("pause-create")
    reserve_path = "/route-link/v2/reserve?" + urllib.parse.urlencode({"group": group, "route_key": route, "operation": "create"})
    pending = pool.submit(request, os.environ["FAULT_REGISTRY"], "POST", reserve_path, route, {})
    command = until(lambda: next((e for e in events()[start:] if e["action"] == "create-received"), None), "no committed pre-acceptance command")
    stable = command["sid"].rsplit("-g", 1)[0]
    assert row(stable) is None, "held command reached the actual Node"
    projection_path = "/route-link/v2/data-route?" + urllib.parse.urlencode({"group": group, "route_key": route, "sid": stable})
    code, before_restart = request(os.environ["FAULT_REGISTRY"], "GET", projection_path)
    assert code == 200 and before_restart["node_sandbox_id"] == command["sid"], "intent was not committed before owner failure"
    phase = "F02.before-restart"
    before_restart_fence = wait_for_group(
        node, group, registry_urls=[os.environ["FAULT_REGISTRY"]],
        api_key=api_key, node_host=host, node_id=os.environ["FAULT_NODE_ID"]
    )["registration"]["fence"]
    phase = "F02.stop-registry"
    restarted.kill()
    restarted.wait(timeout=10)
    mode("disconnect")
    phase = "F02.wait-disconnect"
    until(lambda: any(e["action"] == "session-closed" for e in events()[start:]), "old paused NodeLink did not close")
    assert not any(e["action"] == "forward-down" and e.get("sid") == command["sid"] for e in events()[start:]), "old command escaped the closed stream"
    phase = "F02.wait-interrupted-request"
    try:
        failed_status, _ = pending.result(timeout=20)
        assert failed_status >= 500, "dead owner returned success"
    except (http.client.RemoteDisconnected, urllib.error.URLError, ConnectionError):
        failed_status = "connection-lost"
    phase = "F02.restart-registry"
    mode("normal")
    with (work / "registry-restarted-before-acceptance.log").open("w") as log:
        restarted = subprocess.Popen([os.environ["FAULT_CLUSTER_CTL"], "registry", "--config", str(config)], stdout=log, stderr=subprocess.STDOUT)
    def intent_reopened():
        try:
            code, current = request(os.environ["FAULT_REGISTRY"], "GET", projection_path)
            return code == 200 and current["node_sandbox_id"] == command["sid"]
        except (urllib.error.URLError, ConnectionError):
            return False
    phase = "F02.wait-intent"
    until(intent_reopened, "fresh Registry lost the committed logical slot")
    phase = "F02.wait-group"
    until(lambda: any(e["type"] == "group_end" for e in events()[start:]), "Node did not republish Group after pre-acceptance restart")
    wait_for_group(node, group, registry_urls=[os.environ["FAULT_REGISTRY"]],
                   api_key=api_key, node_host=host, node_id=os.environ["FAULT_NODE_ID"],
                   after_fence=before_restart_fence)
    phase = "F02.recover"
    status, recovered = request(router, "POST", "/sandboxes", route, {})
    assert status == 201 and recovered["sandboxID"] == stable, "pre-acceptance restart rebound the logical identity"
    local = assert_live(recovered, route)
    assert local["id"] != command["sid"], "ambiguous local execution identity was reused"
    create_ids = {e["command"] for e in events()[start:] if e["action"] == "create-received"}
    sent = [e for e in events()[start:] if e["action"] == "forward-down" and e.get("command") in create_ids]
    assert len(sent) == 1 and sent[0]["sid"] == local["id"], "pre-acceptance recovery redelivered the old execution"
    record({"case": "F02-after-socket-before-Node-acceptance", "registry_signal": "SIGKILL", "interrupted_status": failed_status, "committed_node_sid": command["sid"], "recovered_node": local, "forwarded_create_deliveries": 1})
    phase = "F02.delete"
    delete(recovered, route)

    phase = "F06.wait-delivery"
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
    phase = "F06.wait-rejection"
    status, _ = pending.result(timeout=50)
    assert status == 409, "stale Group command was not rejected: " + str(status)
    with sqlite3.connect(work / "cl/node-ctl.db") as db:
        assert db.execute("select count(*) from sandboxes where id=?", (command["sid"],)).fetchone()[0] == 0, "rejected delivery created a Node row"
    assert not (work / "cr/sandboxes" / command["sid"]).exists(), "rejected delivery created a RunDir"
    phase = "F06.wait-group"
    wait_for_group(node, group, registry_urls=[os.environ["FAULT_REGISTRY"]],
                   api_key=api_key, node_host=host, node_id=os.environ["FAULT_NODE_ID"])
    phase = "F06.recover"
    status, created = request(router, "POST", "/sandboxes", route, {})
    assert status == 201, "fresh request after exact rejection failed: " + str(status)
    local = assert_live(created, route)
    assert local["id"] != command["sid"], "retired execution identity was reused"
    record({"case": "F06", "rejected_node_sid": command["sid"], "public_status": 409, "replacement": local})

    # The same actual initial Node rejection, without its structured proof,
    # must not be generalized into permission to retire the pending identity.
    phase = "F07.wait-delivery"
    ambiguous_route = "fault/F07"
    start = len(events())
    mode("pause-create")
    # Observe one Registry decision directly. The public Router may retry a
    # temporary response after the old logical slot is retired; that is a
    # separate invocation and must not hide this decision's unknown outcome.
    reserve_path = "/route-link/v2/reserve?" + urllib.parse.urlencode({"group": group, "route_key": ambiguous_route, "operation": "create"})
    pending = pool.submit(request, os.environ["FAULT_REGISTRY"], "POST", reserve_path, ambiguous_route, {})
    ambiguous = until(lambda: next((e for e in events()[start:] if e["action"] == "create-received"), None), "no ambiguous delivery")
    status, detail = request(node, "GET", group_path)
    assert status == 200
    revised = dict(detail["context"])
    revised["metadata"] = {"fault-window": "unstructured-rejection"}
    status, _ = request(node, "PUT", group_path, body={"expected_version": detail["version"], "context": revised})
    assert status == 200
    mode("generic-group-rejection")
    phase = "F07.wait-unstructured-rejection"
    until(lambda: any(e["action"] == "strip-proof" for e in events()[start:]), "generic-only rejection was not injected")
    ambiguous_sid = ambiguous["sid"].rsplit("-g", 1)[0]
    projection_path = "/route-link/v2/data-route?" + urllib.parse.urlencode({"group": group, "route_key": ambiguous_route, "sid": ambiguous_sid})
    code, retained = request(os.environ["FAULT_REGISTRY"], "GET", projection_path)
    assert code == 200 and retained["node_sandbox_id"] == ambiguous["sid"], "generic conflict retired its pending identity"
    code, _ = request(node, "GET", "/sandboxes/" + ambiguous["sid"])
    assert code == 404, "the rejected delivery unexpectedly materialized"
    assert not pending.done(), "negative Node query released the pending caller"
    phase = "F07.wait-temporary-failure"
    status, _ = pending.result(timeout=50)
    assert status == 503, "ambiguous Create did not end as a temporary failure: " + str(status)
    record({"case": "F07-generic-conflict", "registry_status": status, "negative_node_query": 404, "retained_node_sid_while_waiting": retained["node_sandbox_id"], "retained_state_while_waiting": retained["state"]})
    mode("normal")

    # Shared work never shares permission. While a real Create is held before
    # Node admission, a second valid waiter may join, but neither a bad MAC nor
    # incompatible Placement may receive the first caller's result.
    phase = "F12.wait-delivery"
    waiter_route = "fault/F12"
    start = len(events())
    mode("pause-create")
    first = pool.submit(request, router, "POST", "/sandboxes", waiter_route, {})
    until(lambda: any(e["action"] == "create-received" for e in events()[start:]), "no held waiter Create")
    phase = "F12.waiter-auth"
    second = pool.submit(request, router, "POST", "/sandboxes", waiter_route, {})
    invalid_key = api_key[:-1] + ("1" if api_key[-1] == "0" else "0")
    code, _ = request(router, "POST", "/sandboxes", waiter_route, {}, {"X-API-KEY": invalid_key})
    assert code == 403, "shared wait authenticated an invalid MAC"
    code, _ = request(router, "POST", "/sandboxes", waiter_route, {}, {"X-Kuasar-Sandbox-Placement": json.dumps({"node_selectors": [{"fixture-impossible": "true"}]})})
    assert code == 409, "shared wait ignored caller Placement"
    assert not first.done() and not second.done(), "held valid waiter unexpectedly finished"
    mode("normal")
    phase = "F12.release-waiters"
    first_status, first_object = first.result(timeout=50)
    second_status, second_object = second.result(timeout=50)
    assert first_status == second_status == 201 and first_object["sandboxID"] == second_object["sandboxID"], "valid waiters did not share the exact object"
    phase = "F12.live"
    waiter_local = assert_live(first_object, waiter_route)
    commands = [e for e in events()[start:] if e["action"] == "create-received"]
    assert len(commands) == 1, "concurrent waiters dispatched another execution"
    record({"case": "F12-concurrent-callers", "valid_statuses": [first_status, second_status], "bad_mac_status": 403, "incompatible_placement_status": 409, "node": waiter_local, "create_deliveries": 1})
    phase = "F12.delete"
    delete(first_object, waiter_route)

    # Delete is later than copied/accepted input; retained object authorization
    # must survive, and the importer must not revive this explicit tombstone.
    phase = "F11.delete-group"
    status, current = request(node, "GET", group_path)
    assert status == 200
    status, tombstone = request(node, "DELETE", group_path + "&expected_version=" + str(current["version"]))
    assert status == 200
    phase = "F11.retained-object"
    assert_live(created, route)
    time.sleep(1.5)
    status, missing = request(node, "GET", group_path)
    assert status == 404 and missing["version"] == tombstone["version"], "background importer revived a tombstone"
    status, _ = request(router, "POST", "/sandboxes", "fault/after-delete", {})
    assert status != 201, "new creation admitted a deleted Group"
    record({"case": "F11-after-acceptance", "node": local, "tombstone_version": tombstone["version"], "new_create_status": status})
    phase = "F11.delete-object"
    delete(created, route)
    pool.shutdown()
    (out / "fault-results.json").write_text(json.dumps(receipts, indent=2) + "\n")
    print("PASS: actual NodeLink lost ACK, report-before-error, initial Group conflict and retained-object authorization")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        try:
            _report_failure(error)
        except Exception:
            pass
        raise
