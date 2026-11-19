"""Exercise workspace handoff through the node API on a real Guest."""
import base64
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlencode

port, domain, bin_dir, proxy, out_dir = sys.argv[1:]
bin_dir = Path(bin_dir)
key = os.environ["E2B_API_KEY"]
root_ref = ("manifest://" + os.environ["WORKSPACE_IMAGE_ROOT"]).encode()
image = "bare-img-" + base64.urlsafe_b64encode(root_ref).decode().rstrip("=")
mounts = [{"path": "/data", "subpath": "project"}, {"path": "/output", "subpath": "results"}]
live = set()
samples = []
metrics_file = Path(out_dir, "workspace-timings.jsonl")


def request(method, path, body=None, *, status=200, headers=None):
    origin = http.client.HTTPConnection("127.0.0.1", int(port), timeout=180)
    try:
        origin.request(method, path, json.dumps(body) if body is not None else None,
                       {"Host": "api." + domain, "X-API-KEY": key, "Content-Type": "application/json", **(headers or {})})
        response = origin.getresponse()
        wire = response.read()
        if response.status != status:
            err = json.loads(wire or b"{}").get("message", "unexpected response")
            raise RuntimeError(f"{method} {path}: expected {status}, received {response.status}: {err}")
        return json.loads(wire) if wire else None
    finally:
        origin.close()


def record(kind, start, **fields):
    sample = dict(phase=kind, ms=round((time.monotonic() - start) * 1000, 2), **fields)
    with metrics_file.open("a") as log:
        print(json.dumps(sample), file=log)
    print(sample, flush=True)


def declared(n, first=None):
    return [] if n == 0 else [
        dict(name="project", mounts=mounts, **(first or {"slot": {}})),
        *(dict(name=f"w{i}", mounts=[{"path": f"/workspace-{i}"}], slot={}) for i in range(1, n))]


def create(template, entries, phase, count):
    body = {"templateID": template, "timeout": 600}
    headers = {"X-Kuasar-Sandbox-Resource": '{"capacity":{"cpu":1,"memory":"512MiB"}}'}
    # Keep the bare fixture's application mount namespace alive for native exec.
    if template == image:
        headers["X-Kuasar-Sandbox-Launch"] = json.dumps({"exec": "/bin/sleep", "args": ["3600"]})
    if entries is not None:
        headers["X-Kuasar-Sandbox-Workspace"] = json.dumps(entries)
    start = time.monotonic()
    sid = request("POST", "/sandboxes", body, status=201, headers=headers)["sandboxID"]
    live.add(sid)
    while True:
        detail = request("GET", f"/sandboxes/{sid}")
        if detail["state"] == "running":
            break
        if detail["state"] == "dead" or time.monotonic() - start > 120:
            raise RuntimeError(f"{phase}: launch {sid}: {detail.get('terminalReason', detail['state'])}")
        time.sleep(.01)
    record("running", start, source=phase, count=count)
    topo = json.loads((detail.get("metadata") or {}).get("kuasar-sandbox.workspace", "[]"))
    assert len(topo) == count and all(set(x) == {"name", "mounts"} for x in topo), topo
    return sid


def control(sid, name, op, body=None, **kw):
    return request("POST", f"/sandboxes/{sid}/workspaces/{op}?" + urlencode({"name": name}), body, **kw)


def states(sid):
    listing = request("GET", f"/sandboxes/{sid}/workspaces")
    return {x["name"]: x for x in listing["workspaces"]}


def guest(sid, cmd, label=None):
    token = request("POST", f"/sandboxes/{sid}/exec-sessions", {}, status=201)["execAccessToken"]
    start = time.monotonic()
    subprocess.run([bin_dir / "sandbox-ctl", "exec", "--proxy", "http://127.0.0.1:" + proxy,
                    "--proxy-header", "E2b-Sandbox-Id: " + sid, "--proxy-header", "E2b-Sandbox-Service: exec",
                    "--proxy-header", "X-Access-Token: " + token, "--", "/bin/sh", "-ec", cmd], check=True, timeout=60)
    if label:
        record("first_guest_read", start, source=label)


def pause(sid, memory):
    request("POST", f"/sandboxes/{sid}/pause", {"memory": memory}, status=204)
    assert request("GET", f"/sandboxes/{sid}")["state"] == "paused"


def template_of(sid):
    return request("POST", f"/sandboxes/{sid}/export", {"keepSource": True, "toTemplate": True})["result"]


def delete(sid):
    request("DELETE", f"/sandboxes/{sid}", status=204)
    live.discard(sid)


def attach(sid, token, label):
    disks = states(sid)
    assert disks["project"]["state"] == "unattached"
    start = time.monotonic()
    subprocess.run([bin_dir / "node-ctl", "workspace", "attach", sid, "--name", "project",
                    "--token-file", "-"], input=token.encode(), check=True, timeout=90)
    record("attach", start, source=label)
    assert states(sid)["project"]["state"] == "attached"
    control(sid, "project", "attach", {"disk": {"migrationToken": token}}, status=204)
    control(sid, "project", "attach", {"disk": {}}, status=409)


def sbx_append():
    source = create(image, declared(1, {"disk": {}}), "sbx-source", 1)
    guest(source, "echo parent > /data/value; sync")
    pause(source, False)
    template = template_of(source)
    delete(source)

    extra = [{"name": "extra", "mounts": [{"path": "/extra"}], "disk": {}},
             {"name": "later", "mounts": [{"path": "/later"}], "slot": {}}]
    child = create(template, extra, "sbx-append", 3)
    assert states(child)["later"]["state"] == "unattached"
    control(child, "later", "attach", {"disk": {}}, status=204)
    guest(child, 'test "$(cat /data/value)" = parent; echo child > /data/value; '
                 'echo extra > /extra/value; echo later > /later/value; sync')
    pause(child, True)
    assert states(child)["extra"]["state"] == "attached"
    request("POST", f"/sandboxes/{child}/connect", {})
    guest(child, 'test "$(cat /data/value)" = child; test "$(cat /extra/value)" = extra; '
                 'test "$(cat /later/value)" = later')
    delete(child)
    sibling = create(template, None, "sbx-parent-reused", 1)
    guest(sibling, 'test "$(cat /data/value)" = parent; test ! -e /extra/value; test ! -e /later/value')
    delete(sibling)


def build_steps(source_template=image, pending_default=False):
    cpu = int(os.environ["BUILDER_CPU"])
    target = {"kind": "sandbox", "memory": False}
    headers = {
        "X-Kuasar-Sandbox-Workspace": json.dumps(
            [{"name": "project", "disk": {}}] if pending_default else declared(1, {"disk": {}})),
        "X-Kuasar-Sandbox-Resource": '{"capacity":{"cpu":1,"memory":"512MiB"}}',
        "X-Kuasar-Sandbox-Builder": json.dumps({
            "target": target, "resources": {"cpu": cpu, "memory": "6GiB", "storage": "4GiB"}}),
    }
    build = request("POST", "/v3/templates", {"name": "workspace-steps-fixed-slot" if pending_default else "workspace-steps", "profile": "bare",
                    "cpuCount": cpu, "memoryMB": 6144}, headers=headers, status=202)
    tid, bid = build["templateID"], build["buildID"]
    steps = [{"type": "RUN", "args": ["echo first > /data/build-only; echo root > /workspace-step"]},
             {"type": "RUN", "args": ['test "$(cat /data/build-only)" = first; echo second >> /workspace-step']}]
    request("POST", f"/v2/templates/{tid}/builds/{bid}", {"fromTemplate": source_template, "steps": steps}, status=202)
    deadline = time.monotonic() + 480
    while time.monotonic() < deadline:
        result = request("GET", f"/templates/{tid}/builds/{bid}/status")
        if result["status"] == "ready":
            break
        if result["status"] == "error":
            raise AssertionError(f"workspace build {bid} failed: {result.get('logs', [])}")
        time.sleep(1)
    else:
        raise TimeoutError(f"workspace build {bid} did not complete")
    assert result["kind"] == "sbx"
    child = create(result["templateID"], None, "workspace-build", 1)
    guest(child, 'test "$(head -1 /workspace-step)" = root; test "$(tail -1 /workspace-step)" = second; '
                 'test ! -e /data/build-only; echo final > /data/final')
    delete(child)


def explicit_size_precedence():
    size = 16 << 20
    disk = {"kind": "disk", "version": 1, "size": size}
    sid = create(image, declared(1, {"disk": disk}), "explicit-size", 1)
    assert states(sid)["project"]["sizeBytes"] == size
    guest(sid, "echo sized > /data/value; sync")
    pause(sid, False)
    saved = template_of(sid)
    delete(sid)
    restored = create(saved, None, "explicit-size-restored", 1)
    assert states(restored)["project"]["sizeBytes"] == size
    guest(restored, 'test "$(cat /data/value)" = sized')
    delete(restored)


def fixed_slot_defaults():
    size = 16 << 20
    fixed = {"slot": {"size": "16MiB"}}
    source = create(image, declared(1, fixed), "fixed-slot", 1)
    assert states(source)["project"]["state"] == "unattached"
    assert states(source)["project"]["sizeBytes"] == size
    pause(source, False)
    empty_template = template_of(source)
    delete(source)

    # Pending Create and Build choose capacity from the exact captured E.
    selected = create(empty_template, [{"name": "project", "disk": {}}], "fixed-slot-create", 1)
    assert states(selected)["project"]["sizeBytes"] == size
    guest(selected, "echo selected > /data/value; sync")
    delete(selected)
    build_steps(empty_template, pending_default=True)

    live_slot = create(empty_template, None, "fixed-slot-attach", 1)
    control(live_slot, "project", "attach", {"disk": {}}, status=204)
    assert states(live_slot)["project"]["sizeBytes"] == size
    guest(live_slot, "echo retained > /data/value; sync")
    control(live_slot, "project", "attach", {"disk": {}}, status=204)
    guest(live_slot, 'test "$(cat /data/value)" = retained')
    pause(live_slot, False)
    captured = template_of(live_slot)
    delete(live_slot)
    restored = create(captured, None, "fixed-slot-restored", 1)
    guest(restored, 'test "$(cat /data/value)" = retained')
    delete(restored)

    dynamic = create(image, declared(1), "dynamic-without-default", 1)
    control(dynamic, "project", "attach", {"disk": {}}, status=400)
    assert states(dynamic)["project"]["state"] == "unattached"
    control(dynamic, "project", "attach", {"disk": {"kind": "disk", "version": 1, "size": size}}, status=204)
    assert states(dynamic)["project"]["sizeBytes"] == size
    guest(dynamic, "echo explicit > /data/value; sync")
    delete(dynamic)


def handoff():
    metrics_file.parent.mkdir(parents=True, exist_ok=True)
    metrics_file.write_text("")
    source = create(image, declared(1, {"disk": {}}), "img-new-disk", 1)
    guest(source, "echo source > /data/value; echo result > /output/value; sync")
    control(source, "project", "export", {}, status=409)
    pause(source, True)
    assert states(source)["project"]["state"] == "attached"
    control(source, "project", "attach", {"disk": {}}, status=409)
    token = subprocess.check_output([bin_dir / "node-ctl", "workspace", "export", source, "--name", "project"], text=True).strip()
    assert token.startswith("kwt1.")
    captured = template_of(source)
    child = create(captured, None, "snp-attached", 1)
    guest(child, "test \"$(cat /data/value)\" = source; test \"$(cat /output/value)\" = result")
    delete(child)
    request("POST", f"/sandboxes/{source}/connect", {})
    guest(source, "echo resumed-source > /data/value; sync")
    delete(source)
    imported = create(image, declared(1, {"disk": {"migrationToken": token}}), "img-import", 1)
    guest(imported, "test \"$(cat /data/value)\" = source; test \"$(cat /output/value)\" = result; echo private-b > /data/value; sync")
    for n in [0, 1, 8]:
        seed = create(image, declared(n) if n else None, "img", n)
        if n:
            assert all(x["state"] == "unattached" for x in states(seed).values())
        pause(seed, True)
        if n:
            control(seed, "project", "export", {}, status=409)
        template = template_of(seed)
        delete(seed)
        restored = create(template, None, "snp", n)
        assert len(states(restored)) == n
        if n:
            attach(restored, token, "snp-" + str(n))
            guest(restored, "test \"$(cat /data/value)\" = source; test \"$(cat /output/value)\" = result; echo private-c > /data/value", f"snp-{n}")
            guest(imported, "test \"$(cat /data/value)\" = private-b")
        delete(restored)
        if n == 1:
            captured_choice = create(template, [{"name": "project", "disk": {"migrationToken": token}}], "snp-create-attach", 1)
            guest(captured_choice, "test \"$(cat /data/value)\" = source")
            delete(captured_choice)
    delete(imported)


try:
    if os.environ.get("WORKSPACE_DEFAULT_TEST") == "fixed-slot":
        fixed_slot_defaults()
    else:
        handoff()
        sbx_append()
        build_steps()
        explicit_size_precedence()
finally:
    for remaining in list(live):
        try:
            delete(remaining)
        except Exception as e:
            print("cleanup:", e, file=sys.stderr)
