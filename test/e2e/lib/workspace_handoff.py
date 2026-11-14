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
    return listing.get("runGeneration"), {x["name"]: x for x in listing["workspaces"]}


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
    gen, disks = states(sid)
    assert disks["project"]["state"] == "unattached"
    control(sid, "project", "attach", {"disk": {}}, status=412, headers={"X-Kuasar-Run-Generation": gen + "-stale"})
    start = time.monotonic()
    subprocess.run([bin_dir / "node-ctl", "workspace", "attach", sid, "--name", "project",
                    "--if-generation", gen, "--token-file", "-"], input=token.encode(), check=True, timeout=90)
    record("attach", start, source=label)
    assert states(sid)[1]["project"]["state"] == "attached"
    control(sid, "project", "attach", {"disk": {"migrationToken": token}}, status=204)
    control(sid, "project", "attach", {"disk": {}}, status=409)


def handoff():
    metrics_file.parent.mkdir(parents=True, exist_ok=True)
    metrics_file.write_text("")
    source = create(image, declared(1, {"disk": {}}), "img-new-disk", 1)
    guest(source, "echo source > /data/value; echo result > /output/value; sync")
    control(source, "project", "export", {}, status=409)
    pause(source, True)
    assert states(source)[1]["project"]["state"] == "attached"
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
            assert all(x["state"] == "unattached" for x in states(seed)[1].values())
        pause(seed, True)
        if n:
            control(seed, "project", "export", {}, status=409)
        template = template_of(seed)
        delete(seed)
        restored = create(template, None, "snp", n)
        assert len(states(restored)[1]) == n
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
    handoff()
finally:
    for remaining in list(live):
        try:
            delete(remaining)
        except Exception as e:
            print("cleanup:", e, file=sys.stderr)
