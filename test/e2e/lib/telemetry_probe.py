#!/usr/bin/env python3
"""Dependency-free assertions for the real guest/telemetry E2E fixture."""

import base64
import datetime
import json
import math
import os
import pathlib
import shlex
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


def serve_metrics(port_file, output):
    class Sink(BaseHTTPRequestHandler):
        def do_POST(self):
            size = int(self.headers.get("Content-Length", "0"))
            if self.path != "/v1/metrics" or not 0 < size <= 8 * 1024 * 1024:
                self.send_error(400)
                return
            metrics = json.loads(self.rfile.read(size))
            assert isinstance(metrics.get("resourceMetrics"), list)
            with open(output, "a", encoding="utf-8") as stream:
                stream.write(json.dumps(metrics) + "\n")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *_args):
            pass

    with HTTPServer(("127.0.0.1", 0), Sink) as server:
        pathlib.Path(port_file).write_text(str(server.server_port), encoding="utf-8")
        server.serve_forever()


def attributes(values):
    return {row["key"]: row["value"].get("stringValue") for row in values}


def native_points(path, sid):
    points = {}
    for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines(keepends=True):
        if not line.endswith("\n"):
            continue  # The HTTP sink may currently be appending the final row.
        for resource in json.loads(line).get("resourceMetrics", []):
            identity = attributes(resource.get("resource", {}).get("attributes", []))
            if identity.get("sandbox.id") != sid or identity.get("sandbox.telemetry.source") != "sandboxstats":
                continue
            assert identity.get("sandbox.stable_id"), "native source lost correlation label"
            assert not {"sandbox.run_id", "run_id", "RunID"} & identity.keys()
            for scope in resource.get("scopeMetrics", []):
                for metric in scope.get("metrics", []):
                    assert "gauge" in metric, "native cumulative read was marked as a Counter"
                    for point in metric["gauge"].get("dataPoints", []):
                        value = float(point["asDouble"])
                        assert math.isfinite(value)
                        labels = attributes(point.get("attributes", []))
                        points.setdefault((metric["name"], labels.get("usage.name", "")), []).append(
                            (int(point["timeUnixNano"]), value))
    return points


def check_native(path, sid, section, reference):
    points = native_points(path, sid)
    expected = json.loads(pathlib.Path(reference).read_text(encoding="utf-8"))

    def matches(name, value, label="", stamp=None, minimum=False):
        candidates = points.get(("sandbox." + section + "." + name, label), [])
        return any((stamp is None or at == stamp) and
                   (number >= value if minimum else math.isclose(number, value, rel_tol=1e-15, abs_tol=1e-9))
                   for at, number in candidates)

    if section == "resource":
        for field, name in {"cpuCapacity": "cpu.capacity", "cpuAllocatable": "cpu.allocatable",
                            "memoryCapacity": "memory.capacity", "memoryHeadroom": "memory.headroom"}.items():
            assert matches(name, expected[field]), f"native resource {field} was not exported"
        for name in ("memory.used", "cpu.seconds"):
            assert any(value > 0 and at % 10**9 == 0 and
                       expected["timestampUnix"] - 2 <= at // 10**9 <= time.time()
                       for at, value in points.get(("sandbox.resource." + name, ""), [])), name
    elif section == "traffic":
        for plane in ("platform", "transit"):
            for field, suffix in {"rxPackets": "rx.packets", "rxBytes": "rx.bytes",
                                  "txPackets": "tx.packets", "txBytes": "tx.bytes"}.items():
                assert matches(plane + "." + suffix, expected[plane][field], minimum=True), (plane, field)
        assert not any(key[0].startswith("sandbox.traffic.egress") for key in points)
    elif section == "usage":
        record = expected["saved"]
        assert record["snapshot"]["sandbox_id"] == sid
        assert matches("saved.available", 1)
        stamp = int(record["saved_utc_ns"])
        measured_cpu = measured_memory = False
        for counter in record["snapshot"]["counters"]:
            total = int(counter["known_total_ns"])
            if total:
                assert matches("cpu.seconds", total / 1e9, counter["name"], stamp), counter["name"]
                measured_cpu = True
        for gauge in record["snapshot"]["gauges"]:
            integral, covered = int(gauge["integral_total_byte_ns"]), int(gauge["covered_total_ns"])
            if integral and covered:
                assert matches("memory.integral", integral / 1e9, gauge["name"], stamp), gauge["name"]
                assert matches("memory.covered", covered / 1e9, gauge["name"], stamp), gauge["name"]
                measured_memory = True
        assert measured_cpu and measured_memory, "fixture lacks actual saved CPU/memory coverage"
    else:
        raise AssertionError("unknown native section")
    print("NATIVE_STATS_EXPORTED", section, sid)


def check_history(path):
    with open(path, encoding="utf-8") as stream:
        rows = json.load(stream)
    assert isinstance(rows, list) and rows, "no envd history"
    fields = {"timestamp", "timestampUnix", "cpuCount", "cpuUsedPct", "memTotal",
              "memUsed", "memCache", "diskTotal", "diskUsed"}
    for row in rows:
        assert set(row) == fields, f"unexpected E2B fields: {set(row)}"
        stamp = datetime.datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
        assert stamp.timestamp() == row["timestampUnix"]
        assert row["cpuCount"] > 0 and row["memTotal"] > 0 and row["diskTotal"] > 0
        assert all(row[key] >= 0 for key in fields - {"timestamp"})


def guest_command(endpoint):
    # The guest sends directly to the mgmt service VIP. The existing connector
    # performs its slot-derived FloatingIP SNAT, not an HTTP proxy/header shim.
    program = f"""
import json, time, urllib.request
payload = {{"resourceMetrics": [{{
  "resource": {{"attributes": [
    {{"key": "sandbox.id", "value": {{"stringValue": "forged-victim"}}}},
    {{"key": "sandbox.stable_id", "value": {{"stringValue": "forged-stable"}}}},
    {{"key": "sandbox.run_id", "value": {{"stringValue": "not-a-metric-identity"}}}}
  ]}},
  "scopeMetrics": [{{"scope": {{"name": "telemetry-e2e"}}, "metrics": [{{
    "name": "e2e.telemetry_ingress", "unit": "1", "gauge": {{"dataPoints": [{{
      "timeUnixNano": str(time.time_ns()), "asDouble": 42
    }}]}}
  }}]}}]
}}]}}
request = urllib.request.Request({endpoint!r}, data=json.dumps(payload).encode(),
    headers={{"Content-Type": "application/json", "X-Forwarded-For": "192.0.2.99"}})
with urllib.request.build_opener(urllib.request.ProxyHandler({{}})).open(request, timeout=5) as response:
    assert response.status == 200, response.status
    response.read()
print("OTLP_PEER_IDENTITY_OK")
"""
    encoded = base64.b64encode(program.encode()).decode()
    return "python3 -c " + shlex.quote(f"import base64; exec(base64.b64decode({encoded!r}))")


def check_netns(pid, namespace, ports):
    own = os.stat("/proc/self/ns/net")
    process = os.stat(f"/proc/{pid}/ns/net")
    selected = os.stat(f"/var/run/netns/{namespace}")
    assert (own.st_dev, own.st_ino) == (process.st_dev, process.st_ino), "telemetry process moved into proxy_netns"
    assert (own.st_dev, own.st_ino) != (selected.st_dev, selected.st_ino), "proxy namespace is not isolated"
    sockets = set()
    for fd in pathlib.Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue
        if target.startswith("socket:["):
            sockets.add(target[8:-1])
    found = set()
    for path in ("/proc/self/net/tcp", "/proc/self/net/tcp6"):
        contents = subprocess.check_output(["ip", "netns", "exec", namespace, "cat", path], text=True, timeout=5)
        for line in contents.splitlines()[1:]:
            fields = line.split()
            if fields[3] == "0A" and fields[9] in sockets:
                found.add(int(fields[1].split(":")[1], 16))
    assert set(map(int, ports)) <= found, f"OTLP sockets are not in proxy_netns: {found}"
    print("OTLP_LISTENER_NAMESPACE_OK")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit("usage: telemetry_probe.py check-history FILE | guest-command URL | check-netns PID NETNS PORT... | serve-metrics PORTFILE JSONL | check-native JSONL SID SECTION REFERENCE")
    if sys.argv[1] == "check-history":
        check_history(sys.argv[2])
    elif sys.argv[1] == "guest-command":
        print(guest_command(sys.argv[2]))
    elif sys.argv[1] == "check-netns":
        check_netns(sys.argv[2], sys.argv[3], sys.argv[4:])
    elif sys.argv[1] == "serve-metrics":
        serve_metrics(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "check-native":
        check_native(*sys.argv[2:])
    else:
        raise SystemExit("unknown telemetry probe command")
