#!/usr/bin/env python3
"""Dependency-free assertions for the real guest/telemetry E2E fixture."""

import base64
import datetime
import json
import shlex
import sys


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


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: telemetry_probe.py check-history FILE | guest-command URL")
    if sys.argv[1] == "check-history":
        check_history(sys.argv[2])
    elif sys.argv[1] == "guest-command":
        print(guest_command(sys.argv[2]))
    else:
        raise SystemExit("unknown telemetry probe command")
