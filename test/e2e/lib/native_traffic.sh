#!/usr/bin/env bash
# Compare conductor's flat planes with actual connector observations. This is
# an E2E assertion helper; production collection calls the bounded native API.

native_traffic_probe() { # $1=require observed platform packets (0|1)
    local required="$1" port code idle
    idle="$(traffic_idle_since "$SID")"
    [ -n "$idle" ] || fail "native traffic probe requires settled Proxy ingress"
    port="$(python3 - "$WORK/lib/node-ctl.db" "$SID" "$WORK/native-traffic-binding.json" <<'PY_BINDING'
import json, pathlib, sqlite3, sys
with sqlite3.connect(pathlib.Path(sys.argv[1]).resolve().as_uri() + "?mode=ro", uri=True) as db:
    row = db.execute("SELECT vswitch_port, floatingip, inner_ip FROM sandboxes WHERE id = ? AND state = 'running'", (sys.argv[2],)).fetchone()
assert row is not None and all(row), row
assert 1 <= int(row[0]) <= 4096, row
pathlib.Path(sys.argv[3]).write_text(json.dumps(dict(port=int(row[0]), floatingip=row[1], inner_ip=row[2])))
print(row[0])
PY_BINDING
    )" || fail "cannot locate current exact SandboxID traffic binding"
    "$BIN/connector-ctl" vswitch stats "$SWITCH" --port="$port" > "$WORK/native-traffic-before.json"
    code="$(req GET "/sandboxes/$SID/stats/traffic" "$AK")"
    [ "$code" = 200 ] || fail "native traffic API returned $code"
    cp "$WORK/resp.body" "$WORK/native-traffic-api.json"
    "$BIN/connector-ctl" vswitch stats "$SWITCH" --port="$port" > "$WORK/native-traffic-after.json"
    python3 - "$WORK" "$required" "$idle" <<'PY_COUNTERS'
import ipaddress, json, pathlib, sys
work = pathlib.Path(sys.argv[1])
def read(name):
    return json.loads((work / name).read_text())
binding = read("native-traffic-binding.json")
before = read("native-traffic-before.json")["ports"][0]
after = read("native-traffic-after.json")["ports"][0]
stats = read("native-traffic-api.json")
assert stats["state"] == "running" and stats["inflight"] == {"parking": 0, "connected": 0}, stats
assert stats["idleSince"] == sys.argv[3], stats
assert stats["egress"] == {}, stats
for row in (before, after):
    assert row["port"] == binding["port"] and row["floating_ip"] == binding["floatingip"], row
    assert row["inner_ip"] == str(ipaddress.ip_interface(binding["inner_ip"]).ip), row
for plane, prefix in (("platform", "mgmt"), ("transit", "transit")):
    assert set(stats[plane]) == {"rxPackets", "rxBytes", "txPackets", "txBytes"}, stats
    for field, suffix in (("rxPackets", "rx_packets"), ("rxBytes", "rx_bytes"), ("txPackets", "tx_packets"), ("txBytes", "tx_bytes")):
        key = prefix + "_" + suffix
        value = stats[plane][field]
        assert isinstance(value, int) and before[key] <= value <= after[key], (plane, field, before[key], value, after[key])
if sys.argv[2] == "1":
    assert stats["platform"]["rxPackets"] > 0 and stats["platform"]["txPackets"] > 0, stats
print("PASS: conductor flat platform/transit match current connector binding and bracketed counters; Proxy idle unchanged")
PY_COUNTERS
}
