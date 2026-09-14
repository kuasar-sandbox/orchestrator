#!/usr/bin/env bash
# Public native usage assertions. Sourced by the real execute suite, which owns
# req, WORK, AK and the node policy. Reads never call sandbox ctl or its files.

assert_native_usage() { # $1=sid, $2=live|paused
    local sid="$1" mode="$2" code="" ready="" attempt
    for attempt in $(seq 1 100); do
        code=$(req GET "/sandboxes/$sid/stats/usage" "$AK" || true)
        if [ "$code" = "200" ] && python3 - "$WORK/resp.body" "$sid" "$mode" "$NATIVE_USAGE_SAMPLE" "$NATIVE_USAGE_FLUSH" <<'PY'
import json, sys
path, sid, mode, sample, flush = sys.argv[1:]
view = json.load(open(path))
if mode == "live":
    live = view.get("live")
    if not view["enabled"] or live is None or not live["counters"]:
        raise SystemExit(1)
    assert live["sandbox_id"] == sid
    assert live["sample_interval_ns"] == str(int(sample.rstrip("s")) * 10**9)
    assert live["flush_interval_ns"] == str(int(flush.rstrip("s")) * 10**9)
    assert live["run_epoch"] and not live["closed"]
else:
    assert "live" not in view
    saved = view.get("saved")
    if saved is None:
        raise SystemExit(1)
    assert saved["snapshot"]["sandbox_id"] == sid
assert isinstance(view["saved_end"], str)
assert isinstance(view["unknown_tail"], bool)
PY
        then
            ready=1
            break
        fi
        sleep 0.1
    done
    [ -n "$ready" ] || { cat "$WORK/resp.body"; fail "native usage $mode missing or wrong policy for $sid (HTTP $code)"; }
    cp "$WORK/resp.body" "$WORK/usage-$sid-$mode.json"
    code=$(req GET "/sandboxes/$sid/stats/usage?view=saved" "$AK")
    [ "$code" = "200" ] || fail "native saved usage=$code"
    python3 - "$WORK/resp.body" "$sid" <<'PY'
import json, sys
view = json.load(open(sys.argv[1]))
assert "live" not in view
if view.get("saved"):
    assert view["saved"]["snapshot"]["sandbox_id"] == sys.argv[2]
PY
    if [ "$mode" = "paused" ]; then
        local cursor
        code=$(req GET "/sandboxes/$sid/stats/usage?view=history&cursor=0&limit=1" "$AK")
        [ "$code" = "200" ] || fail "native usage history=$code"
        cursor=$(python3 - "$WORK/resp.body" "$sid" <<'PY'
import json, sys
page = json.load(open(sys.argv[1]))
assert len(page["records"]) == 1
assert page["records"][0]["snapshot"]["sandbox_id"] == sys.argv[2]
assert isinstance(page["next_cursor"], str) and int(page["next_cursor"]) > 0
print(page["next_cursor"])
PY
        ) || fail "native history identity/precision invalid"
        code=$(req GET "/sandboxes/$sid/stats/usage?view=history&cursor=$cursor&limit=1" "$AK")
        [ "$code" = "200" ] || fail "native usage cursor=$code"
        python3 - "$WORK/resp.body" "$sid" <<'PY'
import json, sys
page = json.load(open(sys.argv[1]))
assert len(page["records"]) <= 1
assert all(record["snapshot"]["sandbox_id"] == sys.argv[2] for record in page["records"])
PY
        [ "$(sandbox_state "$sid")" = "paused" ] || fail "native usage read woke sandbox"
    fi
    echo "==> PASS: native usage $mode, saved and applicable history/cursor via conductor for $sid"
}
