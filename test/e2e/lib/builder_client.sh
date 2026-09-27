#!/usr/bin/env bash
req() { # method path key [body]
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method"
                -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "${REQ_MMDS_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-MMDS: ${REQ_MMDS_HEADER}")
    [ -n "${REQ_BUILDER_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Builder: ${REQ_BUILDER_HEADER}")
    [ -n "${REQ_RESOURCE_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Resource: ${REQ_RESOURCE_HEADER}")
    [ -n "${REQ_NETWORK_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Network: ${REQ_NETWORK_HEADER}")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}
assert_trigger_conflict() { # $1=exact internal state
    python3 - "$WORK/resp.body" "$1" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))
state = sys.argv[2]
assert body == {
    "message": f"build cannot be triggered from state {state}",
    "state": state,
}, body
PY
}
wait_running() { # sid
    local sid="$1" code state=""
    for _ in $(seq 1 180); do
        code=$(req GET "/sandboxes/$sid" "$AK" || true)
        if [ "$code" = "200" ]; then
            state=$(json_field "$WORK/resp.body" state)
            [ "$state" = "running" ] && return 0
            [ "$state" = "dead" ] && break
        fi
        sleep 0.5
    done
    echo "sandbox $sid state=$state, want running" >&2
    return 1
}
wait_resource_capacity() { # $1=sid, $2=expected memoryCapacity bytes
    local sid="$1" expected="$2" code=""
    for _ in $(seq 1 240); do
        code=$(req GET "/sandboxes/$sid/stats/resource" "$AK" || true)
        if [ "$code" = "200" ] && python3 - "$WORK/resp.body" "$expected" <<'PY'
import json, sys
stats = json.load(open(sys.argv[1]))
assert stats.get("memoryCapacity") == int(sys.argv[2]), stats
PY
        then
            return 0
        fi
        sleep 0.25
    done
    echo "resource stats sid=$sid status=$code expected memoryCapacity=$expected body=$(cat "$WORK/resp.body" 2>/dev/null)" >&2
    return 1
}
register() { # name [profile] [target-json] [sandbox-config=0|1] → sets TID/BID
    local code body expected_profile got_profile target_json sandbox_config target_suffix
    expected_profile="${2:-e2b}"
    target_json="${3:-}"
    sandbox_config="${4:-0}"
    # A/B sandbox resources derive from this immutable Build vector.
    # Parent services/slices add no CPU/memory policy. Target Sandbox capacity is
    # a separate Create input and is included only for Sandbox-producing cases.
    body="{\"name\":\"$1\",\"cpuCount\":$BUILDER_CPU,\"memoryMB\":6144}"
    [ -z "${2:-}" ] || body="{\"name\":\"$1\",\"profile\":\"$2\",\"cpuCount\":$BUILDER_CPU,\"memoryMB\":6144}"
    target_suffix=""
    [ -z "$target_json" ] || target_suffix=",\"target\":$target_json"
    REQ_BUILDER_HEADER="{\"resources\":{\"cpu\":$BUILDER_CPU,\"memory\":\"6GiB\",\"storage\":\"4GiB\"}$target_suffix}"
    if [ -n "${REQ_NETWORK_HEADER:-}" ]; then
        # Force the guest import path when testing request network overrides.
        REQ_BUILDER_HEADER="${REQ_BUILDER_HEADER%?},\"referer\":{\"enabled\":false}}"
    fi
    if [ "$sandbox_config" = "1" ]; then
        REQ_RESOURCE_HEADER='{"capacity":{"cpu":2,"memory":"3GiB"},"allocatable":{"cpu":1,"memory":"512MiB"},"startup":{"memory":"3GiB"}}'
        body="${body%?},\"envVars\":{\"BUILD_TARGET_ENV\":\"portable-e2e\"}}"
    fi
    code=$(req POST /v3/templates "$AK" "$body")
    unset REQ_BUILDER_HEADER REQ_RESOURCE_HEADER REQ_NETWORK_HEADER
    [ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register $1 = $code (want 202)"; }
    TID=$(json_field "$WORK/resp.body" templateID)
    BID=$(json_field "$WORK/resp.body" buildID)
    got_profile=$(json_field "$WORK/resp.body" profile)
    [ "$got_profile" = "$expected_profile" ] \
        || fail "register $1 profile=$got_profile (want $expected_profile)"
    python3 - "$WORK/resp.body" "$target_json" <<'PY' \
        || fail "register $1 did not preserve requested target"
import json, sys
response = json.load(open(sys.argv[1]))
expected = json.loads(sys.argv[2]) if sys.argv[2] else None
if expected is not None and expected.get("memory") is False:
    expected.pop("memory")
assert response.get("target") == expected, response
PY
    [ ! -e "$WORK/run/builds/$BID" ] \
        || fail "registered Build created BuildRunDir before execution claim"
    [ ! -e "$WORK/lib/builds/$BID" ] \
        || fail "registered Build created BuildBaseDir before execution claim"
}
diag() { # bid — failure diagnostics (BuildRunDir/BuildBaseDir are reaped by the orchestrator)
    echo "---- orchestrator log (tail) ----"
    tail -40 "$WORK/orch.log" 2>/dev/null | sed 's/^/    /'
    echo "---- journal build $1 (tail) ----"
    journalctl KUASAR_BUILD_ID="$1" --no-pager -n 120 2>/dev/null | sed 's/^/    /'
}
wait_ready() { # tid bid label requested-target-json-or-null kind → sets PERSIST
    local tid="$1" bid="$2" label="$3" expected_target="$4" expected_kind="$5" status="" code
    for _ in $(seq 1 240); do
        code=$(req GET "/templates/$tid/builds/$bid/status" "$AK")
        [ "$code" = "200" ] || fail "$label status = $code (want 200)"
        status=$(json_field "$WORK/resp.body" status)
        case "$status" in
            ready)
                PERSIST=$(json_field "$WORK/resp.body" templateID)
                valid_persist_id "$PERSIST" || fail "$label ready but invalid persist id: $(cat "$WORK/resp.body")"
                python3 - "$WORK/resp.body" "$expected_target" "$expected_kind" <<'PY' \
                    || fail "$label status target/kind contract mismatch"
import json, sys
status = json.load(open(sys.argv[1]))
expected = json.loads(sys.argv[2])
if expected is not None and expected.get("memory") is False:
    expected.pop("memory")
assert status.get("target") == expected, status
assert status.get("kind") == sys.argv[3], status
PY
                return 0;;
            error)
                echo "    $label error response: $(cat "$WORK/resp.body")"
                diag "$bid"; fail "$label build status=error";;
        esac
        sleep 2
    done
    diag "$bid"; fail "$label did not reach ready (last status=$status)"
}
wait_error() { # tid bid label
    local tid="$1" bid="$2" label="$3" status="" code
    for _ in $(seq 1 240); do
        code=$(req GET "/templates/$tid/builds/$bid/status" "$AK")
        [ "$code" = "200" ] || fail "$label status = $code (want 200)"
        status=$(json_field "$WORK/resp.body" status)
        case "$status" in
            error) return 0 ;;
            ready) fail "$label unexpectedly reached ready" ;;
        esac
        sleep 0.5
    done
    diag "$bid"; fail "$label did not reach error (last status=$status)"
}
wait_build_row_deleted() { # bid — terminal_ttl + 5s reaper cadence
    local bid="$1"
    for _ in $(seq 1 80); do
        if python3 - "$WORK/lib/node-ctl.db" "$bid" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    count = db.execute("select count(*) from builds where build_id=?", (sys.argv[2],)).fetchone()[0]
raise SystemExit(0 if count == 0 else 1)
PY
        then
            return 0
        fi
        sleep 0.25
    done
    return 1
}
