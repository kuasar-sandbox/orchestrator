#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
write_orchestrator_config unset controller
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_ready_template
BUILD_SNAPSHOT_COUNT=$(snapshot_argv_count)
[ "$BUILD_SNAPSHOT_COUNT" -gt 0 ] || fail "builder did not invoke sandbox-ctl snapshot"
for ((i=0; i<BUILD_SNAPSHOT_COUNT; i++)); do
    assert_snapshot_has_no_policy_flags "$i" || fail "builder snapshot received Pause-only policy flags"
    assert_snapshot_mode "$i" local || fail "builder snapshot did not receive checkpoint.mode=local"
done
echo "==> PASS: builder snapshots received checkpoint.mode=local without Pause-only policy flags"

create_guest
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")"
exec_through_connect "$SID" "$EXEC_TOKEN" "READY_$RANDOM"
wait_sandbox_state "$SID" running 1200 || fail "sandbox did not reach running"
MARK="STATE_$RANDOM"
. "$SCRIPT_DIR/guest_service.sh"
start_freeze_service
"$BIN/sandbox-ctl" exec --path-id "$SID" --run-root "$WORK/run/sandboxes" -- /bin/sh -ceu '
pid=$1
global=/proc/1/root/sys/fs/cgroup
real=$global/app
grep -qx "0::/init" /proc/self/cgroup
test ! -s /sys/fs/cgroup/cgroup.procs
test ! -s "$real/cgroup.procs"
grep -qx "$$" "$real/init/cgroup.procs"
service_path=$(cut -d: -f3 "/proc/$pid/cgroup")
case "$service_path" in /user|/user/*) ;; *) exit 1 ;; esac
grep -qx "$pid" "$real$service_path/cgroup.procs"
for controller in cpu memory io; do
    grep -qw "$controller" "$global/cgroup.subtree_control"
    grep -qw "$controller" "$real/cgroup.subtree_control"
done
for group in init user ptys socats; do
    test -d "$real/$group"
    test -e "$real/$group/cpu.weight"
    test -e "$real/$group/memory.max"
    test -e "$real/$group/io.weight"
done
' sh "$FREEZE_PID" >"$WORK/cgroup-topology.out" 2>&1 \
    || { sed 's/^/  cgroup| /' "$WORK/cgroup-topology.out"; fail "delegated envd cgroup topology"; }
echo "==> PASS: real /app is empty; /init + envd user/ptys/socats and cpu/memory/io delegation verified"

# ---- local Pause, all policy fields unset -> exact legacy argv + restore ---
# Write a marker file in the guest BEFORE pausing; after resume it must still be
# there — proving both the local snapshot/restore overlay AND that a resumed img
# sandbox restores (not cold-boots). /home/user is user-owned.
PERSIST="PERSIST_$MARK"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "echo $PERSIST > /home/user/persist.txt; cat /home/user/persist.txt" > "$WORK/wr.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/wr.out" || { sed 's/^/  guest| /' "$WORK/wr.out"; fail "could not write /home/user/persist.txt as the guest user (ownership not preserved?)"; }
echo "==> wrote /home/user/persist.txt in the guest (as user)"
freeze_service_probe || fail "envd freeze service disappeared before local pause"
FREEZE_COUNTER_BEFORE_B=$FREEZE_COUNTER

UNSET_CALL=$(snapshot_argv_count)
echo "==> local pause with all policy fields unset; deterministically disconnect accepted caller: $SID"
printf '%s\n' "$SID" > "$PAUSE_BARRIER_TARGET"
rm -f "$PAUSE_BARRIER_REACHED" "$PAUSE_BARRIER_RELEASE"
curl -sS --noproxy '*' -o "$WORK/pause-cancel.body" -w '%{http_code}' -X POST \
    -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
    --data '{}' "http://127.0.0.1:$PORT/sandboxes/$SID/pause" \
    >"$WORK/pause-cancel.code" 2>"$WORK/pause-cancel.stderr" &
PAUSE_CURL_PID=$!
PIDS+=("$PAUSE_CURL_PID")
for _ in $(seq 1 1500); do
    [ -e "$PAUSE_BARRIER_REACHED" ] && break
    if ! kill -0 "$PAUSE_CURL_PID" 2>/dev/null; then
        wait "$PAUSE_CURL_PID" 2>/dev/null || true
        for i in "${!PIDS[@]}"; do
            [ "${PIDS[$i]}" = "$PAUSE_CURL_PID" ] && unset 'PIDS[i]'
        done
        fail "Pause HTTP client completed before snapshot barrier"
    fi
    sleep 0.02
done
[ -e "$PAUSE_BARRIER_REACHED" ] || fail "accepted Pause did not reach deterministic snapshot barrier"
if ! kill -0 "$PAUSE_CURL_PID" 2>/dev/null; then
    wait "$PAUSE_CURL_PID" 2>/dev/null || true
    for i in "${!PIDS[@]}"; do
        [ "${PIDS[$i]}" = "$PAUSE_CURL_PID" ] && unset 'PIDS[i]'
    done
    fail "Pause HTTP client completed before deterministic cancellation"
fi
if ! kill -TERM "$PAUSE_CURL_PID"; then
    if ! kill -0 "$PAUSE_CURL_PID" 2>/dev/null; then
        wait "$PAUSE_CURL_PID" 2>/dev/null || true
        for i in "${!PIDS[@]}"; do
            [ "${PIDS[$i]}" = "$PAUSE_CURL_PID" ] && unset 'PIDS[i]'
        done
    fi
    fail "could not cancel Pause HTTP client"
fi
set +e
wait "$PAUSE_CURL_PID"
PAUSE_CURL_RC=$?
set -e
# The client has been reaped; cleanup must not signal a later user of its PID.
for i in "${!PIDS[@]}"; do
    [ "${PIDS[$i]}" = "$PAUSE_CURL_PID" ] && unset 'PIDS[i]'
done
[ "$PAUSE_CURL_RC" = 143 ] || fail "Pause HTTP client cancellation rc=$PAUSE_CURL_RC (want SIGTERM 143)"
: > "$PAUSE_BARRIER_RELEASE"
rm -f "$PAUSE_BARRIER_TARGET"
wait_sandbox_state "$SID" paused 1200 || {
    echo "==> pause client canceled at snapshot barrier but durable state did not become paused:"
    grep -iE 'snapshot|pause|api error' "$WORK/orch.log" | tail -10 | sed 's/^/  orch| /'
    SID_JOURNAL=$(journalctl KUASAR_SANDBOX_ID="$SID" --no-pager -n 30 2>/dev/null | grep -iE 'snapshot|ctl.sock|error' | tail -8)
    [ -n "$SID_JOURNAL" ] && echo "$SID_JOURNAL" | sed 's/^/  unit| /'
    fail "accepted local Pause did not commit after caller cancellation"
}
wait_proxy_traffic_stats "$SID" paused || fail "paused traffic stats were not stable"
# Pause commits the durable paused state before StopUnit makes sandboxer release
# its live controller reservation. During that bounded cleanup window, returning
# the still-real report is valid; the stable paused state must converge to 409.
wait_resource_status "$SID" 409 || fail "paused resource stats did not converge to 409"
wait_paused_cleanup "$SID" || fail "paused runtime ownership did not durably clear"
assert_native_usage "$SID" paused
# Keep the sandbox durably paused for longer than several service counter ticks.
# On restore the counter must resume from the frozen snapshot rather than track
# this host wall-clock interval.
sleep 3
assert_snapshot_argv "$UNSET_CALL" \
    snapshot --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode local --run-root "$WORK/run/sandboxes" \
    || fail "all-unset local Pause did not pass the configured mode"
B_LOCAL="$CHECKPOINT_ROOT/$SID/checkpoint/$SID.snapshot"
[ -f "$B_LOCAL" ] || fail "local Pause did not create $B_LOCAL"
B_ARTIFACT="$(readlink -f "$B_LOCAL")"
[ -f "$B_ARTIFACT" ] || fail "local B target is missing: $B_ARTIFACT"
B_SNAPSHOT_BASENAME="$(basename "$B_ARTIFACT")"
"$BIN/sandbox-ctl" info --json "$B_LOCAL" >"$WORK/b-local.json" \
    || fail "all-unset local B is not a readable snapshot bundle"
echo "==> PASS: caller canceled at accepted barrier; all-unset Pause still committed B and passed no policy flags"

echo "==> accept paused -> starting through POST /connect, then activate native exec immediately"
code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"timeout":113}')
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "Connect paused=$code (want 200)"; }
CONNECT_RETURN_STATE="$(sandbox_state "$SID")"
case "$CONNECT_RETURN_STATE" in starting|running) ;; *) fail "state immediately after Connect=$CONNECT_RETURN_STATE (must not remain paused)";; esac
RESUME_MARK="NATIVE_EXEC_RESUME_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK"
resumed=""
for _ in $(seq 1 90); do
    code=$(curl -sS --max-time 1 --unix-socket "$ENVD_SOCK" \
        -o /dev/null -w '%{http_code}' http://envd/health 2>/dev/null || true)
    case "$code" in 200|204) resumed=1; break ;; esac
    sleep 0.5
done
[ -n "$resumed" ] || fail "envd did not become ready after local restore"
wait_resource_stats "$SID" || fail "resource stats did not recover after restore"
assert_native_usage "$SID" live
echo "==> PASS: Connect returned after durable starting acceptance (observed $CONNECT_RETURN_STATE); immediate native exec parked to running"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" > "$WORK/exec2.out" 2>&1 || true
sed 's/^/  guest2| /' "$WORK/exec2.out"
grep -q "$PERSIST" "$WORK/exec2.out" || fail "pre-pause state LOST after local restore"
freeze_service_probe || fail "envd freeze service/listener missing after local B restore"
FREEZE_COUNTER_AFTER_B=$FREEZE_COUNTER
FREEZE_DELTA_B=$((FREEZE_COUNTER_AFTER_B - FREEZE_COUNTER_BEFORE_B))
[ "$FREEZE_DELTA_B" -ge 0 ] && [ "$FREEZE_DELTA_B" -lt 100 ] \
    || fail "envd service counter advanced across frozen local B window: before=$FREEZE_COUNTER_BEFORE_B after=$FREEZE_COUNTER_AFTER_B"
sleep 1
freeze_service_probe || fail "envd freeze service stopped after local B restore"
[ "$FREEZE_COUNTER" -gt "$FREEZE_COUNTER_AFTER_B" ] \
    || fail "envd freeze service did not resume counter after local B restore"
echo "==> PASS: all-unset B restored envd-managed PID $FREEZE_PID + listener; frozen counter delta=$FREEZE_DELTA_B, then advanced"

code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalizer retained row"
[ ! -e "$WORK/run/sandboxes/$SID" ] && [ ! -e "$WORK/lib/sandboxes/$SID" ] || fail "delete retained owned directories"
[ -f "$WORK/lib/node-ctl.db" ] && [ -S "$WORK/node-ctl.socket" ] || fail "finalizer removed node files"
# Restart the conductor against the same store with explicit node defaults.
# Local mode must remain warning-free; these values are inherited only when a
# higher layer leaves the corresponding field unset.
stop_orchestrator
write_orchestrator_config node-policy
start_orchestrator "$WORK/orch-node-policy.log"
wait_mmds_listener
echo "==> PASS: conductor restarted in local mode with node merge_ref=true/drop_caches=false"

# ---- Create metadata/header + Pause body/header fieldwise overlays --------
CREATE_POLICY_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.checkpoint": json.dumps({"merge_ref": True, "drop_caches": True})
    },
}))
PY
)
REQ_CHECKPOINT_HEADER='{"merge_ref":false,"drop_caches":null}'
code=$(req POST /sandboxes "$AK" "$CREATE_POLICY_BODY")
unset REQ_CHECKPOINT_HEADER
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "policy create=$code (want 201)"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
assert_no_default_exec_token "$WORK/resp.body" || fail "policy create exposed a default exec token"

# The Create header overrides merge_ref, while its null drop_caches inherits
# the body. The stored request-scoped namespace must be canonical.
python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import json, sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select metadata_json from sandboxes where id=?", (sys.argv[2],)).fetchone()
if row is None:
    raise SystemExit("created sandbox row not found")
metadata = json.loads(row[0])
got = metadata.get("kuasar-sandbox.checkpoint")
want = '{"merge_ref":false,"drop_caches":true}'
if got != want:
    raise SystemExit(f"stored checkpoint metadata={got!r}, want {want!r}")
PY
echo "==> PASS: Create checkpoint header overlaid body per field and persisted canonical metadata"

ENVD_SOCK="$WORK/run/sandboxes/$SID/envd.sock"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue policy sandbox exec capability"
POLICY_START_MARK="POLICY_CREATE_ACTIVATION_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$POLICY_START_MARK"
wait_sandbox_state "$SID" running 20 || fail "policy sandbox did not reach running after exec activation"
POLICY_PERSIST="POLICY_PERSIST_$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "echo $POLICY_PERSIST > /home/user/policy-persist.txt" >"$WORK/policy-write.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/policy-write.out" || { sed 's/^/  guest| /' "$WORK/policy-write.out"; fail "write policy sandbox marker"; }

POLICY_CALL=$(snapshot_argv_count)
# Body overrides stored metadata; the header then overrides only merge_ref.
# Its null drop_caches must preserve the body's false.
REQ_CHECKPOINT_HEADER='{"merge_ref":false,"drop_caches":null}'
code=$(req POST "/sandboxes/$SID/pause" "$AK" \
    '{"memory":true,"checkpoint_merge_ref":true,"checkpoint_drop_caches":false}')
unset REQ_CHECKPOINT_HEADER
[ "$code" = "204" ] || { cat "$WORK/resp.body"; sed 's/^/  orch| /' "$WORK/orch-node-policy.log"; fail "policy pause=$code (want 204)"; }
assert_snapshot_argv "$POLICY_CALL" \
    snapshot --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode local --run-root "$WORK/run/sandboxes" \
    --merge-ref=false --drop-caches=false \
    || fail "Pause body/header policy did not reach sandbox-ctl exactly"
W_POLICY="$CHECKPOINT_ROOT/$SID/checkpoint/$SID.snapshot"
[ -f "$W_POLICY" ] || fail "policy Pause did not create $W_POLICY"
"$BIN/sandbox-ctl" info --json "$W_POLICY" >"$WORK/w-policy.json" || fail "policy W is unreadable"
python3 - "$WORK/w-policy.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    info = json.load(source)
refs = info.get("FromRefs") or []
if not refs:
    raise SystemExit("working-set W has no memory lower reference")
if "kuasar-sandbox.checkpoint" in (info.get("Metadata") or {}):
    raise SystemExit("host-only checkpoint policy leaked into snapshot.cfg metadata")
PY
echo "==> PASS: Pause header null inherited body, explicit false flags reached local capture, W self remains separate from its memory lower"

# Reuse this managed sandbox to exercise repeated working-set capture and
# false -> true -> false, restoring only after old files have disappeared.
CHECKPOINT_DIR="$CHECKPOINT_ROOT/$SID/checkpoint"
python3 - "$CHECKPOINT_DIR" "$SID" <<'PY_PROTECT'
import os, sys
root, sid = sys.argv[1:]
for name in ("user.snapshot", "user.tmp", sid+"2.snapshot.12.partial", sid+".snapshot.01.partial"):
    open(os.path.join(root, name), "w").write("protected fixture")
os.mkdir(os.path.join(root, "unfamiliar"))
os.symlink("../outside-user-file", os.path.join(root, "f"*64+".image"))
PY_PROTECT
for merge in false false false true false; do
    previous_pair=$(checkpoint_pair "$SID" snapshot "$CHECKPOINT_DIR/$SID.snapshot") || fail "invalid previous history S/E pair"
    code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"timeout":120}')
    [ "$code" = 200 ] || fail "history restore=$code"
    wait_sandbox_state "$SID" running 1200 || fail "history restore did not reach running"
    python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
        "grep -qx $POLICY_PERSIST /home/user/policy-persist.txt; echo history-$merge >> /home/user/history-rounds" \
        >"$WORK/history-restore.out" 2>&1 || true
    grep -q 'EXIT_CODE 0' "$WORK/history-restore.out" || fail "restored checkpoint lost disk state"
    # Both interrupted partials and complete failed-attempt garbage are found
    # by enumeration, without needing successful response bookkeeping.
    printf incomplete >"$CHECKPOINT_DIR/$SID.snapshot.4294967295.partial"
    printf garbage >"$CHECKPOINT_DIR/$(printf '%064d' 8).snapshot"
    code=$(req POST "/sandboxes/$SID/pause" "$AK" \
        "{\"memory\":true,\"checkpoint_merge_ref\":$merge,\"checkpoint_drop_caches\":false}")
    [ "$code" = 204 ] || fail "history capture=$code"
    wait_paused_cleanup "$SID" || fail "history checkpoint cleanup did not finish"
    "$BIN/sandbox-ctl" info --json "$CHECKPOINT_DIR/$SID.snapshot" >"$WORK/history-current.json"
    current_pair=$(checkpoint_pair "$SID" snapshot "$CHECKPOINT_DIR/$SID.snapshot") || fail "invalid current history S/E pair"
    python3 - "$WORK/history-current.json" "$WORK/w-policy.json" "$merge" "$CHECKPOINT_DIR" "$SID" "$previous_pair" "$current_pair" <<'PY_HISTORY'
import json, os, sys
current, original = (json.load(open(path)) for path in sys.argv[1:3])
refs, external = current.get("FromRefs") or [], original.get("FromRefs") or []
count = 0 if sys.argv[3] == "true" else 1
if len(refs) != count+len(external) or refs[count:] != external:
    raise SystemExit(f"unbounded/local boundary changed: {refs!r}, suffix={external!r}")
root, sid, before, after = sys.argv[4:]
# checkpoint_pair validated actual stored S/E identities and both physical files
# while each source was current. Snapshot capture creates only the S alias.
old_e, current_e = (json.loads(pair)[3] for pair in (before, after))
def checkpoint_path(ref):
    return os.path.join(root, ref.removeprefix("file://").split("@", 1)[0])
old_path, current_path = checkpoint_path(old_e), checkpoint_path(current_e)
if old_path != current_path and os.path.lexists(old_path):
    raise SystemExit(f"unused previous E remains after source commit: {old_e!r}")
for name in ("user.snapshot", "user.tmp", sid+"2.snapshot.12.partial", sid+".snapshot.01.partial", "unfamiliar", "f"*64+".image"):
    if not os.path.lexists(os.path.join(root, name)):
        raise SystemExit(f"protected entry deleted: {name}")
for name in (sid+".snapshot.4294967295.partial", "8".zfill(64)+".snapshot"):
    if os.path.lexists(os.path.join(root, name)):
        raise SystemExit(f"owned failed-attempt garbage remains: {name}")
# Runtime disk lower lists may retain external/image layers, but local writable
# E/overlay ancestry must not grow with memory working-set capture.
for node in [current["Boot"]["Root"], *(current["Boot"]["Disks"] or [])]:
    writable = node["Overlay"] if node["Overlay"] is not None else node
    for raw in writable["BaseFromRefs"] or []:
        if raw.startswith("file://") and "@location:" not in raw and (".sandbox@" in raw or ".overlay@" in raw):
            raise SystemExit(f"local writable disk lower was not absorbed: {raw}")
PY_HISTORY
done
echo "==> PASS: repeated managed false/true/false restores preserved state, bounded history, and directory protection"

S_COLD_RUN_CALL=$(run_argv_count)
code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"timeout":113,"memory":false}')
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "Snapshot cold Connect=$code (want 200)"; }
wait_sandbox_state "$SID" running 1200 || fail "Snapshot S cold selection did not reach running"
assert_run_source_mode "$S_COLD_RUN_CALL" "$SID" from \
    || fail "Connect(memory=false) on Snapshot S did not execute sandbox-ctl run --from E"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/policy-persist.txt" >"$WORK/policy-read.out" 2>&1 || true
grep -q "$POLICY_PERSIST" "$WORK/policy-read.out" || { sed 's/^/  guest| /' "$WORK/policy-read.out"; fail "Snapshot S cold launch lost E disk state"; }
echo "==> PASS: Connect(memory=false) selected E from Snapshot S and cold-launched its disk state"

E_LOCAL_EXPORT_CALL=$(export_argv_count)
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":false}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "local Sandbox E pause=$code (want 204)"; }
assert_export_argv "$E_LOCAL_EXPORT_CALL" \
    export --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode local --run-root "$WORK/run/sandboxes" \
    || fail "Pause(memory=false) did not execute sandbox-ctl export in local mode"
E_LOCAL="$CHECKPOINT_ROOT/$SID/checkpoint/$SID.sandbox"
[ -e "$E_LOCAL" ] || fail "Pause(memory=false) did not create $E_LOCAL"
checkpoint_pair "$SID" sandbox "$E_LOCAL" >/dev/null \
    || fail "local Sandbox E durable source is incorrect"
code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"memory":true}')
[ "$code" = "409" ] || { cat "$WORK/resp.body"; fail "Sandbox E Connect(memory=true)=$code (want 409)"; }
[ "$(sandbox_state "$SID")" = "paused" ] || fail "memory-unavailable conflict changed paused Sandbox E state"

printf '%s\n' park >"$WORK/inject-sandbox-run"
E_WAKE_RUN_CALL=$(run_argv_count)
(
    code=$(DP_MAX_TIME=180 dp "49983-$SID" /health "$ENVD_TOKEN" || true)
    printf '%s\n' "$code" >"$WORK/e-wake-data.code"
) &
IMMEDIATE_DATA_PID=$!
wait_sandbox_state "$SID" starting 120 || fail "Sandbox E traffic Wake was not durably accepted"
wait_proxy_traffic_stats "$SID" parking \
    || fail "first Sandbox E Wake request was not parked while cold launch was starting"
wait_run_argv "$E_WAKE_RUN_CALL" || fail "Sandbox E traffic Wake did not start its runner"
assert_run_source_mode "$E_WAKE_RUN_CALL" "$SID" from \
    || fail "Sandbox E traffic Wake did not execute sandbox-ctl run --from"
rm -f "$WORK/inject-sandbox-run"
e_wake_status=0
wait "$IMMEDIATE_DATA_PID" || e_wake_status=$?
IMMEDIATE_DATA_PID=""
[ "$e_wake_status" = 0 ] || fail "parked Sandbox E Wake client exited $e_wake_status"
code=$(cat "$WORK/e-wake-data.code")
{ [ "$code" = "204" ] || [ "$code" = "200" ]; } \
    || { cat "$WORK/dp.body"; fail "parked Sandbox E Wake request=$code"; }
wait_sandbox_state "$SID" running 1200 || fail "Sandbox E cold Wake did not reach running"
assert_native_usage "$SID" live
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/policy-persist.txt" >"$WORK/e-wake-read.out" 2>&1 || true
grep -q "$POLICY_PERSIST" "$WORK/e-wake-read.out" \
    || { sed 's/^/  guest| /' "$WORK/e-wake-read.out"; fail "Sandbox E Wake lost disk state"; }
echo "==> PASS: E rejected memory resume, then the original traffic request parked through cold --from and forwarded after running"
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill policy sandbox=$code"
unset EXEC_TOKEN
SID_POLICY="$SID"

# ---- automatic Pause: metadata merge_ref > node; node supplies drop_caches -
AUTO_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.checkpoint": json.dumps({"merge_ref": False})
    },
}))
PY
)
AUTO_CALL=$(snapshot_argv_count)
code=$(req POST /sandboxes "$AK" "$AUTO_BODY")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "auto-pause create=$code"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
code=$(req POST "/sandboxes/$SID/timeout" "$AK" '{"timeout":1}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "arm auto-pause timeout=$code"; }
AUTO_PAUSED=""
for _ in $(seq 1 180); do
    state=$(python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select state from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "missing")
PY
)
    [ "$state" = "paused" ] && { AUTO_PAUSED=1; break; }
    sleep 0.5
done
[ -n "$AUTO_PAUSED" ] || { sed 's/^/  orch| /' "$WORK/orch-node-policy.log"; fail "reaper did not auto-pause policy sandbox (last state=$state)"; }
wait_paused_cleanup "$SID" || fail "auto-paused runtime ownership did not durably clear"
assert_snapshot_argv "$AUTO_CALL" \
    snapshot --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode local --run-root "$WORK/run/sandboxes" \
    --merge-ref=false --drop-caches=false \
    || fail "auto-pause did not resolve metadata > node fieldwise"
[ -f "$CHECKPOINT_ROOT/$SID/checkpoint/$SID.snapshot" ] || fail "auto-pause did not create local W"
echo "==> PASS: reaper auto-pause used metadata merge_ref=false and node drop_caches=false"

code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill auto-paused sandbox=$code (want 204)"

# autoPauseMemory=false affects only TTL capture. An explicit omitted-memory
# Pause must still produce Snapshot S; after its default memory Wake, expiry
# must produce Sandbox E without carrying snapshot-only policy flags.
AUTO_E_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "autoPauseMemory": False,
}))
PY
)
code=$(req POST /sandboxes "$AK" "$AUTO_E_BODY")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "autoPauseMemory=false create=$code"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
ENVD_SOCK="$WORK/run/sandboxes/$SID/envd.sock"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "autoPauseMemory=false exec capability"
exec_through_connect "$SID" "$EXEC_TOKEN" "AUTO_E_START_$RANDOM"
wait_sandbox_state "$SID" running 1200 || fail "autoPauseMemory=false sandbox did not reach running"

AUTO_E_EXPLICIT_S_CALL=$(snapshot_argv_count)
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "autoPauseMemory=false explicit Pause({})=$code"; }
wait_paused_cleanup "$SID" || fail "explicit paused runtime ownership did not durably clear"
assert_snapshot_argv "$AUTO_E_EXPLICIT_S_CALL" \
    snapshot --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode local --run-root "$WORK/run/sandboxes" \
    --merge-ref=true --drop-caches=false \
    || fail "explicit Pause({}) inherited AutoPauseMemory=false instead of capturing Snapshot S"
python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY' \
    || fail "explicit Pause({}) on autoPauseMemory=false did not persist Snapshot S"
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute(
        "select state, resume_source_kind, auto_pause_memory from sandboxes where id=?",
        (sys.argv[2],),
    ).fetchone()
if row != ("paused", "snapshot", 0):
    raise SystemExit(f"explicit pause row={row!r}, want ('paused', 'snapshot', 0)")
PY
AUTO_E_MEMORY_RUN_CALL=$(run_argv_count)
exec_through_connect "$SID" "$EXEC_TOKEN" "AUTO_E_MEMORY_WAKE_$RANDOM"
wait_sandbox_state "$SID" running 1200 || fail "explicit Snapshot S did not resume"
assert_run_source_mode "$AUTO_E_MEMORY_RUN_CALL" "$SID" restore \
    || fail "ordinary Wake of explicit Snapshot S did not default to memory restore"

AUTO_E_TTL_CALL=$(export_argv_count)
code=$(req POST "/sandboxes/$SID/timeout" "$AK" '{"timeout":1}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "arm autoPauseMemory=false timeout=$code"; }
AUTO_E_PAUSED=""
for _ in $(seq 1 180); do
    [ "$(sandbox_state "$SID")" = "paused" ] && { AUTO_E_PAUSED=1; break; }
    sleep 0.5
done
[ -n "$AUTO_E_PAUSED" ] || fail "autoPauseMemory=false TTL did not pause"
wait_paused_cleanup "$SID" || fail "TTL paused runtime ownership did not durably clear"
assert_export_argv "$AUTO_E_TTL_CALL" \
    export --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode local --run-root "$WORK/run/sandboxes" \
    || fail "autoPauseMemory=false TTL did not capture Sandbox E"
python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY' \
    || fail "autoPauseMemory=false TTL durable source is incorrect"
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute(
        "select state, resume_source_kind, auto_pause_memory, launch_mode from sandboxes where id=?",
        (sys.argv[2],),
    ).fetchone()
if row != ("paused", "sandbox", 0, ""):
    raise SystemExit(f"TTL row={row!r}, want ('paused', 'sandbox', 0, '')")
PY
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill autoPauseMemory=false sandbox=$code"
unset EXEC_TOKEN
echo "==> PASS: explicit Pause({}) remained S while autoPauseMemory=false TTL captured E"

# ---- teardown -------------------------------------------------------------
echo "==> PASS: all local checkpoint-policy sandboxes killed"

echo "PASS orchestrator.pause-wake.sh"
