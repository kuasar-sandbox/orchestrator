#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","kvm","netns","systemd","tun","userfaultfd"]
# Real conductor/current-disk lifecycle. SDK multi-disk capture boundaries are
# also covered by sandboxer snapshot.reset.sh and artifact fault-injection tests.
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/execute_checkpoint.sh"

reset_cleanup() {
    local status=$? cleanup_status=0
    set +e
    if [ "$status" != 0 ]; then
        for log in "$WORK"/orch*.log; do
            [ ! -f "$log" ] || tail -40 "$log" | sed -E 's/[0-9a-f]{64}/<redacted>/g' >&2
        done
    fi
    cleanup || cleanup_status=$?
    [ "$status" != 0 ] || status=$cleanup_status
    exit "$status"
}
trap reset_cleanup EXIT

# Test-only process barriers surround the real SDK command. Killing conductor
# and this exact wrapper models lost preparation/results without changing code.
mv "$ORCH_BIN_DIR/sandbox-ctl" "$ORCH_BIN_DIR/sandbox-ctl.original"
cat >"$ORCH_BIN_DIR/sandbox-ctl" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [ "\${1:-}" = reset ]; then
    mode=""
    [ ! -f "$WORK/reset-mode" ] || mode="\$(cat "$WORK/reset-mode")"
    barrier() {
        printf '%s\n' "\$\$" >"$WORK/reset-reached"
        while [ -f "$WORK/reset-mode" ]; do sleep 0.02; done
    }
    [ "\$mode" != before ] || barrier
    "$BIN/sandbox-ctl" "\$@" >"$WORK/reset-result.json"
    [ "\$mode" != after ] || barrier
    cat "$WORK/reset-result.json"
    exit 0
fi
if [ "\${1:-}" = run ] && [ -f "$WORK/reset-fail-ready" ]; then
    exec python3 "$WORK/reset-ready-failure.py" "$BIN/sandbox-ctl" "\$@"
fi
exec "$ORCH_BIN_DIR/sandbox-ctl.original" "\$@"
EOF
chmod +x "$ORCH_BIN_DIR/sandbox-ctl"

# Form real disks and save a guest payload, then corrupt only the readiness
# event seen by conductor. The launch must roll back before running admission.
cat >"$WORK/reset-ready-failure.py" <<'PY'
import os, subprocess, sys
binary, args = sys.argv[1], sys.argv[2:]
def value(name):
    for i, arg in enumerate(args):
        if arg == name: return args[i+1]
        if arg.startswith(name+'='): return arg.split('=',1)[1]
    raise ValueError(name)
ready = int(value('--ready-fd'))
readfd, writefd = os.pipe()
childargs = list(args)
for i, arg in enumerate(childargs):
    if arg.startswith('--ready-fd='): childargs[i] = '--ready-fd='+str(writefd)
    elif arg == '--ready-fd': childargs[i+1] = str(writefd)
# Preserve the runner's validated cgroup directory while replacing only the
# readiness pipe. Popen otherwise closes that inherited runtime prerequisite.
cgroup = value('--cgroup-path')
assert cgroup.startswith('fd='), cgroup
child = subprocess.Popen([binary, *childargs], pass_fds=(writefd, int(cgroup[3:])))
os.close(writefd)
with os.fdopen(readfd,'rb') as events:
    assert events.readline()==b'control_ready\n'
    os.write(ready,b'control_ready\n')
    assert events.readline()==b'ready\n'
    subprocess.run([binary,'exec','--path-id',value('--path-id'),'--run-root',value('--run-root'),
                    '--','/bin/sh','-c',"printf formed >/reset-payload; sync"],check=True,timeout=30)
    os.write(ready,b'invalid_runtime_event\n')
    os.close(ready)
sys.exit(child.wait())
PY

docker save "$E2E_IMAGE" | "$BIN/flatten-ctl" export --output "$WORK/reset.image" --no-progress
ROOT=$(MANIFEST_KEY="$MK" "$BIN/manifest-ctl" store --manifest-config "$WORK/manifest.yaml" --no-progress "$WORK/reset.image")
TEMPLATE=$(python3 - "$ROOT" <<'PY'
import base64,sys
print('bare-img-'+base64.urlsafe_b64encode(('manifest://'+sys.argv[1]).encode()).decode().rstrip('='))
PY
)
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null

create_reset_guest() {
    REQ_RESOURCE_HEADER='{"capacity":{"cpu":1,"memory":"512MiB"}}'
    code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":600,\"autoReset\":$1}")
    unset REQ_RESOURCE_HEADER
    [ "$code" = 201 ] || fail "Create=$code"
    SID=$(json_field "$WORK/resp.body" sandboxID)
    wait_sandbox_state "$SID" running 1200 || fail "guest did not start"
    checkpoint_guest /bin/true
}
write_value() { checkpoint_guest /bin/sh -c "printf '%s' '$1' >/reset-payload; sync"; }
read_value() {
    checkpoint_guest /bin/cat /reset-payload >"$CASE_OUT/$1.actual"
    printf '%s' "$2" >"$CASE_OUT/$1.expected"
    cmp "$CASE_OUT/$1.actual" "$CASE_OUT/$1.expected"
}
stop_run() {
    OLD_RUN=$(sandbox_run_id "$SID")
    [ -n "$OLD_RUN" ] || fail "missing run identity"
    systemctl stop "${RUNNER_PREFIX}${OLD_RUN}.service"
}
reset_manual() {
    code=$(req POST "/sandboxes/$SID/reset" "$AK")
    [ "$code" = 204 ] || { cat "$WORK/resp.body"; fail "Reset=$code"; }
    wait_paused_cleanup "$SID" 300 || fail "Reset did not commit complete paused"
    code=$(req POST "/sandboxes/$SID/reset" "$AK")
    [ "$code" = 204 ] || fail "idempotent Reset=$code"
}
resume_reset() {
    code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"memory":true}')
    [ "$code" = 409 ] || fail "E-only memory Resume=$code"
    resume_checkpoint_guest 0 reset
    NEW_RUN=$(sandbox_run_id "$SID")
    [ -n "$NEW_RUN" ] && [ "$NEW_RUN" != "$OLD_RUN" ] || fail "Reset reused RunID"
    python3 - "$WORK/lib/node-ctl.db" "$SID" "$NEW_RUN" "$CASE_OUT/identities.jsonl" <<'PY'
import json,pathlib,sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
    row=db.execute('select instance_id,base_dir,auto_reset,dead_unix,sandbox_result_json,auto_reset_error from sandboxes where id=?',(sys.argv[2],)).fetchone()
assert row[3:]==(0,'',''),row
d=json.loads((pathlib.Path(row[1])/'checkpoint/current-disks.json').read_text())
assert d['run']==dict(sandbox_id=sys.argv[2],instance_id=row[0],run_id=sys.argv[3]),d['run']
with open(sys.argv[4],'a') as out: out.write(json.dumps(dict(sid=sys.argv[2],run=sys.argv[3],instance=row[0],autoReset=bool(row[2])))+'\n')
PY
}
crash_conductor() {
    local old=$ORCH_PID
    kill -KILL "$old"
    wait "$old" 2>/dev/null || true
    for i in "${!PIDS[@]}"; do [ "${PIDS[$i]}" != "$old" ] || unset 'PIDS[i]'; done
    ORCH_PID=""
}
restart_conductor() {
    start_conductor "$WORK/orch-$1.log"
    "$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
    wait_proxy_ready "$PROXY_PID" 127.0.0.1 "$PROXY_PORT" "$WORK/proxy-stats.sock" "$WORK/proxy.log" || fail "proxy did not resynchronize after conductor restart"
}
assert_deleted() {
    local deleted=$SID old=$OLD_RUN
    delete_checkpoint_guest
    [ ! -e "$EXECUTE_RUN_ROOT/sandboxes/$deleted" ] || fail "deleted RunDir remains"
    ! systemctl is-active --quiet "${RUNNER_PREFIX}${old}.service" || fail "deleted runner active"
    code=$(req POST "/sandboxes/$deleted/reset" "$AK")
    [ "$code" = 404 ] || fail "deleted parent revived: $code"
}

# No stored checkpoint; alive process adoption retains exactly the same run.
create_reset_guest false
write_value initial
OLD_RUN=$(sandbox_run_id "$SID")
crash_conductor
restart_conductor alive
[ "$(sandbox_run_id "$SID")" = "$OLD_RUN" ] || fail "live run was replaced on restart"
read_value adopted initial
stop_run
wait_sandbox_state "$SID" dead 1200 || fail "termination did not retain dead"
[ -f "$CHECKPOINT_ROOT/$SID/checkpoint/current-disks.json" ] || fail "current disks lost"
reset_manual
resume_reset
read_value no-checkpoint initial

# User checkpoint A remains immutable while Reset captures later writes B.
write_value A
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":false,"checkpoint":true}')
[ "$code" = 200 ] || fail "retained checkpoint=$code"
CP_A=$(json_field "$WORK/resp.body" checkpointID)
wait_paused_cleanup "$SID" 300 || fail "checkpoint cleanup"
resume_checkpoint_guest 0 checkpoint
write_value B
stop_run
wait_sandbox_state "$SID" dead 1200 || fail "second termination"
E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" --checkpoint "$CP_A" \
    --to-template --json --socket "$WORK/node-ctl.socket" >"$WORK/export-A.json"
HISTORY_TEMPLATE=$(json_field "$WORK/export-A.json" result)
reset_manual
resume_reset
read_value current-after-A B
assert_deleted
TEMPLATE=$HISTORY_TEMPLATE
create_reset_guest false
read_value history-A A
OLD_RUN=$(sandbox_run_id "$SID")
assert_deleted

# Durable automatic responsibility survives a process crash before preparation
# and after E creation, always with the same operation ID and no partial paused.
create_reset_guest true
write_value automatic
for boundary in before after; do
    printf '%s\n' "$boundary" >"$WORK/reset-mode"
    stop_run
    for _ in $(seq 1 600); do [ ! -f "$WORK/reset-reached" ] || break; sleep 0.05; done
    [ -f "$WORK/reset-reached" ] || fail "Reset did not reach $boundary barrier"
    OP=$(python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
    row=db.execute('select state,reset_state,reset_id from sandboxes where id=?',(sys.argv[2],)).fetchone()
assert row[:2]==('dead','pending'),row
print(row[2])
PY
)
    crash_conductor
    kill -KILL "$(cat "$WORK/reset-reached")"
    rm "$WORK/reset-mode" "$WORK/reset-reached"
    restart_conductor "$boundary"
    wait_sandbox_state "$SID" paused 1200 || fail "pending operation lost on restart"
    python3 - "$WORK/lib/node-ctl.db" "$SID" "$OP" <<'PY'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
    row=db.execute('select reset_id,reset_state,resume_source_kind,resume_sandbox_ref from sandboxes where id=?',(sys.argv[2],)).fetchone()
assert row==(sys.argv[3],'succeeded','sandbox',''),row
PY
    resume_reset
    read_value "automatic-$boundary" automatic
done

# No live callback can report this exit: startup reconciliation must discover it,
# retain current disks and register exactly one durable automatic operation.
OLD_RUN=$(sandbox_run_id "$SID")
crash_conductor
systemctl stop "${RUNNER_PREFIX}${OLD_RUN}.service"
restart_conductor missing-run
wait_sandbox_state "$SID" paused 1200 || fail "missing run did not auto Reset on reconciliation"
resume_reset
read_value automatic-missing-run automatic
assert_deleted

# Fresh Create rollback with complete disks can auto Reset those exact disks.
touch "$WORK/reset-fail-ready"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":600,\"autoReset\":true}")
[ "$code" = 201 ] || fail "formed first-launch acceptance=$code"
SID=$(json_field "$WORK/resp.body" sandboxID)
wait_sandbox_state "$SID" paused 1200 || fail "complete first-launch disks did not auto Reset"
rm "$WORK/reset-fail-ready"
OLD_RUN=$(python3 - "$CHECKPOINT_ROOT/$SID/checkpoint/current-disks.json" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))['run']['run_id'])
PY
)
resume_reset
read_value first-launch-formed formed
assert_deleted

# A first launch that never formed disks must fail once without inventing them.
printf '%s\n' runtime-wire-failure >"$WORK/inject-sandbox-run"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":600,\"autoReset\":true}")
[ "$code" = 201 ] || fail "failed-launch acceptance=$code"
SID=$(json_field "$WORK/resp.body" sandboxID)
wait_sandbox_state "$SID" dead 1200 || fail "first failure did not reach dead"
rm "$WORK/inject-sandbox-run"
failure_record() {
    python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import json,sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
    row=db.execute('select state,dead_unix,reset_id,reset_state,sandbox_result_json,auto_reset_error from sandboxes where id=?',(sys.argv[2],)).fetchone()
assert row[0]=='dead' and row[1]>0 and row[3]=='failed' and row[5],row
# A readiness failure can fence the runner before its execution report arrives.
# Unknown results remain absent; compare the exact original value after restart.
print(json.dumps(row))
PY
}
for _ in $(seq 1 300); do failure_record >"$WORK/failure-before.json" 2>/dev/null && break; sleep 0.05; done
failure_record >"$CASE_OUT/failure-before.json"
crash_conductor
restart_conductor failed-reset
failure_record >"$CASE_OUT/failure-after.json"
cmp "$CASE_OUT/failure-before.json" "$CASE_OUT/failure-after.json"
OLD_RUN=""
delete_checkpoint_guest
echo 'PASS: conductor adoption, manual/automatic Reset, durable replay, failure permanence, checkpoint history, exact runs and parent deletion'
