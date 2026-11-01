#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/cluster_env.sh"

GROUP=/e2e/cluster/reset
ROUTE_KEY=user1/reset
MANIFEST_KEY="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MANIFEST_KEY")"
API_SECRET_FP="$("$BIN/e2b-key-ctl" fingerprint "$API_SECRET")"
BUILD_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
CLUSTER_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ENC_KEY="$("$BIN/e2b-key-ctl" gen-key)"
start_store_zot_vswitch
make_ext4_templates
build_template_with_standalone_node
write_group_record
write_registry_configs 1
write_placer_router_configs
start_cluster_control_plane
NODE_ID=real-reset-node
start_cluster_node "$NODE_ID"
wait_cluster_node_key_pair
PLACER_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/placer_readiness.py" \
    --url "http://127.0.0.1:$PLACER_PORT" --group "$GROUP" --expected-node "$NODE_ID" \
    --route-key "$ROUTE_KEY" --registry-url "http://127.0.0.1:$CONTROL_PORT"

field() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$WORK/router-resp.body" "$1"; }
node_run() {
    python3 - "$WORK/cl/node-ctl.db" "$SID" <<'PY'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
    row=db.execute('select id,run_id from sandboxes where stable_id=?',(sys.argv[2],)).fetchone()
assert row,row
print(*row,sep='\t')
PY
}
wait_state() {
    local state=$1
    for _ in $(seq 1 600); do
        code=$(router_req GET "/sandboxes/$SID" "$CLUSTER_API_KEY" "$ROUTE_KEY" || true)
        if [ "$code" = 200 ] && [ "$(field state)" = "$state" ]; then return; fi
        sleep 0.05
    done
    fail "cluster state did not reach $state"
}
guest() {
    timeout -k 5s 120 "$BIN/sandbox-ctl" exec --proxy "http://127.0.0.1:$ROUTER_PORT" \
        --proxy-header "E2b-Sandbox-Id: $SID" --proxy-header 'E2b-Sandbox-Service: exec' \
        --proxy-header "X-Access-Token: $EXEC_TOKEN" --proxy-header "X-Kuasar-Sandbox-Group: $GROUP" \
        --proxy-header "X-Kuasar-Route-Key: $ROUTE_KEY" -- "$@"
}
write_value() { guest /bin/sh -c "printf '%s' '$1' >/cluster-reset; sync"; }
read_value() {
    guest /bin/cat /cluster-reset >"$CASE_OUT/$1.actual"
    printf '%s' "$2" >"$CASE_OUT/$1.expected"
    cmp "$CASE_OUT/$1.actual" "$CASE_OUT/$1.expected"
}
create_guest() {
    code=$(router_req POST /sandboxes "$CLUSTER_API_KEY" "$ROUTE_KEY" "{\"autoReset\":$1}")
    [ "$code" = 201 ] || fail "cluster Create=$code"
    SID=$(field sandboxID)
    EXEC_TOKEN=$(issue_cluster_exec_session "$SID")
    wait_state running
    python3 - "$WORK/router-resp.body" "$1" <<'PY'
import json,sys
assert json.load(open(sys.argv[1]))['autoReset']==(sys.argv[2]=='true')
PY
}
terminate_guest() {
    IFS=$'\t' read -r NODE_SID OLD_RUN < <(node_run)
    [ -n "$OLD_RUN" ] || fail "missing exact cluster RunID"
    systemctl stop "sandbox-runner@$OLD_RUN.service"
}
reset_and_wake() {
    code=$(router_req POST "/sandboxes/$SID/reset" "$CLUSTER_API_KEY" "$ROUTE_KEY")
    [ "$code" = 204 ] || fail "cluster Reset=$code"
    # Reuse the capability immediately: the first exec performs ordinary cold Wake.
    read_value "$1" "$2"
    local after_id after_run
    IFS=$'\t' read -r after_id after_run < <(node_run)
    [ "$after_id" = "$NODE_SID" ] && [ -n "$after_run" ] && [ "$after_run" != "$OLD_RUN" ] || fail "cluster Reset changed parent or reused RunID"
}
delete_guest() {
    code=$(router_req DELETE "/sandboxes/$SID" "$CLUSTER_API_KEY" "$ROUTE_KEY")
    [ "$code" = 204 ] || fail "cluster Delete=$code"
    wait_node_sandbox_finalized "$NODE_SID" || fail "cluster Delete left exact parent data"
    for _ in $(seq 1 200); do
        code=$(router_req GET "/sandboxes/$SID" "$CLUSTER_API_KEY" "$ROUTE_KEY")
        [ "$code" != 404 ] || return 0
        sleep 0.05
    done
    fail "hard-deleted cluster management location remains"
}

create_guest false
write_value A
code=$(router_req POST "/sandboxes/$SID/pause" "$CLUSTER_API_KEY" "$ROUTE_KEY" '{"memory":false,"checkpoint":true}')
[ "$code" = 200 ] || fail "cluster checkpoint=$code"
CP=$(field checkpointID)
write_value B
terminate_guest
wait_state dead
cp "$WORK/router-resp.body" "$CASE_OUT/dead-before-restart.json"
code=$(router_req POST /sandboxes "$CLUSTER_API_KEY" "$ROUTE_KEY" '{}')
[ "$code" = 409 ] || fail "same route key overwrote dead parent: $code"

# Restart the conductor while dead. A fresh node-link full projection must keep
# management lookup and must not reinterpret retained data as a missing route.
kill -KILL "$CLUSTER_CONDUCTOR_PID"
wait "$CLUSTER_CONDUCTOR_PID" 2>/dev/null || true
for i in "${!PIDS[@]}"; do [ "${PIDS[$i]}" != "$CLUSTER_CONDUCTOR_PID" ] || PIDS[$i]=""; done
"$BIN/node-ctl" conductor serve --config "$WORK/cluster-node.yaml" >>"$WORK/cluster-node.log" 2>&1 &
CLUSTER_CONDUCTOR_PID=$!
PIDS+=("$CLUSTER_CONDUCTOR_PID")
wait_api_health "$NODE_PORT" "restarted cluster conductor"
wait_cluster_node_key_pair
wait_state dead
python3 - "$CASE_OUT/dead-before-restart.json" "$WORK/router-resp.body" <<'PY'
import json,sys
a,b=[json.load(open(p)) for p in sys.argv[1:]]
for key in ('sandboxID','state','deadAt','autoReset','executionResult'): assert a.get(key)==b.get(key),(key,a,b)
PY
code=$(router_req POST "/sandboxes/$SID/export" "$CLUSTER_API_KEY" "$ROUTE_KEY" "{\"checkpointID\":\"$CP\",\"toTemplate\":true}")
[ "$code" = 200 ] || fail "dead checkpoint Export through cluster=$code"
reset_and_wake current-B B

code=$(router_req POST "/sandboxes/$SID/pause" "$CLUSTER_API_KEY" "$ROUTE_KEY" '{"memory":false,"checkpoint":true}')
[ "$code" = 200 ] || fail "second cluster checkpoint=$code"
CP=$(field checkpointID)
read_value checkpoint-wake B
terminate_guest
wait_state dead
code=$(router_req DELETE "/sandboxes/$SID/checkpoints/$CP" "$CLUSTER_API_KEY" "$ROUTE_KEY")
[ "$code" = 204 ] || fail "dead checkpoint Delete through cluster=$code"
delete_guest

create_guest true
write_value automatic
terminate_guest
wait_state paused
reset_and_wake automatic automatic
delete_guest
echo 'PASS: cluster dead management, route-key fence, restart projection, checkpoint paths, Reset/Wake, autoReset and exact parent reclamation'
