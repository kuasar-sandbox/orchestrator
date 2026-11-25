#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/cluster_env.sh"
GROUP=/e2e/cluster/migration
ROUTE_KEY=migration/stable
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
NODE_ID=migration-source
start_cluster_node "$NODE_ID"
SOURCE_PID=$CLUSTER_CONDUCTOR_PID
SOURCE_PORT=$NODE_PORT
wait_cluster_node_key_pair
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --node-id "$NODE_ID" --group "$GROUP" --registry-url "http://127.0.0.1:$CONTROL_PORT"
code=$(create_sandbox "$WORK/migration-create.secret")
[ "$code" = 201 ] || fail "migration source Create=$code"
SID=$(json_field "$WORK/migration-create.secret" sandboxID)
EXEC_TOKEN=$(issue_cluster_exec_session "$SID")
guest() {
  timeout -k 5s 90 "$BIN/sandbox-ctl" exec --proxy "http://127.0.0.1:$ROUTER_PORT" \
    --proxy-header "E2b-Sandbox-Id: $SID" --proxy-header 'E2b-Sandbox-Service: exec' \
    --proxy-header "X-Access-Token: $EXEC_TOKEN" --proxy-header "X-Kuasar-Sandbox-Group: $GROUP" \
    --proxy-header "X-Kuasar-Route-Key: $ROUTE_KEY" -- "$@"
}
guest /bin/sh -c 'printf migration-guest-marker >/migration-marker; sync'
code=$(router_req POST "/sandboxes/$SID/pause" "$CLUSTER_API_KEY" "$ROUTE_KEY" '{"memory":true}')
[ "$code" = 204 ] || fail "migration Snapshot pause=$code"
code=$(router_req POST "/sandboxes/$SID/export" "$CLUSTER_API_KEY" "$ROUTE_KEY" '{"keepSource":true}')
[ "$code" = 200 ] || fail "migration export=$code"
cp "$WORK/router-resp.body" "$WORK/migration-export.secret"
chmod 600 "$WORK/migration-export.secret"
MAIN_WORK=$WORK
WORK=$MAIN_WORK/target
mkdir -p "$WORK/cr" "$WORK/cl"
cp "$MAIN_WORK/manifest.yaml" "$WORK/manifest.yaml"
NODE_ID=migration-target
TARGET_RUNNER="sandbox-runner-migration-${BASHPID}@.service"
TARGET_BUILDER="sandbox-builder-migration-${BASHPID}@.service"
for unit in "$TARGET_RUNNER" "$TARGET_BUILDER"; do
  [ ! -e "$UNIT_DIR/$unit" ] || fail "target unit already exists"
  OURS+=("$UNIT_DIR/$unit")
done
cluster_extra_cleanup() {
  WORK=$MAIN_WORK
  stop_cluster_extra_node_units "$MAIN_WORK/target" "$TARGET_RUNNER" "$TARGET_BUILDER"
}
start_cluster_node "$NODE_ID" "$TARGET_RUNNER" "$TARGET_BUILDER"
TARGET_PORT=$NODE_PORT
wait_cluster_node_key_pair
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --node-id "$NODE_ID" --group "$GROUP" --registry-url "http://127.0.0.1:$CONTROL_PORT"
WORK=$MAIN_WORK
export MIGRATION_NODE_ID="$NODE_ID" MIGRATION_REGISTRY="http://127.0.0.1:$CONTROL_PORT"
export MIGRATION_WORK="$WORK" MIGRATION_OUT="$CASE_OUT" MIGRATION_SID="$SID" MIGRATION_API_KEY="$CLUSTER_API_KEY" MIGRATION_GROUP="$GROUP" MIGRATION_ROUTE="$ROUTE_KEY" MIGRATION_DOMAIN="$DOMAIN" MIGRATION_ROUTER_PORT="$ROUTER_PORT" MIGRATION_NODE_PORT="$TARGET_PORT" MIGRATION_SOURCE_PID="$SOURCE_PID"
python3 "${MIGRATION_CLIENT:-$SCRIPT_DIR/cluster_migration.py}" >"$WORK/cluster-migration.log" 2>&1
guest /bin/cat /migration-marker > "$CASE_OUT/migration-marker.actual"
test "$(cat "$CASE_OUT/migration-marker.actual")" = migration-guest-marker || fail "migration lost actual guest data"
# Return the original Node with its old paused row and old local identity.
"$BIN/node-ctl" conductor serve --config "$WORK/cluster-node.yaml" >>"$WORK/cluster-node.log" 2>&1 &
PIDS+=("$!")
wait_api_health "$SOURCE_PORT" returned-source
python3 - "$WORK" "$SID" "$CASE_OUT/migration-results.json" <<'PY'
import json,pathlib,sqlite3,sys,time
root=pathlib.Path(sys.argv[1]);sid=sys.argv[2]
def rows(path):
 with sqlite3.connect(path) as db:
  db.row_factory=sqlite3.Row
  return [dict(r) for r in db.execute('select id,stable_id,run_id,state from sandboxes where stable_id=?',(sid,))]
for _ in range(600):
 if not rows(root/'cl/node-ctl.db'):break
 time.sleep(.1)
else:raise AssertionError('late source row not reclaimed')
target=rows(root/'target/cl/node-ctl.db')
assert len(target)==1 and target[0]['state']=='running', target
path=pathlib.Path(sys.argv[3]);result=json.loads(path.read_text());result['late_source_cleaned']=True;result['target_after_cleanup']=target[0];path.write_text(json.dumps(result,indent=2)+'\n')
PY
guest /bin/cat /migration-marker > "$CASE_OUT/migration-marker-after-cleanup.actual"
cmp "$CASE_OUT/migration-marker.actual" "$CASE_OUT/migration-marker-after-cleanup.actual"
WORK=$MAIN_WORK/target
TARGET_LOCAL_SID=$(node_sandbox_id "$SID") || fail "missing target local identity before Delete"
WORK=$MAIN_WORK
code=$(router_req DELETE "/sandboxes/$SID" "$CLUSTER_API_KEY" "$ROUTE_KEY")
[ "$code" = 204 ] || fail "migration target Delete=$code"
WORK=$MAIN_WORK/target
wait_node_sandbox_finalized "$TARGET_LOCAL_SID" || fail "migration target not finalized"
WORK=$MAIN_WORK
rm -f "$WORK/migration-create.secret" "$WORK/migration-export.secret"
echo 'PASS: opaque Snapshot token migration, stable public identity/credentials, new local generation and late-source cleanup'
