#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
# A real current capture with only its runtime annotation omitted exercises the
# legacy unannotated-reference path. It does not claim old-producer provenance.
set -euo pipefail
export CLUSTER_USE_PLACER=0
. "${E2E_LIB:?}/orchestrator/cluster_env.sh"
GROUP=/e2e/cluster/legacy-runtime
ROUTE_KEY=legacy/capture
MANIFEST_KEY="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MANIFEST_KEY")"
API_SECRET_FP="$("$BIN/e2b-key-ctl" fingerprint "$API_SECRET")"
BUILD_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
CLUSTER_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ENC_KEY="$("$BIN/e2b-key-ctl" gen-key)"
start_store_zot_vswitch
make_ext4_templates
build_template_with_standalone_node
case "$TEMPLATE_REF" in e2b-img-*) ;; *) fail "expected image template for configured-runtime capture" ;; esac
write_registry_configs 1
sed -i 's/park_timeout: "180s"/park_timeout: "12s"/' "$WORK/registry-1.yaml"
write_placer_router_configs
start_cluster_control_plane
mkdir -p "$WORK/capture-runtime" "$WORK/incompatible-runtime" "$WORK/legacy-capture"
CLUSTER_NODE_RUNTIME="$WORK/capture-runtime/runtime-v1.bundle"
cp --reflink=auto "$BIN/sandbox-runtime.bundle" "$CLUSTER_NODE_RUNTIME"
python3 "$SCRIPT_DIR/runtime_variant.py" "$CLUSTER_NODE_RUNTIME" "$WORK/incompatible-runtime/runtime-v2.bundle"
NODE_ID=legacy-incompatible-node
start_cluster_node "$NODE_ID"
install_cluster_group
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" \
    --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --node-id "$NODE_ID" \
    --group "$GROUP" --registry-url "http://127.0.0.1:$CONTROL_PORT" --fence-file "$WORK/legacy-before-restart.fence"
code=$(create_sandbox "$WORK/legacy-create.secret")
[ "$code" = 201 ] || fail "legacy fixture source Create"
SID=$(json_field "$WORK/legacy-create.secret" sandboxID)
EXEC_TOKEN=$(issue_cluster_exec_session "$SID") || fail "legacy source exec capability"
exec_through_cluster_connect "$SID" "$EXEC_TOKEN" legacy-capture-ready
unset EXEC_TOKEN
LOCAL_SID=$(node_sandbox_id "$SID") || fail "legacy capture source local identity"
"$BIN/sandbox-ctl" snapshot --json --path-id "$LOCAL_SID" --run-root "$WORK/cr/sandboxes" \
    --output "$WORK/legacy-capture" --timeout 120 >"$WORK/legacy-capture.private" 2>"$WORK/legacy-capture.log" \
    || fail "actual legacy fixture capture"
code=$(router_req DELETE "/sandboxes/$SID" "$CLUSTER_API_KEY" "$ROUTE_KEY")
[ "$code" = 204 ] || fail "delete captured source"
wait_node_sandbox_finalized "$LOCAL_SID" || fail "source cleanup after capture"
# Preserve all actual checkpoint bytes and metadata, changing only the external
# reference syntax. The Node must reach runtime validation on this real SNP.
TEMPLATE_REF=$(python3 - "$WORK/legacy-capture.private" <<'PY'
import base64,json,sys
ref=json.load(open(sys.argv[1]))['snapshotRef']
assert ref.startswith('file://') and '@runtime:runtime-v1.bundle:digest:' in ref
ref=ref.split('@runtime:',1)[0]
assert '@location:' not in ref
ref+='@location:legacy'
print('e2b-snp-'+base64.urlsafe_b64encode(ref.encode()).decode().rstrip('='))
PY
)
install_cluster_group
kill "$CLUSTER_CONDUCTOR_PID"
wait "$CLUSTER_CONDUCTOR_PID" || true
for i in "${!PIDS[@]}"; do [ "${PIDS[$i]}" != "$CLUSTER_CONDUCTOR_PID" ] || PIDS[$i]=""; done
python3 - "$WORK" <<'PY'
import hashlib,pathlib,sys
root=pathlib.Path(sys.argv[1]);digest=hashlib.sha256(b'legacy').hexdigest()
target=root/'legacy-locations'/digest[:2]/digest[2:4]/'legacy'
target.parent.mkdir(parents=True)
target.symlink_to(root/'legacy-capture',target_is_directory=True)
p=root/'cluster-node.yaml';s=p.read_text()
s=s.replace(str(root/'capture-runtime/runtime-v1.bundle'),str(root/'incompatible-runtime/runtime-v2.bundle'))
s=s.replace('checkpoint: { mode: local }','checkpoint: { mode: local, remote: { ref_location_parent: "file://'+str(root/'legacy-locations')+'" } }')
p.write_text(s)
PY
"$BIN/node-ctl" conductor serve --config "$WORK/cluster-node.yaml" >>"$WORK/cluster-node.log" 2>&1 &
CLUSTER_CONDUCTOR_PID=$!
PIDS+=("$CLUSTER_CONDUCTOR_PID")
wait_api_health "$NODE_PORT" "incompatible runtime Node"
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" \
    --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --node-id "$NODE_ID" \
    --group "$GROUP" --registry-url "http://127.0.0.1:$CONTROL_PORT" --after-fence "$(cat "$WORK/legacy-before-restart.fence")"
export LEGACY_TEST_WORK="$WORK" LEGACY_TEST_OUT="$CASE_OUT" LEGACY_TEST_GROUP="$GROUP"
export LEGACY_TEST_API_KEY="$CLUSTER_API_KEY" LEGACY_TEST_TEMPLATE="$TEMPLATE_REF" LEGACY_TEST_ROUTE=legacy/runtime-incompatible
export LEGACY_TEST_ROUTER="http://127.0.0.1:$ROUTER_PORT" LEGACY_TEST_HOST="api.$DOMAIN"
export LEGACY_TEST_REGISTRY="http://127.0.0.1:$CONTROL_PORT" LEGACY_TEST_NODE_ID="$NODE_ID"
python3 "$SCRIPT_DIR/cluster_legacy_runtime.py" >"$WORK/cluster-legacy-runtime.log" 2>&1
echo 'PASS: actual unannotated SNP reached Node runtime validation and rejected an incompatible valid bundle'
