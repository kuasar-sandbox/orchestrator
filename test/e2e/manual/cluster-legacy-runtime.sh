#!/usr/bin/env bash
# Manual C02: an unannotated legacy SNP passes cluster selection and is rejected
# by the Node's existing runtime validation when no local candidate matches.
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/cluster_env.sh"
: "${LEGACY_CAPTURE_JSON:?actual legacy capture response}"
: "${LEGACY_LOCATION_PARENT:?parent containing the legacy artifact directory}"
: "${LEGACY_LOCATION:?legacy artifact directory name}"
: "${INCOMPATIBLE_RUNTIME:?valid bundle with the wrong runtime identity}"
GROUP=/e2e/cluster/legacy-runtime
ROUTE_KEY=legacy/runtime-incompatible
MANIFEST_KEY="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MANIFEST_KEY")"
API_SECRET_FP="$("$BIN/e2b-key-ctl" fingerprint "$API_SECRET")"
BUILD_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
CLUSTER_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ENC_KEY="$("$BIN/e2b-key-ctl" gen-key)"
TEMPLATE_REF="$(python3 - "$LEGACY_CAPTURE_JSON" "$LEGACY_LOCATION" <<'PY'
import base64,json,pathlib,sys
ref=json.loads(pathlib.Path(sys.argv[1]).read_text())['snapshotRef']
assert '@runtime:' not in ref and '@location:' not in ref and ref.startswith('file://')
ref += '@location:' + sys.argv[2]
assert len(ref.encode()) <= 256
print('e2b-snp-'+base64.urlsafe_b64encode(ref.encode()).decode().rstrip('='))
PY
)"
start_store_zot_vswitch
make_ext4_templates
write_group_record
write_registry_configs 1
sed -i 's/park_timeout: "180s"/park_timeout: "12s"/' "$WORK/registry-1.yaml"
write_placer_router_configs
start_cluster_control_plane
NODE_ID=legacy-incompatible-node
start_cluster_node "$NODE_ID"
wait_cluster_node_key_pair
# Restart this task's conductor with the wrong runtime in an isolated directory.
# The captured runtime's basename must be absent beside it, preventing fallback.
kill "$CLUSTER_CONDUCTOR_PID"
wait "$CLUSTER_CONDUCTOR_PID" || true
mkdir "$WORK/incompatible-runtime"
ln -s "$INCOMPATIBLE_RUNTIME" "$WORK/incompatible-runtime/runtime-v2.bundle"
python3 - "$WORK/legacy-locations" "$LEGACY_LOCATION_PARENT/$LEGACY_LOCATION" "$LEGACY_LOCATION" <<'PY'
import hashlib,pathlib,sys
root=pathlib.Path(sys.argv[1]);name=sys.argv[3]
digest=hashlib.sha256(name.encode()).hexdigest()
target=root/digest[:2]/digest[2:4]/name
target.parent.mkdir(parents=True)
target.symlink_to(pathlib.Path(sys.argv[2]),target_is_directory=True)
PY
LEGACY_LOCATION_PARENT="$WORK/legacy-locations"
python3 - "$WORK/cluster-node.yaml" "$BIN/sandbox-runtime.bundle" "$WORK/incompatible-runtime/runtime-v2.bundle" "$LEGACY_LOCATION_PARENT" <<'PY'
import pathlib,sys
p=pathlib.Path(sys.argv[1]);s=p.read_text()
old='runtime: '+sys.argv[2];assert old in s
s=s.replace(old,'runtime: '+sys.argv[3])
old='checkpoint: { mode: local }';assert old in s
s=s.replace(old,'checkpoint: { mode: local, remote: { ref_location_parent: "file://'+sys.argv[4]+'" } }')
p.write_text(s)
PY
"$BIN/node-ctl" conductor serve --config "$WORK/cluster-node.yaml" >>"$WORK/cluster-node.log" 2>&1 &
CLUSTER_CONDUCTOR_PID=$!
PIDS+=("$CLUSTER_CONDUCTOR_PID")
wait_api_health "$NODE_PORT" "incompatible runtime Node"
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --group "$GROUP" --registry-url "http://127.0.0.1:$CONTROL_PORT"
export LEGACY_TEST_WORK="$WORK" LEGACY_TEST_OUT="$CASE_OUT" LEGACY_TEST_GROUP="$GROUP"
export LEGACY_TEST_API_KEY="$CLUSTER_API_KEY" LEGACY_TEST_TEMPLATE="$TEMPLATE_REF" LEGACY_TEST_ROUTE="$ROUTE_KEY"
export LEGACY_TEST_ROUTER="http://127.0.0.1:$ROUTER_PORT" LEGACY_TEST_HOST="api.$DOMAIN"
python3 "${LEGACY_RUNTIME_CLIENT:-$SCRIPT_DIR/cluster_legacy_runtime.py}"
echo 'PASS C02: valid unannotated SNP reached the selected Node; existing runtime validation rejected incompatible local candidates'
