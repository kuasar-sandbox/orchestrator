#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/builder_env.sh"
write_builder_config
start_conductor
. "$SCRIPT_DIR/builder_proxy.sh"
start_builder_proxy
# This prerequisite is built through the real API from the prepared image.
register source-image
SOURCE_TID="$TID"; SOURCE_BID="$BID"
code=$(req POST "/v2/templates/$SOURCE_TID/builds/$SOURCE_BID" "$AK" "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = 202 ] || { cat "$WORK/resp.body"; fail "source image trigger=$code"; }
wait_ready "$SOURCE_TID" "$SOURCE_BID" source-image null img
SOURCE_IMAGE="$PERSIST"
# ---- B7: explicit top-level Sandbox E from Image → no phase VM ------------
echo "==> B7: fromTemplate=$SOURCE_IMAGE (IMG), explicit sandbox memory=false"
register e2e-sandbox-e e2b '{"kind":"sandbox","memory":false}' 1
B7_TID="$TID"; B7_BID="$BID"
code=$(req POST "/v2/templates/$B7_TID/builds/$B7_BID" "$AK" \
    "{\"fromTemplate\":\"$SOURCE_IMAGE\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B7 trigger = $code (want 202)"; }
wait_ready "$B7_TID" "$B7_BID" B7 '{"kind":"sandbox","memory":false}' sbx
B7_PERSIST="$PERSIST"
case "$B7_PERSIST" in e2b-sbx-*) : ;; *) fail "B7 persist=$B7_PERSIST (want e2b-sbx-…)";; esac
assert_phase_history "$B7_BID" - a B7
assert_phase_history "$B7_BID" - b B7
assert_phase_history "$B7_BID" - c B7
B7_REF=$(persist_ref "$B7_PERSIST") || fail "B7 persistent id is invalid"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$B7_REF" >"$WORK/b7-sandbox.json" 2>"$WORK/b7-sandbox.err" \
    || { cat "$WORK/b7-sandbox.err"; fail "sandbox-ctl info B7 Sandbox"; }
python3 - "$WORK/b7-sandbox.json" <<'PY' \
    || fail "B7 top-level Sandbox E lost registered Create configuration"
import json, sys
config = json.load(open(sys.argv[1]))
assert config["Resources"]["Capacity"] == {"CPU": 2, "Memory": "3GiB"}, config["Resources"]
assert config["Launch"]["Env"]["BUILD_TARGET_ENV"] == "portable-e2e", config["Launch"]
root = config["Boot"]["Root"]
assert root["Base"] == "self" and isinstance(root.get("Overlay"), dict), root
assert "manifest://" not in json.dumps(root), root
metadata = config.get("Metadata") or {}
assert "e2b.start_cmd" not in metadata and "e2b.ready_cmd" not in metadata, metadata
PY
echo "==> PASS: B7 direct top-level Sandbox E assembly preserved target resources/env and started no A/B/C VM"

# ---- B9: synchronous rejection, then source-dependent auto Image ----------
# B7 has no command defaults. An allocatable override larger than its capacity
# must be ignored once auto resolves to Image, including conductor preparation.
REQ_BUILDER_HEADER="{\"resources\":{\"cpu\":$BUILDER_CPU,\"memory\":\"6GiB\",\"storage\":\"4GiB\"}}"
code=$(req POST /v3/templates "$AK" '{"profile":"bare","envVars":{"IGNORED_AUTO_ENV":"not-an-image-default"}}')
[ "$code" = "400" ] || { cat "$WORK/resp.body"; fail "bare auto env registration = $code (want 400)"; }
REQ_RESOURCE_HEADER='{"allocatable":{"memory":"512GiB"}}'
code=$(req POST /v3/templates "$AK" '{"name":"e2e-auto-options","profile":"e2b","envVars":{"IGNORED_AUTO_ENV":"not-an-image-default"},"secure":true}')
unset REQ_BUILDER_HEADER REQ_RESOURCE_HEADER
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B9 registration = $code (want 202)"; }
B9_TID=$(json_field "$WORK/resp.body" templateID)
B9_BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$B9_TID/builds/$B9_BID" "$AK" "{\"fromTemplate\":\"$SOURCE_IMAGE\"}")
[ "$code" = "400" ] || { cat "$WORK/resp.body"; fail "B9 known Image trigger = $code (want 400)"; }
[ ! -e "$WORK/run/builds/$B9_BID" ] || fail "B9 rejected Trigger started execution"
# Retry the same registration with an E source whose commands are task-local.
code=$(req POST "/v2/templates/$B9_TID/builds/$B9_BID" "$AK" "{\"fromTemplate\":\"$B7_PERSIST\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B9 source-dependent Trigger = $code (want 202)"; }
wait_ready "$B9_TID" "$B9_BID" B9 null img
B9_PERSIST="$PERSIST"
assert_phase_history "$B9_BID" b c B9
B9_REF=$(persist_ref "$B9_PERSIST") || fail "B9 persistent id is invalid"
MANIFEST_KEY="$MK" "$BIN/flatten-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$B9_REF" >"$WORK/b9-image.json" 2>"$WORK/b9-image.err" \
    || { cat "$WORK/b9-image.err"; fail "flatten-ctl info B9 image"; }
python3 - "$WORK/b9-image.json" <<'PY' || fail "B9 injected unsupported registered env into Image"
import json, sys
assert "IGNORED_AUTO_ENV" not in json.dumps(json.load(open(sys.argv[1])))
PY
echo "==> PASS: B9 rejected known Image synchronously; E-source auto ignored incompatible options and skipped C"

# ---- B8: SBX source + explicit memory Sandbox, no commands -----------------
echo "==> B8: fromTemplate=$B7_PERSIST (SBX), explicit sandbox memory=true, no steps/start/ready"
register e2e-memory e2b '{"kind":"sandbox","memory":true}' 1
B8_TID="$TID"; B8_BID="$BID"
B8_STARTED=$(date +%s)
code=$(req POST "/v2/templates/$B8_TID/builds/$B8_BID" "$AK" \
    "{\"fromTemplate\":\"$B7_PERSIST\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B8 trigger = $code (want 202)"; }
wait_ready "$B8_TID" "$B8_BID" B8 '{"kind":"sandbox","memory":true}' snp
B8_ELAPSED=$(( $(date +%s) - B8_STARTED ))
[ "$B8_ELAPSED" -ge 20 ] || fail "B8 completed in ${B8_ELAPSED}s; fixed no-ready wait was skipped"
B8_PERSIST="$PERSIST"
case "$B8_PERSIST" in e2b-snp-*) : ;; *) fail "B8 persist=$B8_PERSIST (want e2b-snp-…)";; esac
assert_phase_history "$B8_BID" b a B8
assert_phase_history "$B8_BID" c a B8
B8_REF=$(persist_ref "$B8_PERSIST") || fail "B8 persistent id is invalid"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$B8_REF" >"$WORK/b8-snapshot.json" 2>"$WORK/b8-snapshot.err" \
    || { cat "$WORK/b8-snapshot.err"; fail "sandbox-ctl info B8 Snapshot"; }
# Snapshot info is intentionally a narrow compatibility view. Follow its new
# SandboxRef and inspect E for the complete shared cold/portable configuration.
B8_SANDBOX_REF=$(json_field "$WORK/b8-snapshot.json" SandboxRef)
[ -n "$B8_SANDBOX_REF" ] && [ "$B8_SANDBOX_REF" != "$B7_REF" ] \
    || fail "B8 Snapshot did not point at a newly captured Sandbox E"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$B8_SANDBOX_REF" >"$WORK/b8-sandbox.json" 2>"$WORK/b8-sandbox.err" \
    || { cat "$WORK/b8-sandbox.err"; fail "sandbox-ctl info B8 Sandbox"; }
python3 - "$WORK/b8-snapshot.json" "$WORK/b8-sandbox.json" "$B7_REF" <<'PY' \
    || fail "B8 Snapshot retained source E or lost shared cold configuration"
import json, sys
snapshot = json.load(open(sys.argv[1]))
config = json.load(open(sys.argv[2]))
encoded = json.dumps([snapshot, config], sort_keys=True)
assert sys.argv[3] not in encoded, encoded
assert config["Resources"]["Capacity"] == {"CPU": 2, "Memory": "3GiB"}, config["Resources"]
assert config["Launch"]["Env"]["BUILD_TARGET_ENV"] == "portable-e2e", config["Launch"]
metadata = config.get("Metadata") or {}
assert "e2b.start_cmd" not in metadata and "e2b.ready_cmd" not in metadata, metadata
PY
echo "==> PASS: B8 SBX source forced B, cold C captured memory after fixed wait, and final S→E excludes source E"

# ---- canonical Create after retention-bounded Build rows are reaped --------
for terminal_bid in "$B7_BID" "$B8_BID" "$B9_BID"; do
    wait_build_row_deleted "$terminal_bid" \
        || fail "terminal Build row $terminal_bid survived builder.terminal_ttl"
done
code=$(req GET "/templates/$B8_TID/builds/$B8_BID/status" "$AK")
[ "$code" = "404" ] || fail "B8 status after terminal TTL = $code (want 404)"
echo "==> PASS: terminal Build rows expired; canonical artifacts remain the sole Create authority"

echo "==> create sandbox from $B8_PERSIST (snapshot restore path)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$B8_PERSIST\",\"timeout\":60}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; diag "$B8_BID"; fail "create = $code (want 201)"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
[ -n "$SID" ] || fail "create returned no sandboxID"
wait_running "$SID" || { diag "$B8_BID"; fail "snapshot-template sandbox did not reach running"; }
wait_resource_capacity "$SID" "$((3 << 30))" \
    || fail "snapshot Create did not preserve the phase-C snapshot capacity"
code=$(req GET /v2/sandboxes "$AK"); [ "$code" = "200" ] || fail "list = $code (want 200)"
grep -q "$SID" "$WORK/resp.body" || fail "created sandbox $SID not in list"
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill = $code (want 204)"
echo "==> PASS: sandbox create → list → kill from the built template"

echo "==> create e2b cold sandbox from $B7_PERSIST (top-level Sandbox E path)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$B7_PERSIST\",\"timeout\":60}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; diag "$B7_BID"; fail "top-level Sandbox E create = $code (want 201)"; }
SANDBOX_E_SID=$(json_field "$WORK/resp.body" sandboxID)
[ -n "$SANDBOX_E_SID" ] || fail "top-level Sandbox E create returned no sandboxID"
wait_running "$SANDBOX_E_SID" || { diag "$B7_BID"; fail "top-level Sandbox E did not reach running"; }
wait_resource_capacity "$SANDBOX_E_SID" "$((3 << 30))" \
    || fail "top-level Sandbox E Create did not preserve target capacity"
code=$(req DELETE "/sandboxes/$SANDBOX_E_SID" "$AK"); [ "$code" = "204" ] || fail "top-level Sandbox E kill = $code (want 204)"
echo "==> PASS: top-level Sandbox E canonical Create reached running and cleaned up after Build-row TTL"

echo "PASS builder.sandbox.sh"
