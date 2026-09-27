#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/builder_env.sh"
write_builder_config
start_conductor
. "$SCRIPT_DIR/builder_proxy.sh"
start_builder_proxy
# ---- #305 publication matrix: parent + false/local, then true/bundle ------
# Keep the database, customer key, Store and API identity stable while the
# conductor is restarted with each node policy. This exercises startup-time
# policy validation and proves that durable TemplateIDs remain sufficient.
restart_conductor local false

# P1: a local Phase-A image must feed top-level E assembly directly. The only
# Manifest root this Build may add is its final Sandbox E; an intermediate IMG
# Manifest would appear as a second key in the exact before/after set.
echo "==> P1: parent + remote.manifest=false, fromImage → top-level Sandbox E"
snapshot_manifest_keys "$WORK/p1-manifests.before"
register e2e-policy-false-sandbox e2b '{"kind":"sandbox","memory":false}' 1
P1_TID="$TID"; P1_BID="$BID"
code=$(req POST "/v2/templates/$P1_TID/builds/$P1_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "P1 trigger = $code (want 202)"; }
wait_ready "$P1_TID" "$P1_BID" P1 '{"kind":"sandbox","memory":false}' sbx
P1_PERSIST="$PERSIST"
P1_REF=$(persist_ref "$P1_PERSIST") || fail "P1 persistent id is invalid"
assert_only_manifest_ref_added "$WORK/p1-manifests.before" "$P1_REF" p1
assert_phase_history "$P1_BID" a - P1
assert_phase_history "$P1_BID" - b P1
assert_phase_history "$P1_BID" - c P1
artifact_info "$P1_REF" "$WORK/p1-sandbox.json" "$WORK/p1-sandbox.err" \
    || { cat "$WORK/p1-sandbox.err"; fail "sandbox-ctl info P1 top-level E"; }
python3 - "$WORK/p1-sandbox.json" <<'PY' || fail "P1 is not a strict self-contained Sandbox E"
import json, sys
config = json.load(open(sys.argv[1]))
root = config["Boot"]["Root"]
assert root["Base"] == "self" and isinstance(root.get("Overlay"), dict), root
assert "manifest://" not in json.dumps(root), root
PY
assert_build_finalized "$P1_BID" P1
echo "==> PASS: P1 added only final manifest://E, with no intermediate IMG Manifest or complete E staging"

# P2 pins checkpoint-class routing independently: the final image is a
# Manifest root, while local mode writes incremental E/S tarstreams to the
# named location. It also executes real Phase C from the published image.
echo "==> P2: parent + remote.manifest=false + local, fromImage → sandbox/memory"
snapshot_manifest_keys "$WORK/p2-manifests.before"
register e2e-policy-false-memory e2b '{"kind":"sandbox","memory":true}' 1
P2_TID="$TID"; P2_BID="$BID"
code=$(req POST "/v2/templates/$P2_TID/builds/$P2_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "P2 trigger = $code (want 202)"; }
wait_ready "$P2_TID" "$P2_BID" P2 '{"kind":"sandbox","memory":true}' snp
P2_PERSIST="$PERSIST"
P2_REF=$(persist_ref "$P2_PERSIST") || fail "P2 persistent id is invalid"
assert_located_final "$P2_REF" .snapshot
artifact_info "$P2_REF" "$WORK/p2-snapshot.json" "$WORK/p2-snapshot.err" \
    || { cat "$WORK/p2-snapshot.err"; fail "sandbox-ctl info P2 Snapshot"; }
P2_SANDBOX_REF=$(json_field "$WORK/p2-snapshot.json" SandboxRef)
assert_located_final "$P2_SANDBOX_REF" .sandbox
P2_IMAGE_REF=$(python3 - "$WORK/p2-snapshot.json" <<'PY'
import json, sys
config = json.load(open(sys.argv[1]))
print(config["Boot"]["Root"]["BaseRef"])
PY
)
assert_only_manifest_ref_added "$WORK/p2-manifests.before" "$P2_IMAGE_REF" p2
assert_phase_history "$P2_BID" a - P2
assert_phase_history "$P2_BID" - b P2
assert_phase_history "$P2_BID" c - P2
assert_build_finalized "$P2_BID" P2
echo "==> PASS: P2 graph is located S → located EΔ → manifest://IMG; local mode emitted role-specific tarstreams"

restart_conductor bundle true

# Under remote.manifest=true, every image-class result is a single-root
# Manifest Bundle in its actual publication location. None of these Builds may
# add a Manifest-store root, including Phase A import and Phase-C boot setup.
echo "==> P3: parent + remote.manifest=true, fromImage → IMG Bundle"
P3_MANIFESTS_BEFORE=$(manifest_count)
register e2e-policy-bundle-image e2b '{"kind":"image"}'
P3_TID="$TID"; P3_BID="$BID"
code=$(req POST "/v2/templates/$P3_TID/builds/$P3_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "P3 trigger = $code (want 202)"; }
wait_ready "$P3_TID" "$P3_BID" P3 '{"kind":"image"}' img
P3_PERSIST="$PERSIST"
P3_REF=$(persist_ref "$P3_PERSIST") || fail "P3 persistent id is invalid"
[ "$(manifest_count)" = "$P3_MANIFESTS_BEFORE" ] \
    || fail "P3 wrote the Manifest store under remote.manifest=true"
assert_bundle_directory_only "$P3_REF" 1
assert_phase_history "$P3_BID" a - P3
assert_phase_history "$P3_BID" - b P3
assert_phase_history "$P3_BID" - c P3
assert_build_finalized "$P3_BID" P3
echo "==> PASS: P3 returned a located image-root Bundle and left the Manifest store unchanged"

echo "==> P4: parent + remote.manifest=true, fromImage → top-level Sandbox E Bundle"
P4_MANIFESTS_BEFORE=$(manifest_count)
register e2e-policy-bundle-sandbox e2b '{"kind":"sandbox","memory":false}' 1
P4_TID="$TID"; P4_BID="$BID"
code=$(req POST "/v2/templates/$P4_TID/builds/$P4_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "P4 trigger = $code (want 202)"; }
wait_ready "$P4_TID" "$P4_BID" P4 '{"kind":"sandbox","memory":false}' sbx
P4_PERSIST="$PERSIST"
P4_REF=$(persist_ref "$P4_PERSIST") || fail "P4 persistent id is invalid"
[ "$(manifest_count)" = "$P4_MANIFESTS_BEFORE" ] \
    || fail "P4 wrote an IMG or E root to the Manifest store"
assert_bundle_directory_only "$P4_REF" 1
artifact_info "$P4_REF" "$WORK/p4-sandbox.json" "$WORK/p4-sandbox.err" \
    || { cat "$WORK/p4-sandbox.err"; fail "sandbox-ctl info P4 Sandbox Bundle"; }
python3 - "$WORK/p4-sandbox.json" <<'PY' || fail "P4 Bundle root is not a self-contained Sandbox E"
import json, sys
config = json.load(open(sys.argv[1]))
root = config["Boot"]["Root"]
assert root["Base"] == "self" and isinstance(root.get("Overlay"), dict), root
assert "manifest://" not in json.dumps(root), root
PY
assert_phase_history "$P4_BID" a - P4
assert_phase_history "$P4_BID" - b P4
assert_phase_history "$P4_BID" - c P4
assert_build_finalized "$P4_BID" P4
echo "==> PASS: P4 directly assembled and published a sandbox-root Bundle, with no IMG Manifest or complete E staging"

echo "==> P5: parent + remote.manifest=true + bundle, fromImage → sandbox/memory"
P5_MANIFESTS_BEFORE=$(manifest_count)
register e2e-policy-bundle-memory e2b '{"kind":"sandbox","memory":true}' 1
P5_TID="$TID"; P5_BID="$BID"
code=$(req POST "/v2/templates/$P5_TID/builds/$P5_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "P5 trigger = $code (want 202)"; }
wait_ready "$P5_TID" "$P5_BID" P5 '{"kind":"sandbox","memory":true}' snp
P5_PERSIST="$PERSIST"
P5_REF=$(persist_ref "$P5_PERSIST") || fail "P5 persistent id is invalid"
[ "$(manifest_count)" = "$P5_MANIFESTS_BEFORE" ] \
    || fail "P5 wrote image/checkpoint roots to the Manifest store"
assert_located_final "$P5_REF" .bundle
artifact_info "$P5_REF" "$WORK/p5-snapshot.json" "$WORK/p5-snapshot.err" \
    || { cat "$WORK/p5-snapshot.err"; fail "sandbox-ctl info P5 Snapshot Bundle"; }
P5_IMAGE_REF=$(python3 - "$WORK/p5-snapshot.json" <<'PY_REF'
import json, sys
config = json.load(open(sys.argv[1]))
print(config["Boot"]["Root"]["BaseRef"])
PY_REF
)
assert_located_final "$P5_IMAGE_REF" .bundle
P5_SNAPSHOT_DIR=$(dirname "$(located_file_path "$P5_REF")")
P5_IMAGE_DIR=$(dirname "$(located_file_path "$P5_IMAGE_REF")")
[ "$P5_SNAPSHOT_DIR" = "$P5_IMAGE_DIR" ] \
    || fail "P5 image and checkpoint publications split across directories: $P5_IMAGE_DIR != $P5_SNAPSHOT_DIR"
assert_bundle_directory_only "$P5_REF" 2
assert_phase_history "$P5_BID" a - P5
assert_phase_history "$P5_BID" - b P5
assert_phase_history "$P5_BID" c - P5
assert_build_finalized "$P5_BID" P5
echo "==> PASS: P5 graph is Bundle S → EΔ → located IMG Bundle; Phase C reopened the portable image and Store count stayed fixed"

# The deterministic publication tests retain matrix-level coverage. P5 keeps one
# bounded real memory+Bundle path across capture, publication and restore.

# The opaque TemplateIDs, not terminal Build rows, remain the authority. P3
# proves a located image-root Bundle can cold boot; P5 proves Snapshot Bundle
# restore follows its external located image dependency.
for terminal_bid in "$P3_BID" "$P4_BID" "$P5_BID"; do
    wait_build_row_deleted "$terminal_bid" \
        || fail "policy Build row $terminal_bid survived builder.terminal_ttl"
done
echo "==> create cold sandbox from $P3_PERSIST (located IMG Bundle)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$P3_PERSIST\",\"timeout\":60}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; diag "$P3_BID"; fail "P3 Create = $code (want 201)"; }
P3_SID=$(json_field "$WORK/resp.body" sandboxID)
wait_running "$P3_SID" || { diag "$P3_BID"; fail "P3 located image sandbox did not reach running"; }
code=$(req DELETE "/sandboxes/$P3_SID" "$AK"); [ "$code" = "204" ] || fail "P3 kill = $code (want 204)"

echo "==> restore sandbox from $P5_PERSIST (Snapshot Bundle + external IMG Bundle)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$P5_PERSIST\",\"timeout\":60}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; diag "$P5_BID"; fail "P5 Create = $code (want 201)"; }
P5_SID=$(json_field "$WORK/resp.body" sandboxID)
wait_running "$P5_SID" || { diag "$P5_BID"; fail "P5 Snapshot Bundle sandbox did not reach running"; }
wait_resource_capacity "$P5_SID" "$((3 << 30))" \
    || fail "P5 Snapshot Bundle Create did not preserve target capacity"
code=$(req DELETE "/sandboxes/$P5_SID" "$AK"); [ "$code" = "204" ] || fail "P5 kill = $code (want 204)"
echo "==> PASS: located IMG and Snapshot Bundle TemplateIDs created real sandboxes after Build-row TTL"

# store actually holds the uploaded chunks/manifests
objs=$(find "$WORK/store" -type f | wc -l)
[ "$objs" -gt 0 ] || fail "store has no objects after the builds"
echo "==> store holds $objs object(s)"

echo "PASS builder.publish.sh"
