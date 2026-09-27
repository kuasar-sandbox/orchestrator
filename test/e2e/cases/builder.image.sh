#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/builder_env.sh"
write_builder_config
start_conductor
. "$SCRIPT_DIR/builder_proxy.sh"
start_builder_proxy
# Before any registration, both size=1 entries must independently prewarm.
# Count only this fixture's RunID pidfiles, never deployment units.
idle_builders=0
for _ in $(seq 1 100); do
    idle_builders=$(find "$WORK/run/runners" -maxdepth 1 -name 'br-*.pid' -type f | wc -l)
    [ "$idle_builders" -eq 2 ] && break
    sleep 0.1
done
[ "$idle_builders" -eq 2 ] || fail "duplicate Builder pools prewarmed $idle_builders workers, want 2"
echo "==> PASS: shared-template Builder pools independently prewarmed two workers"
# ---- B1: fromImage → e2b-img -----------------------------------------------
echo "==> B1: fromImage=$NETWORK_PULL_REF (auto image, request DNS, in-guest pull + flatten)"
REQ_NETWORK_HEADER="$BUILD_NETWORK_HEADER"
register e2e-img
B1_TID="$TID"; B1_BID="$BID"
code=$(req POST "/v2/templates/$B1_TID/builds/$B1_BID" "$AK" "{\"fromImage\":\"$NETWORK_PULL_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B1 trigger = $code (want 202)"; }
wait_phase_reservation a "$B1_BID" \
    || fail "B1 phase A never exposed a connected settled nodectl reservation"
wait_ready "$B1_TID" "$B1_BID" B1 null img
B1_PERSIST="$PERSIST"
case "$B1_PERSIST" in e2b-img-*) : ;; *) fail "B1 persist=$B1_PERSIST (want e2b-img-…)";; esac
B1_REF=$(persist_ref "$B1_PERSIST") || fail "B1 persistent id is invalid"
[[ "$B1_REF" == manifest://* ]] || fail "B1 default manifest=false image result is not in Manifest store: $B1_REF"
B1_BEFORE_RETRY=$(build_trigger_signature "$B1_BID")
code=$(req POST "/v2/templates/$B1_TID/builds/$B1_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\",\"force\":true}")
[ "$code" = "409" ] || { cat "$WORK/resp.body"; fail "B1 second trigger = $code (want 409)"; }
assert_trigger_conflict ready || fail "B1 second trigger did not report exact ready state"
[ "$(build_trigger_signature "$B1_BID")" = "$B1_BEFORE_RETRY" ] \
    || fail "B1 second trigger changed the terminal build row"
code=$(req GET /templates "$AK")
[ "$code" = "200" ] || fail "template list after B1 conflict = $code (want 200)"
python3 - "$WORK/resp.body" "$B1_BID" "$B1_PERSIST" <<'PY'
import json, sys
items = json.load(open(sys.argv[1]))
matches = [item for item in items if item.get("buildID") == sys.argv[2] and item.get("templateID") == sys.argv[3]]
assert len(matches) == 1, items
assert matches[0].get("target") is None, matches[0]
assert matches[0].get("kind") == "img", matches[0]
PY
echo "==> PASS: B1 ready → $B1_PERSIST"

# ---- B5: bare profile fromImage → bare-img -------------------------------
echo "==> B5: profile=bare fromImage=$NETWORK_PULL_REF (image-only, request DNS)"
REQ_NETWORK_HEADER="$BUILD_NETWORK_HEADER"
register e2e-bare bare
B5_TID="$TID"; B5_BID="$BID"
code=$(req POST "/v2/templates/$B5_TID/builds/$B5_BID" "$AK" \
    "{\"fromImage\":\"$NETWORK_PULL_REF\",\"startCmd\":\"sleep 60\"}")
[ "$code" = "400" ] || { cat "$WORK/resp.body"; fail "B5 startCmd = $code (want 400)"; }
code=$(req POST "/v2/templates/$B5_TID/builds/$B5_BID" "$AK" "{\"fromImage\":\"$NETWORK_PULL_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B5 trigger = $code (want 202)"; }
wait_ready "$B5_TID" "$B5_BID" B5 null img
B5_PERSIST="$PERSIST"
case "$B5_PERSIST" in bare-img-*) : ;; *) fail "B5 persist=$B5_PERSIST (want bare-img-…)";; esac
echo "==> PASS: B5 ready → $B5_PERSIST (bare profile, auto image, request DNS)"

for terminal_bid in "$B1_BID" "$B5_BID"; do
    wait_build_row_deleted "$terminal_bid" || fail "terminal Build row survived TTL: $terminal_bid"
done
echo "==> create e2b cold sandbox from $B1_PERSIST (image path)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$B1_PERSIST\",\"timeout\":60}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; diag "$B1_BID"; fail "e2b cold create = $code (want 201)"; }
E2B_COLD_SID=$(json_field "$WORK/resp.body" sandboxID)
[ -n "$E2B_COLD_SID" ] || fail "e2b cold create returned no sandboxID"
wait_running "$E2B_COLD_SID" || { diag "$B1_BID"; fail "e2b cold sandbox did not reach running"; }
wait_resource_capacity "$E2B_COLD_SID" "$((2 << 30))" \
    || fail "IMG Create inherited the build node's phase patch instead of target node policy"
code=$(req DELETE "/sandboxes/$E2B_COLD_SID" "$AK"); [ "$code" = "204" ] || fail "e2b cold kill = $code (want 204)"
echo "==> PASS: e2b image cold Create reached running and cleaned up"

echo "==> create bare sandbox from $B5_PERSIST (image cold-boot path)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$B5_PERSIST\",\"timeout\":60}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; diag "$B5_BID"; fail "bare create = $code (want 201)"; }
BARE_SID=$(json_field "$WORK/resp.body" sandboxID)
[ -n "$BARE_SID" ] || fail "bare create returned no sandboxID"
wait_running "$BARE_SID" || { diag "$B5_BID"; fail "bare cold sandbox did not reach running"; }
code=$(req GET /v2/sandboxes "$AK"); [ "$code" = "200" ] || fail "bare list = $code (want 200)"
grep -q "$BARE_SID" "$WORK/resp.body" || fail "created bare sandbox $BARE_SID not in list"
code=$(req DELETE "/sandboxes/$BARE_SID" "$AK"); [ "$code" = "204" ] || fail "bare kill = $code (want 204)"
echo "==> PASS: bare sandbox create → list → kill from bare-img build"

echo "PASS builder.image.sh"
