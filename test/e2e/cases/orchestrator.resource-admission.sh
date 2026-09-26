#!/usr/bin/env bash
set -euo pipefail
: "${E2E_LIB:?E2E_LIB must point to prepared helpers}"
. "$E2E_LIB/orchestrator/resource.sh"
resource_init
trap resource_cleanup EXIT

resource_write_controller "$WORK/controller.yaml" true
resource_start_controller "$WORK/controller.yaml"

for i in 1 2 3 4 5; do
    sid="resource-admission-$i"
    resource_setup_sandbox "$sid"
    resource_write_sandbox "$sid" 64 256 64 true
done

for i in 1 2 3 4; do
    resource_run_sandbox "resource-admission-$i"
done
resource_wait_reservations 4 10

set +e
timeout -k 10s 15 "$BIN/sandbox-ctl" run     --config "$WORK/resource-admission-5.yaml"     --sandbox-id resource-admission-5     --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/run"     >"$WORK/resource-admission-5.log" 2>&1
rc=$?
set -e
[ "$rc" -ne 0 ] || resource_fail "fifth sandbox was admitted despite red-zone budget"
grep -qiE 'rejected|node in zone|headroom|insufficient' "$WORK/resource-admission-5.log"     || resource_fail "admission rejection omitted its resource reason"

echo "PASS orchestrator.resource-admission.sh"
