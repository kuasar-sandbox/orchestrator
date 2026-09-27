#!/usr/bin/env bash
set -euo pipefail
: "${E2E_LIB:?E2E_LIB must point to prepared helpers}"
. "$E2E_LIB/orchestrator/resource.sh"
resource_init
trap resource_cleanup EXIT

# Capacity describes virtual hardware. The cold startup reservation is 512MiB,
# independent of the 8GiB capacity and 128MiB steady allocation.
resource_write_controller "$WORK/controller-startup.yaml" false
resource_start_controller "$WORK/controller-startup.yaml"
sid=resource-startup
resource_setup_sandbox "$sid"
resource_write_sandbox "$sid" 128 8192 512 false
python3 - "$WORK/$sid.yaml" <<'PY_STARTUP'
from pathlib import Path
import sys
p = Path(sys.argv[1]); s = p.read_text()
s = s.replace('capacity: { cpu: 1,', 'capacity: { cpu: 2,')
s = s.replace('allocatable: { cpu: 1,', 'allocatable: { cpu: 0.1,')
s = s.replace('  exec: /bin/sleep\n  args: ["300"]',
              '  exec: /usr/local/bin/python3\n  args: ["-u", "-c", "import time; print(\'PYBOOT-OK\'); time.sleep(300)"]')
p.write_text(s)
PY_STARTUP
resource_run_sandbox "$sid"
startup_pid=$RESOURCE_LAST_PID
resource_wait_state "$sid" "$startup_pid" settled 120
deadline=$((SECONDS+120))
until grep -q PYBOOT-OK "$WORK/$sid.log"; do
    kill -0 "$startup_pid" || resource_fail "startup guest exited before Python ran"
    [ "$SECONDS" -lt "$deadline" ] || resource_fail "startup guest did not run Python"
    sleep .1
done
cap=$((8*1024*1024*1024)); initial=$((512*1024*1024)); balloon=$((cap-initial))
for pattern in '--cpus boot=2' "memory-zone.*size=$cap" "--balloon size=$balloon" \
    "controller admit: .*initial_reservation=$initial" "initial cold Budget reserved=$initial"; do
    grep -q -- "$pattern" "$WORK/$sid.log" || { cat "$WORK/$sid.log"; resource_fail "missing startup assertion: $pattern"; }
done
resource_shutdown_pid "$startup_pid"
deadline=$((SECONDS+30))
while [ "$(resource_reservation_count)" != 0 ]; do
    [ "$SECONDS" -lt "$deadline" ] || resource_fail "startup reservation survived shutdown"
    sleep .1
done
RESOURCE_SANDBOX_PIDS=()
resource_stop_controller

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
