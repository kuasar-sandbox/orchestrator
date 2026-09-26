#!/usr/bin/env bash
set -euo pipefail
: "${E2E_LIB:?}"
. "$E2E_LIB/orchestrator/resource.sh"
resource_init
trap resource_cleanup EXIT

resource_write_controller "$WORK/controller.yaml" false
resource_start_controller "$WORK/controller.yaml"
sid=resource-dynamic
resource_setup_sandbox "$sid"
resource_write_sandbox "$sid" 320 1024 512 false
python3 - "$WORK/$sid.yaml" <<'PY'
import sys
p=sys.argv[1]; s=open(p).read()
s=s.replace('launch:\n  exec: /bin/sleep\n  args: ["300"]\n',
'''launch:
  exec: /usr/local/bin/python3
  env: { PYTHONUNBUFFERED: "1" }
  args:
    - "-c"
    - |
      import time
      blocks=[]
      print("control-workload-ready", flush=True)
      time.sleep(3)
      for _ in range(6):
          blocks.append(bytearray(48*1024*1024))
          blocks[-1][::4096]=b"\\1"*(len(blocks[-1])//4096)
          print("pressure", len(blocks), flush=True)
          time.sleep(1)
      print("workload done", flush=True)
      time.sleep(30)
''')
open(p,"w").write(s)
PY

resource_run_sandbox "$sid"
pid=$RESOURCE_LAST_PID
resource_wait_state "$sid" "$pid" settled 30
deadline=$((SECONDS+30))
while [ "$SECONDS" -lt "$deadline" ]; do
    if resource_memory_control_observed "$sid" && grep -q control-workload-ready "$WORK/$sid.log" 2>/dev/null; then break; fi
    kill -0 "$pid" || resource_fail "$sid exited before control readiness"
    sleep 0.2
done
resource_memory_control_observed "$sid" || resource_fail "memory control did not initialize"
reservation_before="$(resource_reservation_memory "$sid")"
read -r target_before actual_before <<<"$(resource_read_balloon "$sid")"
grows=$(grep -c 'memory: grow accepted Budget=' "$WORK/$sid.log" 2>/dev/null) || grows=0

deadline=$((SECONDS+45))
reservation_after=$reservation_before
while [ "$SECONDS" -lt "$deadline" ]; do
    reservation_after="$(resource_reservation_memory "$sid" 2>/dev/null || echo 0)"
    if [ "$reservation_after" -gt "$reservation_before" ]; then break; fi
    kill -0 "$pid" || resource_fail "$sid exited before reservation grow"
    sleep 0.25
done
[ "$reservation_after" -gt "$reservation_before" ] || resource_fail "node reservation did not grow"
if [ "$grows" -eq 0 ]; then resource_wait_local_grow "$sid" "$pid" "$grows" 30; fi

deadline=$((SECONDS+30))
covered=0
while [ "$SECONDS" -lt "$deadline" ]; do
    read -r target_after actual_after <<<"$(resource_read_balloon "$sid")" || true
    reservation_after="$(resource_reservation_memory "$sid" 2>/dev/null || echo 0)"
    if [[ "$target_after" =~ ^[0-9]+$ ]] && [ "$target_after" -lt "$target_before" ]; then
        applied=$((1024*1024*1024-target_after))
        if [ "$reservation_after" -ge "$applied" ]; then covered=1; break; fi
    fi
    kill -0 "$pid" || resource_fail "$sid exited before covered CH grow"
    sleep 0.25
done
[ "$covered" = 1 ] || resource_fail "CH grow was not covered by live node reservation"

deadline=$((SECONDS+45))
while [ "$SECONDS" -lt "$deadline" ]; do
    grep -q 'workload done' "$WORK/$sid.log" && break
    kill -0 "$pid" || resource_fail "$sid exited before workload completion"
    sleep 0.25
done
grep -q 'workload done' "$WORK/$sid.log" || resource_fail "workload did not complete"
resource_assert_no_oom "$sid"
! grep -qE 'virtio_balloon: pressure at [0-9]+ pages -> cap [0-9]+ pages .*converging' "$WORK/$sid.log"     || resource_fail "guest self-cap fired before proactive controller delivery"
resource_shutdown_pid "$pid"
echo "PASS orchestrator.resource-reservation.sh"
