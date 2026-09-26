#!/usr/bin/env bash
set -euo pipefail
: "${E2E_LIB:?}"
. "$E2E_LIB/orchestrator/resource.sh"
resource_init
trap resource_cleanup EXIT

validate_grow_events() {
    local sid="$1" initial_budget="$2" capacity="$3"
    awk -v initial="$initial_budget" -v capacity="$capacity" '
        /memory: grow accepted Budget=/ {
            line=$0
            sub(/^.*memory: grow accepted /, "", line)
            sub(/\r$/, "", line)
            if (line !~ /^Budget=[0-9]+ target=[0-9]+ reservation=[0-9]+$/) {
                invalid=1
                next
            }
            split(line, fields, " ")
            split(fields[1], budget, "=")
            split(fields[2], target, "=")
            split(fields[3], reservation, "=")
            b=budget[2]+0; t=target[2]+0; r=reservation[2]+0
            if (b <= 0 || b > capacity || t > capacity ||
                b != capacity - t || r < b || r > capacity) invalid=1
            if (b > initial) grown=1
        }
        END { if (invalid) exit 2; if (!grown) exit 1 }
    ' "$WORK/$sid.log"
}


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
capacity=$((1024 * 1024 * 1024))
startup_budget=$((512 * 1024 * 1024))
settled_headroom=$((320 * 1024 * 1024))
initial_target=$((capacity - startup_budget))
reservation_before="$(resource_reservation_memory "$sid")"
read -r target_before actual_before <<<"$(resource_read_balloon "$sid")"
[[ "$reservation_before" =~ ^[0-9]+$ ]] || resource_fail "invalid initial node reservation"
[[ "$target_before" =~ ^[0-9]+$ && "$actual_before" =~ ^[0-9]+$ ]] || resource_fail "invalid initial CH balloon state"
[ "$reservation_before" -le "$capacity" ] && [ "$target_before" -le "$capacity" ] && [ "$actual_before" -le "$capacity" ] ||
    resource_fail "initial dynamic resource state exceeds capacity"
grows=$(grep -c 'memory: grow accepted Budget=' "$WORK/$sid.log" 2>/dev/null) || grows=0
grow_phase=pressure
target_reference=$target_before
reservation_after=$reservation_before
if [ "$grows" -gt 0 ] && [ "$target_before" -lt "$initial_target" ] && [ "$reservation_before" -gt "$settled_headroom" ]; then
    # Boot-time PSI may already have produced a complete dynamic grow before
    # workload pressure begins. Retain that valid authorization rather than
    # requiring an artificial second grant.
    grow_phase=prepressure
    target_reference=$initial_target
else
    deadline=$((SECONDS+45))
    while [ "$SECONDS" -lt "$deadline" ]; do
        reservation_after="$(resource_reservation_memory "$sid" 2>/dev/null || echo 0)"
        if [[ "$reservation_after" =~ ^[0-9]+$ ]] && [ "$reservation_after" -gt "$reservation_before" ]; then break; fi
        kill -0 "$pid" || resource_fail "$sid exited before reservation grow"
        sleep 0.25
    done
    [ "$reservation_after" -gt "$reservation_before" ] || resource_fail "node reservation did not grow"
    resource_wait_local_grow "$sid" "$pid" "$grows" 30
fi

validate_grow_events "$sid" "$startup_budget" "$capacity" ||
    resource_fail "accepted grow evidence is missing, malformed, or not covered by reservation"

deadline=$((SECONDS+30))
covered=0
while [ "$SECONDS" -lt "$deadline" ]; do
    read -r target_after actual_after <<<"$(resource_read_balloon "$sid")" || true
    reservation_after="$(resource_reservation_memory "$sid" 2>/dev/null || echo 0)"
    if [[ "$target_after" =~ ^[0-9]+$ && "$actual_after" =~ ^[0-9]+$ ]] &&
       [ "$target_after" -le "$capacity" ] && [ "$actual_after" -le "$capacity" ] &&
       [ "$target_after" -lt "$target_reference" ] &&
       [[ "$reservation_after" =~ ^[0-9]+$ ]] && [ "$reservation_after" -le "$capacity" ]; then
        applied=$((capacity - target_after))
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
echo "PASS orchestrator.resource-reservation.sh (grow_phase=$grow_phase reservation=$reservation_after)"
