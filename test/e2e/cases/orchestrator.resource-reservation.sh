#!/usr/bin/env bash
set -euo pipefail
: "${E2E_LIB:?}"
. "$E2E_LIB/orchestrator/resource.sh"
resource_init

resource_write_controller "$WORK/controller.yaml" false
resource_start_controller "$WORK/controller.yaml"
sid=resource-dynamic
resource_setup_sandbox "$sid"
resource_write_sandbox "$sid" 320 1024 512 false
resource_write_pressure_workload "$sid" dynamic \
    /tmp/resource-dynamic.start /tmp/resource-dynamic.delivery

resource_run_sandbox "$sid"
pid=$RESOURCE_LAST_PID
resource_wait_dynamic_control_ready "$sid" "$pid" 20
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
fi
"$BIN/sandbox-ctl" exec --run-root "$WORK/run" --sandbox-id "$sid" -- /bin/sh -c 'touch /tmp/resource-dynamic.start'
deadline=$((SECONDS + 20))
while [ "$SECONDS" -lt "$deadline" ]; do
    grep -q 'workload pressure probe ready rss=' "$WORK/$sid.log" && break
    kill -0 "$pid" || resource_fail "$sid exited before pressure probe"
    sleep 0.1
done
grep -q 'workload pressure probe ready rss=' "$WORK/$sid.log" || resource_fail "pressure probe did not become ready"
if [ "$grow_phase" = pressure ]; then
    deadline=$((SECONDS+20))
    while [ "$SECONDS" -lt "$deadline" ]; do
        reservation_after="$(resource_reservation_memory "$sid" 2>/dev/null || echo 0)"
        if [[ "$reservation_after" =~ ^[0-9]+$ ]] && [ "$reservation_after" -gt "$reservation_before" ]; then break; fi
        kill -0 "$pid" || resource_fail "$sid exited before reservation grow"
        sleep 0.25
    done
    [ "$reservation_after" -gt "$reservation_before" ] || resource_fail "node reservation did not grow"
    resource_wait_local_grow "$sid" "$pid" "$grows" 20
fi

# Both authorization and its current CH target must be observed within the
# original 15-second delivery budget while the first allocation is held.
deadline=$((SECONDS+15))
reservation_after="$(resource_wait_reserved_grow "$sid" "$pid" 15 "$startup_budget" "$capacity")"
reservation_after="$(resource_wait_covered_grow "$sid" "$pid" "$((deadline-SECONDS))" "$target_reference" "$capacity")"
"$BIN/sandbox-ctl" exec --run-root "$WORK/run" --sandbox-id "$sid" -- /bin/sh -c 'touch /tmp/resource-dynamic.delivery'

deadline=$((SECONDS+45))
while [ "$SECONDS" -lt "$deadline" ]; do
    ! resource_self_cap_observed "$sid" || resource_fail "guest self-cap fired during workload"
    grep -q 'workload done' "$WORK/$sid.log" && break
    kill -0 "$pid" || resource_fail "$sid exited before workload completion"
    sleep 0.25
done
grep -q 'workload done' "$WORK/$sid.log" || resource_fail "workload did not complete"
resource_grow_events_valid "$sid" "$startup_budget" "$capacity" ||
    resource_fail "workload emitted an invalid or unreserved grow"
resource_assert_no_oom "$sid"
! resource_self_cap_observed "$sid" \
    || resource_fail "guest self-cap fired before proactive controller delivery"
resource_shutdown_pid "$pid"
echo "PASS orchestrator.resource-reservation.sh (grow_phase=$grow_phase reservation=$reservation_after)"
