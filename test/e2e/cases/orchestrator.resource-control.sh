#!/usr/bin/env bash
set -euo pipefail
: "${E2E_LIB:?}"
. "$E2E_LIB/orchestrator/resource.sh"
resource_init

sid=resource-static
resource_setup_sandbox "$sid"
# Static mode deliberately omits the node controller: this proves the
# sandbox-local memory control path can deliver a real CH grow by itself.
resource_write_sandbox "$sid" 320 1024 512 false
resource_write_pressure_workload "$sid" static \
    /tmp/resource-static.start /tmp/resource-static.delivery

resource_run_sandbox "$sid"
pid=$RESOURCE_LAST_PID
resource_wait_static_control_ready "$sid" "$pid" 20
read -r target_before actual_before <<<"$(resource_read_balloon "$sid")"
[[ "$target_before" =~ ^[0-9]+$ && "$actual_before" =~ ^[0-9]+$ ]] || resource_fail "invalid initial CH balloon state"
capacity=$((1024 * 1024 * 1024))
initial_target=$((512 * 1024 * 1024))
[ "$target_before" -le "$capacity" ] && [ "$actual_before" -le "$capacity" ] || resource_fail "initial CH balloon state exceeds capacity"
grows=$(grep -c 'memory: grow accepted Budget=' "$WORK/$sid.log" 2>/dev/null) || grows=0
grow_phase=pressure
target_reference=$target_before
if [ "$grows" -gt 0 ] && [ "$target_before" -lt "$initial_target" ]; then
    grow_phase=prepressure
    target_reference=$initial_target
fi
"$BIN/sandbox-ctl" exec --run-root "$WORK/run" --sandbox-id "$sid" -- /bin/sh -c 'touch /tmp/resource-static.start'
deadline=$((SECONDS + 20))
while [ "$SECONDS" -lt "$deadline" ]; do
    grep -q 'workload pressure probe ready rss=' "$WORK/$sid.log" && break
    kill -0 "$pid" || resource_fail "$sid exited before pressure probe"
    sleep 0.1
done
grep -q 'workload pressure probe ready rss=' "$WORK/$sid.log" || resource_fail "pressure probe did not become ready"
if [ "$grow_phase" = pressure ]; then resource_wait_local_grow "$sid" "$pid" "$grows" 20; fi

deadline=$((SECONDS+15))
target_after=$target_before
while [ "$SECONDS" -lt "$deadline" ]; do
    target_after=-1 actual_after=-1
    read -r target_after actual_after <<<"$(resource_read_balloon "$sid")" || true
    kill -0 "$pid" || resource_fail "$sid exited before CH accepted grow"
    if [[ "$target_after" =~ ^[0-9]+$ && "$actual_after" =~ ^[0-9]+$ ]]; then
        [ "$target_after" -le "$capacity" ] && [ "$actual_after" -le "$capacity" ] ||
            resource_fail "post-grow CH balloon state exceeds capacity"
        [ "$target_after" -ge "$target_reference" ] || break
    fi
    sleep 0.25
done
[[ "$target_after" =~ ^[0-9]+$ && "$actual_after" =~ ^[0-9]+$ ]] || resource_fail "invalid post-grow CH balloon state"
[ "$target_after" -lt "$target_reference" ] || resource_fail "static grow did not reduce CH balloon target below $target_reference"
"$BIN/sandbox-ctl" exec --run-root "$WORK/run" --sandbox-id "$sid" -- /bin/sh -c 'touch /tmp/resource-static.delivery'
deadline=$((SECONDS+45))
while [ "$SECONDS" -lt "$deadline" ]; do
    grep -q 'workload done' "$WORK/$sid.log" && break
    kill -0 "$pid" || resource_fail "$sid exited before workload completion"
    sleep 0.25
done
grep -q 'workload done' "$WORK/$sid.log" || resource_fail "workload did not complete"
resource_assert_no_oom "$sid"
resource_shutdown_pid "$pid"
echo "PASS orchestrator.resource-control.sh"
