#!/usr/bin/env bash
# Live resource observations and bounded assertion primitives; cases own workloads.

resource_reservation_matches() {
    local sid="$1" mode="$2" reservations
    reservations=$("$BIN/node-ctl" resource list \
        --socket "$WORK/sandbox-resource.sock" 2>/dev/null) || return 1
    SID="$sid" MODE="$mode" RESERVATIONS_JSON="$reservations" python3 - 2>/dev/null <<'PY'
import json
import os

rows = json.loads(os.environ["RESERVATIONS_JSON"])
sid = os.environ["SID"]
mode = os.environ["MODE"]
matches = [row for row in rows if row.get("sandbox_id") == sid]
assert len(matches) == 1, rows
row = matches[0]
assert row.get("connected") is True, row
assert row.get("provisional", False) is False, row
if mode in {"settled", "synced"}:
    assert row.get("stage") == "settled", row
if mode == "settled":
    assert row.get("recovery_source") in {"admit", "synced"}, row
elif mode == "synced":
    assert len(rows) == 1, rows
    assert row.get("recovery_source") == "synced", row
else:
    raise AssertionError(f"unknown reservation mode: {mode}")
PY
}

resource_grow_events_valid() {
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

resource_wait_reserved_grow() {
    local sid="$1" pid="$2" timeout="$3" initial_budget="$4" capacity="$5"
    local deadline=$((SECONDS + timeout)) reservation=0 state="" target=-1 actual=-1 budget=0 grow_status=0
    while [ "$SECONDS" -lt "$deadline" ]; do
        kill -0 "$pid" 2>/dev/null || resource_fail "$sid: sandbox exited before a reserved grow target was observed"
        grow_status=0
        resource_grow_events_valid "$sid" "$initial_budget" "$capacity" || grow_status=$?
        [ "$grow_status" -le 1 ] || resource_fail "$sid: invalid or unreserved accepted grow event"
        if [ "$grow_status" -eq 0 ] \
            && reservation=$(resource_reservation_memory "$sid") \
            && state=$(resource_read_balloon "$sid" 2>/dev/null) \
            && read -r target actual <<<"$state" \
            && [[ "$reservation" =~ ^[0-9]+$ ]] \
            && [[ "$target" =~ ^[0-9]+$ ]] && [[ "$actual" =~ ^[0-9]+$ ]] \
            && [ "$reservation" -gt "$initial_budget" ] && [ "$reservation" -le "$capacity" ] \
            && [ "$target" -le "$capacity" ] && [ "$actual" -le "$capacity" ]; then
            budget=$((capacity - target))
            if [ "$budget" -gt "$initial_budget" ] && [ "$budget" -le "$reservation" ]; then
                echo "$reservation"
                return 0
            fi
        fi
        sleep 0.25
    done
    resource_fail "$sid: no sandbox-originated reserved grow target above InitialBudget=$initial_budget within ${timeout}s (reservation=$reservation target=$target current=$actual)"
}

resource_wait_dynamic_control_ready() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if resource_reservation_matches "$sid" settled \
            && grep -qE 'sensor: (PSI|events_poll) mode active' "$WORK/$sid.log" 2>/dev/null \
            && resource_memory_control_observed "$sid" \
            && grep -q 'control-workload-ready' "$WORK/$sid.log" 2>/dev/null; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || resource_fail "$sid: sandbox exited before controller/sensor readiness barrier"
        sleep 0.1
    done
    resource_fail "$sid: controller/sensor readiness barrier not reached within ${timeout}s"
}

resource_wait_static_control_ready() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if grep -qE 'sensor: (PSI|events_poll) mode active' "$WORK/$sid.log" 2>/dev/null \
            && resource_memory_control_observed "$sid" \
            && grep -q 'control-workload-ready' "$WORK/$sid.log" 2>/dev/null; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || resource_fail "$sid: sandbox exited before static control readiness barrier"
        sleep 0.1
    done
    resource_fail "$sid: static control readiness barrier not reached within ${timeout}s"
}

resource_self_cap_observed() {
    local sid="$1"
    grep -qE 'virtio_balloon: pressure at [0-9]+ pages -> cap [0-9]+ pages .*converging' \
        "$WORK/$sid.log" 2>/dev/null
}

resource_wait_covered_grow() {
    local sid="$1" pid="$2" timeout="$3" target_baseline="$4"
    local capacity="$5"
    local deadline=$((SECONDS + timeout)) state="" target=-1 actual=-1 applied_budget=0
    local node_reservation=-1 reservation_status=not_sampled ch_status=unavailable
    while [ "$SECONDS" -lt "$deadline" ]; do
        target=-1 actual=-1 applied_budget=0 node_reservation=-1
        ch_status=unavailable reservation_status=not_sampled
        state=$(resource_read_balloon "$sid" 2>/dev/null) || state=""
        if read -r target actual <<<"$state" \
            && [[ "$target" =~ ^[0-9]+$ ]] && [[ "$actual" =~ ^[0-9]+$ ]]; then
            [ "$target" -le "$capacity" ] && [ "$actual" -le "$capacity" ] \
                || resource_fail "$sid: CH balloon state exceeds Capacity (target=$target actual=$actual capacity=$capacity)"
            ch_status=not_reduced
            if [ "$target" -lt "$target_baseline" ]; then
                ch_status=accepted
                applied_budget=$((capacity - target))
                # Read the exact live authorization after the CH observation.
                # Later grants may supersede the one captured at the workload gate.
                # Failed reads never retain an earlier, possibly larger value.
                reservation_status=unavailable
                if node_reservation=$(resource_reservation_memory "$sid") \
                    && [[ "$node_reservation" =~ ^[0-9]+$ ]] \
                    && [ "$node_reservation" -le "$capacity" ]; then
                    reservation_status=insufficient
                    if [ "$applied_budget" -le "$node_reservation" ]; then
                        reservation_status=covered
                    fi
                fi
            fi
        fi
        # Matching observations cannot hide an already-failed sandbox.
        if resource_self_cap_observed "$sid"; then
            resource_fail "$sid: guest self-cap fired before controller grant delivery"
        fi
        kill -0 "$pid" 2>/dev/null || resource_fail "$sid: sandbox exited before controller grant delivery"
        if [ "$reservation_status" = covered ]; then
            echo "$node_reservation"
            return 0
        fi
        sleep 0.25
    done
    resource_fail "$sid: CH grow target lacked a live sufficient reservation within ${timeout}s (ch_status=$ch_status target=$target actual=$actual applied_budget=$applied_budget reservation_status=$reservation_status reservation=$node_reservation)"
}
