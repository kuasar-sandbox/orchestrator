#!/usr/bin/env bash
failed_unit_count() { # $1=unit pattern; list-units does not load missing instances
    systemctl list-units --all --state=failed --type=service --no-legend --no-pager "$1" 2>/dev/null \
        | awk 'NF { count++ } END { print count + 0 }'
}
wait_unit_collected() { # $1=exact unit
    local unit="$1" loaded
    for _ in $(seq 1 100); do
        loaded=$(systemctl list-units --all --type=service --no-legend --no-pager "$unit" 2>/dev/null) \
            || return 1
        if ! grep -Fq -- "$unit" <<<"$loaded"; then
            return 0
        fi
        sleep 0.1
    done
    systemctl list-units --all --type=service --no-legend --no-pager "$unit" >&2 || true
    return 1
}
wait_unit_journal_contains() { # $1=unit, $2=fixed string, $3=output file
    local unit="$1" pattern="$2" output="$3"
    for _ in $(seq 1 100); do
        journalctl -u "$unit" --no-pager --output=cat >"$output" 2>/dev/null || true
        grep -Fq -- "$pattern" "$output" && return 0
        sleep 0.1
    done
    return 1
}
build_journal_unit() { # $1=build id
    journalctl KUASAR_BUILD_ID="$1" --no-pager --output=json 2>/dev/null \
        | python3 -c 'import json,sys
for line in sys.stdin:
    try:
        unit = json.loads(line).get("_SYSTEMD_UNIT", "")
    except json.JSONDecodeError:
        continue
    if unit.startswith("sandbox-builder@") and unit.endswith(".service"):
        # Drain journalctl so pipefail cannot turn a match into SIGPIPE.
        for _ in sys.stdin:
            pass
        print(unit)
        raise SystemExit(0)
raise SystemExit(1)'
}
assert_phase_history() { # bid required-phase-or-dash forbidden-phase-or-dash label
    local bid="$1" required="$2" forbidden="$3" label="$4" unit journal required_sid forbidden_sid
    unit=$(build_journal_unit "$bid") || fail "$label journal lost its builder unit identity"
    journal="$WORK/$label-builder.journal"
    journalctl --no-pager -o cat -u "$unit" >"$journal" 2>&1 || true
    if [ "$required" != "-" ]; then
        required_sid=$(phase_sandbox_id "$required" "$bid")
        grep -Fq -- "$required_sid" "$journal" \
            || { cat "$journal" >&2; fail "$label did not run required Phase $required"; }
    fi
    if [ "$forbidden" != "-" ]; then
        forbidden_sid=$(phase_sandbox_id "$forbidden" "$bid")
        ! grep -Fq -- "$forbidden_sid" "$journal" \
            || { cat "$journal" >&2; fail "$label unexpectedly ran Phase $forbidden"; }
    fi
}
