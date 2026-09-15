#!/usr/bin/env bash
# Stop only instances whose actual ExecStart names this fixture's private
# config socket/path. Listing a pattern is read-only; Stop always uses one exact
# verified instance, including a unit still starting before its pidfile exists.
stop_build_fixture_units() {
    local work="$1" unit spec
    while IFS= read -r unit; do
        [ -n "$unit" ] || continue
        spec=$(systemctl show --property=ExecStart --value "$unit") || continue
        case "$spec" in
            *"$work/"*) systemctl stop "$unit" ;;
        esac
    done < <(systemctl list-units 'sandbox-runner@*.service' 'sandbox-builder@*.service' \
        --all --output=json --no-pager | python3 -c 'import json,sys
for unit in json.load(sys.stdin):
    print(unit["unit"])')
}
