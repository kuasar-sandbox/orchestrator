#!/usr/bin/env bash
# Test-unit ownership is reserved before creation and can be recovered with the
# existing execute topology record. Never select units by a broad name scan.
proxy_lifecycle_units() {
    printf '%s\n' "kuasar-proxy-$1-data.socket" "kuasar-proxy-$1-mmds.socket" "kuasar-proxy-$1.service"
}

proxy_lifecycle_reserve_units() {
    local work="$1" key="$2" unit state record="$1/proxy-lifecycle.units"
    [[ "$key" =~ ^e-[A-Za-z0-9]{6}$ ]] || return 1
    execute_state_path_absent "$record" || return 1
    while IFS= read -r unit; do
        execute_state_path_absent "$(execute_state_unit_dir)/$unit" || return 1
        state="$(systemctl show --property=LoadState --value "$unit" 2>/dev/null || true)"
        [ "$state" = not-found ] || return 1
    done < <(proxy_lifecycle_units "$key")
    (set -C; umask 077; printf '%s\n' "$key" >"$record")
}

proxy_lifecycle_cleanup_units() {
    local work="$1" key="$2" record="$1/proxy-lifecycle.units" unit state path group remaining
    execute_state_path_absent "$record" && return 0
    [[ "$key" =~ ^e-[A-Za-z0-9]{6}$ ]] || return 1
    [ -f "$record" ] && [ ! -L "$record" ] \
        && [ "$(stat -Lc '%u:%a:%h:%s' "$record")" = "$(id -u):600:1:9" ] \
        && [ "$(cat "$record")" = "$key" ] || return 1
    local -a loaded=()
    while IFS= read -r unit; do
        path="$(execute_state_unit_dir)/$unit"
        if ! execute_state_path_absent "$path"; then
            [ -f "$path" ] && [ ! -L "$path" ] && [ "$(stat -Lc '%u:%h' "$path")" = "$(id -u):1" ] || return 1
        fi
        state="$(systemctl show --property=LoadState --value "$unit" 2>/dev/null || true)"
        case "$state" in
            not-found) ;;
            loaded|error|bad-setting) loaded+=("$unit");;
            *) echo "cannot inspect owned test unit $unit" >&2; return 1;;
        esac
    done < <(proxy_lifecycle_units "$key")
    if [ "${#loaded[@]}" -gt 0 ]; then
        systemctl stop "${loaded[@]}" || return 1
        for unit in "${loaded[@]}"; do
            group="$(systemctl show --property=ControlGroup --value "$unit")" || return 1
            case "$group" in
                '') continue;;
                /*) ;;
                *) return 1;;
            esac
            path="/sys/fs/cgroup$group/cgroup.procs"
            if [ -e "$path" ]; then
                remaining="$(cat "$path")" || return 1
                if [ -n "$remaining" ]; then
                    echo "owned unit $unit still has processes: $remaining" >&2
                    return 1
                fi
            fi
        done
        systemctl reset-failed "${loaded[@]}" >/dev/null 2>&1 || true
    fi
    while IFS= read -r unit; do
        journalctl -u "$unit" --no-pager >"$work/$unit.journal" 2>/dev/null || true
        path="$(execute_state_unit_dir)/$unit"
        if ! execute_state_path_absent "$path"; then
            [ -f "$path" ] && [ ! -L "$path" ] && [ "$(stat -Lc '%u' "$path")" = "$(id -u)" ] || return 1
            rm -f -- "$path" || return 1
        fi
    done < <(proxy_lifecycle_units "$key")
    systemctl daemon-reload || return 1
    rm -f -- "$record"
}
