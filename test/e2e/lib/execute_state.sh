#!/usr/bin/env bash
# Durable ownership and recovery for the randomized e2e_execute host topology.
#
# GitHub Actions can terminate the complete job process tree before Bash runs an
# EXIT trap.  The root-owned state file reserves names only after every target
# has been observed absent.  A later suite can therefore reclaim precisely the
# resources recorded here without inferring ownership from a route or device
# name alone.

execute_state_path() {
    printf '%s\n' "${KUASAR_EXECUTE_STATE_PATH:-/run/kuasar-e2e-execute.state}"
}

execute_state_unit_dir() {
    printf '%s\n' "${KUASAR_EXECUTE_UNIT_DIR:-/run/systemd/system}"
}

execute_state_expected_uid() {
    id -u
}

execute_state_path_absent() {
    [ ! -e "$1" ] && [ ! -L "$1" ]
}

execute_state_record() { # run-key work switch switch-netns proxy-netns host-veth peer-veth original-forward
    local run_key="$1" work="$2" switch="$3" switch_netns="$4"
    local proxy_netns="$5" proxy_veth_host="$6" proxy_veth_ns="$7" original_forward="$8"
    printf 'v1\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tsandbox-runner-%s@\tsandbox-builder-%s@\t%s' \
        "$run_key" "$work" "$switch" "$switch_netns" "$proxy_netns" \
        "$proxy_veth_host" "$proxy_veth_ns" "$run_key" "$run_key" "$original_forward"
}

execute_state_validate_fields() {
    [[ "$EXECUTE_STATE_RUN_KEY" =~ ^e-[A-Za-z0-9]{6}$ ]] \
        || { echo "invalid execute recovery run key" >&2; return 1; }
    [ "$EXECUTE_STATE_WORK" = "/tmp/$EXECUTE_STATE_RUN_KEY" ] \
        || { echo "invalid execute recovery work directory" >&2; return 1; }
    local name
    for name in "$EXECUTE_STATE_SWITCH" "$EXECUTE_STATE_SWITCH_NETNS" \
        "$EXECUTE_STATE_PROXY_NETNS" "$EXECUTE_STATE_PROXY_VETH_HOST" \
        "$EXECUTE_STATE_PROXY_VETH_NS"; do
        [[ "$name" =~ ^[A-Za-z0-9_-]{1,15}$ ]] \
            || { echo "invalid execute recovery network name" >&2; return 1; }
    done
    [ "$EXECUTE_STATE_RUNNER_PREFIX" = "sandbox-runner-${EXECUTE_STATE_RUN_KEY}@" ] \
        || { echo "invalid execute recovery runner prefix" >&2; return 1; }
    [ "$EXECUTE_STATE_BUILDER_PREFIX" = "sandbox-builder-${EXECUTE_STATE_RUN_KEY}@" ] \
        || { echo "invalid execute recovery builder prefix" >&2; return 1; }
    case "$EXECUTE_STATE_ORIGINAL_FORWARD" in
        -|0|1) ;;
        *) echo "invalid execute recovery forwarding state" >&2; return 1 ;;
    esac
}

execute_state_load() {
    local path metadata expected_uid
    local -a lines fields
    path="$(execute_state_path)"
    execute_state_path_absent "$path" && return 3
    if [ -L "$path" ] || [ ! -f "$path" ]; then
        echo "execute recovery state is not a regular file: $path" >&2
        return 1
    fi
    metadata="$(stat -Lc '%u:%a:%h' "$path")" || return 1
    expected_uid="$(execute_state_expected_uid)"
    [ "$metadata" = "$expected_uid:600:1" ] \
        || { echo "execute recovery state has unsafe ownership or mode: $path" >&2; return 1; }
    mapfile -t lines <"$path"
    [ "${#lines[@]}" -eq 1 ] \
        || { echo "execute recovery state must contain exactly one line" >&2; return 1; }
    IFS=$'\t' read -r -a fields <<<"${lines[0]}"
    if [ "${#fields[@]}" -ne 11 ] || [ "${fields[0]}" != v1 ]; then
        echo "invalid execute recovery state schema" >&2
        return 1
    fi

    EXECUTE_STATE_RUN_KEY="${fields[1]}"
    EXECUTE_STATE_WORK="${fields[2]}"
    EXECUTE_STATE_SWITCH="${fields[3]}"
    EXECUTE_STATE_SWITCH_NETNS="${fields[4]}"
    EXECUTE_STATE_PROXY_NETNS="${fields[5]}"
    EXECUTE_STATE_PROXY_VETH_HOST="${fields[6]}"
    EXECUTE_STATE_PROXY_VETH_NS="${fields[7]}"
    EXECUTE_STATE_RUNNER_PREFIX="${fields[8]}"
    EXECUTE_STATE_BUILDER_PREFIX="${fields[9]}"
    EXECUTE_STATE_ORIGINAL_FORWARD="${fields[10]}"
    EXECUTE_STATE_RECORD="${lines[0]}"
    execute_state_validate_fields
}

execute_state_write_record() { # record replace(0|1)
    local record="$1" replace="$2" path directory temporary
    path="$(execute_state_path)"
    directory="${path%/*}"
    [ "$directory" != "$path" ] || directory=.
    [ -d "$directory" ] || { echo "execute recovery state directory is absent: $directory" >&2; return 1; }
    if [ "$replace" = 0 ] && ! execute_state_path_absent "$path"; then
        echo "execute recovery state already exists: $path" >&2
        return 1
    fi
    temporary="$(mktemp "$directory/.kuasar-e2e-execute.state.XXXXXX")" || return 1
    chmod 0600 "$temporary" || { rm -f -- "$temporary"; return 1; }
    if ! printf '%s\n' "$record" >"$temporary" || ! mv -T "$temporary" "$path"; then
        rm -f -- "$temporary"
        return 1
    fi
}

execute_state_reserve() { # same arguments as execute_state_record
    local record
    record="$(execute_state_record "$@")" || return 1
    execute_state_write_record "$record" 0
}

execute_state_remember_forwarding() { # original-forward
    local original_forward="$1" expected record
    case "$original_forward" in
        0|1) ;;
        *) echo "invalid original forwarding state" >&2; return 1 ;;
    esac
    execute_state_load || return 1
    [ "$EXECUTE_STATE_ORIGINAL_FORWARD" = - ] \
        || { echo "execute recovery forwarding state was already recorded" >&2; return 1; }
    expected="$(execute_state_record "$EXECUTE_STATE_RUN_KEY" "$EXECUTE_STATE_WORK" \
        "$EXECUTE_STATE_SWITCH" "$EXECUTE_STATE_SWITCH_NETNS" "$EXECUTE_STATE_PROXY_NETNS" \
        "$EXECUTE_STATE_PROXY_VETH_HOST" "$EXECUTE_STATE_PROXY_VETH_NS" -)"
    [ "$EXECUTE_STATE_RECORD" = "$expected" ] || return 1
    record="$(execute_state_record "$EXECUTE_STATE_RUN_KEY" "$EXECUTE_STATE_WORK" \
        "$EXECUTE_STATE_SWITCH" "$EXECUTE_STATE_SWITCH_NETNS" "$EXECUTE_STATE_PROXY_NETNS" \
        "$EXECUTE_STATE_PROXY_VETH_HOST" "$EXECUTE_STATE_PROXY_VETH_NS" "$original_forward")"
    execute_state_write_record "$record" 1
}

execute_state_netns_exists() {
    local output
    if ! output="$(ip netns list 2>&1)"; then
        echo "cannot enumerate network namespaces: $output" >&2
        return 2
    fi
    printf '%s\n' "$output" | awk '{print $1}' | grep -Fxq -- "$1"
}

execute_state_netns_absent() {
    local status=0
    execute_state_netns_exists "$1" || status=$?
    case "$status" in
        0) return 1 ;;
        1) return 0 ;;
        *) return "$status" ;;
    esac
}

execute_state_rule_absent() {
    local status=0
    iptables -C FORWARD "$@" -j ACCEPT >/dev/null 2>&1 || status=$?
    case "$status" in
        0) return 1 ;;
        1) return 0 ;;
        *) echo "cannot inspect execute forwarding rule" >&2; return "$status" ;;
    esac
}

execute_state_link_absent() {
    local status=0
    ip link show "$1" >/dev/null 2>&1 || status=$?
    case "$status" in
        0) return 1 ;;
        1) return 0 ;;
        *) echo "cannot inspect execute network interface: $1" >&2; return "$status" ;;
    esac
}

execute_state_delete_rule() {
    local status=0
    while true; do
        status=0
        iptables -C FORWARD "$@" -j ACCEPT >/dev/null 2>&1 || status=$?
        case "$status" in
            0) iptables -D FORWARD "$@" -j ACCEPT || return 1 ;;
            1) return 0 ;;
            *) echo "cannot inspect execute forwarding rule" >&2; return "$status" ;;
        esac
    done
}

execute_state_restore_forwarding() { # original value; this test writes 1
    local original="$1" current
    case "$original" in
        ''|-) return 0 ;;
        0|1) ;;
        *) echo "invalid original forwarding state" >&2; return 1 ;;
    esac
    if ! current="$(sysctl -n net.ipv4.ip_forward 2>/dev/null)"; then
        echo "cannot inspect net.ipv4.ip_forward during execute cleanup" >&2
        return 1
    fi
    case "$current" in
        0|1) ;;
        *) echo "invalid current net.ipv4.ip_forward value" >&2; return 1 ;;
    esac
    [ "$current" = "$original" ] && return 0
    if [ "$current" != 1 ]; then
        echo "==> preserving newer net.ipv4.ip_forward=$current during execute cleanup" >&2
        return 0
    fi
    sysctl -q -w net.ipv4.ip_forward=0 >/dev/null
}

execute_state_assert_targets_absent() { # bin switch switch-netns proxy-netns host-veth peer-veth
    local bin="$1" switch="$2" switch_netns="$3" proxy_netns="$4"
    local proxy_veth_host="$5" proxy_veth_ns="$6" status=0 name
    "$bin/connector-ctl" vswitch status "$switch" >/dev/null 2>&1 || status=$?
    [ "$status" -eq 3 ] \
        || { echo "vSwitch is not absent; refusing foreign or inconsistent resource: $switch" >&2; return 1; }
    execute_state_path_absent "/sys/fs/bpf/$switch" \
        || { echo "vSwitch BPF state already exists; refusing inconsistent resource: $switch" >&2; return 1; }
    for name in "$switch_netns" "$proxy_netns"; do
        status=0
        execute_state_netns_absent "$name" || status=$?
        case "$status" in
            0) ;;
            1) echo "network namespace already exists; refusing foreign resource: $name" >&2; return 1 ;;
            *) return "$status" ;;
        esac
    done
    for name in "$proxy_veth_host" "$proxy_veth_ns" "${switch}m0"; do
        status=0
        execute_state_link_absent "$name" || status=$?
        case "$status" in
            0) ;;
            1) echo "network interface already exists; refusing foreign resource: $name" >&2; return 1 ;;
            *) return "$status" ;;
        esac
    done
    for name in "$proxy_veth_host:${switch}m0" "${switch}m0:$proxy_veth_host"; do
        status=0
        execute_state_rule_absent -i "${name%%:*}" -o "${name#*:}" || status=$?
        case "$status" in
            0) ;;
            1) echo "forwarding rule already exists; refusing foreign resource" >&2; return 1 ;;
            *) return "$status" ;;
        esac
    done
}

execute_state_assert_units_absent() {
    local prefix output unit
    for prefix in "$@"; do
        if ! output="$(systemctl list-units --all --plain --no-legend --no-pager "$prefix*.service")"; then
            echo "cannot enumerate execute unit names for $prefix" >&2
            return 1
        fi
        while IFS= read -r unit; do
            case "$unit" in
                "$prefix"*.service)
                    echo "systemd unit already exists; refusing foreign resource: $unit" >&2
                    return 1 ;;
            esac
        done < <(printf '%s\n' "$output" | awk '{print $1}')
    done
}

execute_state_stop_owned_units() {
    local prefix unit output
    for prefix in "$@"; do
        if ! output="$(systemctl list-units --all --plain --no-legend --no-pager "$prefix*.service")"; then
            echo "cannot enumerate execute-owned units for $prefix" >&2
            return 1
        fi
        while IFS= read -r unit; do
            case "$unit" in
                "$prefix"*.service)
                    systemctl stop "$unit" >/dev/null \
                        || { echo "cannot stop execute-owned unit $unit" >&2; return 1; }
                    systemctl reset-failed "$unit" >/dev/null 2>&1 || true ;;
            esac
        done < <(printf '%s\n' "$output" | awk '{print $1}')
    done
}

execute_state_owned_units_absent() {
    local prefix output unit
    for prefix in "$EXECUTE_STATE_RUNNER_PREFIX" "$EXECUTE_STATE_BUILDER_PREFIX"; do
        if ! output="$(systemctl list-units --plain --no-legend --no-pager \
            --state=active,activating,deactivating "$prefix*.service")"; then
            return 1
        fi
        while IFS= read -r unit; do
            case "$unit" in "$prefix"*.service) return 1 ;; esac
        done < <(printf '%s\n' "$output" | awk '{print $1}')
    done
}

execute_state_netns_pids() {
    local namespace="$1" output
    if ! output="$(ip netns pids "$namespace" 2>&1)"; then
        echo "cannot enumerate processes in $namespace: $output" >&2
        return 1
    fi
    printf '%s\n' "$output"
}

execute_state_stop_netns_processes() {
    local namespace="$1" output pid status=0
    execute_state_netns_exists "$namespace" || status=$?
    case "$status" in 0) ;; 1) return 0 ;; *) return "$status" ;; esac
    output="$(execute_state_netns_pids "$namespace")" || return 1
    while read -r pid; do
        if [ -n "$pid" ]; then kill -TERM "$pid" 2>/dev/null || true; fi
    done <<<"$output"
    for _ in $(seq 1 20); do
        output="$(execute_state_netns_pids "$namespace")" || return 1
        [ -z "$output" ] && return 0
        sleep 0.1
    done
    while read -r pid; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || true; fi
    done <<<"$output"
    for _ in $(seq 1 20); do
        output="$(execute_state_netns_pids "$namespace")" || return 1
        [ -z "$output" ] && return 0
        sleep 0.1
    done
    echo "processes in $namespace survived TERM and KILL; refusing to delete it" >&2
    return 1
}

execute_state_switch_absent() { # bin switch
    local status=0
    "$1/connector-ctl" vswitch status "$2" >/dev/null 2>&1 || status=$?
    [ "$status" -eq 3 ]
}

execute_state_resources_absent() { # bin
    local bin="$1" unit_dir
    unit_dir="$(execute_state_unit_dir)"
    execute_state_switch_absent "$bin" "$EXECUTE_STATE_SWITCH" || return 1
    execute_state_netns_absent "$EXECUTE_STATE_SWITCH_NETNS" || return 1
    execute_state_netns_absent "$EXECUTE_STATE_PROXY_NETNS" || return 1
    execute_state_link_absent "$EXECUTE_STATE_PROXY_VETH_HOST" || return 1
    execute_state_link_absent "$EXECUTE_STATE_PROXY_VETH_NS" || return 1
    execute_state_link_absent "${EXECUTE_STATE_SWITCH}m0" || return 1
    execute_state_rule_absent -i "$EXECUTE_STATE_PROXY_VETH_HOST" -o "${EXECUTE_STATE_SWITCH}m0" || return 1
    execute_state_rule_absent -i "${EXECUTE_STATE_SWITCH}m0" -o "$EXECUTE_STATE_PROXY_VETH_HOST" || return 1
    execute_state_path_absent "/sys/fs/bpf/$EXECUTE_STATE_SWITCH" || return 1
    execute_state_path_absent "$unit_dir/${EXECUTE_STATE_RUNNER_PREFIX}.service" || return 1
    execute_state_path_absent "$unit_dir/${EXECUTE_STATE_BUILDER_PREFIX}.service" || return 1
    execute_state_path_absent "$unit_dir/${EXECUTE_STATE_RUNNER_PREFIX}.service.d" || return 1
    execute_state_path_absent "$unit_dir/${EXECUTE_STATE_BUILDER_PREFIX}.service.d" || return 1
    execute_state_path_absent "$unit_dir/sandbox-runner.slice" || return 1
    execute_state_path_absent "$unit_dir/sandbox-builder.slice" || return 1
    execute_state_owned_units_absent || return 1
}

execute_state_remove_unit_files() {
    local unit_dir prefix dropin
    unit_dir="$(execute_state_unit_dir)"
    for prefix in "$EXECUTE_STATE_RUNNER_PREFIX" "$EXECUTE_STATE_BUILDER_PREFIX"; do
        dropin="$unit_dir/${prefix}.service.d"
        rm -f -- "$dropin/override.conf" || return 1
        rmdir -- "$dropin" 2>/dev/null || true
        rm -f -- "$unit_dir/${prefix}.service" || return 1
    done
    rm -f -- "$unit_dir/sandbox-runner.slice" "$unit_dir/sandbox-builder.slice" || return 1
    systemctl daemon-reload || return 1
}

execute_state_recover() { # bin
    local bin="$1" status=0 namespace link output
    execute_state_load || { status=$?; [ "$status" -eq 3 ] && return 0; return "$status"; }
    [ -x "$bin/connector-ctl" ] \
        || { echo "execute recovery requires $bin/connector-ctl" >&2; return 1; }
    "$bin/connector-ctl" vswitch status "$EXECUTE_STATE_SWITCH" >/dev/null 2>&1 || status=$?
    case "$status" in
        0|3) ;;
        *) echo "cannot validate interrupted vSwitch $EXECUTE_STATE_SWITCH" >&2; return 1 ;;
    esac
    echo "==> recovering interrupted e2e_execute run $EXECUTE_STATE_RUN_KEY (switch=$EXECUTE_STATE_SWITCH)" >&2

    execute_state_stop_owned_units "$EXECUTE_STATE_RUNNER_PREFIX" "$EXECUTE_STATE_BUILDER_PREFIX" || return 1
    for namespace in "$EXECUTE_STATE_PROXY_NETNS" "$EXECUTE_STATE_SWITCH_NETNS"; do
        execute_state_stop_netns_processes "$namespace" || return 1
    done

    case "$status" in
        0) "$bin/connector-ctl" vswitch stop "$EXECUTE_STATE_SWITCH" --force || return 1 ;;
        3) ;;
    esac

    execute_state_delete_rule -i "$EXECUTE_STATE_PROXY_VETH_HOST" -o "${EXECUTE_STATE_SWITCH}m0" || return 1
    execute_state_delete_rule -i "${EXECUTE_STATE_SWITCH}m0" -o "$EXECUTE_STATE_PROXY_VETH_HOST" || return 1
    for link in "$EXECUTE_STATE_PROXY_VETH_HOST" "$EXECUTE_STATE_PROXY_VETH_NS" "${EXECUTE_STATE_SWITCH}m0"; do
        status=0
        execute_state_link_absent "$link" || status=$?
        case "$status" in
            0) ;;
            1) ip link del "$link" || return 1 ;;
            *) return "$status" ;;
        esac
    done
    for namespace in "$EXECUTE_STATE_PROXY_NETNS" "$EXECUTE_STATE_SWITCH_NETNS"; do
        status=0
        execute_state_netns_exists "$namespace" || status=$?
        case "$status" in
            0)
                output="$(execute_state_netns_pids "$namespace")" || return 1
                [ -z "$output" ] || return 1
                ip netns del "$namespace" || return 1 ;;
            1) ;;
            *) return "$status" ;;
        esac
    done
    execute_state_remove_unit_files || return 1
    execute_state_restore_forwarding "$EXECUTE_STATE_ORIGINAL_FORWARD" || return 1
    execute_state_resources_absent "$bin" \
        || { echo "interrupted execute resources remain; preserving recovery state" >&2; return 1; }
    rm -rf -- "$EXECUTE_STATE_WORK" || return 1
    rm -f -- "$(execute_state_path)" || return 1
}

execute_state_finish() { # bin expected-record
    local bin="$1" expected="$2" status=0
    execute_state_load || { status=$?; [ "$status" -eq 3 ] && return 0; return "$status"; }
    [ "$EXECUTE_STATE_RECORD" = "$expected" ] \
        || { echo "execute recovery state no longer belongs to this run" >&2; return 1; }
    execute_state_resources_absent "$bin" \
        || { echo "execute cleanup incomplete; preserving recovery state" >&2; return 1; }
    rm -f -- "$(execute_state_path)" || return 1
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    set -euo pipefail
    : "${BIN:?BIN must point to the assembled platform binary directory}"
    if [ "$(id -u)" -ne 0 ]; then
        exec sudo -nE env BIN="$BIN" KUASAR_EXECUTE_STATE_PATH="${KUASAR_EXECUTE_STATE_PATH:-}" \
            KUASAR_EXECUTE_UNIT_DIR="${KUASAR_EXECUTE_UNIT_DIR:-}" "$0" "$@"
    fi
    [ "${1:-}" = recover ] || { echo "usage: BIN=/path $0 recover" >&2; exit 2; }
    execute_state_recover "$BIN"
fi
