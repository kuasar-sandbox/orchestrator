#!/usr/bin/env bash

# Enter the privilege level required by the delegated systemd runtask case.
# Do not preserve the caller environment wholesale across sudo: the owner case
# only needs its prepared products/helpers, runner workspace and diagnostics.
# A fixed root PATH keeps unrelated caller PATH entries out of the invocation.
RUNTASK_ROOT_PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

runtask_enter_privileged() {
    local before_exec="$1"
    local script_path="$2"
    shift 2

    if [ "$(id -u)" -eq 0 ]; then
        return
    fi

    command -v sudo >/dev/null 2>&1 || fail "run-sandbox handoff requires root or passwordless sudo"

    local -a root_env=(
        /usr/bin/env -i
        "PATH=$RUNTASK_ROOT_PATH"
        "BIN=${BIN:?prepared products are required}"
        "E2E_LIB=${E2E_LIB:?prepared helpers are required}"
        "WORK=${WORK:?runner workspace is required}"
    )
    [ "${OUT+x}" != x ] || root_env+=("OUT=$OUT")
    if [ "${E2E_KEEP+x}" = x ]; then
        root_env+=("E2E_KEEP=$E2E_KEEP")
    fi

    # Probe the same sudo + clean-environment mechanism used for the actual
    # re-entry. A selected product case must fail if privilege is unavailable.
    if ! sudo -n "${root_env[@]}" /usr/bin/true >/dev/null 2>&1; then
        fail "run-sandbox handoff requires root or passwordless sudo"
    fi

    "$before_exec"
    exec sudo -n "${root_env[@]}" /bin/bash "$script_path" "$@"
}
