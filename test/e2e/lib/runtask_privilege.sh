[Reading 22 lines from start (total: 22 lines, 0 remaining)]

#!/usr/bin/env bash

# Enter the privilege level required by the delegated systemd runtask case.
# The caller supplies skip(), which preserves e2e_runtask's optional/required
# prerequisite semantics.
runtask_enter_privileged() {
    local before_exec="$1"
    shift

    if [ "$(id -u)" -eq 0 ]; then
        return
    fi

    if ! command -v sudo >/dev/null 2>&1 || ! sudo -n true >/dev/null 2>&1; then
        skip "run-sandbox handoff requires root or passwordless sudo"
    fi

    # Keep the environment selected by the owner job (notably BIN and
    # REQUIRE_RUNTASK) while re-entering this same test as root.
    "$before_exec"
    exec sudo -nE "$0" "$@" || skip "passwordless sudo could not re-exec runtask"
}