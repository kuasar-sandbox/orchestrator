#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${BIN:?BIN must point to the assembled platform binary directory}"
export BIN
export REQUIRE_KVM=1
export REQUIRE_EXEC=1
export REQUIRE_CLUSTER_STUB=1
export REQUIRE_CLUSTER_REAL=1
export REQUIRE_ORCH=1
export REQUIRE_PROXY=1
export REQUIRE_BUILDER=1
export REQUIRE_RUNTASK=1

# A cancelled execute case may be killed before its EXIT trap. Recover the
# exact root-owned reservation before an earlier fixed-name case tries to add
# the same floating-IP return route.
recovery_status=0
flock --nonblock --exclusive --close --conflict-exit-code 75 \
    /run/systemd/system env BIN="$BIN" bash "$SCRIPT_DIR/lib/execute_state.sh" recover \
    || recovery_status=$?
[ "$recovery_status" -ne 75 ] \
    || { echo "another execute/MMDS case holds the host runtime lock" >&2; exit 1; }
[ "$recovery_status" -eq 0 ] || exit "$recovery_status"

shopt -s nullglob
cases=("$SCRIPT_DIR"/cases/*.sh)
[ "${#cases[@]}" -gt 0 ] || { echo "orchestrator e2e suite contains no cases" >&2; exit 1; }

# Reuse the real sandbox/cluster/Build cases, rather than booting duplicate
# fixtures solely for logging. Cursor and exact executable scope exclude old
# records and other source workspaces on the same journal host.
journal_work="$(mktemp -d)"
trap 'rm -rf "$journal_work"' EXIT
journal=(journalctl)
if [ "$(id -u)" -ne 0 ]; then journal=(sudo -n journalctl); fi
for script in "${cases[@]}"; do
    echo
    echo "========================================="
    echo "  orchestrator/$(basename "$script")"
    echo "========================================="
    kind=""
    bash "$script"
    if [ -n "$kind" ]; then
        "${journal[@]}" --sync
        "${journal[@]}" --after-cursor="$cursor" --no-pager -o json \
            "_EXE=$(readlink -f "$BIN/sandbox-ctl")" >"$journal_work/entries.jsonl"
        python3 "$SCRIPT_DIR/lib/journal_identity.py" "$kind" "$journal_work/entries.jsonl"
    fi
done
