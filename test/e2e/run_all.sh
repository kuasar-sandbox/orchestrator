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

shopt -s nullglob
cases=("$SCRIPT_DIR"/e2e_*.sh)
[ "${#cases[@]}" -gt 0 ] || {
    echo "orchestrator e2e suite contains no cases" >&2
    exit 1
}

for script in "${cases[@]}"; do
    echo
    echo "========================================="
    echo "  orchestrator/$(basename "$script")"
    echo "========================================="
    bash "$script"
done
