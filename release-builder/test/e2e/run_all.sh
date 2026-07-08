#!/usr/bin/env bash
#
# Full release e2e runner. This script is packaged under test/e2e/ and assumes
# component archives have been extracted into one release root with a shared
# bin/ directory. OBS remains the only credential-gated exception; e2e_obs.sh
# exits 0 when OBS_E2E=1 is not set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${BIN:-$RELEASE_ROOT/bin}"
export BIN

export REQUIRE_KVM="${REQUIRE_KVM:-1}"
export REQUIRE_EXEC="${REQUIRE_EXEC:-1}"
export REQUIRE_CLUSTER_STUB="${REQUIRE_CLUSTER_STUB:-1}"
export REQUIRE_CLUSTER_REAL="${REQUIRE_CLUSTER_REAL:-1}"
export REQUIRE_ORCH="${REQUIRE_ORCH:-1}"
export REQUIRE_PROXY="${REQUIRE_PROXY:-1}"
export REQUIRE_BUILDER="${REQUIRE_BUILDER:-1}"
export REQUIRE_RUNTASK="${REQUIRE_RUNTASK:-1}"
export REQUIRE_CONNECTOR_E2E="${REQUIRE_CONNECTOR_E2E:-1}"
export REQUIRE_GUEST_RUNTIME="${REQUIRE_GUEST_RUNTIME:-1}"

echo "==> full release e2e"
echo "==> BIN=$BIN"

shopt -s nullglob
e2e_scripts=("$SCRIPT_DIR"/e2e_*.sh)
if [ "${#e2e_scripts[@]}" -eq 0 ]; then
    echo "==> FAIL: no e2e_*.sh scripts found in $SCRIPT_DIR" >&2
    exit 1
fi

for script in "${e2e_scripts[@]}"; do
    echo
    echo "========================================="
    echo "  $(basename "$script")"
    echo "========================================="
    bash "$script"
done

connector_dir="$RELEASE_ROOT/test/connector/examples"
if [ -d "$connector_dir" ]; then
    for script in "$connector_dir"/*_test.sh; do
        [ -f "$script" ] || continue
        echo
        echo "========================================="
        echo "  connector/$(basename "$script")"
        echo "========================================="
        sudo env REQUIRE_CONNECTOR_E2E=1 bash "$script" all
    done
fi

echo
echo "==> full release e2e: OK"
