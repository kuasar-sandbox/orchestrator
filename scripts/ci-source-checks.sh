#!/usr/bin/env bash
# Required source/race/vet and privileged Collector regressions, separate from E2E.
set -euo pipefail
mode="${1:---all}"
[[ "$#" -le 1 && "$mode" =~ ^--(all|ordinary|privileged)$ ]] || {
    echo "usage: ci-source-checks.sh [--ordinary|--privileged]" >&2
    exit 2
}
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [ "$mode" != --privileged ]; then
    go test -count=1 -timeout=5m ./...
    CGO_ENABLED=1 go test -race -count=1 -timeout=5m ./...
    go vet ./...
    PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-environment-tools.py
    PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-source-checks.py
    PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-telemetry-image-pull.py
    bash test/source/runtask_privilege.sh
    bash test/source/vmm_cgroup.sh
    bash test/source/journal_contract.sh
    bash test/source/capture_cli.sh
    REQUIRE_CLUSTER_STUB=1 bash test/source/cluster_stub.sh
    bash test/source/builder_state.sh
    # Helper/source regressions stay in this compiler-capable gate, not in the
    # prepared product E2E runner. Discover the maintained helper tests as one set so
    # execute/resource/cluster/capture regressions cannot disappear during cutover.
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_*.py' -v
fi

if [ "$mode" != --ordinary ]; then
    REQUIRE_BUILDER=1 bash test/source/builder_unit_upgrade.sh
    bash test/source/telemetry_backends.sh
    work="$(mktemp -d)"
    trap 'rm -rf "$work"' EXIT
    bash scripts/ci-e2e-build.sh source "$(uname -m)" "$work"
    telemetry_privilege=()
    if [ "$(id -u)" -ne 0 ]; then telemetry_privilege=(sudo -n); fi
    "${telemetry_privilege[@]}" env REQUIRE_TELEMETRY_NETNS=1 "$work/telemetry.test" -test.v -test.timeout=90s -test.run='^TestOTLPProxyNetNS'
fi
