#!/usr/bin/env bash
# Required source/race/vet and privileged Collector regressions, separate from E2E.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
go test -count=1 -timeout=5m ./...
CGO_ENABLED=1 go test -race -count=1 -timeout=5m ./...
go vet ./...
bash test/source/runtask_privilege.sh
bash test/source/vmm_cgroup.sh
REQUIRE_BUILDER=1 bash test/source/builder_unit_upgrade.sh
bash test/source/journal_contract.sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p test_orchestrator_proxy_go.py -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p test_capture_cli.py -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p test_placer_readiness.py -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p test_registry_redirect.py -v
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
bash scripts/ci-e2e-build.sh source "$(uname -m)" "$work"
telemetry_privilege=()
if [ "$(id -u)" -ne 0 ]; then telemetry_privilege=(sudo -n); fi
"${telemetry_privilege[@]}" env REQUIRE_TELEMETRY_NETNS=1 "$work/telemetry.test" -test.v -test.timeout=90s -test.run='^TestOTLPProxyNetNS'
TELEMETRY_SOURCE_ROOT="$ROOT" bash test/e2e/e2e_telemetry_backends.sh
