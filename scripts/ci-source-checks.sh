#!/usr/bin/env bash
# Required source/race/vet and privileged Collector regressions, separate from E2E.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
go test -count=1 -timeout=5m ./...
CGO_ENABLED=1 go test -race -count=1 -timeout=5m ./...
go vet ./...
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p test_orchestrator_proxy_go.py -v
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
bash scripts/ci-e2e-build.sh source "$(uname -m)" "$work"
telemetry_privilege=()
if [ "$(id -u)" -ne 0 ]; then telemetry_privilege=(sudo -n); fi
"${telemetry_privilege[@]}" env REQUIRE_TELEMETRY_NETNS=1 "$work/telemetry.test" -test.v -test.timeout=90s -test.run='^TestOTLPProxyNetNS'
TELEMETRY_SOURCE_ROOT="$ROOT" bash test/e2e/e2e_telemetry_backends.sh
