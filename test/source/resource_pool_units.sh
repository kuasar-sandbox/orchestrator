#!/usr/bin/env bash
# Compile and run the isolated actual-systemd resource-pool placement fixture.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
work="$(mktemp -d)"
trap 'rm -rf -- "$work"' EXIT
go test -c -o "$work/resource-pools.test" ./internal/orch
privilege=()
if [ "$(id -u)" -ne 0 ]; then privilege=(sudo -n); fi
"${privilege[@]}" env REQUIRE_RESOURCE_POOL_SYSTEMD=1 \
  RESOURCE_POOL_NUMA_NODES="${RESOURCE_POOL_NUMA_NODES:-0}" \
  "$work/resource-pools.test" -test.v -test.timeout=90s -test.run=TestResourcePoolSystemdPlacement
