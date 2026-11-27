#!/usr/bin/env bash
# Required source/race/vet and privileged Collector regressions, separate from E2E.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
bash test/source/go_consumer.sh
# Validate the required Builder contracts from this ordinary run, including
# every started subcase, instead of executing the same tests a third time.
go test -json -count=1 -timeout=5m ./... \
    | python3 -B test/source/builder_state.py --results -
# Give each race-instrumented package the source Workbench's two CPUs. Tests
# retain their own goroutine/subprocess concurrency and per-package deadline.
CGO_ENABLED=1 go test -race -p=1 -count=1 -timeout=5m ./...
# Native repositories and command wiring run against an isolated SSD database,
# through the same pinned loader and client runtime as the shipped executable.
python3 -B scripts/with-test-fdb.py -- \
    python3 -B scripts/cluster-native.py test --arch "$(uname -m)" -- \
    -race -p=1 -count=1 -timeout=5m
go vet ./...
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-environment-tools.py
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-telemetry-image-pull.py
bash test/source/runtask_privilege.sh
bash test/source/vmm_cgroup.sh
REQUIRE_BUILDER=1 bash test/source/builder_unit_upgrade.sh
bash test/source/resource_pool_units.sh
bash test/source/capture_cli.sh
bash test/source/cluster_fixture_configs.sh
# All 13 topology cases keep private databases and fault state. Only immutable
# CLI bytes are reused, inside this invocation's exact source/dependency workspace.
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
BIN="$work/cluster-native" CLUSTER_STUB_BACKEND=foundationdb CLUSTER_STUB_BUILD_ONLY=1 bash test/source/cluster_stub.sh
BIN="$work/cluster-pure" CLUSTER_STUB_BACKEND=sqlite CLUSTER_STUB_BUILD_ONLY=1 bash test/source/cluster_stub.sh
BIN="$work/cluster-native" CLUSTER_STUB_BACKEND=foundationdb CLUSTER_STUB_PREBUILT=1 REQUIRE_CLUSTER_STUB=1 bash test/source/cluster_stub.sh
BIN="$work/cluster-pure" CLUSTER_STUB_PREBUILT=1 CLUSTER_STUB_BACKEND=sqlite REQUIRE_CLUSTER_STUB=1 bash test/source/cluster_stub.sh
# Provision real PostgreSQL explicitly: these backend contracts must never
# silently skip because the DSN was absent from the required source gate.
BIN="$work/cluster-pure" CLUSTER_STUB_PREBUILT=1 bash scripts/with-test-postgres.sh -- bash -euc '
    test -n "$STORAGE_TEST_POSTGRES_DSN"
    CGO_ENABLED=1 go test -race -p=1 -count=1 -timeout=5m \
        ./internal/storage/sqlengine ./internal/cluster/metastore ./internal/registry
    CLUSTER_STUB_BACKEND=postgres REQUIRE_CLUSTER_STUB=1 bash test/source/cluster_stub.sh
'
bash test/source/telemetry_backends.sh
# Helper/source regressions stay in this compiler-capable gate, not in the
# prepared product E2E runner. Discover the maintained helper tests as one set so
# execute/resource/cluster/capture regressions cannot disappear during cutover.
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_*.py' -v
bash scripts/ci-e2e-build.sh source "$(uname -m)" "$work"
telemetry_privilege=()
if [ "$(id -u)" -ne 0 ]; then telemetry_privilege=(sudo -n); fi
"${telemetry_privilege[@]}" env REQUIRE_TELEMETRY_NETNS=1 "$work/telemetry.test" -test.v -test.timeout=90s -test.run='^TestOTLPProxyNetNS'
