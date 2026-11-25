#!/usr/bin/env bash
# Parse the actual generated E2E Registry configuration with the product CLI.
# The database startup seams are replaced because config validation must not
# connect to storage; the YAML generator and parser remain the real ones.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
CLUSTER_CTL="${CLUSTER_CTL:-$WORK/cluster-ctl}"
if [ ! -x "$CLUSTER_CTL" ]; then
    (cd "$ROOT" && CGO_ENABLED=0 go build -trimpath -o "$CLUSTER_CTL" ./cmd/cluster-ctl)
fi
fail() { printf 'FAIL: generated cluster fixture configuration\n' >&2; exit 1; }
free_port() { printf '17700\n'; }
. "$ROOT/test/e2e/lib/cluster_process.sh"
start_cluster_fdb() {
    CLUSTER_FDB_FILE="$WORK/fdb.cluster"
    CLUSTER_FDB_PREFIX=/fixture/
}
start_test_postgres() {
    export STORAGE_TEST_POSTGRES_DSN=postgres://fixture:fixture@127.0.0.1:5432/fixture?sslmode=disable
}
for CLUSTER_STORAGE_BACKEND in foundationdb sqlite postgres; do
    write_registry_configs 1
    "$CLUSTER_CTL" config registry --config "$WORK/registry-1.yaml" \
        -o "$WORK/normalized-$CLUSTER_STORAGE_BACKEND.yaml" >"$WORK/parse.log" 2>&1 || fail
done
# A spelling that used to pass mocked generator tests must fail the real parser.
sed 's/backend: postgres/backend: foundationdb/' "$WORK/registry-1.yaml" >"$WORK/invalid.yaml"
if "$CLUSTER_CTL" config registry --config "$WORK/invalid.yaml" >"$WORK/invalid.log" 2>&1; then fail; fi
printf 'PASS: generated FDB, SQLite and PostgreSQL fixture configs parse with cluster-ctl\n'
