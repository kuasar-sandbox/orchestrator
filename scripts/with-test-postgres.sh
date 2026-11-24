#!/usr/bin/env bash
# Compiler/source integration owns its disposable DB; E2E never downloads images.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ "${1:-}" != -- ] || shift
[ "$#" -gt 0 ] || { echo 'usage: with-test-postgres.sh -- COMMAND [ARGUMENTS...]' >&2; exit 2; }
export ORCHESTRATOR_POSTGRES_IMAGE="${ORCHESTRATOR_POSTGRES_IMAGE:-postgres:17-bookworm@sha256:1eff563f6c868b9b433566e9f3cf6e5c79fbd3b95bf70ec7277d376cdbfc50d7}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/cluster-pg-XXXXXX")"
TEST_POSTGRES_CONTAINER=""
TEST_POSTGRES_NETWORK=""
. "$ROOT/test/e2e/lib/postgres_fixture.sh"
cleanup() {
    local result=$?
    trap - EXIT
    stop_test_postgres || { [ "$result" != 0 ] || result=1; }
    rm -rf -- "$WORK"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if ! docker image inspect "$ORCHESTRATOR_POSTGRES_IMAGE" >/dev/null 2>&1; then
    # One bounded source-stage preparation attempt; never silently substitute an
    # unpinned image or drop the PostgreSQL backend when its input is unavailable.
    if ! timeout --kill-after=5s 180 docker pull "$ORCHESTRATOR_POSTGRES_IMAGE" >"$WORK/pull.private" 2>&1; then
        echo 'FAIL: pinned PostgreSQL test image preparation failed' >&2
        exit 1
    fi
fi
start_test_postgres || { echo 'FAIL: isolated PostgreSQL transaction readiness failed' >&2; exit 1; }
echo 'PostgreSQL ready (isolated database; committed read/write checked)'
"$@"
