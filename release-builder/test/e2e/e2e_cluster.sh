#!/usr/bin/env bash
#
# Umbrella cluster e2e entry.
#
# This delegates to orchestrator's cluster stub suite while using the
# binaries produced by this repo's `make build`. The delegated suite starts real
# cluster-ctl registry/router/placer processes and node-stub-ctl; node-stub-ctl
# simulates node-link, key distribution, sandbox/build commands, node reboot and
# data forwarding without launching microVMs.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORG_ROOT="$(cd "$REPO_ROOT/../.." && pwd)"
HOST_ARCH="$(uname -m)"
case "$HOST_ARCH" in
  amd64) HOST_ARCH=x86_64 ;;
  arm64) HOST_ARCH=aarch64 ;;
esac

BIN="${BIN:-$REPO_ROOT/bin/$HOST_ARCH}"
ORCH_BIN="$ORG_ROOT/orchestrator/bin/$HOST_ARCH"
ORCH_E2E="$ORG_ROOT/orchestrator/test/e2e/e2e_cluster_stub.sh"
REQUIRE_CLUSTER_STUB="${REQUIRE_CLUSTER_STUB:-${REQUIRE_EXEC:-0}}"

skip() {
  echo "==> e2e_cluster: skipping ($*)" >&2
  if [ "$REQUIRE_CLUSTER_STUB" = "1" ]; then
    exit 1
  fi
  exit 0
}

[ -f "$ORCH_E2E" ] || skip "missing orchestrator cluster stub suite at $ORCH_E2E"

if [ ! -x "$BIN/cluster-ctl" ] || [ ! -x "$BIN/node-stub-ctl" ] || [ ! -x "$BIN/e2b-key-ctl" ]; then
  if [ -x "$ORCH_BIN/cluster-ctl" ] && [ -x "$ORCH_BIN/node-stub-ctl" ] && [ -x "$ORCH_BIN/e2b-key-ctl" ]; then
    echo "==> e2e_cluster: BIN=$BIN missing cluster e2e binaries; falling back to $ORCH_BIN" >&2
    BIN="$ORCH_BIN"
  fi
fi

echo "==> e2e_cluster: running orchestrator cluster stub suite" >&2
echo "==> e2e_cluster: BIN=$BIN" >&2

BIN="$BIN" REQUIRE_CLUSTER_STUB="$REQUIRE_CLUSTER_STUB" bash "$ORCH_E2E"
