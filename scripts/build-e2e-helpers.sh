#!/usr/bin/env bash
# Complete declared test-helper inventory; invoked only in the source-build stage.
set -euo pipefail
[ "$#" = 2 ] || { echo 'usage: build-e2e-helpers.sh ARCH OUTPUT' >&2; exit 2; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
case "$1" in x86_64) export GOARCH=amd64 ;; aarch64) export GOARCH=arm64 ;; *) exit 2 ;; esac
export GOOS=linux CGO_ENABLED=0
source "$ROOT/scripts/ci-e2e-build.sh"
bash "$ROOT/scripts/ci-e2e-build.sh" fixtures "$1" "$2"
# Retain the admitted workspace: node-ctl tests consume the selected SDK sources.
e2e_go -C "$ROOT" test -c -trimpath -o "$2/node-ctl-runner-test" ./cmd/node-ctl
e2e_go -C "$ROOT" build -trimpath -o "$2/proxy-lifecycle" ./test/e2e/proxy-lifecycle
e2e_go -C "$ROOT" build -trimpath -ldflags '-X main.buildIdentity=target' \
    -o "$2/proxy-lifecycle-target" ./test/e2e/proxy-lifecycle
e2e_go -C "$ROOT" build -trimpath -ldflags '-X main.version=inplace-target' \
    -o "$2/node-ctl-inplace-target" ./cmd/node-ctl
e2e_go -C "$ROOT" build -trimpath -ldflags '-buildid=orchestrator-inplace-target' \
    -o "$2/custom-proxy-inplace-target" ./examples/custom-proxy
