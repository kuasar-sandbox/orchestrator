#!/usr/bin/env bash
# Build test-only executables once, before preparing an immutable E2E workspace.
set -euo pipefail
skip() { echo "==> build fixture: skipping ($*)"; [ "${REQUIRE_PROXY:-0}" = "1" ] && { echo "REQUIRE_PROXY=1; failing" >&2; exit 1; }; exit 0; }

e2e_go() {
    # sudo may reset PATH while preserving the explicitly selected distribution.
    # Keep its driver/compiler paired; an invalid explicit GOROOT must fail.
    "${KUASAR_E2E_GO:-${GOROOT:+$GOROOT/bin/}go}" "$@"
}

build_custom_proxy() {
    (cd "$CUSTOM_PROXY_SOURCE_ROOT" && GOWORK=off e2e_go build -o "$CUSTOM_PROXY_BIN" ./examples/custom-proxy) \
        || skip "failed to build examples/custom-proxy"
}


build_telemetry_netns() {
    (cd "$telemetry_source" && GOWORK=off CGO_ENABLED=0 e2e_go test -c -o "$WORK/telemetry.test" ./internal/telemetry)
}

build_telemetry_grpc() {
    GOWORK=off CGO_ENABLED=0 e2e_go build -trimpath -o "$WORK/telemetry-grpc-probe" "$SCRIPT_DIR/telemetryprobe/main.go"
}

build_capture_cli() {
    (cd "$ROOT" && GOWORK=off CGO_ENABLED=0 e2e_go test -c -trimpath -o "$WORK/orch-cli.test" ./internal/orch)
}

main() {
    [ "$#" = 3 ] || { echo "usage: ci-e2e-build.sh fixtures|source|cli <x86_64|aarch64> <output>" >&2; exit 2; }
    local mode="$1" ROOT
    ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    case "$2" in x86_64) export GOARCH=amd64 ;; aarch64) export GOARCH=arm64 ;; *) exit 2 ;; esac
    export GOOS=linux CGO_ENABLED=0 REQUIRE_PROXY=1
    mkdir -p "$3"
    WORK="$(cd "$3" && pwd)"
    SCRIPT_DIR="$ROOT/test/e2e"
    CUSTOM_PROXY_SOURCE_ROOT="$ROOT"
    CUSTOM_PROXY_BIN="$WORK/custom-proxy"
    telemetry_source="$ROOT"
    case "$mode" in
        fixtures) build_custom_proxy; build_telemetry_grpc ;;
        source) build_telemetry_netns ;;
        cli) build_capture_cli ;;
        *) exit 2 ;;
    esac
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then main "$@"; fi
