#!/usr/bin/env bash
set -euo pipefail
: "${BIN:?}"
: "${WORK:?}"
: "${E2E_LIB:?}"
. "$E2E_LIB/orchestrator/proxy.sh"
. "$E2E_LIB/orchestrator/proxy_case.sh"
NODE="$BIN/node-ctl"
[ -x "$NODE" ] || { echo "missing prepared node-ctl" >&2; exit 1; }
command -v curl >/dev/null
mkdir -p "$WORK/run"
PORT="$(proxy_case_free_port)"
STATS="$WORK/proxy-stats.sock"
CONFIG_SOCKET="$WORK/config.sock"
ROUTES="$WORK/routes.shm"
LOG="$WORK/proxy.log"

# A proxy cannot register without a conductor plugin endpoint. This focused case
# uses the real conductor with no sandbox creation; it validates the independent
# data listener/WorkerExtension boundary without KVM or a template build.
API_PORT="$(proxy_case_free_port)"
mkdir -p "$WORK/lib" "$WORK/units"
cat >"$WORK/conductor.yaml" <<EOF
api: { domain: sandboxes.e2e.local, listen: "127.0.0.1:$API_PORT" }
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
proxy: { auth: enforce }
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $CONFIG_SOCKET }
units: { dir: $WORK/units, install: false }
sandbox:
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle }
EOF
printf 'manifest: { key: "" }\n' >"$WORK/manifest.yaml"
"$NODE" conductor serve --config "$WORK/conductor.yaml" >"$WORK/conductor.log" 2>&1 &
CONDUCTOR=$!
PROXY=""
cleanup() { set +e; [ -n "$PROXY" ] && stop_proxy "$PROXY"; kill "$CONDUCTOR" 2>/dev/null; wait "$CONDUCTOR" 2>/dev/null || true; }
trap cleanup EXIT
proxy_case_wait_http "$CONDUCTOR" "http://127.0.0.1:$API_PORT/health" api.sandboxes.e2e.local "$WORK/conductor.log"     || { echo "conductor not ready" >&2; exit 1; }

write_proxy_config "$WORK/proxy.yaml" "$CONFIG_SOCKET" "$WORK/run" "127.0.0.1:$PORT" - "$STATS" "$ROUTES" 128 1 enforce 30s -
start_proxy "$NODE" "$WORK/proxy.yaml" "$LOG"
PROXY=$PROXY_HELPER_PID
proxy_case_wait_data "$PROXY" 127.0.0.1 "$PORT" "$STATS" "$LOG" || exit 1

code="$(curl -sS --noproxy '*' -o "$WORK/data.body" -w '%{http_code}' -H 'Host: 49983-unknown.sandboxes.e2e.local' "http://127.0.0.1:$PORT/health")"
[ "$code" = 401 ] || { cat "$WORK/data.body"; echo "data listener did not enforce WorkerExtension auth: $code" >&2; exit 1; }

code="$(curl -sS --noproxy '*' -o "$WORK/api-data.body" -w '%{http_code}' -H 'Host: 49983-unknown.sandboxes.e2e.local' "http://127.0.0.1:$API_PORT/health")"
[ "$code" = 404 ] || { cat "$WORK/api-data.body"; echo "conductor incorrectly served data host: $code" >&2; exit 1; }

echo "PASS orchestrator.proxy.sh"
