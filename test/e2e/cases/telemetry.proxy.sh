#!/usr/bin/env bash
set -euo pipefail
: "${BIN:?BIN must point to prepared products}"
: "${WORK:?WORK must be provided by the E2E runner}"
: "${E2E_LIB:?E2E_LIB must point to prepared helpers}"
. "$E2E_LIB/orchestrator/proxy.sh"
. "$E2E_LIB/orchestrator/proxy_case.sh"
. "$E2E_LIB/orchestrator/case_workspace.sh"

NODE="$BIN/node-ctl"
[ -x "$NODE" ] || { echo "missing prepared node-ctl" >&2; exit 1; }
command -v curl >/dev/null
command -v python3 >/dev/null
case_workspace_init

DATA_PORT="$(proxy_case_free_port)"
API_PORT="$(proxy_case_free_port)"
METRICS_PORT="$(proxy_case_free_port)"
CONFIG_SOCKET="$WORK/config.sock"
STATS="$WORK/proxy-stats.sock"
ROUTES="$WORK/routes.shm"
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/units"

cat >"$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
EOF
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

CONDUCTOR=""
PROXY=""
cleanup() {
    set +e
    [ -n "$PROXY" ] && stop_proxy "$PROXY"
    if [ -n "$CONDUCTOR" ]; then kill -TERM "$CONDUCTOR" 2>/dev/null; wait "$CONDUCTOR" 2>/dev/null || true; fi
    case_workspace_cleanup
}
trap cleanup EXIT

"$NODE" conductor serve --config "$WORK/conductor.yaml" >"$WORK/conductor.log" 2>&1 &
CONDUCTOR=$!
proxy_case_wait_http "$CONDUCTOR" "http://127.0.0.1:$API_PORT/health" api.sandboxes.e2e.local "$WORK/conductor.log" ||
    { echo "conductor not ready" >&2; exit 1; }

write_proxy_config "$WORK/proxy.yaml" "$CONFIG_SOCKET" "$WORK/run"     "127.0.0.1:$DATA_PORT" - "$STATS" "$ROUTES" 128 2 enforce 30s     "127.0.0.1:$METRICS_PORT"
start_proxy "$NODE" "$WORK/proxy.yaml" "$WORK/proxy.log"
PROXY=$PROXY_HELPER_PID
proxy_case_wait_data "$PROXY" 127.0.0.1 "$DATA_PORT" "$STATS" "$WORK/proxy.log" || exit 1

notfound_total() {
    python3 - "$1" <<'PY'
import math
import re
import sys

total = 0.0
with open(sys.argv[1], encoding="utf-8") as stream:
    for line in stream:
        match = re.fullmatch(
            r'data_requests_total\{([^}]*)\}\s+(\S+)(?:\s+\S+)?',
            line.strip(),
        )
        if match and re.search(r'(?:^|,)\s*result="notfound"(?:,|$)', match[1]):
            value = float(match[2])
            if not math.isfinite(value) or value < 0:
                raise SystemExit("invalid notfound counter")
            total += value
print(total)
PY
}

# Readiness traffic can increment badrequest. Only the target notfound series
# may satisfy this assertion, and it must grow beyond the pre-request baseline.
metrics_ready=0
deadline=$((SECONDS + 20))
while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS --noproxy '*' "http://127.0.0.1:$METRICS_PORT/metrics" >"$WORK/metrics.before" 2>/dev/null; then
        baseline="$(notfound_total "$WORK/metrics.before")"
        metrics_ready=1
        break
    fi
    kill -0 "$PROXY" 2>/dev/null || { cat "$WORK/proxy.log" >&2; exit 1; }
    sleep 0.2
done
[ "$metrics_ready" = 1 ] || { echo "proxy master metrics did not become ready" >&2; exit 1; }

code="$(curl -sS --noproxy '*' -o "$WORK/data.body" -w '%{http_code}'     -H 'Host: 49983-unknown.sandboxes.e2e.local'     "http://127.0.0.1:$DATA_PORT/health")"
[ "$code" = 404 ] || { cat "$WORK/data.body"; echo "missing route returned $code" >&2; exit 1; }

deadline=$((SECONDS + 20))
while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS --noproxy '*' "http://127.0.0.1:$METRICS_PORT/metrics" >"$WORK/metrics.out" 2>/dev/null; then
        current="$(notfound_total "$WORK/metrics.out")"
        if python3 -c 'import sys; sys.exit(0 if float(sys.argv[2]) > float(sys.argv[1]) else 1)' "$baseline" "$current"; then
            echo "PASS telemetry.proxy.sh"
            exit 0
        fi
    fi
    kill -0 "$PROXY" 2>/dev/null || { cat "$WORK/proxy.log" >&2; exit 1; }
    sleep 0.2
done
cat "$WORK/proxy.log" >&2
echo "proxy master notfound counter did not increase beyond $baseline" >&2
exit 1
