#!/usr/bin/env bash
set -euo pipefail

: "${BIN:?BIN must point to the prepared platform binary directory}"
: "${WORK:?WORK must be provided by the platform E2E runner}"
: "${E2E_LIB:?E2E_LIB must point to prepared E2E helpers}"
NODE="$BIN/node-ctl"
PROBE="$E2E_LIB/orchestrator/telemetry_backend_probe.py"
[ -x "$NODE" ] || { echo "missing prepared product: $NODE" >&2; exit 1; }
[ -f "$PROBE" ] || { echo "missing prepared telemetry probe: $PROBE" >&2; exit 1; }
for tool in docker curl python3; do command -v "$tool" >/dev/null || { echo "missing prerequisite: $tool" >&2; exit 1; }; done
docker info >/dev/null
OUT="$WORK/backends"
mkdir -p "$OUT"
: "${TELEMETRY_PROMETHEUS_IMAGE:?prepared Prometheus image is required}"
: "${TELEMETRY_CLICKHOUSE_IMAGE:?prepared ClickHouse image is required}"
PROM="$TELEMETRY_PROMETHEUS_IMAGE"
CLICK="$TELEMETRY_CLICKHOUSE_IMAGE"
docker image inspect "$PROM" "$CLICK" >"$OUT/images.json" || { echo "prepared telemetry backend images are required" >&2; exit 1; }
PROM_ID=""; CLICK_ID=""; NET=""
cleanup() {
  set +e
  [ -n "$PROM_ID" ] && docker logs "$PROM_ID" >"$OUT/prometheus.log" 2>&1 || true
  [ -n "$CLICK_ID" ] && docker logs "$CLICK_ID" >"$OUT/clickhouse.log" 2>&1 || true
  [ -n "$PROM_ID" ] && docker rm -fv "$PROM_ID" >/dev/null || true
  [ -n "$CLICK_ID" ] && docker rm -fv "$CLICK_ID" >/dev/null || true
  [ -n "$NET" ] && docker network rm "$NET" >/dev/null || true
}
trap cleanup EXIT
NET="$(docker network create --driver bridge --label kuasar.test=telemetry-backends "telemetry-backends-$(python3 -c 'import uuid;print(uuid.uuid4().hex)')")"
cat >"$OUT/prometheus.yml" <<'YAML'
global:
  scrape_interval: 60s
scrape_configs: []
storage:
  tsdb:
    out_of_order_time_window: 5m
YAML
PROM_ID="$(docker create --network "$NET" --label kuasar.test=telemetry-backends -p 127.0.0.1::9090 "$PROM" --config.file=/etc/prometheus/prometheus.yml --web.enable-remote-write-receiver --storage.tsdb.retention.time=1h --enable-feature=native-histograms)"
docker cp "$OUT/prometheus.yml" "$PROM_ID:/etc/prometheus/prometheus.yml"
docker start "$PROM_ID" >/dev/null
CLICK_ID="$(docker create --network "$NET" --label kuasar.test=telemetry-backends -p 127.0.0.1::8123 --ulimit nofile=262144:262144 -e CLICKHOUSE_SKIP_USER_SETUP=1 "$CLICK")"
docker start "$CLICK_ID" >/dev/null
PROM_URL="http://$(docker port "$PROM_ID" 9090/tcp)"
CLICK_URL="http://$(docker port "$CLICK_ID" 8123/tcp)"
for endpoint in "$PROM_URL/-/ready" "$CLICK_URL/ping"; do
  ready=0; for _ in $(seq 1 100); do curl --noproxy '*' -fsS --max-time 1 "$endpoint" >/dev/null && { ready=1; break; }; sleep 0.2; done
  [ "$ready" = 1 ] || { echo "backend not ready: $endpoint" >&2; exit 1; }
done
curl --noproxy '*' -fsS "$PROM_URL/api/v1/status/buildinfo" >"$OUT/prometheus-version.json"
curl --noproxy '*' -fsS --get --data-urlencode 'query=SELECT version()' "$CLICK_URL" >"$OUT/clickhouse-version.txt"
sha256sum "$NODE" >"$OUT/probe-executable.txt"
python3 "$PROBE" "$NODE" "$OUT" "$PROM_URL" "$CLICK_URL"
echo "PASS telemetry.backends.sh"
