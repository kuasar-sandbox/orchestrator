#!/usr/bin/env bash
# Real standard Collector exporter -> independent Reader acceptance. This case
# owns its containers and storage, and never uses a deployment database.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ci_mode=""
if [ -n "${KUASAR_CI_DIR:-}" ]; then
    ci_mode="$(python3 - "$KUASAR_CI_DIR/run.tsv" <<'PY'
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1]), delimiter='\t'))
assert len(rows) == 1 and rows[0]['mode'] in ('source', 'exact-assets'), 'invalid CI execution mode'
print(rows[0]['mode'])
PY
)"
fi
if [ -n "${BIN:-}" ]; then
    BIN="$(cd "$BIN" && pwd)"
    [ -x "$BIN/node-ctl" ] || { echo 'FAIL: BIN/node-ctl is required' >&2; exit 1; }
fi
TELEMETRY_SOURCE_ROOT="${TELEMETRY_SOURCE_ROOT:-}"
if [ "$ci_mode" = exact-assets ]; then
    # Release validation runs the shipped binary without rebuilding from any
    # adjacent checkout. The binary/backend acceptance below is still required.
    TELEMETRY_SOURCE_ROOT=""
elif [ -z "$TELEMETRY_SOURCE_ROOT" ]; then
    candidates=("$SCRIPT_DIR/../..")
    # Source CI invokes the assembled owner suite and supplies platform/bin/ARCH,
    # just as the existing custom Proxy and guest telemetry cases expect.
    if [ -n "${BIN:-}" ]; then candidates+=("$BIN/../../../orchestrator"); fi
    for candidate in "${candidates[@]}"; do
        if [ -f "$candidate/internal/telemetry/deploy_integration_test.go" ]; then
            TELEMETRY_SOURCE_ROOT="$(cd "$candidate" && pwd)"
            break
        fi
    done
fi
if [ "$ci_mode" = source ] || [ -n "$TELEMETRY_SOURCE_ROOT" ]; then
    [ -f "$TELEMETRY_SOURCE_ROOT/internal/telemetry/deploy_integration_test.go" ] \
        || { echo 'FAIL: exact orchestrator sources required (TELEMETRY_SOURCE_ROOT)' >&2; exit 1; }
    command -v go >/dev/null || { echo 'FAIL: Go is required for source validation' >&2; exit 1; }
elif [ -z "${BIN:-}" ]; then
    echo 'FAIL: BIN is required for validation without component sources' >&2
    exit 1
fi
for tool in docker curl python3 timeout; do
    command -v "$tool" >/dev/null || { echo "FAIL: $tool is required" >&2; exit 1; }
done
docker info >/dev/null
if [ -z "${TELEMETRY_BACKEND_OUT_DIR:-}" ]; then
    if [ -n "${KUASAR_CI_DIR:-}" ]; then
        TELEMETRY_BACKEND_OUT_DIR="$KUASAR_CI_DIR/telemetry-backends"
    else
        TELEMETRY_BACKEND_OUT_DIR="$(mktemp -d /tmp/telemetry-backends-XXXXXX)"
    fi
fi
mkdir -p "$TELEMETRY_BACKEND_OUT_DIR"
TELEMETRY_BACKEND_OUT_DIR="$(cd "$TELEMETRY_BACKEND_OUT_DIR" && pwd)"
prometheus_id=""
clickhouse_id=""
backend_network=""
telemetry_bin=""
cleanup() {
    local status=$?
    trap - EXIT
    for item in "prometheus:$prometheus_id" "clickhouse:$clickhouse_id"; do
        local name="${item%%:*}" id="${item#*:}"
        [ -n "$id" ] || continue
        docker logs "$id" >"$TELEMETRY_BACKEND_OUT_DIR/$name.log" 2>&1 || true
        docker inspect "$id" >"$TELEMETRY_BACKEND_OUT_DIR/$name-container.json" || true
        docker rm -fv "$id" >/dev/null || status=1
    done
    if [ -n "$backend_network" ]; then
        docker network inspect "$backend_network" >"$TELEMETRY_BACKEND_OUT_DIR/network.json" || status=1
        docker network rm "$backend_network" >/dev/null || status=1
    fi
    if [ -n "$telemetry_bin" ]; then rm -rf -- "$telemetry_bin"; fi
    echo "Telemetry backend evidence: $TELEMETRY_BACKEND_OUT_DIR"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Fixed upstream multi-platform manifests; the resolved platform image is also
# recorded. Tests use only loopback-published, disposable service endpoints.
prometheus_image='prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996'
clickhouse_image='clickhouse/clickhouse-server:25.8@sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77'
prepare_image() {
    local image="$1"
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        # Reuse the public Docker Hub transport already used by platform's
        # source and exact-assets suites. Docker verifies the unchanged pinned
        # manifest digest; this does not select another tag or backend version.
        image="m.daocloud.io/docker.io/$image"
        if ! docker image inspect "$image" >/dev/null 2>&1; then
            timeout 3m docker pull "$image" >&2 || return 1
        fi
    fi
    printf '%s\n' "$image"
}
prometheus_image="$(prepare_image "$prometheus_image")"
clickhouse_image="$(prepare_image "$clickhouse_image")"
docker image inspect "$prometheus_image" "$clickhouse_image" >"$TELEMETRY_BACKEND_OUT_DIR/images.json"
# Own the network just like the containers and data. Privileged guest suites
# may leave the daemon's default bridge absent; never repair shared docker0 or
# make a backend fixture depend on that unrelated interface's lifecycle.
backend_network="$(docker network create --driver bridge --label kuasar.test=telemetry-backends \
    "telemetry-backends-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')")"
cat >"$TELEMETRY_BACKEND_OUT_DIR/prometheus.yml" <<'YAML'
global:
  scrape_interval: 60s
scrape_configs: []
storage:
  tsdb:
    out_of_order_time_window: 5m
YAML
prometheus_id="$(docker create --network "$backend_network" --label kuasar.test=telemetry-backends -p 127.0.0.1::9090 \
    "$prometheus_image" --config.file=/etc/prometheus/prometheus.yml \
    --web.enable-remote-write-receiver --storage.tsdb.retention.time=1h \
    --enable-feature=native-histograms)"
docker cp "$TELEMETRY_BACKEND_OUT_DIR/prometheus.yml" "$prometheus_id:/etc/prometheus/prometheus.yml"
docker start "$prometheus_id" >/dev/null
# Record ownership before start so a failed start cannot leave an untracked
# ClickHouse container behind.
clickhouse_id="$(docker create --network "$backend_network" --label kuasar.test=telemetry-backends -p 127.0.0.1::8123 \
    --ulimit nofile=262144:262144 -e CLICKHOUSE_SKIP_USER_SETUP=1 "$clickhouse_image")"
docker start "$clickhouse_id" >/dev/null
export TELEMETRY_PROMETHEUS_TEST_URL="http://$(docker port "$prometheus_id" 9090/tcp)"
export TELEMETRY_CLICKHOUSE_TEST_URL="http://$(docker port "$clickhouse_id" 8123/tcp)"
for endpoint in "$TELEMETRY_PROMETHEUS_TEST_URL/-/ready" "$TELEMETRY_CLICKHOUSE_TEST_URL/ping"; do
    ready=0
    for _ in $(seq 1 100); do
        if curl --noproxy '*' --silent --fail --max-time 1 "$endpoint" >/dev/null; then ready=1; break; fi
        sleep 0.2
    done
    [ "$ready" = 1 ] || { echo "FAIL: backend did not become ready: $endpoint" >&2; exit 1; }
done
curl --noproxy '*' --silent --show-error --fail "$TELEMETRY_PROMETHEUS_TEST_URL/api/v1/status/buildinfo" >"$TELEMETRY_BACKEND_OUT_DIR/prometheus-version.json"
curl --noproxy '*' --silent --show-error --fail --get --data-urlencode 'query=SELECT version()' \
    "$TELEMETRY_CLICKHOUSE_TEST_URL" >"$TELEMETRY_BACKEND_OUT_DIR/clickhouse-version.txt"
if [ -n "$TELEMETRY_SOURCE_ROOT" ]; then
(
    cd "$TELEMETRY_SOURCE_ROOT"
    env -u GOTMPDIR GOWORK=off CGO_ENABLED=0 REQUIRE_TELEMETRY_BACKENDS=1 go test -json -count=1 -timeout=3m \
        ./internal/telemetry -run '^Test(Prometheus.*Integration|ClickHouse.*Integration|RemoteDeploymentExamplesIntegration)$'
) >"$TELEMETRY_BACKEND_OUT_DIR/tests.jsonl" 2>"$TELEMETRY_BACKEND_OUT_DIR/build.log" \
    || { cat "$TELEMETRY_BACKEND_OUT_DIR/build.log" "$TELEMETRY_BACKEND_OUT_DIR/tests.jsonl"; exit 1; }
# Sealed bootstrap requires Linux ownership/modes. Keep binaries in this
# private Linux directory even if diagnostic output is on a mounted volume.
telemetry_bin="$(mktemp -d /tmp/telemetry-bin-XXXXXX)"
export TELEMETRY_NODE_TEST_BIN="$telemetry_bin/node-ctl"
export TELEMETRY_CUSTOM_TEST_BIN="$telemetry_bin/custom-telemetry"
(
    cd "$TELEMETRY_SOURCE_ROOT"
    GOWORK=off CGO_ENABLED=0 go build -o "$TELEMETRY_NODE_TEST_BIN" ./cmd/node-ctl
    GOWORK=off CGO_ENABLED=0 go build -o "$TELEMETRY_CUSTOM_TEST_BIN" ./examples/custom-telemetry
    go version -m "$TELEMETRY_NODE_TEST_BIN" "$TELEMETRY_CUSTOM_TEST_BIN" >"$TELEMETRY_BACKEND_OUT_DIR/executables.txt"
    sha256sum "$TELEMETRY_NODE_TEST_BIN" "$TELEMETRY_CUSTOM_TEST_BIN" >>"$TELEMETRY_BACKEND_OUT_DIR/executables.txt"
) >"$TELEMETRY_BACKEND_OUT_DIR/executable-build.log" 2>&1 \
    || { cat "$TELEMETRY_BACKEND_OUT_DIR/executable-build.log"; exit 1; }
fi
# Always exercise the actual assembled/released node-ctl when BIN is supplied.
# Standalone source runs use the exact binary built above. This needs no Go or
# component sources in exact-assets mode and never substitutes a mock backend.
probe_binary="${TELEMETRY_NODE_TEST_BIN:-}"
if [ -n "${BIN:-}" ]; then probe_binary="$BIN/node-ctl"; fi
sha256sum "$probe_binary" >"$TELEMETRY_BACKEND_OUT_DIR/probe-executable.txt"
python3 "$SCRIPT_DIR/lib/telemetry_backend_probe.py" "$probe_binary" "$TELEMETRY_BACKEND_OUT_DIR" \
    "$TELEMETRY_PROMETHEUS_TEST_URL" "$TELEMETRY_CLICKHOUSE_TEST_URL"
if [ -n "$TELEMETRY_SOURCE_ROOT" ]; then
(
    cd "$TELEMETRY_SOURCE_ROOT"
    env -u GOTMPDIR GOWORK=off CGO_ENABLED=0 REQUIRE_TELEMETRY_BACKENDS=1 go test -json -count=1 -timeout=90s \
        ./internal/telemetryapp -run '^TestCustomExecutableIntegration$'
) >>"$TELEMETRY_BACKEND_OUT_DIR/tests.jsonl" 2>>"$TELEMETRY_BACKEND_OUT_DIR/build.log" \
    || { cat "$TELEMETRY_BACKEND_OUT_DIR/build.log" "$TELEMETRY_BACKEND_OUT_DIR/tests.jsonl"; exit 1; }
python3 - "$TELEMETRY_BACKEND_OUT_DIR/tests.jsonl" <<'PY'
import json, sys
events = [json.loads(line) for line in open(sys.argv[1])]
assert not any(e['Action'] in ('skip', 'fail') for e in events), 'no skip/failure counts as backend acceptance'
passed = {e.get('Test') for e in events if e['Action'] == 'pass'}
required = {'TestPrometheusIntegration', 'TestClickHouseIntegration',
            'TestPrometheusMetricKindsIntegration', 'TestCustomExecutableIntegration',
            'TestClickHouseMetricKindsIntegration', 'TestRemoteDeploymentExamplesIntegration/prometheus',
            'TestRemoteDeploymentExamplesIntegration/clickhouse'}
assert required <= passed, f'missing backend cases: {required - passed}'
for name in sorted(passed - {None}):
    print('PASS telemetry/backend/' + name)
PY
fi
