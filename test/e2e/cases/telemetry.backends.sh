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
PYTHONPATH="$E2E_LIB/orchestrator" python3 - "$NODE" "$OUT" "$PROM_URL" "$CLICK_URL" <<'PY_BACKENDS'
from telemetry_backend_probe import *
def terminate(signum, _frame):
    raise SystemExit(128 + signum)
signal.signal(signal.SIGTERM, terminate)
binary, evidence, prometheus, clickhouse = sys.argv[1:]
output = Path(evidence)
for kind, endpoint in [("prometheus", prometheus), ("clickhouse", clickhouse)]:
    token = uuid.uuid4().hex
    sid, other, stable = "backend-" + token, "other-" + token, "stable-" + token
    stamp = int(time.time()) // 5 * 5 - 60
    with tempfile.TemporaryDirectory(prefix="telemetry-probe-", dir="/tmp") as directory:
        work = Path(directory)
        api = work / "query.sock"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        exporter = "prometheusremotewrite" if kind == "prometheus" else "clickhouse"
        cfg = {"config_socket": str(work / "conductor.sock"), "api_socket": str(api),
               "local": {"enabled": False, "path": str(work / "must-not-exist")},
               "query": {"backend": kind, "handler": "e2b", kind: {"endpoint": endpoint}},
               "collector": {
                   "receivers": {"otlp/fixture": {"protocols": {"http": {"endpoint": f"127.0.0.1:{port}"}}}},
                   "processors": {"batch": {"timeout": "200ms"}},
                   "exporters": {},
                   "service": {"telemetry": {"metrics": {"level": "none"}}, "pipelines": {
                       "metrics": {"receivers": ["otlp/fixture"], "processors": ["batch"], "exporters": [exporter]}}}}}
        if kind == "prometheus":
            cfg["query"]["e2b"] = {"metrics": dict(zip(FIELDS, [name.replace(".", "_") for name in METRICS]))}
            cfg["collector"]["exporters"][exporter] = {
                "endpoint": endpoint + "/api/v1/write", "add_metric_suffixes": False,
                "resource_to_telemetry_conversion": {"enabled": True}, "target_info": {"enabled": False},
                "remote_write_queue": {"enabled": True, "num_consumers": 1}}
        else:
            tables = {name: "probe_" + token + "_" + name for name in
                      ["gauge", "sum", "summary", "histogram", "exponential_histogram"]}
            cfg["query"][kind]["tables"] = tables
            cfg["collector"]["exporters"][exporter] = {
                "endpoint": endpoint, "database": "default", "create_schema": True,
                "metrics_tables": {name: {"name": table} for name, table in tables.items()},
                "sending_queue": {"enabled": True, "num_consumers": 1}}
        config_file = work / "telemetry.json"
        config_file.write_text(json.dumps(cfg), encoding="utf-8")
        (output / f"{kind}-probe-config.json").write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        payload = {"resourceMetrics": [
            resource(sid, stable, "envd", [(stamp - 1, [99999] * 7), (stamp + 1, FIRST),
                                         (stamp + 4, SECOND), (stamp + 5, THIRD), (stamp + 11, FIRST[:-1])]),
            resource(other, stable, "envd", [(stamp + 1, [v * 2 for v in FIRST])]),
            resource(sid, stable, "otlp", [(stamp + 1, [99999] * 7)])]}
        (output / f"{kind}-probe-input.json").write_text(json.dumps(payload), encoding="utf-8")
        with (output / f"{kind}-probe.log").open("w") as log:
            process = subprocess.Popen([binary, "telemetry", "serve", "--config", str(config_file)],
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + 45
                while True:
                    assert process.poll() is None, "node-ctl exited before OTLP acceptance"
                    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                    try:
                        connection.request("POST", "/v1/metrics", json.dumps(payload), {"Content-Type": "application/json"})
                        response = connection.getresponse()
                        body = response.read()
                        assert response.status == 200, (response.status, body)
                        assert not json.loads(body).get("partialSuccess", {}).get("rejectedDataPoints", 0), body
                        break
                    except OSError:
                        assert time.monotonic() < deadline, "OTLP receiver did not become ready"
                        time.sleep(0.1)
                    finally:
                        connection.close()
                wanted = [expected(stamp, [max(a, b) for a, b in zip(FIRST, SECOND)]), expected(stamp + 5, THIRD)]
                while True:
                    assert process.poll() is None, "node-ctl exited during remote query"
                    if not api.exists():
                        assert time.monotonic() < deadline, "query UDS did not become ready"
                        time.sleep(0.1)
                        continue
                    rows = query(api, sid, stamp, stamp + 12)
                    if observations(rows) == wanted:
                        break
                    assert time.monotonic() < deadline, ("remote write/read did not converge", rows, wanted)
                    time.sleep(0.2)
                assert observations(query(api, other, stamp, stamp + 12)) == [expected(stamp, [v * 2 for v in FIRST])]
                assert query(api, stable, stamp, stamp + 12) == [], "StableID became a query alias"
                assert query(api, "unknown-" + token, stamp, stamp + 12) == [], "exact SID isolation failed"
                assert query(api, sid, stamp + 10, stamp + 12) == [], "incomplete bucket invented missing data"
                assert query(api, sid, stamp + 20, stamp + 25) == [], "last Gauge was extended"
                assert observations(query(api, sid, stamp + 5, stamp + 5)) == [expected(stamp + 5, THIRD)], "inclusive boundary changed"
                assert not (work / "must-not-exist").exists(), "remote mode created a local TSDB"
                (output / f"{kind}-probe-result.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise AssertionError("node-ctl did not shut down cleanly")
                assert process.returncode == 0, f"node-ctl exited with {process.returncode}; see {kind}-probe.log"
        assert not api.exists(), "query UDS leaked after shutdown"
    print(f"PASS telemetry/backend/InstalledBinary/{kind}: native OTLP -> batch/queue -> real exporter -> Reader -> E2B; exact SID, source, MAX, gaps, no local DB, cleanup", flush=True)

PY_BACKENDS
echo "PASS telemetry.backends.sh"
