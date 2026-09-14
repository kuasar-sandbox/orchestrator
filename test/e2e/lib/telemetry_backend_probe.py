#!/usr/bin/env python3
"""Exercise a shipped node-ctl against the harness's real disposable backends.

This trusted infrastructure OTLP fixture and private query UDS test the installed
Collector/exporter/Reader/E2B path. Guest identity and conductor authorization are
covered separately by the real guest cases in the same owner suite.
"""

import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid


FIELDS = ["cpuCount", "cpuUsedPct", "memTotal", "memUsed", "memCache", "diskTotal", "diskUsed"]
METRICS = ["sandbox.cpu.count", "sandbox.cpu.used", "sandbox.memory.total",
           "sandbox.memory.used", "sandbox.memory.cache", "sandbox.disk.total", "sandbox.disk.used"]
UNITS = ["{cpu}", "%", "By", "By", "By", "By", "By"]
FIRST = [2, 12.5, 2048, 1100, 120, 8192, 700]
SECOND = [1, 22.25, 1024, 900, 150, 4096, 800]
THIRD = [3, 5.25, 4096, 2100, 220, 16384, 1400]


class UnixHTTPConnection(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.host)


def query(path, sid, start, end):
    connection = UnixHTTPConnection(str(path), timeout=10)
    try:
        connection.request("GET", f"/sandboxes/{sid}/metrics?start={start}&end={end}", headers={"Host": "localhost"})
        response = connection.getresponse()
        body = response.read()
        assert response.status == 200, (response.status, body)
        return json.loads(body)
    finally:
        connection.close()


def resource(sid, stable, source, samples):
    metrics = []
    for index, (name, unit) in enumerate(zip(METRICS, UNITS)):
        points = [{"timeUnixNano": str(stamp * 1_000_000_000), "asDouble": values[index]}
                  for stamp, values in samples if index < len(values)]
        metrics.append({"name": name, "unit": unit, "gauge": {"dataPoints": points}})
    identity = {"sandbox.id": sid, "sandbox.stable_id": stable, "sandbox.telemetry.source": source}
    return {"resource": {"attributes": [{"key": key, "value": {"stringValue": value}}
                                          for key, value in identity.items()]},
            "scopeMetrics": [{"metrics": metrics}]}


def expected(stamp, values):
    return {"timestampUnix": stamp, **dict(zip(FIELDS, values))}


def observations(rows):
    return [{key: row[key] for key in ["timestampUnix", *FIELDS]} for row in rows]


def run_backend(binary, output, kind, endpoint):
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


if __name__ == "__main__":
    def terminate(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, terminate)
    executable, evidence, prometheus, clickhouse = sys.argv[1:]
    for backend, address in [("prometheus", prometheus), ("clickhouse", clickhouse)]:
        run_backend(os.path.abspath(executable), Path(evidence), backend, address)
