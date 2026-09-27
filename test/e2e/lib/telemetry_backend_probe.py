#!/usr/bin/env python3
"""OTLP fixture encoding and Unix HTTP query primitives."""

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
