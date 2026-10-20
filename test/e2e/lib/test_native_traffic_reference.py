"""Exercise the shipped reference flow against a local HTTP server, without a VM."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
import unittest


LIB = Path(__file__).resolve().parent
SOURCE = (LIB.parent / "cases/telemetry.guest.sh").read_text()
FLOW = re.search(r"(?ms)^    for section in resource traffic; do\n.*?^    done\n", SOURCE).group()
REQ = re.search(r"(?ms)^req\(\) \{\n.*?^\}", (LIB / "execute_client.sh").read_text()).group()
BODIES = {"resource": b'{"kind":"resource","value":456}\n',
          "traffic": b'{"kind":"traffic","value":123}\n'}


class NativeTrafficReference(unittest.TestCase):
    def run_case(self, statuses=(200,), *, mode="", resource_status=200,
                 wrong_body=False, fail_export=False, flow=FLOW):
        requests = []
        release = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                section = self.path.rsplit("/", 1)[-1]
                requests.append((self.path, dict(self.headers)))
                if section == "traffic":
                    if mode == "hang":
                        release.wait(15)
                        return
                    if mode == "disconnect":
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    index = sum(path.endswith("/traffic") for path, _ in requests) - 1
                    status = statuses[min(index, len(statuses) - 1)]
                else:
                    status = resource_status
                body = BODIES[section] if status == 200 else b'{"error":"fixture unavailable"}\n'
                if wrong_body and section == "traffic" and status == 200:
                    body = b'{"wrong":"reference"}\n'
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        worker.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                work = Path(directory)
                for section, body in BODIES.items():
                    (work / ("expected-" + section + ".json")).write_bytes(body)
                script = """set -euo pipefail
. "$STATS_LIB"
fail() { printf 'FAIL: %s\\n' "$*" >&2; exit 1; }
dump_logs() { printf 'diagnostics\\n' >>"$WORK/diagnostics"; }
wait_native_export() {
    [ "$1" = "$SID" ]
    cmp "$WORK/expected-$2.json" "$3"
    printf '%s\\n' "$2" >>"$WORK/exports"
    [ "$2" != traffic ] || [ "$FAIL_EXPORT" = 0 ]
}
""" + REQ + "\n" + flow
                env = {**os.environ, "WORK": directory, "SID": "exact-sandbox",
                       "DOMAIN": "fixture.invalid", "AK": "fixture-api-key",
                       "PORT": str(server.server_port), "STATS_LIB": str(LIB / "telemetry_stats.sh"),
                       "FAIL_EXPORT": str(int(fail_export))}
                start = time.monotonic()
                result = subprocess.run(["bash", "-c", script], env=env, capture_output=True,
                                        text=True, timeout=14)
                elapsed = time.monotonic() - start
                exports = (work / "exports").read_text().splitlines() if (work / "exports").exists() else []
                reference = work / "telemetry-native-traffic.json"
                body = reference.read_bytes() if reference.exists() else None
                diagnostics = (work / "diagnostics").exists()
                return result, requests, elapsed, exports, body, diagnostics
        finally:
            release.set()
            server.shutdown()
            server.server_close()
            worker.join(timeout=1)

    def test_200_reference_bytes_identity_and_export_are_preserved(self):
        result, requests, _, exports, body, diagnostics = self.run_case()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(exports, ["resource", "traffic"])
        self.assertEqual(body, BODIES["traffic"])
        self.assertFalse(diagnostics)
        self.assertEqual([path for path, _ in requests],
                         ["/sandboxes/exact-sandbox/stats/resource",
                          "/sandboxes/exact-sandbox/stats/traffic"])
        for _, headers in requests:
            self.assertEqual(headers["Host"], "api.fixture.invalid")
            self.assertEqual(headers["X-API-KEY"], "fixture-api-key")

    def test_transient_503_waits_for_a_successful_reference(self):
        result, requests, _, exports, body, diagnostics = self.run_case((503, 503, 200))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(requests), 4)
        self.assertEqual(exports, ["resource", "traffic"])
        self.assertEqual(body, BODIES["traffic"])
        self.assertFalse(diagnostics)

    def test_other_http_failures_are_not_retried(self):
        for status in (400, 401, 403, 404, 409, 429, 500, 502, 504):
            with self.subTest(status=status):
                result, requests, _, exports, body, diagnostics = self.run_case((status, 200))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("native traffic reference=" + str(status), result.stderr)
                self.assertEqual(len(requests), 2)
                self.assertEqual(exports, ["resource"])
                self.assertIsNone(body)
                self.assertTrue(diagnostics)

    def test_persistent_503_is_bounded_and_keeps_failure_diagnostics(self):
        result, requests, elapsed, exports, body, diagnostics = self.run_case((503,))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("native traffic reference=503 after 10s", result.stderr)
        self.assertIn("fixture unavailable", result.stdout)
        self.assertGreater(len(requests), 2)
        self.assertLess(elapsed, 12.5)
        self.assertEqual(exports, ["resource"])
        self.assertIsNone(body)
        self.assertTrue(diagnostics)

    def test_transport_failure_is_not_retried(self):
        result, requests, _, exports, body, diagnostics = self.run_case(mode="disconnect")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("native traffic reference transport=", result.stderr)
        self.assertEqual(len(requests), 2)
        self.assertEqual(exports, ["resource"])
        self.assertIsNone(body)
        self.assertTrue(diagnostics)

    def test_stalled_request_obeys_the_shared_deadline(self):
        result, requests, elapsed, exports, body, diagnostics = self.run_case(mode="hang")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("native traffic reference transport=28", result.stderr)
        self.assertLess(elapsed, 12.5)
        self.assertEqual(len(requests), 2)
        self.assertEqual(exports, ["resource"])
        self.assertIsNone(body)
        self.assertTrue(diagnostics)

    def test_reference_and_export_failures_still_fail(self):
        for kwargs in ({"wrong_body": True}, {"fail_export": True}):
            with self.subTest(kwargs=kwargs):
                result, requests, _, _, _, _ = self.run_case(**kwargs)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(requests), 2)

    def test_submillisecond_budget_never_disables_curl_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            script = """set -euo pipefail
. "$STATS_LIB"
unset EPOCHREALTIME
EPOCHREALTIME=100.000000
sleep() { EPOCHREALTIME=109.999500; }
dump_logs() { :; }
fail() { printf 'FAIL: %s\\n' "$*" >&2; exit 1; }
curl() {
    local arg previous=""
    for arg in "$@"; do
        [ "$previous" != --max-time ] || printf '%s\\n' "$arg" >>"$WORK/timeouts"
        previous="$arg"
    done
    printf '{}' >"$WORK/resp.body"
    if [ "$(wc -l <"$WORK/timeouts")" = 1 ]; then printf 503; else printf 200; fi
}
wait_native_traffic_reference exact-sandbox
"""
            env = {**os.environ, "WORK": directory, "STATS_LIB": str(LIB / "telemetry_stats.sh"),
                   "DOMAIN": "fixture.invalid", "AK": "fixture-api-key", "PORT": "1"}
            result = subprocess.run(["bash", "-c", script], env=env, capture_output=True,
                                    text=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("native traffic reference=503 after 10s", result.stderr)
            self.assertEqual((Path(directory) / "timeouts").read_text().splitlines(), ["10.000"])

    def test_resource_reference_remains_fail_fast(self):
        result, requests, _, exports, body, _ = self.run_case(resource_status=503)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("native resource reference=503", result.stderr)
        self.assertEqual(len(requests), 1)
        self.assertEqual(exports, [])
        self.assertIsNone(body)


if __name__ == "__main__":
    unittest.main()
