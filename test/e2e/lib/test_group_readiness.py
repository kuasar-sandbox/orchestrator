"""Readiness observes Group import without allocating a sandbox or Build."""

import ast
import functools
import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
import urllib.error

from group_readiness import wait_for_group


class Response:
    def __init__(self, value, status=200):
        self.status = status
        self.body = value if isinstance(value, bytes) else json.dumps(value).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def read(self):
        return self.body


class SequenceOpener:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


class Clock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += max(seconds, 0.1)


def group(context=None):
    return {"group": "/group", "version": 2, "context": context or {}}


class GroupReadinessTest(unittest.TestCase):
    def test_registry_then_node_reads_retry_until_imported(self):
        opener = SequenceOpener([
            Response({}, 503),
            Response({}), Response({}, 404),
            Response({}), Response(b"invalid-json"),
            Response({}), Response({"group": "/other", "version": 2, "context": {}}),
            Response({}), Response(group()),
            Response({}), Response(group({"templateID": "fixture", "registry_auth": {"password": "private"}})),
        ])
        clock = Clock()
        value = wait_for_group("http://node", "/group", timeout=2,
                               require_template=True, registry_urls=["http://registry"],
                               api_key="fixture-only", opener=opener,
                               monotonic=clock.monotonic, sleep=clock.sleep)
        self.assertEqual(value, {"group": "/group", "version": 2})
        self.assertTrue(all(r.method == "GET" and r.data is None for r in opener.requests))
        self.assertEqual(opener.requests[-1].full_url, "http://node/groups?group=%2Fgroup")
        self.assertEqual(opener.requests[-2].full_url, "http://registry/route-link/v2/verify-key?group=%2Fgroup")
        self.assertTrue(all(r.get_header("X-api-key") == "fixture-only" for r in opener.requests))

    def test_empty_context_is_valid_for_build_and_all_registries_must_authenticate(self):
        opener = SequenceOpener([Response({}), Response({}, 503),
                                 Response({}), Response({}), Response(group())])
        clock = Clock()
        wait_for_group("http://node", "/group", timeout=1,
                       registry_urls=["http://r1", "http://r2"], api_key="fixture-only",
                       opener=opener, monotonic=clock.monotonic, sleep=clock.sleep)
        self.assertEqual([r.host for r in opener.requests], ["r1", "r2", "r1", "r2", "node"])

    def test_cli_reads_real_http_and_does_not_disclose_private_context(self):
        requests = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                requests.append((self.path, self.headers.get("X-API-KEY")))
                self.send_response(503 if len(requests) == 1 else 200)
                self.end_headers()
                if self.path.startswith("/groups?"):
                    self.wfile.write(json.dumps(group({"templateID": "fixture", "image_repo": "private"})).encode())

        with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            url = f"http://127.0.0.1:{server.server_port}"
            try:
                result = subprocess.run([
                    sys.executable, str(Path(__file__).with_name("group_readiness.py")),
                    "--node-url", url, "--registry-url", url, "--group", "/group", "--timeout", "2",
                ], env={**os.environ, "GROUP_API_KEY": "fixture-only"},
                    capture_output=True, text=True, timeout=5)
            finally:
                server.shutdown()
                thread.join(timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([r[0] for r in requests], [
            "/route-link/v2/verify-key?group=%2Fgroup",
            "/route-link/v2/verify-key?group=%2Fgroup", "/groups?group=%2Fgroup"])
        self.assertNotIn("fixture-only", result.stdout + result.stderr)
        self.assertNotIn("private", result.stdout + result.stderr)

    def registration(self, opener):
        path = Path(__file__).with_name("build_client.py")
        module = ast.parse(path.read_text(), filename=str(path))
        register = next(n for n in module.body if isinstance(n, ast.FunctionDef) and n.name == "register")
        clock = Clock()
        gate = Mock(wraps=functools.partial(wait_for_group, timeout=0.2, opener=opener,
                                          monotonic=clock.monotonic, sleep=clock.sleep))
        write = Mock(return_value=({"templateID": "transient-test", "buildID": "build-test"}, {}))
        scope = {"args": SimpleNamespace(node_url="http://node", group="/group", cpu=2,
                                         post_restart=True, registry_url=["http://registry"]),
                 "wait_for_group": gate, "require": write, "json": json, "key": "fixture-only"}
        exec(compile(ast.Module(body=[register], type_ignores=[]), str(path), "exec"), scope)
        return scope["register"], write, gate

    def test_each_registration_rechecks_authentication_and_does_not_retry_write(self):
        opener = SequenceOpener([Response({}), Response(group()), Response({}), Response(group())])
        register, write, gate = self.registration(opener)
        register("first")
        write.side_effect = AssertionError("registration returned 503")
        with self.assertRaisesRegex(AssertionError, "registration returned 503"):
            register("second")
        self.assertEqual(gate.call_count, 2)
        self.assertEqual(write.call_count, 2)
        for call in write.call_args_list:
            self.assertEqual(call.args[:3], ("POST", "/v3/templates", 202))
            self.assertEqual(call.args[3]["memoryMB"], 6144)

    def test_not_imported_prevents_registration(self):
        opener = SequenceOpener([Response({}, 403) for _ in range(4)])
        register, write, _ = self.registration(opener)
        with self.assertRaisesRegex(AssertionError, "timeout waiting for Group import"):
            register("not-imported")
        write.assert_not_called()

    def test_wiring_retains_status_before_import_gate(self):
        source = (Path(__file__).resolve().parents[1] / "cases/orchestrator.cluster-lifecycle.sh").read_text()
        restart = source.index('wait_for("fixture Router/Registry restart"')
        retained = source.index('require("GET", status_path(wt, wb), 200)', restart)
        enabled = source.index("args.post_restart = True", retained)
        self.assertLess(restart, retained)
        self.assertLess(retained, enabled)


if __name__ == "__main__":
    unittest.main()
