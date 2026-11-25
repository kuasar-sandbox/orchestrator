"""Publication gates must correlate the local Group with its current Node session."""
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


def group(version=2, context=None):
    return {"group": "/group", "version": version,
            "context": context if context is not None else {"templateID": "fixture", "password": "private"}}


def publication(**overrides):
    return {"group": "/group", "node_id": "node-1", "storage_readable": True,
            "published": True, "group_version": 2,
            "registration": {"node_id": "node-1", "link_owner": "registry-1", "fence": "generation-1"},
            **overrides}


class GroupReadinessTest(unittest.TestCase):
    def wait(self, opener, **kwargs):
        clock = Clock()
        return wait_for_group("http://node", "/group", timeout=1,
                              require_template=True, registry_urls=["http://registry"],
                              node_id="node-1", api_key="fixture-only", opener=opener,
                              monotonic=clock.monotonic, sleep=clock.sleep, **kwargs)

    def test_stale_other_node_and_incomplete_publication_do_not_pass(self):
        responses = []
        for stale in (publication(group_version=1), publication(node_id="node-2"),
                      publication(published=False), publication(registration={}),
                      publication(storage_readable=False)):
            responses.extend((Response(group()), Response(stale)))
        responses.extend((Response(group()), Response(publication()), Response(group())))
        opener = SequenceOpener(responses)
        value = self.wait(opener)
        self.assertEqual(value, {"group": "/group", "node_id": "node-1", "version": 2,
                                 "registration": publication()["registration"]})
        self.assertEqual(len(opener.requests), 13)
        self.assertTrue(all(r.method == "GET" and r.data is None for r in opener.requests))
        self.assertTrue(all(r.get_header("X-api-key") == "fixture-only" for r in opener.requests))
        self.assertEqual(opener.requests[-2].full_url,
                         "http://registry/route-link/v2/group-status?group=%2Fgroup&node_id=node-1")

    def test_restart_cannot_reuse_old_durable_registration(self):
        fresh = {"node_id": "node-1", "link_owner": "registry-1", "fence": "generation-2"}
        opener = SequenceOpener([Response(group()), Response(publication()),
            Response(group()), Response(publication(registration=fresh)), Response(group())])
        value = self.wait(opener, after_fence="generation-1")
        self.assertEqual(value["registration"], fresh)
        self.assertEqual(len(opener.requests), 5)

    def test_local_change_during_observation_requires_new_publication(self):
        opener = SequenceOpener([Response(group()), Response(publication()), Response(group(3)),
                                 Response(group(3)), Response(publication(group_version=3)), Response(group(3))])
        self.assertEqual(self.wait(opener)["version"], 3)

    def test_all_registries_must_observe_same_current_registration(self):
        opener = SequenceOpener([
            Response(group(context={})), Response(publication()),
            Response(publication(registration={"node_id": "node-1", "link_owner": "registry-2", "fence": "new"})),
            Response(group(context={})), Response(publication()), Response(publication()), Response(group(context={}))])
        clock = Clock()
        value = wait_for_group("http://node", "/group", timeout=1,
                               registry_urls=["http://r1", "http://r2"], node_id="node-1", api_key="fixture-only",
                               opener=opener, monotonic=clock.monotonic, sleep=clock.sleep)
        self.assertEqual(value["version"], 2)
        self.assertEqual([r.host for r in opener.requests], ["node", "r1", "r2", "node", "r1", "r2", "node"])

    def test_cli_reads_real_http_and_never_discloses_private_context(self):
        requests = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                requests.append((self.path, self.headers.get("X-API-KEY")))
                if self.path.startswith("/groups?") and self.headers.get("Host") != "api.fixture.local":
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(503 if len(requests) == 1 else 200)
                self.end_headers()
                self.wfile.write(json.dumps(group() if self.path.startswith("/groups?") else publication()).encode())

        with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            url = f"http://127.0.0.1:{server.server_port}"
            try:
                result = subprocess.run([
                    sys.executable, str(Path(__file__).with_name("group_readiness.py")),
                    "--node-url", url, "--registry-url", url, "--group", "/group", "--timeout", "2",
                    "--node-host", "api.fixture.local", "--node-id", "node-1",
                ], env={**os.environ, "GROUP_API_KEY": "fixture-only"},
                    capture_output=True, text=True, timeout=5)
            finally:
                server.shutdown()
                thread.join(timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([r[0] for r in requests], ["/groups?group=%2Fgroup", "/groups?group=%2Fgroup",
            "/route-link/v2/group-status?group=%2Fgroup&node_id=node-1", "/groups?group=%2Fgroup"])
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
        scope = {"args": SimpleNamespace(node_url="http://node", node_id="node-1", host="api.fixture.local",
                                         group="/group", cpu=2, post_restart=True, registry_url=["http://registry"]),
                 "wait_for_group": gate, "require": write, "json": json, "key": "fixture-only"}
        exec(compile(ast.Module(body=[register], type_ignores=[]), str(path), "exec"), scope)
        return scope["register"], write, gate

    def test_each_registration_rechecks_publication_without_retrying_write(self):
        opener = SequenceOpener([Response(group()), Response(publication()), Response(group())] * 2)
        register, write, gate = self.registration(opener)
        register("first")
        write.side_effect = AssertionError("registration returned 503")
        with self.assertRaisesRegex(AssertionError, "registration returned 503"):
            register("second")
        self.assertEqual(gate.call_count, 2)
        self.assertEqual(write.call_count, 2)

    def test_unpublished_group_prevents_registration(self):
        opener = SequenceOpener([Response(group()), Response(publication(published=False))] * 4)
        register, write, _ = self.registration(opener)
        with self.assertRaisesRegex(AssertionError, "timeout waiting for Group publication"):
            register("unpublished")
        write.assert_not_called()

    def test_missing_node_identity_is_rejected(self):
        with self.assertRaises(ValueError):
            wait_for_group("http://node", "/group", registry_urls=["http://registry"], api_key="fixture-only")

    def test_wiring_retains_status_before_import_gate(self):
        source = (Path(__file__).resolve().parents[1] / "cases/orchestrator.cluster-lifecycle.sh").read_text()
        restart = source.index('wait_for("fixture Router/Registry restart"')
        retained = source.index('require("GET", status_path(wt, wb), 200)', restart)
        enabled = source.index("args.post_restart = True", retained)
        self.assertLess(restart, retained)
        self.assertLess(retained, enabled)


if __name__ == "__main__":
    unittest.main()
