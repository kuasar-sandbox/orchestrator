"""Regression coverage for registry-redirect's post-restart placer gate."""

import ast
import functools
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
import urllib.error

from placer_readiness import BUILD_RESOURCES, wait_for_placer


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
        self.requests.append((request, timeout))
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


class PlacerReadinessTest(unittest.TestCase):
    def test_rejects_failures_and_bad_answers_until_expected_node(self):
        opener = SequenceOpener([
            urllib.error.URLError("offline"),
            Response(b"unavailable", status=503),
            Response(b"not-json"),
            Response({"error": "group unavailable"}),
            Response({"no_node": True}),
            Response({"node_id": "wrong-node"}),
            Response({"node_id": "real-node-redirect-07"}),
        ])
        clock = Clock()
        result = wait_for_placer(
            "http://placer", "/e2e/cluster/real/registry-redirect",
            "real-node-redirect-07", timeout=2, opener=opener,
            monotonic=clock.monotonic, sleep=clock.sleep)

        self.assertEqual(result["node_id"], "real-node-redirect-07")
        request, _ = opener.requests[-1]
        self.assertEqual(request.full_url, "http://placer/placer-link/place")
        payload = json.loads(request.data)
        self.assertEqual(payload["group"], "/e2e/cluster/real/registry-redirect")
        self.assertTrue(payload["build"])
        self.assertEqual(payload["build_resources"], BUILD_RESOURCES)

    def test_wrong_node_times_out(self):
        class WrongNodeOpener:
            def open(self, _request, timeout):
                del timeout
                return Response({"node_id": "wrong-node"})

        clock = Clock()
        with self.assertRaisesRegex(AssertionError, "wrong-node"):
            wait_for_placer("http://placer", "/group", "expected", timeout=0.2,
                            opener=WrongNodeOpener(), monotonic=clock.monotonic,
                            sleep=clock.sleep)

    def test_wiring_preserves_retained_status_before_placer_gate(self):
        source = (Path(__file__).with_name("build_actions.py")).read_text()
        restart = source.index('wait_for("fixture Router/Registry restart"')
        retained = source.index('require("GET", status_path(wt, wb), 200)', restart)
        enabled = source.index("post_restart = True", retained)
        self.assertLess(restart, retained)
        self.assertLess(retained, enabled)

    def registration(self, opener, *, post_restart=True, placer_url="http://placer"):
        # Execute the fixture's actual nested registration function without
        # starting a VM. The readiness loop itself is not mocked.
        path = Path(__file__).with_name("build_actions.py")
        module = ast.parse(path.read_text(), filename=str(path))
        main = next(node for node in module.body
                    if isinstance(node, ast.FunctionDef) and node.name == "main")
        register = next(node for node in main.body
                        if isinstance(node, ast.FunctionDef) and node.name == "register")
        clock = Clock()
        gate = Mock(wraps=functools.partial(
            wait_for_placer, timeout=0.2, opener=opener,
            monotonic=clock.monotonic, sleep=clock.sleep))
        write = Mock(return_value=({"templateID": "transient-test", "buildID": "build-test"}, {}))
        scope = {"args": SimpleNamespace(placer_url=placer_url, group="/group",
                                        expected_node="expected", cpu=2),
                 "post_restart": post_restart, "wait_for_placer": gate,
                 "require": write, "json": json}
        exec(compile(ast.Module(body=[register], type_ignores=[]), str(path), "exec"), scope)
        return scope["register"], write, gate

    def test_each_registration_rechecks_a_previously_ready_view(self):
        opener = SequenceOpener([Response({"node_id": "expected"}),
                                 Response({"no_node": True}),
                                 Response({"node_id": "expected"})])
        register, write, gate = self.registration(opener)
        register("cancel-canonical-peer")
        self.assertEqual(len(opener.requests), 1)
        register("query-hang")
        self.assertEqual(len(opener.requests), 3)
        self.assertEqual(gate.call_count, 2)
        self.assertEqual(write.call_count, 2)
        for call in write.call_args_list:
            self.assertEqual(call.args[:3], ("POST", "/v3/templates", 202))
            self.assertEqual(call.args[3]["memoryMB"], 6144)

    def test_not_ready_prevents_registration(self):
        opener = SequenceOpener([Response({"no_node": True}) for _ in range(3)])
        register, write, _ = self.registration(opener)
        with self.assertRaisesRegex(AssertionError, "timeout waiting for placer"):
            register("query-hang")
        write.assert_not_called()

    def test_mutating_registration_failure_is_not_retried(self):
        register, write, _ = self.registration(SequenceOpener([Response({"node_id": "expected"})]))
        write.side_effect = AssertionError("registration returned 503")
        with self.assertRaisesRegex(AssertionError, "registration returned 503"):
            register("query-hang")
        write.assert_called_once()

    def test_pre_restart_and_standalone_do_not_add_a_gate(self):
        for options in ({"post_restart": False}, {"placer_url": None}):
            with self.subTest(options=options):
                register, write, gate = self.registration(SequenceOpener([]), **options)
                register("cancel-hang")
                gate.assert_not_called()
                write.assert_called_once()


if __name__ == "__main__":
    unittest.main()
