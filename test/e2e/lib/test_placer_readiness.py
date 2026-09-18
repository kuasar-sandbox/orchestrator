"""Regression coverage for registry-redirect's post-restart placer gate."""

import json
from pathlib import Path
import unittest
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
        placer = source.index("wait_for_placer(args.placer_url", retained)
        self.assertLess(restart, retained)
        self.assertLess(retained, placer)


if __name__ == "__main__":
    unittest.main()
