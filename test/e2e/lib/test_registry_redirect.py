"""Execute the real redirect recovery sequence with observable gate failures."""
import os
import contextlib
import io
import json
from pathlib import Path
import re
import subprocess
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).parents[1] / "e2e_cluster_real.sh"


def function(name):
    match = re.search(r"^" + name + r"\(\) \{.*?^\}", SCRIPT.read_text(), re.M | re.S)
    if not match:
        raise AssertionError("missing redirect gate: " + name)
    return match.group()


class RegistryRedirectTest(unittest.TestCase):
    def test_restart_owner_comes_from_validated_actual_redirect(self):
        source = function("choose_redirect_node_id").split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
        for owner in ("registry-2", "registry-3"):
            target = {"member_id": owner, "endpoint": "127.0.0.1:" + {"registry-2": "456", "registry-3": "789"}[owner]}
            payload = json.dumps({"hello": {"redirect": {"targets": [target]}}}).encode()
            output = io.StringIO()
            with patch.object(sys, "argv", ["probe", "http://fixture", "123", "456", "789"]), \
                 patch("urllib.request.urlopen", return_value=io.BytesIO(struct.pack("<I", len(payload)) + payload)), \
                 contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as exit_status:
                exec(compile(source, str(SCRIPT), "exec"), {})
            self.assertEqual(exit_status.exception.code, 0)
            self.assertEqual(output.getvalue().strip().split("\t")[1:], [owner, target["endpoint"]])
        for target in ({}, {"member_id": "registry-9", "endpoint": None},
                       {"member_id": "registry-2", "endpoint": "127.0.0.1:123"}):
            payload = json.dumps({"hello": {"redirect": {"targets": [target]}}}).encode()
            with patch.object(sys, "argv", ["probe", "http://fixture", "123", "456", "789"]), \
                 patch("urllib.request.urlopen", return_value=io.BytesIO(struct.pack("<I", len(payload)) + payload)), \
                 self.assertRaisesRegex(SystemExit, "one fixture Registry owner"):
                exec(compile(source, str(SCRIPT), "exec"), {})

    def recovery(self, failure="", source=None):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "sequence"
            script = r'''
set -euo pipefail
WORK="$FIXTURE_WORK"
CLUSTER_API_KEY=test-key
ROUTE_KEY=test-route
step() { :; }
sleep() { :; }
fail() { exit 19; }
record() { echo "$1" >> "$SEQUENCE"; [ "$1" != "$FAILURE" ]; }
restart_redirect_owner() { record restart; }
wait_redirect_node_link() { record node-link; }
wait_redirect_placer() { record placer; }
create_sandbox() {
    record create
    if [ "$FAILURE" = create-http ]; then echo 503; else echo 201; fi
}
assert_no_default_exec_token() { :; }
sandbox_route() { printf 'exact-sid\ttest-token\ttest-forward\n'; }
data_by_sid_code() {
    [ "$2" = exact-sid ] && [ "$3" = test-token ] || exit 18
    record request
    if [ "$FAILURE" = http ] || { [ "$FAILURE" = slow-boot ] && [ "$(grep -c '^request$' "$SEQUENCE")" -lt 3 ]; }; then
        echo 503
    else
        echo 204
    fi
}
wait_cluster_traffic_stats() { record traffic; }
router_req() {
    if [ "$1" = DELETE ]; then
        record delete; echo 204
    else
        record list; echo '[]' > "$WORK/router-resp.body"; echo 200
    fi
}
wait_node_sandbox_finalized() { record finalized; }
''' + function("retry_data_by_sid") + '\n' + (source or function("run_redirect_recovery")) + '\n' + function("run_redirect_flow") + '\nrun_redirect_flow\n'
            result = subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                                    timeout=5, env={**os.environ, "SEQUENCE": str(log), "FAILURE": failure,
                                                   "FIXTURE_WORK": directory})
            return result.returncode, log.read_text().splitlines() if log.exists() else []

    def test_restart_node_link_and_placer_precede_real_create_data_delete(self):
        status, calls = self.recovery()
        self.assertEqual(status, 0)
        self.assertEqual(calls, ["restart", "node-link", "placer", "create", "request", "traffic",
                                 "delete", "list", "finalized"])

    def test_each_failed_gate_blocks_the_post_restart_request(self):
        for gate in ("restart", "node-link", "placer"):
            with self.subTest(gate=gate):
                status, calls = self.recovery(gate)
                self.assertNotEqual(status, 0)
                self.assertEqual(calls[-1], gate)
                self.assertNotIn("create", calls)
                self.assertNotIn("request", calls)

    def test_failed_create_is_never_retried(self):
        status, calls = self.recovery("create-http")
        self.assertNotEqual(status, 0)
        self.assertEqual(calls.count("create"), 1)
        self.assertNotIn("request", calls)
        self.assertNotIn("delete", calls)

    def test_read_only_data_readiness_converges_without_repeating_create(self):
        status, calls = self.recovery("slow-boot")
        self.assertEqual(status, 0)
        self.assertEqual(calls, ["restart", "node-link", "placer", "create",
                                 "request", "request", "request", "traffic",
                                 "delete", "list", "finalized"])

    def test_data_readiness_exhausts_existing_bound_without_repeating_create(self):
        status, calls = self.recovery("http")
        self.assertNotEqual(status, 0)
        self.assertEqual(calls.count("create"), 1)
        self.assertEqual(calls.count("request"), 12)
        self.assertNotIn("traffic", calls)
        self.assertNotIn("delete", calls)

    def test_missing_readiness_gate_mutant_is_detected(self):
        source = function("run_redirect_recovery")
        mutant = re.sub(r"^    wait_redirect_placer.*\n", "", source, flags=re.M)
        self.assertNotEqual(source, mutant)
        status, calls = self.recovery("placer", mutant)
        self.assertEqual(status, 0)
        self.assertIn("create", calls)  # A ports-only restart would wrongly start placement.
        status, calls = self.recovery("placer", source)
        self.assertNotEqual(status, 0)
        self.assertNotIn("request", calls)

    def test_actual_flow_invokes_recovery_before_delete_and_creates_once(self):
        source = function("run_redirect_flow")
        self.assertNotIn("retry_create_sandbox", source)
        self.assertIn('retry_data_by_sid "$sid" "$envd_token"', source)
        self.assertLess(source.index("run_redirect_recovery"), source.index('code="$(create_sandbox'))
        self.assertLess(source.index("run_redirect_recovery"), source.index("router_req DELETE"))
        gate = function("wait_redirect_placer")
        self.assertIn("lib/placer_readiness.py", gate)
        self.assertIn('--expected-node "$NODE_ID"', gate)
        restart = function("restart_redirect_owner")
        self.assertIn('"$WORK/$REDIRECT_OWNER.yaml"', restart)
        self.assertIn('"${REGISTRY_PIDS[$index]}"', restart)
        self.assertNotIn("registry-1", restart)


if __name__ == "__main__":
    unittest.main()
