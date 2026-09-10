"""Exercise failure output without starting nodes or using real credentials."""
import ast
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "e2e_cluster_stub.sh"
SOURCE = SCRIPT.read_text()
CANARY = "fixture-capability-must-not-enter-ci-diagnostics"


class ClusterStubDiagnostics(unittest.TestCase):
    def test_daemon_output_stays_private_while_the_process_runs(self):
        redirects = re.findall(r'>(?:[^\n]*tee[^\n]*|"\$WORK/(?:registry-\$i|placer-\$i|node-stub|router)\.log" 2>&1 &)', SOURCE)
        self.assertEqual(len(redirects), 4)
        self.assertNotIn("tee ", SOURCE)
        with tempfile.TemporaryDirectory() as directory:
            for redirect in redirects:
                result = subprocess.run(["bash", "-c",
                    'set -euo pipefail\numask 077\nWORK=$1\ni=1\n'
                    + '(printf "%s\\n" "$2"; printf "%s\\n" "$2" >&2) '
                    + redirect + '\nwait\n', "_", directory, CANARY],
                    text=True, capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn(CANARY, result.stdout + result.stderr)
            logs = list(Path(directory).glob("*.log"))
            self.assertEqual(len(logs), 4)
            for log in logs:
                self.assertEqual(log.read_text(), (CANARY + "\n") * 2)
                self.assertEqual(log.stat().st_mode & 0o777, 0o600)

    def test_private_umask_is_applied_only_after_binary_builds(self):
        mask_position = SOURCE.index("\numask 077\n")
        self.assertLess(SOURCE.rindex("\nbuild_cluster_stub_binaries\n"), mask_position)
        self.assertLess(SOURCE.index("make -C"), mask_position)
        self.assertLess(mask_position, SOURCE.index('WORK="$(mktemp -d)"'))
        with tempfile.TemporaryDirectory() as directory:
            prefix = SOURCE[:SOURCE.index('if [ -z "${CLUSTER_STUB_CASE:-}" ]; then')]
            prefix = prefix.replace('ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"', 'ROOT=$1')
            script = prefix + '\nmake() { mkdir -p "$ROOT/bin"; printf "fixture\\n" >"$ROOT/bin/fixture"; chmod +x "$ROOT/bin/fixture"; }\n'
            script += 'build_cluster_stub_binaries\numask 077\nprintf "private\\n" >"$ROOT/diagnostic"\n'
            result = subprocess.run(["bash", "-c", "umask 022\n" + script, "_", directory],
                                    env={"PATH": os.defpath}, text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((Path(directory) / "bin/fixture").stat().st_mode & 0o777, 0o755)
            self.assertEqual((Path(directory) / "diagnostic").stat().st_mode & 0o777, 0o600)

    def test_failure_withholds_private_file_contents(self):
        function = re.search(r"(?ms)^fail\(\) \{\n.*?^\}", SOURCE).group()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("create.body", "nodes.json", "node.log"):
                (root / name).write_text(CANARY + "\n")
            environment = dict(os.environ, WORK=directory, ADMIN="")
            result = subprocess.run(
                ["bash", "-c", "set -euo pipefail\n" + function + "\nfail expected-failure"],
                env=environment, text=True, capture_output=True, timeout=10,
            )
            self.assertEqual(result.returncode, 1)
            self.assertNotIn(CANARY, result.stdout + result.stderr)
            for name in ("create.body", "nodes.json", "node.log"):
                self.assertIn(name, result.stderr)
                self.assertEqual((root / name).read_text(), CANARY + "\n")
            self.assertIn("CLUSTER_STUB_KEEP_WORK=1", result.stderr)

    def test_assertions_never_format_observed_values(self):
        assertions = 0
        for line in SOURCE.splitlines():
            if not line.lstrip().startswith("assert "):
                continue
            node = ast.parse(line.strip()).body[0]
            self.assertIsInstance(node, ast.Assert)
            if node.msg is not None:
                self.assertIsInstance(node.msg, ast.Constant)
                self.assertIsInstance(node.msg.value, str)
            assertions += 1
        self.assertGreater(assertions, 50)
        self.assertNotIn("$(cat", SOURCE)
        self.assertIn("\numask 077\n", SOURCE)

    def test_exit_diagnostics_never_format_observed_values(self):
        for line in SOURCE.splitlines():
            if not line.lstrip().startswith("raise SystemExit("):
                continue
            node = ast.parse(line.strip()).body[0]
            self.assertIsInstance(node, ast.Raise)
            self.assertEqual(len(node.exc.args), 1)
            self.assertIsInstance(node.exc.args[0], ast.Constant)
            self.assertIsInstance(node.exc.args[0].value, str)

    def test_create_contract_failures_do_not_print_capabilities(self):
        function = SOURCE.split("sandbox_credentials() {", 1)[1].split("\nPY\n}", 1)[0]
        program = function.split("<<'PY'\n", 1)[1]
        cases = (
            {"routeKey": "wrong", "envdAccessToken": CANARY},
            {"routeKey": "expected", "envdAccessToken": CANARY, "sandboxID": "fixture-sid"},
        )
        for case in cases:
            with self.subTest(case=case["routeKey"]), tempfile.TemporaryDirectory() as directory:
                response = Path(directory) / "response.json"
                response.write_text(json.dumps(case))
                result = subprocess.run(
                    ["python3", "-", str(response), "expected"], input=program,
                    text=True, capture_output=True, timeout=10,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("AssertionError", result.stderr)
                self.assertNotIn(CANARY, result.stdout + result.stderr)

    def test_exec_contract_failure_does_not_print_capability(self):
        section = SOURCE.split('python3 - "$WORK/exec-session1.response" "$WORK/exec-session1.headers"', 1)[1]
        program = section.split("\n", 1)[1].split("\nPY\n", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            response = Path(directory) / "response.json"
            headers = Path(directory) / "headers"
            response.write_text(json.dumps({"execAccessToken": CANARY, "unexpected": True}))
            headers.write_text("cache-control: no-store\n")
            result = subprocess.run(
                ["python3", "-", str(response), str(headers)], input=program,
                text=True, capture_output=True, timeout=10,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("AssertionError", result.stderr)
            self.assertNotIn(CANARY, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
