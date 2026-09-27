"""Run the prepared guest pressure fixture and its host-controlled barriers."""
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest


LIB = Path(__file__).with_name("resource.sh")
WORKLOAD = Path(__file__).with_name("workload.py")


class ResourceWorkload(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name)
        helpers = self.work / "helpers/orchestrator"
        helpers.mkdir(parents=True)
        shutil.copyfile(WORKLOAD, helpers / "workload.py")
        self.start = self.work / "start"
        self.delivery = self.work / "delivery"

    def fixture(self, mode):
        result = subprocess.run(["bash", "-c", r'''
set -euo pipefail
. "$1"
WORK="$2"
E2E_LIB="$WORK/helpers"
BIN="$WORK/bin"
RESOURCE_BASE_REF=file:///prepared/base.img
declare -A RESOURCE_TAPS=([fixture]=fixture-tap)
resource_write_sandbox fixture 320 1024 512 false
resource_write_pressure_workload fixture "$3" "$WORK/start" "$WORK/delivery"
''', "_", str(LIB), str(self.work), mode], capture_output=True, text=True, timeout=5)
        return result

    def launch(self, mode):
        result = self.fixture(mode)
        self.assertEqual(result.returncode, 0, result.stderr)
        config = (self.work / "fixture.yaml").read_text()
        # JSON is also YAML; the rest of this file is the unchanged sandbox fixture.
        import json
        launch = json.loads(next(line.removeprefix("launch: ") for line in config.splitlines()
                                 if line.startswith("launch: ")))
        return config, launch

    def assert_pressure_contract(self, launch):
        self.assertEqual(launch["exec"], "/usr/local/bin/python3")
        self.assertEqual(launch["restart"], "never")
        env = launch["env"]
        self.assertEqual({key: env[key] for key in (
            "WL_MODE", "WL_SEED", "WL_DURATION", "WL_CYCLES", "WL_RMIN_MIB", "WL_RMAX_MIB")}, {
            "WL_MODE": "cycles", "WL_SEED": "42", "WL_DURATION": "15", "WL_CYCLES": "2",
            "WL_RMIN_MIB": "256", "WL_RMAX_MIB": "384"})
        self.assertEqual(env["WL_START_GATE"], str(self.start))
        self.assertEqual(env["WL_DELIVERY_GATE"], str(self.delivery))
        self.assertEqual(env["WL_START_GATE_TIMEOUT"], "60")
        self.assertEqual(env["WL_DELIVERY_GATE_TIMEOUT"], "60")
        self.assertIn(WORKLOAD.read_text(), launch["args"][1])

    def test_static_fixture_keeps_pressure_without_node_controller(self):
        config, launch = self.launch("static")
        self.assertNotIn("controller:", config)
        self.assert_pressure_contract(launch)

    def test_dynamic_fixture_keeps_same_pressure_with_node_controller(self):
        config, launch = self.launch("dynamic")
        self.assertIn(f"controller: {self.work}/sandbox-resource.sock", config)
        self.assert_pressure_contract(launch)

    def test_missing_prepared_workload_fails_without_source_fallback(self):
        (self.work / "helpers/orchestrator/workload.py").unlink()
        result = self.fixture("static")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FileNotFoundError", result.stderr)

    def test_real_workload_holds_first_allocation_until_delivery(self):
        _, launch = self.launch("static")
        env = {**os.environ, **launch["env"], "WL_RMIN_MIB": "8", "WL_RMAX_MIB": "8",
               "WL_DURATION": "2"}
        # Keep the source test light; product E2E retains the original 256-384MiB.
        process = subprocess.Popen([sys.executable, *launch["args"]], env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        output = queue.Queue()
        reader = threading.Thread(target=lambda: [output.put(line) for line in process.stdout],
                                  daemon=True)
        reader.start()
        lines = []

        def wait_for(marker):
            while True:
                try:
                    line = output.get(timeout=5)
                except queue.Empty:
                    self.fail(f"missing {marker!r}; exit={process.poll()}; output={lines}")
                lines.append(line)
                if marker in line:
                    return

        try:
            wait_for("workload waiting for start gate")
            with self.assertRaises(queue.Empty):
                output.get(timeout=.15)
            self.assertIsNone(process.poll())
            self.start.touch()
            wait_for("workload waiting for delivery gate")
            self.assertIn("workload pressure probe ready rss=8MiB\n", lines)
            with self.assertRaises(queue.Empty):
                output.get(timeout=.15)
            self.assertIsNone(process.poll())
            self.delivery.touch()
            wait_for("workload done")
            self.assertIn("cycle 0: free rss=8MiB\n", lines)
            self.assertIn("cycle 1: free rss=8MiB\n", lines)
        finally:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
            reader.join(timeout=3)
            process.stdout.close()

    def test_guest_start_gate_has_a_real_failure_deadline(self):
        _, launch = self.launch("dynamic")
        result = subprocess.run([sys.executable, *launch["args"]],
                                env={**os.environ, **launch["env"], "WL_START_GATE_TIMEOUT": ".1"},
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertIn("workload start gate timed out", result.stderr)
        self.assertNotIn("workload done", result.stdout)


if __name__ == "__main__":
    unittest.main()
