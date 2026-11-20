"""Run the prepared guest pressure fixture and its host-controlled barriers."""
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock


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

    def test_raw_template_is_not_created_as_the_active_diff(self):
        # Exercise the real setup/configuration functions, recording their
        # filesystem commands without requiring privileged host tools.
        result = subprocess.run(["bash", "-c", r'''
set -euo pipefail
. "$1"
WORK="$2"
BIN="$WORK/bin"
RESOURCE_BASE_REF=file:///prepared/base.img
RESOURCE_SANDBOX_IDS=()
declare -A RESOURCE_TAPS=()
mkdir() { :; }
ip() { [ "$1 $2" != "link show" ]; }
truncate() { printf '%s\n' "$*" >"$WORK/truncate.args"; }
mkfs.ext4() { printf '%s\n' "$*" >"$WORK/mkfs.args"; }
resource_setup_sandbox fixture
resource_write_sandbox fixture 320 1024 512 false
''', "_", str(LIB), str(self.work)], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        template = self.work / "fixture.template.ext4"
        self.assertEqual((self.work / "truncate.args").read_text(), f"-s 1G {template}\n")
        self.assertEqual((self.work / "mkfs.args").read_text(), f"-q -F -O ^has_journal {template}\n")
        self.assertFalse((self.work / "fixture.diff").exists())
        config = (self.work / "fixture.yaml").read_text()
        self.assertIn(f"diff: file://{self.work}/fixture.diff,", config)
        self.assertIn(f"diff_template: file://{template}", config)

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

    def test_hold_workload_keeps_memory_and_primary_identity(self):
        state = self.work / "held.json"
        env = {**os.environ, "WL_MODE": "hold", "WL_DURATION": "10",
               "WL_RMAX_MIB": "8", "WL_STATE_FILE": str(state), "WL_START_GATE": ""}
        process = subprocess.Popen([sys.executable, str(WORKLOAD)], env=env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 5
            first = None
            while time.monotonic() < deadline:
                self.assertIsNone(process.poll(), "primary workload exited")
                if state.exists():
                    current = json.loads(state.read_text())
                    self.assertEqual(current["held"], 8 << 20)
                    if first is None:
                        first = current
                    elif current["tick"] > first["tick"]:
                        self.assertEqual(current["nonce"], first["nonce"])
                        break
                time.sleep(.02)
            else:
                self.fail("retained memory did not continue making progress")
        finally:
            process.terminate()
            process.communicate(timeout=3)

    def test_hold_workload_reports_first_bad_page_and_still_exits_one(self):
        for location in ("first", "middle"):
            with self.subTest(location=location):
                state = self.work / f"corrupt-{location}.json"
                result = subprocess.run([sys.executable, "-c", r'''
import ctypes
import importlib.util
import json
import os
import sys

spec = importlib.util.spec_from_file_location("workload", sys.argv[1])
workload = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workload)
held = bytearray(b"\xff" * (1 << 20))
base = ctypes.addressof(ctypes.c_ubyte.from_buffer(held))
page_size = os.sysconf("SC_PAGE_SIZE")
offset = 0 if sys.argv[2] == "first" else 2 * page_size
page_address = (base + offset) // page_size * page_size
start = max(0, page_address - base)
end = min(len(held), page_address + page_size - base)
held[start:end] = b"\x00" * (end - start)
first_bad = (start + 4095) // 4096 * 4096
print(json.dumps(dict(base=base, start=start, end=end, first_bad=first_bad)), flush=True)
workload.grow_to = lambda *_: held
workload.run_hold(1, 1)
''', str(WORKLOAD), location], env={**os.environ, "WL_STATE_FILE": str(state)},
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 1, result.stderr)
                expected = json.loads(result.stdout)
                message = next(line.removeprefix("RuntimeError: ")
                               for line in result.stderr.splitlines()
                               if line.startswith("RuntimeError: held-memory contents changed"))
                self.assertLess(len(message), 2048)
                details = json.loads(message.removeprefix("held-memory contents changed: "))
                self.assertEqual(details["offset"], expected["first_bad"])
                self.assertEqual(details["actual"], 0)
                self.assertEqual(details["expected"], 255)
                self.assertEqual(details["held_bytes"], 1 << 20)
                self.assertEqual(int(details["buffer_address"], 16), expected["base"])
                address = expected["base"] + expected["first_bad"]
                self.assertEqual(int(details["address"], 16), address)
                self.assertEqual(details["page_offset"], address % details["page_size"])
                page_address = address - details["page_offset"]
                self.assertEqual(int(details["page_address"], 16), page_address)
                page = details["page"]
                self.assertEqual((page["start"], page["end"]),
                                 (expected["start"], expected["end"]))
                size = expected["end"] - expected["start"]
                self.assertEqual(page["offset_in_page"], expected["base"] + expected["start"] - page_address)
                self.assertEqual(page["length"], size)
                self.assertEqual(page["non_ff"], size)
                self.assertEqual(page["sha256"], hashlib.sha256(bytes(size)).hexdigest())
                self.assertEqual(page["bad_points"], [dict(offset=index, actual=0)
                                 for index in range(expected["start"], expected["start"] + 8)])
                self.assertTrue("present" in details["pagemap"] or
                                "unavailable" in details["pagemap"])
                self.assertFalse(state.exists())

    def test_hold_workload_preserves_failure_when_diagnostics_are_unavailable(self):
        spec = importlib.util.spec_from_file_location("workload", WORKLOAD)
        workload = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(workload)
        held = bytearray(b"\xff" * (1 << 20))
        held[4096] = 0
        workload.grow_to = lambda *_: held

        with mock.patch.object(workload.os, "open", side_effect=PermissionError("pagemap denied")):
            with self.assertRaises(RuntimeError) as failure:
                workload.run_hold(1, 1)
        details = json.loads(str(failure.exception).removeprefix("held-memory contents changed: "))
        self.assertEqual(details["offset"], 4096)
        self.assertIn("PermissionError: pagemap denied", details["pagemap"]["unavailable"])
        self.assertEqual(details["page"]["non_ff"], 1)

        with mock.patch.object(ctypes, "addressof", side_effect=RuntimeError("x" * 10000)):
            with self.assertRaises(RuntimeError) as failure:
                workload.run_hold(1, 1)
        message = str(failure.exception)
        self.assertLess(len(message), 2048)
        details = json.loads(message.removeprefix("held-memory contents changed: "))
        self.assertEqual(details["offset"], 4096)
        self.assertEqual(details["actual"], 0)
        self.assertIn("RuntimeError:", details["diagnostic_error"])

        with mock.patch.object(workload, "held_memory_failure", side_effect=RuntimeError("diagnostic failed")):
            with self.assertRaisesRegex(RuntimeError, "^held-memory contents changed$"):
                workload.run_hold(1, 1)


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
