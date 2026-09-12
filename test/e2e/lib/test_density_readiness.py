"""Exercise the density readiness gates without KVM or host cgroup writes."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


SOURCE = (Path(__file__).resolve().parents[1] / "e2e_density.sh").read_text()
OBSERVATION = "memory: initial CH observation accepted epoch=1 seq=1\n"
SENSOR = "sensor: PSI mode active\n"
WORKLOAD = "workload waiting for start gate\n"


def function(name):
    match = re.search(r"(?ms)^" + name + r"\(\) \{[^\n]*\n.*?^\}", SOURCE)
    return match.group() if match else ""


class DensityReadiness(unittest.TestCase):
    def run_gate(self, mode, log=OBSERVATION + SENSOR + WORKLOAD, *,
                 high="671088640", maximum="1073741824", settled=True, observation_delay=0,
                 alive=True, direct=False, gate_timeout=1, premature_return=False):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / "fixture.log").write_text(log)
            if high is not None:
                (work / "memory.high").write_text(high + "\n")
            if maximum is not None:
                (work / "memory.max").write_text(maximum + "\n")
            script = '''set -euo pipefail
fail() { printf 'FAIL: %s\\n' "$*" >&2; exit 1; }
cat() {
    [ "$#" -eq 1 ] || return 95
    case "$1" in
        /sys/fs/cgroup/sandboxes/fixture/memory.high) command cat "$WORK/memory.high" ;;
        /sys/fs/cgroup/sandboxes/fixture/memory.max) command cat "$WORK/memory.max" ;;
        *) return 95 ;;
    esac
}
resource_reservation_matches() {
    [ "$1" = fixture ] && [ "$2" = settled ] && [ "$SETTLED" = 1 ]
}
'''
            script += function("memory_control_observed") + "\n"
            script += function("wait_for_" + mode + "_control_ready") + "\n"
            if premature_return:
                script += "wait_for_" + mode + "_control_ready() { return 0; }\n"
            if observation_delay:
                script += '''(
    sleep "$OBSERVATION_DELAY"
    printf '%s' "$OBSERVATION" >> "$WORK/fixture.log"
    printf '%s\\n' 671088640 > "$WORK/memory.high"
) &
'''
            script += 'printf "%s\\n" "$EPOCHREALTIME" > "$WORK/gate-started"\n'
            if direct:
                script += "memory_control_observed fixture\n"
            else:
                # kill -0 only; never signal any external process in this test.
                script += 'pid=$$\n[ "$ALIVE" = 1 ] || pid=2147483647\n'
                script += "wait_for_" + mode + '_control_ready fixture "$pid" "$GATE_TIMEOUT"\n'
            # Record the gate return itself, before joining the delayed writer.
            script += 'printf "%s\\n" "$EPOCHREALTIME" > "$WORK/gate-completed"\nwait\n'
            env = {**os.environ, "WORK": directory, "SETTLED": str(int(settled)),
                   "OBSERVATION_DELAY": str(observation_delay), "OBSERVATION": OBSERVATION,
                   "ALIVE": str(int(alive)), "GATE_TIMEOUT": str(gate_timeout)}
            result = subprocess.run(["bash", "-c", script], env=env,
                                    text=True, capture_output=True, timeout=gate_timeout + 5)
            elapsed = None
            if (work / "gate-completed").exists():
                elapsed = (float((work / "gate-completed").read_text())
                           - float((work / "gate-started").read_text()))
            return result, elapsed

    def test_fresh_observation_and_finite_high_do_not_require_shrink(self):
        for mode in ("dynamic", "static"):
            with self.subTest(mode=mode):
                result, _ = self.run_gate(mode)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_shrink_log_alone_is_not_fresh_observation(self):
        for mode in ("dynamic", "static"):
            with self.subTest(mode=mode):
                result, _ = self.run_gate(mode, SENSOR + WORKLOAD +
                                         "memory: shrink committed Budget=671088640\n")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("within 1s", result.stderr)

    def test_sensor_and_workload_gates_are_still_required(self):
        for mode in ("dynamic", "static"):
            for log in (OBSERVATION + SENSOR, OBSERVATION + WORKLOAD):
                with self.subTest(mode=mode, log=log):
                    result, _ = self.run_gate(mode, log)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("within 1s", result.stderr)

    def test_dynamic_gate_still_requires_settled_reservation(self):
        result, _ = self.run_gate("dynamic", settled=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("within 1s", result.stderr)

    def test_static_gate_does_not_require_a_node_controller(self):
        result, _ = self.run_gate("static", settled=False,
                                  log=OBSERVATION + SENSOR.replace("PSI", "events_poll") + WORKLOAD)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_boot_deferred_missing_or_invalid_high_is_not_ready(self):
        for high in ("max", None, "0", "-1", "invalid"):
            with self.subTest(high=high):
                result, _ = self.run_gate("static", high=high, direct=True)
                self.assertNotEqual(result.returncode, 0)

    def test_high_above_hard_limit_is_not_ready(self):
        result, _ = self.run_gate("static", high="1073741825", direct=True)
        self.assertNotEqual(result.returncode, 0)

    def test_high_equal_to_hard_limit_is_valid(self):
        result, _ = self.run_gate("static", high="1073741824", direct=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_or_invalid_hard_limit_is_not_ready(self):
        for maximum in (None, "max", "0", "-1", "invalid"):
            with self.subTest(maximum=maximum):
                result, _ = self.run_gate("static", maximum=maximum, direct=True)
                self.assertNotEqual(result.returncode, 0)

    def test_invalid_or_rejected_report_is_not_a_completed_observation(self):
        for line in ("memory: initial CH observation accepted epoch=0 seq=1\n",
                     "memory: initial CH observation accepted epoch=1 seq=0\n",
                     "memory: initial CH observation accepted epoch=1 seq=1invalid\n",
                     "memory: held report behind launch/restore observation barrier epoch=1 seq=1\n",
                     "memory: report epoch=1 seq=1: memory: incomplete CH target/current observation\n"):
            with self.subTest(line=line):
                result, _ = self.run_gate("static", line, direct=True)
                self.assertNotEqual(result.returncode, 0)

    def test_waits_for_actual_observation_and_applied_high(self):
        for mode in ("dynamic", "static"):
            with self.subTest(mode=mode):
                # Bash SECONDS has integer precision; a one-second deadline
                # may expire before the delayed writer's first observation.
                # Give this asynchronous positive case margin, while negative
                # timeout cases retain their short one-second fixture deadline.
                result, elapsed = self.run_gate(mode, SENSOR + WORKLOAD, high="max",
                                                observation_delay=0.15, gate_timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertGreaterEqual(elapsed, 0.15)

    def test_timing_detects_a_gate_returning_before_its_writer(self):
        for mode in ("dynamic", "static"):
            with self.subTest(mode=mode):
                result, elapsed = self.run_gate(mode, SENSOR + WORKLOAD, high="max",
                                                observation_delay=0.3, gate_timeout=5,
                                                premature_return=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertLess(elapsed, 0.15)

    def test_exited_sandbox_is_not_waited_until_timeout(self):
        for mode in ("dynamic", "static"):
            with self.subTest(mode=mode):
                result, _ = self.run_gate(mode, SENSOR + WORKLOAD, alive=False)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("sandbox exited before", result.stderr)
                self.assertNotIn("within 1s", result.stderr)


if __name__ == "__main__":
    unittest.main()
