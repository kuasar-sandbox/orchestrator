"""Exercise Phase A's real-grant predicate without KVM or host resources."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


SOURCE = (Path(__file__).resolve().parents[1] / "e2e_density.sh").read_text()
MIB = 1024 * 1024
GROW = "memory: grow accepted Budget=536870912 target=536870912 reservation=536870912\n"


def function(name):
    match = re.search(r"(?ms)^" + name + r"\(\) \{[^\n]*\n.*?^\}", SOURCE)
    if match is None:
        raise AssertionError("missing density helper: " + name)
    return match.group()


class DensityGrant(unittest.TestCase):
    def run_grant(self, *, reservation=512 * MIB, target=512 * MIB,
                  actual=1024 * MIB, log=GROW, rows=None, alive=True,
                  delayed=False, ch_available=True):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            row = {"sandbox_id": "fixture", "connected": True,
                   "provisional": False, "allocatable_memory": reservation}
            (work / "reservations.json").write_text(json.dumps([row] if rows is None else rows))
            (work / "fixture.log").write_text(log)
            if ch_available:
                (work / "ch-state").write_text(f"{target} {actual}\n")
            node = work / "node-ctl"
            node.write_text('''#!/bin/sh
[ "$#" -eq 4 ] && [ "$1" = resource ] && [ "$2" = list ] && [ "$3" = --socket ] || exit 95
cat "$WORK/reservations.json"
''')
            node.chmod(0o700)
            script = '''set -euo pipefail
fail() { printf 'FAIL: %s\\n' "$*" >&2; exit 1; }
read_ch_balloon_state() {
    [ "$1" = fixture ] || return 95
    cat "$WORK/ch-state"
}
'''
            script += function("resource_reservation_memory") + "\n"
            script += function("wait_for_phase_a_grant") + "\n"
            if delayed:
                (work / "next-reservations.json").write_text(json.dumps([
                    {**row, "allocatable_memory": 256 * MIB}]))
                script += '''(
    sleep 0.15
    cp "$WORK/next-reservations.json" "$WORK/reservations.json"
    printf '%s\\n' '805306368 134217728' > "$WORK/ch-state"
    printf '%s\\n' 'memory: grow accepted Budget=268435456 target=805306368 reservation=268435456' >> "$WORK/fixture.log"
) &
'''
            # kill -0 only; never signal an external process in this fixture.
            script += 'pid=$$\n[ "$ALIVE" = 1 ] || pid=2147483647\n'
            script += 'wait_for_phase_a_grant fixture "$pid" "$GRANT_TIMEOUT" 134217728 1073741824\n'
            script += "wait\n"
            result = subprocess.run(
                ["bash", "-c", script], text=True, capture_output=True, timeout=10,
                env={**os.environ, "WORK": directory, "BIN": directory,
                     "ALIVE": str(int(alive)), "GRANT_TIMEOUT": "5" if delayed else "1"})
            return result

    def assert_rejected(self, **kwargs):
        result = self.run_grant(**kwargs)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("no sandbox-originated reserved grow target", result.stderr)

    def test_retained_pre_workload_grant_needs_no_second_growth(self):
        result = self.run_grant()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(512 * MIB))

    def test_grow_after_workload_release_is_also_observed(self):
        result = self.run_grant(reservation=128 * MIB, target=896 * MIB,
                                actual=128 * MIB, log="", delayed=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(256 * MIB))

    def test_initial_admission_and_workload_completion_are_not_grow(self):
        self.assert_rejected(reservation=128 * MIB, target=896 * MIB,
                             log="workload done\n")

    def test_reservation_and_target_without_local_grow_are_insufficient(self):
        self.assert_rejected(log="workload done\n")

    def test_local_grow_without_extra_node_reservation_is_insufficient(self):
        self.assert_rejected(reservation=128 * MIB)

    def test_reservation_without_ch_accepted_grow_is_insufficient(self):
        self.assert_rejected(target=896 * MIB)

    def test_ch_budget_must_be_covered_by_node_reservation(self):
        self.assert_rejected(reservation=256 * MIB, target=512 * MIB)

    def test_retained_reservation_may_exceed_a_shrinking_target_budget(self):
        result = self.run_grant(reservation=512 * MIB, target=576 * MIB,
                                actual=512 * MIB)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(512 * MIB))

    def test_grow_does_not_require_current_budget_convergence(self):
        result = self.run_grant(actual=128 * MIB)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_invalid_or_unavailable_observations_cannot_pass(self):
        for kwargs in ({"reservation": "invalid"}, {"target": -1},
                       {"actual": "invalid"}, {"ch_available": False},
                       {"reservation": 1025 * MIB}, {"target": 1025 * MIB},
                       {"actual": 1025 * MIB}):
            with self.subTest(**kwargs):
                self.assert_rejected(**kwargs)

    def test_node_reservation_must_be_precise_connected_and_owned(self):
        row = {"sandbox_id": "fixture", "connected": True,
               "provisional": False, "allocatable_memory": 512 * MIB}
        for rows in ([], [row, row], [{**row, "sandbox_id": "other"}],
                     [{**row, "connected": False}], [{**row, "provisional": True}]):
            with self.subTest(rows=rows):
                self.assert_rejected(rows=rows)

    def test_exited_sandbox_cannot_pass_on_retained_observations(self):
        result = self.run_grant(alive=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("sandbox exited before", result.stderr)
        self.assertNotIn("within 1s", result.stderr)

    def test_phase_a_uses_grant_check_and_keeps_workload_and_oom_checks(self):
        phase = function("phase_a")
        self.assertIn('wait_for_phase_a_grant "$sid" "$pid" 20', phase)
        self.assertNotIn("wait_for_reservation_growth", phase)
        self.assertIn('wait_for_workload "$sid" "$pid" 40', phase)
        self.assertIn('[ "$oom" -eq 0 ] || fail', phase)


if __name__ == "__main__":
    unittest.main()
