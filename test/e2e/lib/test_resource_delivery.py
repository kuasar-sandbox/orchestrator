"""Exercise Phase B2 delivery against changing live observations."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


SOURCE = (Path(__file__).with_name("resource.sh").read_text()
          + Path(__file__).with_name("resource_observation.sh").read_text())
MIB = 1024 * 1024


def function(name):
    match = re.search(r"(?ms)^" + name + r"\(\) \{[^\n]*\n.*?^\}", SOURCE)
    if match is None:
        raise AssertionError("missing resource helper: " + name)
    return match.group()


class ResourceDelivery(unittest.TestCase):
    def run_delivery(self, *, states, reservations, alive=True, self_cap=False,
                     target_baseline=512 * MIB):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / "fixture.log").write_text(
                "virtio_balloon: pressure at 1 pages -> cap 2 pages converging\n"
                if self_cap else "")
            (work / "states").write_text("\n".join(states) + "\n")
            (work / "reservations").write_text(
                "\n".join(json.dumps(value) if value is not None else "FAIL"
                            for value in reservations) + "\n")
            node = work / "node-ctl"
            node.write_text('''#!/bin/bash
set -euo pipefail
[ "$#" -eq 4 ] && [ "$1 $2 $3" = "resource list --socket" ] || exit 95
index=$(cat "$WORK/reservation-index" 2>/dev/null || echo 1)
line=$(sed -n "${index}p" "$WORK/reservations")
[ -n "$line" ] || line=$(tail -n 1 "$WORK/reservations")
printf '%s\n' "$((index + 1))" > "$WORK/reservation-index"
[ "$line" != FAIL ] || exit 1
printf '%s\n' "$line"
''')
            node.chmod(0o700)
            script = '''set -euo pipefail
resource_fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
resource_read_balloon() {
    [ "$1" = fixture ] || return 95
    index=$(cat "$WORK/state-index" 2>/dev/null || echo 1)
    line=$(sed -n "${index}p" "$WORK/states")
    [ -n "$line" ] || line=$(tail -n 1 "$WORK/states")
    printf '%s\n' "$line"
    printf '%s\n' "$((index + 1))" > "$WORK/state-index"
}
sleep() { SECONDS=$((SECONDS + 1)); }
'''
            for name in ("resource_reservation_memory",
                         "resource_self_cap_observed", "resource_wait_covered_grow"):
                script += function(name) + "\n"
            script += 'pid=$$\n[ "$ALIVE" = 1 ] || pid=2147483647\n'
            script += f"resource_wait_covered_grow fixture \"$pid\" 3 {target_baseline} {1024 * MIB}\n"
            return subprocess.run(
                ["bash", "-c", script], text=True, capture_output=True, timeout=5,
                env={**os.environ, "WORK": directory, "BIN": directory,
                     "ALIVE": str(int(alive))})

    @staticmethod
    def row(memory=704 * MIB, **changes):
        row = {"sandbox_id": "fixture", "connected": True,
               "provisional": False, "allocatable_memory": memory}
        row.update(changes)
        return [row]

    def test_live_640_to_704_mib_reservation_covers_superseding_target(self):
        result = self.run_delivery(
            states=[f"{320 * MIB} {512 * MIB}", f"{320 * MIB} {512 * MIB}"],
            reservations=[self.row(640 * MIB), self.row(704 * MIB)])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(704 * MIB))

    def test_pending_current_does_not_block_authorized_target(self):
        result = self.run_delivery(
            states=[f"{320 * MIB} {128 * MIB}"], reservations=[self.row()],
            target_baseline=384 * MIB)
        self.assertEqual(result.returncode, 0, result.stderr)

        # A retained prepressure grow is compared with InitialTarget instead.
        prepressure = self.run_delivery(
            states=[f"{320 * MIB} {128 * MIB}"], reservations=[self.row()],
            target_baseline=512 * MIB)
        self.assertEqual(prepressure.returncode, 0, prepressure.stderr)

    def test_delayed_and_superseding_reads_are_retried(self):
        result = self.run_delivery(
            states=[f"{384 * MIB} {128 * MIB}", f"{256 * MIB} {128 * MIB}"],
            reservations=[None, self.row(768 * MIB)])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(768 * MIB))

    def test_invalid_missing_or_under_reserved_rows_never_pass(self):
        invalid_rows = (
            [],
            self.row(704 * MIB) * 2,
            self.row(704 * MIB, sandbox_id="other"),
            self.row(704 * MIB, connected=False),
            self.row(704 * MIB, provisional=True),
            self.row("malformed"),
            self.row(-1),
            self.row(True),
            self.row(704.5),
            self.row(0),
            self.row(703 * MIB),
            self.row(1025 * MIB),
        )
        for rows in invalid_rows:
            with self.subTest(rows=rows):
                result = self.run_delivery(
                    states=[f"{320 * MIB} {512 * MIB}"], reservations=[rows])
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("lacked a live sufficient reservation", result.stderr)

        malformed_ch = self.run_delivery(states=["malformed"],
                                         reservations=[self.row()])
        self.assertNotEqual(malformed_ch.returncode, 0)
        self.assertIn("lacked a live sufficient reservation", malformed_ch.stderr)

    def test_later_shrink_uses_current_not_max_seen_reservation(self):
        result = self.run_delivery(
            states=[f"{256 * MIB} {704 * MIB}", f"{448 * MIB} {704 * MIB}"],
            reservations=[self.row(704 * MIB), self.row(576 * MIB)])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(576 * MIB))

    def test_lookup_failure_cannot_reuse_previous_reservation(self):
        result = self.run_delivery(
            states=[f"{256 * MIB} {512 * MIB}", f"{320 * MIB} {512 * MIB}"],
            reservations=[self.row(704 * MIB), None])
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("reservation_status=unavailable", result.stderr)

    def test_authorization_without_reduced_target_is_not_delivery(self):
        result = self.run_delivery(
            states=[f"{512 * MIB} {512 * MIB}"], reservations=[self.row()])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ch_status=not_reduced", result.stderr)

    def test_missing_ch_is_reported_separately_from_reservation(self):
        result = self.run_delivery(states=[""], reservations=[self.row()])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ch_status=unavailable", result.stderr)
        self.assertIn("reservation_status=not_sampled", result.stderr)

    def test_capacity_self_cap_and_exit_guards_remain_fatal(self):
        capacity = self.run_delivery(
            states=[f"{320 * MIB} {1025 * MIB}"], reservations=[self.row()])
        self.assertNotEqual(capacity.returncode, 0)
        self.assertIn("exceeds Capacity", capacity.stderr)

        self_cap = self.run_delivery(
            states=[f"{384 * MIB} {512 * MIB}"],
            reservations=[self.row(704 * MIB)], self_cap=True)
        self.assertNotEqual(self_cap.returncode, 0)
        self.assertIn("self-cap fired", self_cap.stderr)

        exited = self.run_delivery(
            states=[f"{384 * MIB} {512 * MIB}"],
            reservations=[self.row(704 * MIB)], alive=False)
        self.assertNotEqual(exited.returncode, 0)
        self.assertIn("sandbox exited", exited.stderr)

    def test_dynamic_case_keeps_workload_and_safety_contract(self):
        case = (Path(__file__).resolve().parents[1] / "cases" /
                "orchestrator.resource-reservation.sh").read_text()
        self.assertIn('resource_wait_covered_grow "$sid" "$pid"', case)
        self.assertIn('"$target_reference" "$capacity"', case)
        self.assertIn('resource_wait_local_grow', case)
        self.assertIn('grow_phase=prepressure', case)
        self.assertIn('resource_assert_no_oom "$sid"', case)
        self.assertIn('resource_self_cap_observed "$sid"', case)


if __name__ == "__main__":
    unittest.main()
