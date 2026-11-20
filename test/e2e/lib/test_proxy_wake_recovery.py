"""Run the real post-restore shell gates with delayed observations."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / "cases/orchestrator.proxy-wake.sh"


class RecoveryProbe(unittest.TestCase):
    def run_gate(self, name, status, *, persistent=False, changed=None):
        source = SOURCE.read_text()
        start = source.index(name + '=\"\"\n')
        end = source.index('\n[ -n "$' + name + '" ] || fail ', start)
        end = source.index('\n', end + 1)
        gate = source[start:end]
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            before = dict(nonce="same-primary", held=384*(1<<20), tick=10)
            after = dict(before, tick=11)
            after.update(changed or {})
            (work / "pressure-before.json").write_text(json.dumps(before))
            (work / "next.json").write_text(json.dumps(after))
            (work / "pressure-status.json").write_text(json.dumps(dict(
                reserved_memory=384*(1<<20), pool_memory=1<<30, pending_recoveries=0)))
            script = r'''set -euo pipefail
fail() { echo "$*" >&2; exit 1; }
sleep() { :; }
seq() { printf '1\n2\n3\n'; }
pressure_status() { :; }
pressure_guest_state() {
    echo probe >> "$WORK/probes"
    if [ "$PERSISTENT" = 1 ] || [ ! -e "$WORK/attempted" ]; then
        : > "$WORK/attempted"
        return "$STATUS"
    fi
    cat "$WORK/next.json"
}
''' + gate
            env = dict(os.environ, WORK=directory, FIRST="guest", STATUS=str(status),
                       PERSISTENT=str(int(persistent)))
            result = subprocess.run(["bash", "-c", script], env=env,
                                    capture_output=True, text=True, timeout=10)
            return result, len((work / "probes").read_text().splitlines())

    def test_timeout_then_valid_observation_retries(self):
        for gate in ("restored", "recovered"):
            with self.subTest(gate=gate):
                result, probes = self.run_gate(gate, 124)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(probes, 2)

    def test_other_exec_errors_fail_immediately(self):
        for gate in ("restored", "recovered"):
            with self.subTest(gate=gate):
                result, probes = self.run_gate(gate, 42)
                self.assertEqual(result.returncode, 42)
                self.assertEqual(probes, 1)

    def test_persistent_timeout_exhausts_gate(self):
        for gate in ("restored", "recovered"):
            with self.subTest(gate=gate):
                result, probes = self.run_gate(gate, 124, persistent=True)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(probes, 3)

    def test_timeout_cannot_bypass_restoration_assertions(self):
        for gate in ("restored", "recovered"):
            for changed in (dict(nonce="replacement"), dict(held=0), dict(tick=10)):
                with self.subTest(gate=gate, changed=changed):
                    result, probes = self.run_gate(gate, 124, changed=changed)
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertEqual(probes, 3)


class RestoreBudgetProbe(unittest.TestCase):
    BUDGET = 603979776

    def records(self, *, saved=None, run="restored-run"):
        saved = self.BUDGET if saved is None else saved
        return [dict(run=run, message=f"restore BudgetAtSnapshot reserved={saved} (balloon target/current=0/0)")]

    def run_gate(self, observations, *, run="restored-run", reader_status=0):
        source = SOURCE.read_text()
        start = source.index('restore_budget=""\n')
        end = source.index('\n[ -n "$restore_budget" ] || fail ', start)
        end = source.index('\n', end + 1)
        gate = source[start:end]
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / "observations.json").write_text(json.dumps(observations))
            (work / "pressure-status.json").write_text(json.dumps(dict(pool_memory=1<<30)))
            script = r'''set -euo pipefail
SECONDS=0
fail() { echo "$*" >&2; exit 1; }
sleep() { SECONDS=$((SECONDS + 5)); }
sandbox_run_id() { printf '%s\n' "$RESTORE_RUN"; }
timeout() { [ "$1" = 3 ]; shift; "$@"; }
journalctl() {
    python3 - "$@" <<'PY_JOURNAL'
import json, os, sys
from pathlib import Path
work = Path(os.environ["WORK"])
queries = work / "queries.jsonl"
attempt = len(queries.read_text().splitlines()) if queries.exists() else 0
with queries.open("a") as out: out.write(json.dumps(sys.argv[1:]) + "\n")
status = int(os.environ["READER_STATUS"])
if status: raise SystemExit(status)
observations = json.loads((work / "observations.json").read_text())
records = observations[min(attempt, len(observations)-1)]
run = next((arg.split("=", 1)[1] for arg in sys.argv[1:] if arg.startswith("KUASAR_RUN_ID=")), None)
for row in records:
    if run is None or row["run"] == run:
        print(row["message"])
PY_JOURNAL
}
''' + gate
            env = dict(os.environ, WORK=directory, FIRST="guest", RESTORE_RUN=run,
                       READER_STATUS=str(reader_status))
            result = subprocess.run(["bash", "-c", script], env=env,
                                    capture_output=True, text=True, timeout=10)
            queries = work / "queries.jsonl"
            calls = [json.loads(line) for line in queries.read_text().splitlines()] if queries.exists() else []
            return result, calls

    def test_visible_matching_admission_passes(self):
        for value in (self.BUDGET, int((1<<30)*.4)+1, 1<<30):
            with self.subTest(value=value):
                result, calls = self.run_gate([self.records(saved=value)])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(len(calls), 1)
                self.assertIn("KUASAR_SANDBOX_ID=guest", calls[0])
                self.assertIn("KUASAR_RUN_ID=restored-run", calls[0])

    def test_delayed_journal_delivery_waits(self):
        records = self.records()
        for partial in ([], [dict(run="restored-run", message="controller admit: token=test initial_reservation=603979776")]):
            with self.subTest(partial=partial):
                result, calls = self.run_gate([[], partial, records])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(len(calls), 3)

    def test_missing_journal_exhausts_deadline(self):
        result, calls = self.run_gate([[]])
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(calls), 4)
        self.assertIn("complete Snapshot budget admission", result.stderr)

    def test_old_run_cannot_establish_restored_admission(self):
        result, calls = self.run_gate([self.records(run="old-run")])
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(calls), 4)

    def test_wrong_budget_fails_without_retry(self):
        for value in (0, int((1<<30)*.4), (1<<30)+1):
            with self.subTest(value=value):
                result, calls = self.run_gate([self.records(saved=value)])
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(len(calls), 1)

    def test_controller_record_cannot_replace_snapshot_budget(self):
        result, calls = self.run_gate([[dict(run="restored-run", message="controller admit: token=test initial_reservation=603979776")]])
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(calls), 4)

    def test_journal_failure_is_not_missing_delivery(self):
        result, calls = self.run_gate([self.records()], reader_status=42)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertIn("read restored Snapshot journal", result.stderr)

    def test_missing_current_run_fails_before_journal(self):
        result, calls = self.run_gate([self.records()], run="")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
