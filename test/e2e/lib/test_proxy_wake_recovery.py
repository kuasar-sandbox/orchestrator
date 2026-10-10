"""Run the real post-restore shell gates under transient probe timeouts."""
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


if __name__ == "__main__":
    unittest.main()
