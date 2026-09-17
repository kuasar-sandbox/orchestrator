"""Run e2e_execute's Pause wrapper, cancellation driver and cleanup offline."""

import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest


SOURCE = (Path(__file__).resolve().parents[1] / "e2e_execute.sh").read_text()


def snippet(start, end):
    # Fail closed if the E2E boundaries move or become ambiguous. Bash itself
    # expands the original heredoc; do not duplicate/unescape the wrapper here.
    if SOURCE.count(start) != 1 or SOURCE.count(end) != 1:
        raise AssertionError(f"ambiguous/missing E2E snippet: {start!r}, {end!r}")
    return SOURCE[SOURCE.index(start):SOURCE.index(end, SOURCE.index(start))]


BARRIER_PATHS = snippet('PAUSE_BARRIER_TARGET="$WORK/', 'mkdir -p "$ORCH_BIN_DIR"')
WRAPPER = snippet('cat > "$ORCH_BIN_DIR/sandbox-ctl" <<EOF', 'declare -a PIDS=()')
LAUNCH = snippet('printf \'%s\\n\' "$SID" > "$PAUSE_BARRIER_TARGET"', 'for _ in $(seq 1 1500); do')
CANCEL = snippet('for _ in $(seq 1 1500); do', 'wait_sandbox_state "$SID" paused 1200 || {')
FAIL = re.search(r"(?m)^fail\(\).*", SOURCE).group()
CLEANUP = snippet('stop_owned_units() {', '\nfree_port() {')


class ExecutePauseCancellation(unittest.TestCase):
    def setUp(self):
        self.fixture = tempfile.TemporaryDirectory(prefix="execute-pause-")
        self.addCleanup(self.fixture.cleanup)
        self.root = Path(self.fixture.name)
        self.events_r, self.events_w = os.pipe()
        self.control_r, self.control_w = os.pipe()
        self.processes = []
        self.addCleanup(self.reap_fixture)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.orch_bin = self.root / "orch-bin"
        self.orch_bin.mkdir()
        self.env = {
            **os.environ, "WORK": str(self.root), "BIN": str(self.bin),
            "ORCH_BIN_DIR": str(self.orch_bin), "SID": "pause-target",
            "SNAPSHOT_ARGV_LOG": str(self.root / "snapshot-argv.jsonl"),
            "EXPORT_ARGV_LOG": str(self.root / "export-argv.jsonl"),
            "RUN_ARGV_LOG": str(self.root / "run-argv.jsonl"),
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "EVENT_FD": str(self.events_w), "CONTROL_FD": str(self.control_r),
        }
        self.target = self.root / "pause-barrier-target"
        self.reached = self.root / "pause-barrier-reached"
        self.release = self.root / "pause-barrier-release"
        self.forwarded = self.root / "forwarded.json"
        # Only the external binary and HTTP client are fixtures. Both are real
        # processes, and kill/wait/143 handling always comes from the E2E code.
        sandbox_ctl = self.bin / "sandbox-ctl"
        sandbox_ctl.write_text(f"#!{sys.executable}\n" + '''
import json, os, pathlib, sys
root = pathlib.Path(os.environ["WORK"])
(root / "forwarded.json").write_text(json.dumps(sys.argv[1:]))
''')
        sandbox_ctl.chmod(0o755)
        curl = self.bin / "curl"
        curl.write_text(f"#!{sys.executable}\n" + '''
import json, os, pathlib, signal
root = pathlib.Path(os.environ["WORK"])
mode = os.environ["CLIENT_MODE"]
def terminate(signum, frame):
    (root / "term.json").write_text(json.dumps({
        "reached": (root / "pause-barrier-reached").exists(),
        "released": (root / "pause-barrier-release").exists(),
        "signal": signum,
    }))
    os.write(int(os.environ["EVENT_FD"]), b"T")
    if mode in ("gated", "wrong-exit"):
        os.read(int(os.environ["CONTROL_FD"]), 1)
    if mode == "wrong-exit":
        os._exit(23)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    os.kill(os.getpid(), signal.SIGTERM)
signal.signal(signal.SIGTERM, terminate)
os.write(int(os.environ["EVENT_FD"]), b"C")
if mode == "early":
    raise SystemExit(0)
while True:
    signal.pause()
''')
        curl.chmod(0o755)
        generated = subprocess.run(
            ["bash", "-c", "set -euo pipefail\n" + BARRIER_PATHS + WRAPPER],
            env=self.env, text=True, capture_output=True, timeout=5,
        )
        self.assertEqual(generated.returncode, 0, generated.stderr)

    def reap_fixture(self):
        # Release fixture handshakes even when an assertion fails. Each shell
        # owns a separate session; its EXIT trap also reaps surviving test jobs.
        os.close(self.control_w)
        self.release.touch()
        for process in self.processes:
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate(timeout=5)
        for fd in (self.events_r, self.events_w, self.control_r):
            os.close(fd)

    def wrapper(self, *args):
        process = subprocess.Popen(
            [str(self.orch_bin / "sandbox-ctl"), *args], env=self.env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self.processes.append(process)
        return process

    def driver(self, mode="pending", between="", *, short_poll=False):
        # All host boundaries in cleanup are disabled; no services or host
        # networking are invoked. Keep WORK so we can inspect release/results.
        setup = '''
set -euo pipefail
DOMAIN=fixture.invalid PORT=1 AK=fixture
PIDS=() TAGS=() OURS=()
MMDS_ROUTES_E2E=0 E2E_KEEP=1 EXECUTE_STATE_EXPECTED=
RUNNER_PREFIX=fixture-runner@ BUILDER_PREFIX=fixture-builder@
IMMEDIATE_DATA_PID= SW_STARTED= ORIG_IP_FORWARD=
FORWARD_TO_SWITCH_OWNED=0 FORWARD_FROM_SWITCH_OWNED=0
PROXY_VETH_OWNED=0 PROXY_NETNS_OWNED=0 SW_NETNS_OWNED=0
systemctl() { :; }
execute_state_restore_forwarding() { :; }
'''
        if short_poll:
            # Only negative cases shorten the iteration source. The extracted
            # E2E sleeps, deadline and assertions are otherwise unmodified.
            setup += 'seq() { [ "$*" = "1 1500" ] && command seq 1 2; }\n'
        script = setup + BARRIER_PATHS + FAIL + "\n" + CLEANUP
        script += '''
trap 'exit 143' TERM
pause_fixture_exit() {
    local status=$? pid
    cleanup
    # Record the real cleanup outcome before emergency fixture reaping. Tests
    # must fail on a surviving child even when the harness prevents a leak.
    jobs -pr > "$WORK/cleanup-survivors"
    # Test-only fallback if the cleanup under test regresses. The wrapper is
    # owned by Python, so this cannot mask a missing E2E barrier release.
    for pid in $(jobs -pr); do kill -TERM "$pid" 2>/dev/null; done
    wait
    return "$status"
}
trap pause_fixture_exit EXIT
''' + LAUNCH
        script += 'printf "%s\\n" "$PAUSE_CURL_PID" > "$WORK/client.pid"\n'
        script += between + "\n" + CANCEL
        script += 'printf "%s\\n" "$PAUSE_CURL_RC" > "$WORK/client.rc"\n'
        script += 'printf "%s\\n" "${PIDS[@]:-}" > "$WORK/owned-pids"\n'
        process = subprocess.Popen(
            ["bash", "-c", script], env={**self.env, "CLIENT_MODE": mode},
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            pass_fds=(self.events_w, self.control_r), start_new_session=True,
        )
        self.processes.append(process)
        return process

    def event(self, expected):
        ready, _, _ = select.select([self.events_r], [], [], 5)
        self.assertTrue(ready, f"timed out waiting for fixture event {expected!r}")
        self.assertEqual(os.read(self.events_r, 1), expected)

    def arrival(self, wrapper):
        deadline = time.monotonic() + 5
        while not self.reached.exists():
            self.assertIsNone(wrapper.poll(), "wrapper exited before barrier")
            self.assertLess(time.monotonic(), deadline, "wrapper never reached barrier")
            time.sleep(0.005)
        # Arrival alone could be observed just before an incorrectly unblocked
        # exec. Assert that the real process stays blocked without a release.
        with self.assertRaises(subprocess.TimeoutExpired):
            wrapper.wait(timeout=0.1)
        self.assertFalse(self.forwarded.exists(), "snapshot crossed unreleased barrier")

    def completed(self, process, status=0, diagnostic=""):
        stdout, stderr = process.communicate(timeout=5)
        self.assertEqual(process.returncode, status, stdout + stderr)
        self.assertIn(diagnostic, stderr)
        survivors = self.root / "cleanup-survivors"
        if survivors.exists():
            self.assertEqual(survivors.read_text().strip(), "",
                             "E2E cleanup left child jobs before fixture fallback")

    def test_exact_target_snapshot_waits_for_release(self):
        self.target.write_text("pause-target\n")
        args = ["snapshot", "--output", "a path with spaces", "--path-id", "pause-target"]
        wrapper = self.wrapper(*args)
        self.arrival(wrapper)
        self.assertEqual(json.loads((self.root / "snapshot-argv.jsonl").read_text()), args)
        self.release.touch()
        self.completed(wrapper)
        self.assertEqual(json.loads(self.forwarded.read_text()), args)

    def test_other_sandbox_snapshot_does_not_wait(self):
        self.target.write_text("pause-target\n")
        for sid in ("pause-target-other", "other-pause-target"):
            with self.subTest(sid=sid):
                args = ["snapshot", "--path-id", sid]
                self.completed(self.wrapper(*args))
                self.assertFalse(self.reached.exists())
                self.assertEqual(json.loads(self.forwarded.read_text()), args)

    def test_other_command_for_target_does_not_wait(self):
        self.target.write_text("pause-target\n")
        args = ["export", "--path-id", "pause-target"]
        self.completed(self.wrapper(*args))
        self.assertFalse(self.reached.exists())
        self.assertEqual(json.loads(self.forwarded.read_text()), args)

    def test_unarmed_snapshot_does_not_wait(self):
        self.completed(self.wrapper("snapshot", "--path-id", "pause-target"))
        self.assertFalse(self.reached.exists())

    def test_arrival_then_actual_sigterm_exit_143_before_release(self):
        driver = self.driver("gated")
        self.event(b"C")
        self.assertFalse(self.reached.exists())
        self.assertFalse(self.release.exists())
        self.assertIsNone(driver.poll())
        wrapper = self.wrapper("snapshot", "--path-id", "pause-target")
        self.event(b"T")
        self.assertEqual(json.loads((self.root / "term.json").read_text()), {
            "reached": True, "released": False, "signal": signal.SIGTERM,
        })
        # The client holds inside its TERM handler until we permit real signal
        # death. This checks wait-before-release without timing a fast exit.
        client_pid = int((self.root / "client.pid").read_text())
        os.kill(client_pid, 0)
        with self.assertRaises(subprocess.TimeoutExpired):
            driver.wait(timeout=0.1)
        self.assertFalse(self.release.exists())
        self.assertFalse(self.forwarded.exists())
        os.write(self.control_w, b"x")
        self.completed(driver)
        self.completed(wrapper)
        self.assertEqual((self.root / "client.rc").read_text(), "143\n")
        self.assertEqual((self.root / "owned-pids").read_text().strip(), "")
        self.assertFalse(self.target.exists())
        with self.assertRaises(ProcessLookupError):
            os.kill(client_pid, 0)

    def test_client_completion_before_barrier_rejects(self):
        # Join the real early-exit client at the launch/poll seam so this case
        # cannot accidentally race with a live client.
        driver = self.driver("early", 'wait "$PAUSE_CURL_PID"')
        self.completed(driver, 1, "Pause HTTP client completed before snapshot barrier")
        self.assertFalse(self.reached.exists())

    def test_client_must_still_be_pending_after_arrival(self):
        driver = self.driver("early", '''
wait "$PAUSE_CURL_PID"
printf D >&"$EVENT_FD"
read -r -u "$CONTROL_FD"
''')
        self.event(b"C")
        self.event(b"D")
        wrapper = self.wrapper("snapshot", "--path-id", "pause-target")
        self.arrival(wrapper)
        os.write(self.control_w, b"go\n")
        self.completed(driver, 1, "Pause HTTP client completed before deterministic cancellation")
        self.completed(wrapper)

    def test_non_sigterm_exit_rejects(self):
        driver = self.driver("wrong-exit")
        self.event(b"C")
        wrapper = self.wrapper("snapshot", "--path-id", "pause-target")
        self.event(b"T")
        self.assertFalse(self.release.exists())
        os.write(self.control_w, b"x")
        self.completed(driver, 1, "Pause HTTP client cancellation rc=23 (want SIGTERM 143)")
        self.completed(wrapper)

    def test_missing_barrier_rejects_with_bounded_polling(self):
        self.completed(self.driver(short_poll=True), 1,
                       "accepted Pause did not reach deterministic snapshot barrier")
        self.assertFalse(self.reached.exists())
        self.assertFalse((self.root / "client.rc").exists())

    def test_failure_cleanup_releases_waiting_owned_wrapper(self):
        driver = self.driver(between='read -r -u "$CONTROL_FD"\nfail "injected failure"')
        self.event(b"C")
        wrapper = self.wrapper("snapshot", "--path-id", "pause-target")
        self.arrival(wrapper)
        os.write(self.control_w, b"go\n")
        self.completed(driver, 1, "injected failure")
        self.completed(wrapper)
        self.assertTrue(self.release.exists())
        self.assertEqual(json.loads(self.forwarded.read_text()),
                         ["snapshot", "--path-id", "pause-target"])
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.root / "client.pid").read_text()), 0)


if __name__ == "__main__":
    unittest.main()
