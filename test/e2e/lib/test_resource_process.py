"""Exercise cleanup deadlines with actual cooperative and stubborn children."""
from pathlib import Path
import subprocess
import tempfile
import unittest


LIB = Path(__file__).with_name("resource.sh")
WORKSPACE = Path(__file__).with_name("case_workspace.sh")


class ResourceProcess(unittest.TestCase):
    def stop_child(self, ignore_term):
        with tempfile.TemporaryDirectory() as directory:
            body = r'''
set -euo pipefail
. "$1"
python3 -u -c 'import signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN if sys.argv[1] == "ignore" else lambda *_: sys.exit(0))
print("ready", flush=True)
while True: time.sleep(1)' "$2" > "$3/child.log" &
pid=$!
trap 'kill -KILL "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do
    grep -q ready "$3/child.log" && break
    sleep .01
done
grep -q ready "$3/child.log"
result=0
resource_stop_process "$pid" fixture 1 || result=$?
! kill -0 "$pid" 2>/dev/null
printf 'result=%s\n' "$result"
'''
            result = subprocess.run(["bash", "-c", body, "_", str(LIB),
                                     "ignore" if ignore_term else "exit", directory],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            return result

    def test_cooperative_child_is_reaped_without_forced_stop(self):
        result = self.stop_child(False)
        self.assertIn("result=0", result.stdout)
        self.assertNotIn("SIGKILL", result.stderr)

    def test_stubborn_child_is_killed_and_reported_as_cleanup_failure(self):
        result = self.stop_child(True)
        self.assertIn("result=1", result.stdout)
        self.assertIn("did not stop after 1s; sending SIGKILL", result.stderr)

    def run_cleanup(self, controller_failure, exit_code):
        with tempfile.TemporaryDirectory() as directory:
            body = r'''
set -euo pipefail
. "$1"
. "$2"
WORK="$3"
case_workspace_init
printf evidence > "$WORK/node.log"
printf controller-diagnostic > "$WORK/resource-controller.log"
printf guest-diagnostic > "$WORK/fixture.log"
RESOURCE_SANDBOX_PIDS=()
RESOURCE_SANDBOX_IDS=(fixture)
declare -A RESOURCE_TAPS=([fixture]=fixture-tap)
RESOURCE_DAEMON_PID=""
# No host network/cgroup ownership in this source-only cleanup fixture.
ip() { :; }
rmdir() { :; }
if [ "$4" = fail ]; then resource_stop_controller() { return 1; }; fi
trap resource_cleanup EXIT
exit "$5"
'''
            result = subprocess.run(["bash", "-c", body, "_", str(LIB), str(WORKSPACE),
                                     directory, "fail" if controller_failure else "ok", str(exit_code)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual((Path(directory) / "runtime/node.log").read_text(), "evidence")
            return result

    def test_cleanup_failure_fails_an_otherwise_successful_case(self):
        result = self.run_cleanup(True, 0)
        self.assertEqual(result.returncode, 1, result.stderr)

    def test_cleanup_preserves_the_original_case_failure(self):
        result = self.run_cleanup(False, 23)
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertIn("controller-diagnostic", result.stderr)
        self.assertIn("guest-diagnostic", result.stderr)


if __name__ == "__main__":
    unittest.main()
