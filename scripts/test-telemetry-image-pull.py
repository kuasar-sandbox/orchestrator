"""Exercise image acquisition without Docker or network access."""
from pathlib import Path
import os
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "test/e2e/e2e_telemetry_backends.sh"
IMAGE = "example/backend:1@sha256:" + "a" * 64
MIRROR = "m.daocloud.io/docker.io/" + IMAGE


class ImagePullTests(unittest.TestCase):
    def run_case(self, failures=0, cache="", failure_code=1):
        source = SCRIPT.read_text()
        function = source[source.index("prepare_image() {"):source.index('\nprometheus_image="$(prepare_image')]
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, TEST_DIR=directory, FAILURES=str(failures),
                       CACHE=cache, FAILURE_CODE=str(failure_code), IMAGE=IMAGE)
            harness = r'''
set -euo pipefail
docker() {
    if [ "$1" = image ]; then
        [ "$3" = "$CACHE" ]
        return
    fi
    echo "$*" >> "$TEST_DIR/pulls"
    local count
    count=$(wc -l < "$TEST_DIR/pulls")
    echo "docker progress"
    [ "$count" -gt "$FAILURES" ] || return "$FAILURE_CODE"
}
timeout() {
    echo "$1" >> "$TEST_DIR/timeouts"
    shift
    "$@"
}
sleep() { echo "$1" >> "$TEST_DIR/sleeps"; }
'''
            result = subprocess.run(["bash", "-c", harness + function + '\nprepare_image "$IMAGE"'],
                                    env=env, text=True, capture_output=True)
            logs = {name: (Path(directory) / name).read_text().splitlines()
                    if (Path(directory) / name).exists() else []
                    for name in ("pulls", "timeouts", "sleeps")}
            return result, logs

    def test_cached_upstream_and_mirror_need_no_pull(self):
        for image in (IMAGE, MIRROR):
            with self.subTest(image=image):
                result, logs = self.run_case(cache=image)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), image)
                self.assertEqual(logs["pulls"], [])

    def test_transient_failures_preserve_digest_and_stdout(self):
        result, logs = self.run_case(failures=2)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), MIRROR)
        self.assertEqual(logs["pulls"], ["pull " + MIRROR] * 3)
        self.assertEqual(logs["timeouts"], ["3m"] * 3)
        self.assertEqual(logs["sleeps"], ["2", "4"])

    def test_persistent_failure_or_timeout_stays_fatal(self):
        for code in (1, 124):
            with self.subTest(code=code):
                result, logs = self.run_case(failures=99, failure_code=code)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertEqual(len(logs["pulls"]), 3)
                self.assertEqual(logs["sleeps"], ["2", "4"])
                self.assertIn("failed after 3 attempts", result.stderr)

    def test_first_attempt_success_does_not_sleep(self):
        result, logs = self.run_case()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(logs["pulls"]), 1)
        self.assertEqual(logs["sleeps"], [])


if __name__ == "__main__":
    unittest.main()
