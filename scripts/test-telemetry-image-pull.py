"""Exercise image acquisition without Docker or network access."""
from pathlib import Path
import os
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "test/source/telemetry_backends.sh"
IMAGE = "example/backend:1@sha256:" + "a" * 64
MIRROR = "m.daocloud.io/docker.io/" + IMAGE
KILL_DIAGNOSTIC = "timeout: sending signal KILL to command 'docker'"


class ImagePullTests(unittest.TestCase):
    def run_case(self, failures=0, cache="", failure_code=1,
                 failure="net/http: TLS handshake timeout", image=IMAGE):
        source = SCRIPT.read_text()
        function = source[source.index("prepare_image() {"):source.index('\nprometheus_image="$(prepare_image')]
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, TEST_DIR=directory, FAILURES=str(failures),
                       CACHE=cache, FAILURE_CODE=str(failure_code), FAILURE=failure, IMAGE=image)
            harness = r'''
set -euo pipefail
docker() {
    if [ "$1" = image ]; then
        echo "$3" >> "$TEST_DIR/inspects"
        [ "$3" = "$CACHE" ]
        return
    fi
    echo "$*" >> "$TEST_DIR/pulls"
    local count
    count=$(wc -l < "$TEST_DIR/pulls")
    echo "docker progress"
    if [ "$count" -le "$FAILURES" ]; then
        echo "$FAILURE" >&2
        return "$FAILURE_CODE"
    fi
}
timeout() {
    echo "$1 $2 $3" >> "$TEST_DIR/timeouts"
    shift 3
    "$@"
}
sleep() { echo "$1" >> "$TEST_DIR/sleeps"; }
'''
            result = subprocess.run(["bash", "-c", harness + function + '\nimage="$(prepare_image "$IMAGE")"\nprintf \'%s\\n\' "$image"\necho reached > "$TEST_DIR/continued"' ],
                                    env=env, text=True, capture_output=True)
            logs = {name: (Path(directory) / name).read_text().splitlines()
                    if (Path(directory) / name).exists() else []
                    for name in ("pulls", "timeouts", "sleeps", "inspects", "continued")}
            if result.returncode != 0:
                self.assertEqual(logs["continued"], [], "failed acquisition must stop set -e caller")
            return result, logs

    def test_cached_requested_image_needs_no_pull(self):
        result, logs = self.run_case(cache=IMAGE)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), IMAGE)
        self.assertEqual(logs["inspects"], [IMAGE])
        self.assertEqual(logs["pulls"], [])
        self.assertEqual(logs["timeouts"], [])
        self.assertEqual(logs["sleeps"], [])

    def test_first_attempt_uses_requested_source_even_with_cached_mirror(self):
        for cache in ("", MIRROR):
            with self.subTest(cache=cache):
                result, logs = self.run_case(cache=cache)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), IMAGE)
                self.assertEqual(logs["inspects"], [IMAGE])
                self.assertEqual(logs["pulls"], ["pull " + IMAGE])
                self.assertEqual(logs["timeouts"], ["--verbose --kill-after=5s 3m"])
                self.assertEqual(logs["sleeps"], [])

    def test_transient_failures_preserve_digest_and_stdout(self):
        for failure in ("net/http: TLS handshake timeout", "read: connection reset by peer",
                        "i/o timeout", "unexpected EOF", "503 Service Unavailable",
                        "net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)",
                        "context deadline exceeded",
                        "502 Bad Gateway", "504 Gateway Timeout",
                        "Temporary failure in name resolution", "connection refused",
                        "connection timed out", 'Get "https://registry.example/v2/": EOF',
                        "EOF", "429 Too Many Requests", "500 Internal Server Error",
                        "unexpected status code: 429", "unexpected status: 500",
                        "failed to fetch authentication token: TLS handshake timeout",
                        "authentication token: failed to connect: TLS handshake timeout"):
            with self.subTest(failure=failure):
                result, logs = self.run_case(failures=2, failure=failure)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), IMAGE)
                self.assertEqual(logs["pulls"], ["pull " + IMAGE] * 3)
                self.assertEqual(logs["timeouts"], ["--verbose --kill-after=5s 3m"] * 3)
                self.assertEqual(logs["sleeps"], ["2", "4"])
                self.assertNotIn("docker progress", result.stderr)

    def test_transient_exhaustion_or_timeout_stays_fatal(self):
        for code, failure in ((1, "TLS handshake timeout"), (124, ""), (137, KILL_DIAGNOSTIC)):
            with self.subTest(code=code):
                result, logs = self.run_case(failures=99, failure_code=code, failure=failure)
                self.assertEqual(result.returncode, code)
                self.assertEqual(result.stdout, "")
                self.assertEqual(logs["pulls"], ["pull " + IMAGE] * 3)
                self.assertEqual(logs["timeouts"], ["--verbose --kill-after=5s 3m"] * 3)
                self.assertEqual(logs["sleeps"], ["2", "4"])
                self.assertIn("failed after 3 attempts", result.stderr)

    def test_timeout_statuses_retry_and_recover(self):
        for code in (124, 137):
            with self.subTest(code=code):
                result, logs = self.run_case(failures=2, failure_code=code,
                                             failure=KILL_DIAGNOSTIC if code == 137 else "")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), IMAGE)
                self.assertEqual(len(logs["pulls"]), 3)
                self.assertEqual(logs["sleeps"], ["2", "4"])

    def test_real_timeout_kill_after_exhausts_bounded_attempts(self):
        source = SCRIPT.read_text()
        function = source[source.index("prepare_image() {"):source.index('\nprometheus_image="$(prepare_image')]
        function = function.replace("--kill-after=5s 3m", "--kill-after=0.1s 0.1s")
        with tempfile.TemporaryDirectory() as directory:
            fake = Path(directory) / "docker"
            fake.write_text("""#!/bin/bash
if [ "$1" = image ]; then exit 1; fi
echo pull >> "$TEST_DIR/pulls"
trap '' TERM
sleep 30
""")
            fake.chmod(0o755)
            env = dict(os.environ, TEST_DIR=directory, IMAGE=IMAGE,
                       PATH=directory + os.pathsep + os.environ["PATH"])
            result = subprocess.run(
                ["bash", "-c", 'set -euo pipefail\nsleep() { :; }\n' + function
                 + '\nprepare_image "$IMAGE"'],
                env=env, text=True, capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 137, result.stderr)
            self.assertEqual(result.stdout, "")
            self.assertEqual((Path(directory) / "pulls").read_text().splitlines(), ["pull"] * 3)
            self.assertIn("failed after 3 attempts (pull timeout)", result.stderr)

    def test_permanent_and_unknown_failures_do_not_retry(self):
        for failure in ("unauthorized: authentication required", "requested access denied",
                        "403 Forbidden", "x509: certificate signed by unknown authority",
                        "manifest unknown", "manifest not found", "no matching manifest",
                        "digest mismatch", "invalid reference format", "no space left on device",
                        "filesystem layer verification failed for digest",
                        "unknown daemon failure", "unauthorized after TLS handshake timeout",
                        "certificate expired after i/o timeout", "digest mismatch after unexpected EOF", "unexpected status code: 5000", "authentication failed after TLS handshake timeout"):
            with self.subTest(failure=failure):
                result, logs = self.run_case(failures=99, failure=failure)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(logs["pulls"], ["pull " + IMAGE])
                self.assertEqual(logs["sleeps"], [])
                self.assertNotIn("docker progress", result.stderr)

    def test_signed_urls_are_neither_logged_nor_used_for_classification(self):
        secret_url = "https://registry.example/eof?token=secret&status=500"
        for failure in (f'Get "{secret_url}": unknown error',
                        f'Get "{secret_url}": EOF'):
            with self.subTest(failure=failure):
                result, logs = self.run_case(failures=99, failure=failure)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn(secret_url, result.stderr)
                self.assertNotIn("secret", result.stderr)
                self.assertNotIn(failure, result.stderr)
                self.assertEqual(len(logs["pulls"]), 3 if failure.endswith(": EOF") else 1)
        # HTTP-looking digits in URL paths cannot turn unknown errors into retries.
        result, logs = self.run_case(failures=99, failure='Get "https://registry.example/status:500": unknown error')
        self.assertEqual(len(logs["pulls"]), 1)
        self.assertEqual(result.stdout, "")

    def test_non_docker_exit_codes_do_not_retry(self):
        for code in (125, 126, 127, 130, 137, 143):
            with self.subTest(code=code):
                result, logs = self.run_case(failures=99, failure_code=code)
                self.assertEqual(result.returncode, code)
                self.assertEqual(result.stdout, "")
                self.assertEqual(len(logs["pulls"]), 1)
                self.assertEqual(logs["sleeps"], [])

    def test_explicit_registry_reference_is_never_rewritten(self):
        image = "registry.example.test/backend@sha256:" + "b" * 64
        result, logs = self.run_case(image=image)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), image)
        self.assertEqual(logs["pulls"], ["pull " + image])


if __name__ == "__main__":
    unittest.main()
