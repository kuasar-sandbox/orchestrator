"""Exercise the CLI reservation count used by live admission cleanup checks."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


LIB = Path(__file__).with_name("resource.sh")


class ResourceReservations(unittest.TestCase):
    def count(self, response, cli_status=0):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "node-ctl"
            binary.write_text('''#!/usr/bin/env bash
set -euo pipefail
[ "$#" = 4 ] && [ "$1" = resource ] && [ "$2" = list ] && \\
    [ "$3" = --socket ] && [ "$4" = "$WORK/sandbox-resource.sock" ] || exit 91
cat "$RESPONSE_FILE"
exit "$CLI_STATUS"
''')
            binary.chmod(0o755)
            output = root / "response.json"
            output.write_text(response)
            env = {**os.environ, "BIN": directory, "WORK": str(root / "case workspace"),
                   "RESPONSE_FILE": str(output), "CLI_STATUS": str(cli_status)}
            return subprocess.run(
                ["bash", "-c", '. "$1"; resource_reservation_count', "_", str(LIB)],
                env=env, capture_output=True, text=True, timeout=5,
            )

    def test_null_is_zero_reservations(self):
        result = self.count("null\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "0\n")

    def test_empty_array_is_zero_reservations(self):
        result = self.count("[]\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "0\n")

    def test_counts_each_live_reservation(self):
        result = self.count('[{"sandbox_id":"first"},{"sandbox_id":"second"}]')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "2\n")

    def test_other_json_types_are_not_empty_reservations(self):
        for response in ('{}', '0', 'false', '""'):
            with self.subTest(response=response):
                result = self.count(response)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")

    def test_missing_or_malformed_output_does_not_report_a_count(self):
        for response in ("", "[", "controller unavailable"):
            with self.subTest(response=response):
                result = self.count(response)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")

    def test_cli_failure_does_not_report_zero_even_with_null_output(self):
        result = self.count("null", cli_status=37)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
