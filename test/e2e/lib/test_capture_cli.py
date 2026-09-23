import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import capture_cli


def results(extra=()):
    lines = []
    for group in capture_cli.GROUPS:
        children = [group + "/current-case", *(group + "/" + name for name in extra)]
        lines.append("=== RUN   " + group)
        for child in children:
            lines.extend(["=== RUN   " + child, "    --- PASS: " + child + " (0.01s)"])
        lines.append("--- PASS: " + group + " (0.01s)")
    return "\n".join([*lines, "PASS", ""])


class CaptureCLI(unittest.TestCase):
    def validate(self, text):
        with contextlib.redirect_stdout(io.StringIO()):
            capture_cli.validate_results(text)

    def test_all_current_subcases_including_added_cases_are_required(self):
        self.validate(results())
        self.validate(results(("new-case", "another-case")))

    def test_zero_exit_or_parent_pass_is_not_execution(self):
        for output in ("PASS\n", "", results().replace("=== RUN", "not run"),
                       "\n".join(line for line in results().splitlines() if "/" not in line),
                       results().replace("PASS\n", "")):
            with self.subTest(output=output), self.assertRaises(ValueError):
                self.validate(output)

    def test_any_skip_failure_missing_group_or_incomplete_subcase_fails(self):
        for output in (results().replace("--- PASS:", "--- SKIP:", 1),
                       results().replace("--- PASS:", "--- FAIL:", 1),
                       results().replace(capture_cli.GROUPS[0], "TestUnselected"),
                       results() + "=== RUN   " + capture_cli.GROUPS[0] + "/unfinished\n"):
            with self.subTest(output=output), self.assertRaises(ValueError):
                self.validate(output)

    def test_input_checks_and_actual_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            products = root / "bin"
            products.mkdir()
            helper = root / "orch-cli.test"
            for path in (products / "sandbox-ctl", products / "node-ctl", helper):
                with self.assertRaisesRegex(ValueError, "missing executable"):
                    capture_cli.run(products, helper)
                path.write_text("fixture")
                path.chmod(0o755)
            def execute(command, **kwargs):
                self.assertEqual(command[0], str(helper))
                self.assertEqual(kwargs["env"]["KUASAR_TEST_SANDBOX_CTL"], str(products / "sandbox-ctl"))
                if command[1].startswith("-test.list="):
                    return subprocess.CompletedProcess(command, 0, "\n".join(capture_cli.GROUPS) + "\n")
                self.assertIn("-test.run=" + capture_cli.PATTERN, command)
                self.assertIn("-test.count=1", command)
                return subprocess.CompletedProcess(command, 0, results())
            with patch.object(subprocess, "run", side_effect=execute), contextlib.redirect_stdout(io.StringIO()):
                capture_cli.run(products, helper)
            for listing in ("", capture_cli.GROUPS[0] + "\n"):
                with patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 0, listing)), \
                     self.assertRaisesRegex(ValueError, "does not select"):
                    capture_cli.run(products, helper)
            with patch.object(subprocess, "run", side_effect=[
                    subprocess.CompletedProcess([], 0, "\n".join(capture_cli.GROUPS)),
                    subprocess.CompletedProcess([], 23, results())]), \
                 contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, "exit 23"):
                capture_cli.run(products, helper)
            with patch.object(subprocess, "run", side_effect=[
                    subprocess.CompletedProcess([], 0, "\n".join(capture_cli.GROUPS)),
                    subprocess.CompletedProcess([], 0, results().replace("--- PASS:", "--- SKIP:", 1))]), \
                 contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, "skipped"):
                capture_cli.run(products, helper)


if __name__ == "__main__":
    unittest.main()
