"""Exercise the maintained source capture gate and its Go JSON result checks."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2] / "source" / "capture_cli.sh"
PARSER = SOURCE.read_text().split('python3 - "$RESULTS" <<\'PY\'\n', 1)[1].split("\nPY\n", 1)[0]
GROUPS = ("TestCapturePairCLIToPausedDatabase", "TestUploadCaptureCLIToPausedDatabase",
          "TestExportPublicationCLIAPI")


def results(extra=()):
    events = []
    for group in GROUPS:
        events.append({"Action": "run", "Test": group})
        for child in ("current-case", *extra):
            name = group + "/" + child
            events.extend(({"Action": "run", "Test": name},
                           {"Action": "pass", "Test": name}))
        events.append({"Action": "pass", "Test": group})
    return events


class SourceCapture(unittest.TestCase):
    def validate(self, events):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.jsonl"
            path.write_text("".join(json.dumps(event) + "\n" for event in events))
            return subprocess.run(["python3", "-", str(path)], input=PARSER,
                                  capture_output=True, text=True, timeout=5)

    def test_all_current_and_added_subcases_must_pass(self):
        for extra in ((), ("new-case", "another-case")):
            result = self.validate(results(extra))
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_empty_output_and_parent_pass_are_not_execution(self):
        for events in ([], [{"Action": "pass"}],
                       [event for event in results() if "/" not in event["Test"]],
                       [event for event in results() if event["Action"] != "run"]):
            with self.subTest(events=events):
                self.assertNotEqual(self.validate(events).returncode, 0)

    def test_skip_failure_missing_group_and_unfinished_child_fail(self):
        for action in ("skip", "fail"):
            events = results()
            events[2]["Action"] = action
            self.assertNotEqual(self.validate(events).returncode, 0)
        missing = [event for event in results() if not event["Test"].startswith(GROUPS[0])]
        self.assertNotEqual(self.validate(missing).returncode, 0)
        unfinished = results() + [{"Action": "run", "Test": GROUPS[0] + "/unfinished"}]
        self.assertNotEqual(self.validate(unfinished).returncode, 0)

    def test_exact_source_input_compiler_invocation_and_nonzero_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sandboxer = root / "sandboxer"
            sandboxer.mkdir()
            fake_bin = root / "bin"
            fake_bin.mkdir()
            go = fake_bin / "go"
            go.write_text('''#!/usr/bin/python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["CAPTURE_CALLS"], "a") as log:
    log.write(json.dumps({"args": args, "cwd": os.getcwd(),
                          "cli": os.environ.get("KUASAR_TEST_SANDBOX_CTL")}) + "\\n")
if args[0] == "build":
    output = pathlib.Path(args[args.index("-o") + 1])
    output.write_text("#!/bin/sh\\nexit 0\\n")
    output.chmod(0o755)
elif args[0] == "test":
    assert os.access(os.environ["KUASAR_TEST_SANDBOX_CTL"], os.X_OK)
    print(pathlib.Path(os.environ["CAPTURE_RESULTS"]).read_text(), end="")
    raise SystemExit(int(os.environ["CAPTURE_EXIT"]))
else:
    raise SystemExit(99)
''')
            go.chmod(0o755)
            events = root / "events"
            events.write_text("".join(json.dumps(event) + "\n" for event in results()))
            calls = root / "calls"
            env = {**os.environ, "PATH": str(fake_bin) + ":" + os.environ["PATH"],
                   "ORG": str(root), "SANDBOXER_SOURCE_ROOT": str(sandboxer),
                   "CAPTURE_CALLS": str(calls), "CAPTURE_RESULTS": str(events),
                   "CAPTURE_EXIT": "0"}
            def run():
                return subprocess.run(["bash", str(SOURCE)], env=env,
                                      capture_output=True, text=True, timeout=5)
            self.assertNotEqual(run().returncode, 0)
            self.assertFalse(calls.exists())
            (sandboxer / "go.mod").write_text("module fixture\n")
            passed = run()
            self.assertEqual(passed.returncode, 0, passed.stderr)
            build, test = [json.loads(line) for line in calls.read_text().splitlines()]
            self.assertEqual(build["cwd"], str(sandboxer))
            self.assertEqual(build["args"][-1], "./cmd/sandbox-ctl")
            self.assertEqual(test["args"][:4], ["test", "-json", "-count=1", "-timeout=5m"])
            self.assertIn("./internal/orch", test["args"])
            self.assertEqual(test["args"][-1], "^(" + "|".join(GROUPS) + ")$")
            env["CAPTURE_EXIT"] = "23"
            failed = run()
            self.assertEqual(failed.returncode, 23, failed.stderr)


if __name__ == "__main__":
    unittest.main()
