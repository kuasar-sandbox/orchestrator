#!/usr/bin/env python3
"""Exercise ordinary/system source-gate selection without privileged host changes."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class SourceChecks(unittest.TestCase):
    def run_gate(self, arguments=(), fail=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "calls.jsonl"
            stub = f"#!{sys.executable}\n" + '''import json, os, pathlib, shutil, sys
name = pathlib.Path(sys.argv[0]).name
if name == 'id':
    print('0')
    raise SystemExit(0)
args = [value.replace(os.environ['TMPDIR'], '<tmp>') for value in sys.argv[1:]]
with open(os.environ['CALLS'], 'a') as log:
    print(json.dumps({'tool': name, 'args': args, 'cgo': os.environ.get('CGO_ENABLED'),
                     'builder': os.environ.get('REQUIRE_BUILDER'),
                     'netns': os.environ.get('REQUIRE_TELEMETRY_NETNS')}), file=log)
if os.environ.get('FAIL_MATCH') and os.environ['FAIL_MATCH'] in ' '.join(sys.argv):
    raise SystemExit(73)
if name == 'bash' and sys.argv[1] == 'scripts/ci-e2e-build.sh':
    shutil.copy2(sys.argv[0], pathlib.Path(sys.argv[4]) / 'telemetry.test')
'''
            for tool in ("go", "bash", "python3", "id"):
                path = root / tool
                path.write_text(stub)
                path.chmod(0o755)
            result = subprocess.run(["/bin/bash", str(ROOT / "scripts/ci-source-checks.sh"), *arguments],
                                    env=dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                                             CALLS=str(log), TMPDIR=str(root), FAIL_MATCH=fail),
                                    text=True, capture_output=True, timeout=10)
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            for call in calls:
                if call["tool"] == "bash" and call["args"][0] == "scripts/ci-e2e-build.sh":
                    call["args"][-1] = "<tmp>/owned-helper-directory"
            self.assertEqual(sorted(p.name for p in root.iterdir()),
                             sorted(["go", "bash", "python3", "id"] + (["calls.jsonl"] if log.exists() else [])))
            return result, calls

    def test_split_preserves_all_default_gates(self):
        result, complete = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        result, ordinary = self.run_gate(("--ordinary",))
        self.assertEqual(result.returncode, 0, result.stderr)
        result, privileged = self.run_gate(("--privileged",))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(complete, ordinary + privileged)
        go = [call for call in ordinary if call["tool"] == "go"]
        self.assertEqual([call["args"] for call in go], [
            ["test", "-count=1", "-timeout=5m", "./..."],
            ["test", "-race", "-count=1", "-timeout=5m", "./..."], ["vet", "./..."]])
        self.assertEqual(go[1]["cgo"], "1")
        scripts = {call["args"][0] for call in ordinary if call["tool"] == "bash"}
        self.assertEqual(scripts, {"test/source/runtask_privilege.sh", "test/source/vmm_cgroup.sh",
                                 "test/source/journal_contract.sh", "test/source/capture_cli.sh",
                                 "test/source/cluster_stub.sh", "test/source/builder_state.sh"})
        self.assertEqual([call["tool"] for call in privileged], ["bash", "bash", "bash", "telemetry.test"])
        self.assertEqual(privileged[0]["args"], ["test/source/builder_unit_upgrade.sh"])
        self.assertEqual(privileged[0]["builder"], "1")
        self.assertEqual(privileged[1]["args"], ["test/source/telemetry_backends.sh"])
        self.assertEqual(privileged[2]["args"][:2], ["scripts/ci-e2e-build.sh", "source"])
        self.assertEqual(privileged[3]["netns"], "1")
        self.assertEqual(privileged[3]["args"], ["-test.v", "-test.timeout=90s", "-test.run=^TestOTLPProxyNetNS"])

    def test_compilation_or_privileged_failure_is_never_accepted(self):
        for selection, fail in (("--ordinary", "-race"), ("--privileged", "builder_unit_upgrade.sh"),
                                ("--privileged", "telemetry_backends.sh"),
                                ("--privileged", "ci-e2e-build.sh"), ("--privileged", "telemetry.test")):
            with self.subTest(selection=selection, fail=fail):
                result, _ = self.run_gate((selection,), fail=fail)
                self.assertEqual(result.returncode, 73, result.stderr)

    def test_invalid_selection_fails_before_any_check(self):
        for arguments in (("--unknown",), ("--ordinary", "--privileged")):
            result, calls = self.run_gate(arguments)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
