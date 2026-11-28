"""Exercise the source stub's real build/recursion entry without backend daemons."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2] / "source" / "cluster_stub.sh"


class ClusterStubPrebuilt(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.script = self.root / "test/source/cluster_stub.sh"
        self.script.parent.mkdir(parents=True)
        # Keep the production build/parent/child entry intact. Backend behavior
        # remains covered by the 13 real topology cases after this boundary.
        prefix = SOURCE.read_text().split('\nREGISTRIES="${REGISTRIES:-1}"', 1)[0]
        self.script.write_text(prefix + '\nprintf "executed-case:%s:%s\\n" "$CLUSTER_STUB_CASE" "$CLUSTER_CTL"\n')
        self.tools = self.root / "tools"
        self.tools.mkdir()
        self.calls = self.root / "calls.jsonl"
        go = self.tools / "go"
        go.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["BUILD_CALLS"], "a") as out:
    out.write(json.dumps(["go", *args]) + "\\n")
if args == ["version"]:
    print("go version fixture")
elif args[0] == "build":
    target = pathlib.Path(args[args.index("-o") + 1])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("#!/bin/sh\\nexit 0\\n")
    target.chmod(0o755)
else:
    raise SystemExit(97)
''')
        go.chmod(0o755)
        native = self.root / "scripts/cluster-native.py"
        native.parent.mkdir()
        native.write_text('''import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["BUILD_CALLS"], "a") as out:
    out.write(json.dumps(["native", *args]) + "\\n")
if args[0] == "build":
    output = pathlib.Path(args[args.index("--output") + 1])
    root = output / "cluster-runtime"
    (root / "bin").mkdir(parents=True)
    for name in ("cluster-ctl", "run-cluster-ctl"):
        target = root / "bin" / name
        target.write_text("#!/bin/sh\\nexit 0\\n")
        target.chmod(0o755)
        (output / name).symlink_to("cluster-runtime/bin/" + name)
    (root / "private-loader").write_text("full runtime fixture")
elif args[0] == "validate":
    root = pathlib.Path(args[args.index("--root") + 1])
    if not (root / "private-loader").is_file():
        raise SystemExit(29)
else:
    raise SystemExit(98)
''')
        self.cluster_file = self.root / "fdb.cluster"
        self.cluster_file.write_text("fixture")

    def run_stub(self, backend, *, binary=None, **extra):
        env = {**os.environ, "PATH": str(self.tools) + ":" + os.environ["PATH"],
               "BUILD_CALLS": str(self.calls), "BIN": str(binary or self.root / backend),
               "CLUSTER_STUB_BACKEND": backend, "FDB_CLUSTER_FILE": str(self.cluster_file),
               "STORAGE_TEST_POSTGRES_DSN": "fixture-dsn"}
        for key in ("CLUSTER_STUB_CASE", "CLUSTER_STUB_PREBUILT", "CLUSTER_STUB_BUILD_ONLY",
                    "CLUSTER_CTL", "NODE_STUB_CTL", "E2B_KEY_CTL", "CLUSTER_STUB_COMMON_BIN"):
            env.pop(key, None)
        env.update(extra)
        return subprocess.run(["bash", str(self.script)], env=env,
                              text=True, capture_output=True, timeout=10)

    def call_rows(self):
        return [json.loads(line) for line in self.calls.read_text().splitlines()] if self.calls.exists() else []

    def assert_cases(self, result, count, backend):
        self.assertEqual(result.returncode, 0, result.stderr)
        cases = [line for line in result.stdout.splitlines() if line.startswith("executed-case:")]
        self.assertEqual(len(cases), count, result.stdout)
        target = "run-cluster-ctl" if backend == "foundationdb" else "cluster-ctl-pure"
        self.assertTrue(all(line.endswith("/" + target) for line in cases))

    def test_standalone_parent_builds_once_and_every_topology_consumes_it(self):
        for backend, count in (("foundationdb", 6), ("sqlite", 1), ("postgres", 6)):
            with self.subTest(backend=backend):
                before = len(self.call_rows())
                self.assert_cases(self.run_stub(backend), count, backend)
                rows = self.call_rows()[before:]
                self.assertEqual(sum(row[:2] == ["go", "build"] for row in rows),
                                 2 if backend == "foundationdb" else 3)
                self.assertEqual(sum(row[:2] == ["native", "build"] for row in rows),
                                 1 if backend == "foundationdb" else 0)

    def test_source_gate_reuses_two_modes_across_all_thirteen_cases(self):
        native, pure = self.root / "native", self.root / "pure"
        for backend, binary in (("foundationdb", native), ("sqlite", pure)):
            result = self.run_stub(backend, binary=binary, CLUSTER_STUB_BUILD_ONLY="1",
                                   FDB_CLUSTER_FILE="", STORAGE_TEST_POSTGRES_DSN="",
                                   CLUSTER_STUB_COMMON_BIN=str(native) if backend == "sqlite" else "")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("executed-case:", result.stdout)
        builds = [row for row in self.call_rows() if row[1] == "build"]
        for backend, count, binary in (("foundationdb", 6, native), ("sqlite", 1, pure), ("postgres", 6, pure)):
            self.assert_cases(self.run_stub(backend, binary=binary, CLUSTER_STUB_PREBUILT="1",
                                            CLUSTER_STUB_COMMON_BIN=str(native) if backend != "foundationdb" else ""), count, backend)
        self.assertEqual([row for row in self.call_rows() if row[1] == "build"], builds)
        self.assertEqual(sum(row[:2] == ["native", "build"] for row in builds), 1)
        self.assertEqual(sum(row[:2] == ["go", "build"] for row in builds), 3)

    def test_missing_common_binary_fails_before_any_build(self):
        result = self.run_stub("sqlite", CLUSTER_STUB_BUILD_ONLY="1",
                               CLUSTER_STUB_COMMON_BIN=str(self.root / "missing"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing common", result.stderr)
        self.assertEqual(self.call_rows(), [])

    def test_missing_prebuilt_binary_fails_without_build_fallback(self):
        result = self.run_stub("sqlite", CLUSTER_STUB_PREBUILT="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing prebuilt", result.stderr)
        self.assertEqual(self.call_rows(), [])

    def test_native_prebuilt_requires_the_full_runtime_closure(self):
        self.assertEqual(self.run_stub("foundationdb", CLUSTER_STUB_BUILD_ONLY="1").returncode, 0)
        (self.root / "foundationdb/cluster-runtime/private-loader").unlink()
        before = len(self.call_rows())
        result = self.run_stub("foundationdb", CLUSTER_STUB_PREBUILT="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid prebuilt FoundationDB runtime closure", result.stderr)
        self.assertEqual([row[:2] for row in self.call_rows()[before:]], [["native", "validate"]])

    def test_postgres_execution_still_requires_its_real_backend_input(self):
        result = self.run_stub("postgres", CLUSTER_STUB_PREBUILT="1", STORAGE_TEST_POSTGRES_DSN="")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("STORAGE_TEST_POSTGRES_DSN is required", result.stderr)
        self.assertEqual(self.call_rows(), [])

    def test_invalid_build_controls_fail(self):
        for values in ({"CLUSTER_STUB_PREBUILT": "yes"}, {"CLUSTER_STUB_BUILD_ONLY": "yes"}):
            with self.subTest(values=values):
                result = self.run_stub("sqlite", **values)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must be 0 or 1", result.stderr)
                self.assertEqual(self.call_rows(), [])


if __name__ == "__main__":
    unittest.main()
