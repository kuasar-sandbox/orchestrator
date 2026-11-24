"""Exercise the actual checkpoint identity assertions with runtime metadata."""
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile
import unittest

LIB = Path(__file__).resolve().parent
RUNTIME = "@runtime:sandbox-runtime.bundle:digest:" + "c" * 64


class CheckpointIdentity(unittest.TestCase):
    def pair(self, extension="snapshot", selector="digest", suffix="", defect=""):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / "lib").mkdir()
            root, disk = "a" * 64 + "." + extension, "b" * 64 + ".sandbox"
            (work / root).write_bytes(b"snapshot")
            (work / disk).write_bytes(b"sandbox")
            alias = work / "sid.snapshot"
            alias.symlink_to(root)
            snapshot = f"file://{root}@{selector}:" + "a" * 64 + suffix
            sandbox = f"file://{disk}@digest:" + "b" * 64
            if defect == "missing-disk":
                (work / disk).unlink()
            elif defect == "wrong-alias":
                alias.unlink()
                alias.symlink_to(disk)
            elif defect == "traversal":
                snapshot = snapshot.replace("file://", "file://../")
            with sqlite3.connect(work / "lib/node-ctl.db") as db:
                db.execute("create table sandboxes(id,state,resume_source_kind,resume_source_ref,resume_sandbox_ref,launch_mode)")
                db.execute("insert into sandboxes values(?,?,?,?,?,?)",
                           ("sid", "paused", "snapshot", snapshot, sandbox, ""))
            result = subprocess.run(
                ["bash", "-c", 'set -euo pipefail; source "$LIB/execute_sandbox.sh"; '
                 'pair=$(checkpoint_pair sid snapshot "$WORK/sid.snapshot"); printf "%s\\n" "$pair"; '
                 'checkpoint_runtime_ref "$pair" "$WORK"'],
                env={**os.environ, "LIB": str(LIB), "WORK": str(work)},
                text=True, capture_output=True, timeout=5)
            return result, snapshot, sandbox, str(work)

    def test_old_and_declared_pairs_keep_full_stored_identity(self):
        for extension, selector in (("snapshot", "digest"), ("snapshot", "hmac"), ("bundle", "manifest")):
            for suffix in ("", RUNTIME):
                with self.subTest(extension=extension, suffix=suffix):
                    result, snapshot, sandbox, work = self.pair(extension, selector, suffix)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    lines = result.stdout.splitlines()
                    self.assertEqual(json.loads(lines[0]), ["paused", "snapshot", snapshot, sandbox, ""])
                    self.assertEqual(lines[1], "file://" + work + "/" + snapshot.removeprefix("file://"))

    def test_declared_pair_still_requires_matching_alias_and_both_real_carriers(self):
        for defect, diagnostic in (("missing-disk", "carrier unavailable"),
                                   ("wrong-alias", "differs from committed alias"),
                                   ("traversal", "content-identified basename")):
            with self.subTest(defect=defect):
                result, _, _, _ = self.pair(suffix=RUNTIME, defect=defect)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(diagnostic, result.stderr)

    def test_memory_parent_assertions_compare_the_physical_identity(self):
        parent = "file://" + "a" * 64 + ".snapshot@digest:" + "a" * 64
        for case in ("orchestrator.snapshot.sh", "orchestrator.pause-wake.sh"):
            source = (LIB.parent / "cases" / case).read_text()
            snippet = re.search(r'(?m)^expected_parent = .*\nassert descriptor.get\("parents"\).*', source).group()
            for suffix in ("", RUNTIME):
                environment = {"os": os, "parent_ref": parent + suffix, "previous_s": parent + suffix,
                               "root": "/checkpoint", "sys": type("Args", (), {"argv": [""] * 6 + ["/checkpoint/current"]}),
                               "descriptor": {"parents": ["file:///checkpoint/" + parent.removeprefix("file://")]}}
                with self.subTest(case=case, suffix=suffix):
                    exec(snippet, environment)
                    environment["descriptor"]["parents"] = ["file:///checkpoint/other.snapshot"]
                    with self.assertRaises(AssertionError):
                        exec(snippet, environment)


if __name__ == "__main__":
    unittest.main()
