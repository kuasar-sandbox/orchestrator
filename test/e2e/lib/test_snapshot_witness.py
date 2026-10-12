import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import retain_checkpoint
import snapshot_witness


class SnapshotWitnessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.sid = "fixture-guest"
        self.source = self.root / "sandboxes" / self.sid / "checkpoint"
        self.source.mkdir(parents=True)
        self.name = "a" * 64 + ".snapshot"
        (self.source / self.name).write_bytes(b"synthetic private source")
        (self.source / (self.sid + ".snapshot")).symlink_to(self.name)
        self.dest = self.root / "retained"
        self.report = retain_checkpoint.retain_checkpoint(self.source, self.root / "sandboxes", self.dest, self.sid)
        self.manifest = self.dest / "manifest.json"
        self.journal = self.root / "journal"
        self.witness = dict(pagemap_status="pfn_visible", guest_physical_address=4096 + 16,
                            pfn=1, page_size=4096, first_bad_va=8192 + 16)
        self.write_witness(self.witness)

    def write_witness(self, value):
        self.journal.write_text("held-memory witness " + json.dumps(value) + "\n")

    def response(self, **changes):
        result = dict(memory_bytes=1 << 30, offset=4112, length=16, nonzero=16,
                      ff_bytes=16, sha256="b" * 64, source_runs={"Data": {"runs": 1, "bytes": 16, "nonzero": 16}})
        result.update(changes)
        return SimpleNamespace(returncode=0, stdout=json.dumps(result), stderr="")

    def test_retention_survives_source_cleanup_and_reader_is_bounded(self):
        original_inode = (self.source / self.name).stat().st_ino
        shutil.rmtree(self.source)
        self.assertEqual((self.dest / self.name).stat().st_ino, original_inode)
        with patch.object(snapshot_witness.subprocess, "run", return_value=self.response()) as run:
            result = snapshot_witness.inspect("/test/runner", self.manifest, self.journal)
        self.assertEqual(result["source_ff_bytes"], 16)
        self.assertEqual(result["status"], "observed")
        self.assertFalse(result["payload_digest_verified"])
        args = run.call_args_list[0].args[0]
        self.assertEqual(args[1], "__e2e_snapshot_window")
        self.assertIn("-length", args)
        self.assertEqual(args[-1], "16")
        self.assertTrue(args[args.index("-ref") + 1].endswith("@digest:" + "a" * 64))
        self.assertEqual(run.call_args.kwargs["timeout"], 4)
        self.assertNotIn("prefix_hex", result)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args_list[1].args[0][1], "__e2e_snapshot_pagetable")
        self.assertEqual(run.call_args_list[1].args[0][-1], str(self.witness["first_bad_va"]))
        self.assertIn("saved_paging", result)

    def test_unusable_page_witness_never_invokes_reader(self):
        cases = [dict(self.witness, pagemap_status="pfn_zero_or_masked"),
                 dict(self.witness, pfn=0), dict(self.witness, page_size=65536),
                 dict(self.witness, guest_physical_address=0),
                 dict(self.witness, guest_physical_address=True),
                 dict(self.witness, guest_physical_address=1 << 31, pfn=1 << 19, first_bad_va=0)]
        for witness in cases:
            with self.subTest(witness=witness):
                self.write_witness(witness)
                with patch.object(snapshot_witness.subprocess, "run") as run:
                    with self.assertRaises(ValueError):
                        snapshot_witness.inspect("/test/runner", self.manifest, self.journal)
                    run.assert_not_called()

    def test_changed_inode_unavailable(self):
        path = self.dest / self.name
        path.unlink()
        path.write_bytes(b"replacement")
        with self.assertRaises(ValueError):
            snapshot_witness.inspect("/test/runner", self.manifest, self.journal)

    def test_reader_failure_and_wrong_geometry_rejected(self):
        for response in [SimpleNamespace(returncode=1, stdout="", stderr="private diagnostic"),
                         self.response(memory_bytes=2 << 30), self.response(offset=0)]:
            with self.subTest(response=response):
                with patch.object(snapshot_witness.subprocess, "run", return_value=response):
                    with self.assertRaises(ValueError):
                        snapshot_witness.inspect("/test/runner", self.manifest, self.journal)

    def test_retention_refuses_overwrite_or_count_limit(self):
        with self.assertRaises(FileExistsError):
            retain_checkpoint.retain_checkpoint(self.source, self.root / "sandboxes", self.dest, self.sid)
        (self.source / ("c" * 64 + ".sandbox")).write_bytes(b"extra")
        with self.assertRaises(ValueError):
            retain_checkpoint.retain_checkpoint(self.source, self.root / "sandboxes", self.root / "count-limited", self.sid, max_files=1)

    def test_retention_rejects_escape_and_canonical_symlink(self):
        alias = self.source / (self.sid + ".snapshot")
        alias.unlink()
        alias.symlink_to("../../outside.snapshot")
        with self.assertRaises(ValueError):
            retain_checkpoint.retain_checkpoint(self.source, self.root / "sandboxes", self.root / "escaped", self.sid)
        alias.unlink()
        alias.symlink_to(self.name)
        (self.source / self.name).unlink()
        (self.source / self.name).symlink_to("/dev/null")
        with self.assertRaises(OSError):
            retain_checkpoint.retain_checkpoint(self.source, self.root / "sandboxes", self.root / "linked", self.sid)


if __name__ == "__main__":
    unittest.main()
