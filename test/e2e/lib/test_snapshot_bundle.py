"""Exercise the shipped Bundle B graph assertion without KVM or product mocks."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile


CASE = Path(__file__).resolve().parents[1] / "cases" / "orchestrator.snapshot.sh"
# Execute the actual E2E assertion, not a separately maintained approximation.
PARSER = CASE.read_text().split("import json, re, sys, zipfile\n", 1)[1].split(
    "\nPY_BUNDLE_B\n", 1)[0]
PARSER = "import json, re, sys, zipfile\n" + PARSER
A, B, H, BASE, LOWER = (char * 64 for char in "abcef")


def ref(key):
    return "manifest://" + key


class SnapshotBundleGraph(unittest.TestCase):
    def validate(self, refs=None, suffix=None, members=None, extra_cfg=None):
        if refs is None:
            refs = [ref(H), ref(BASE), ref(LOWER)]
        if suffix is None:
            suffix = [ref(BASE), ref(LOWER)]
        if members is None:
            members = ["manifest/" + key for key in (B, H)]
        cfg = {"FromRefs": refs, **(extra_cfg or {})}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current, parent, bundle = (root / name for name in ("b.json", "a.json", "b.bundle"))
            current.write_text(json.dumps(cfg))
            parent.write_text(json.dumps({"FromRefs": suffix}))
            with zipfile.ZipFile(bundle, "w") as archive:
                for name in members:
                    archive.writestr(name, b"fixture")
            return subprocess.run(
                [sys.executable, "-", str(current), str(parent), str(bundle), B],
                input=PARSER, text=True, capture_output=True, timeout=5)

    def assert_rejected(self, diagnostic, **kwargs):
        result = self.validate(**kwargs)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(diagnostic, result.stderr)

    def test_trimmed_single_parent_uses_actual_history_not_original_a(self):
        # No A member: the original assertion rejects this valid #380 graph.
        result = self.validate()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_legacy_all_dirty_parent_may_retain_its_identity(self):
        result = self.validate(refs=[ref(A), ref(BASE), ref(LOWER)],
                               members=["manifest/" + key for key in (B, A, BASE, LOWER)])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_one_local_history_without_external_suffix(self):
        result = self.validate(refs=[ref(H)], suffix=[], members=["manifest/" + key for key in (B, H)])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_actual_history_cannot_be_masked_by_original_a(self):
        self.assert_rejected("omits referenced memory history manifest", members=[
            "manifest/" + key for key in (B, A, BASE, LOWER)])

    def test_portable_suffix_may_remain_remote_or_already_be_embedded(self):
        # The inherited published template is not a local history carrier.
        # Absence of bundle/refs must not imply recursive Store materialization.
        for keys in ((B, H), (B, H, BASE), (B, H, BASE, LOWER)):
            with self.subTest(keys=keys):
                result = self.validate(members=["manifest/" + key for key in keys])
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_external_refs_entry_is_rejected_even_when_all_manifests_exist(self):
        self.assert_rejected("unexpectedly retained external Bundle locations", members=[
            *("manifest/" + key for key in (B, H, BASE, LOWER)), "bundle/refs"])

    def test_missing_and_extra_history_are_rejected(self):
        for refs in ([], [ref(BASE), ref(LOWER)], [ref(H), ref(A), ref(BASE), ref(LOWER)]):
            with self.subTest(refs=refs):
                self.assert_rejected("want one history plus inherited", refs=refs)

    def test_inherited_suffix_cannot_change_or_reorder(self):
        for refs in ([ref(H), ref(LOWER), ref(BASE)], [ref(H), ref(A), ref(LOWER)]):
            with self.subTest(refs=refs):
                self.assert_rejected("want one history plus inherited", refs=refs)

    def test_history_must_be_a_canonical_logical_manifest_ref(self):
        for history in ("file:///old.bundle@manifest:" + H, "manifest://" + "g" * 64,
                        "manifest://short", "https://example.test/history", None):
            with self.subTest(history=history):
                self.assert_rejected("not logical Manifest references", refs=[history, ref(BASE), ref(LOWER)])

    def test_self_reference_is_rejected_even_with_a_matching_member(self):
        self.assert_rejected("references its own root", refs=[ref(B), ref(BASE), ref(LOWER)])

    def test_physical_selectors_cannot_hide_in_nested_configuration(self):
        self.assert_rejected("physical Bundle selectors leaked", extra_cfg={
            "Nested": {"values": ["file:///old.bundle@manifest:" + A]}})


if __name__ == "__main__":
    unittest.main()
