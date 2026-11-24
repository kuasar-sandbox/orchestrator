"""Exercise the shipped portable Bundle graph assertions without KVM."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile


CASE = Path(__file__).resolve().parents[1] / "cases" / "orchestrator.snapshot.sh"
# Execute the actual E2E assertions, not separately maintained approximations.
def shipped_assertion(marker):
    body = CASE.read_text().split("<<'" + marker + "'", 1)[1].split("\n" + marker + "\n", 1)[0]
    return body[body.index("\nimport ") + 1:]


PARSER = shipped_assertion("PY_BUNDLE_B")
A, B, H, PUBLIC, BASE, LOWER = (char * 64 for char in "abcdef")


def ref(key):
    return "manifest://" + key


def located(key):
    return "file://" + key + ".bundle@manifest:" + key + "@location:bundle"


class SnapshotBundleGraph(unittest.TestCase):
    def validate(self, refs=None, suffix=None, members=None, extra_cfg=None,
                 root_members=None, missing_history=False, parser=PARSER, annotation=""):
        if refs is None:
            refs = [located(H), ref(BASE), ref(LOWER)]
        if suffix is None:
            suffix = [ref(BASE), ref(LOWER)]
        if members is None:
            members = ["manifest/" + H]
        if root_members is None:
            root_members = ["manifest/" + PUBLIC]
        cfg = {"FromRefs": refs, **(extra_cfg or {})}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current, parent = (root / name for name in ("b.json", "a.json"))
            current.write_text(json.dumps(cfg))
            parent.write_text(json.dumps({"FromRefs": suffix}))
            carriers = [(PUBLIC, root_members)]
            if not missing_history:
                carriers.append((H, members))
            for key, names in carriers:
                with zipfile.ZipFile(root / (key + ".bundle"), "w") as archive:
                    for name in names:
                        archive.writestr(name, b"fixture")
            return subprocess.run(
                [sys.executable, "-", str(current), str(parent), str(root), located(PUBLIC) + annotation, A, B],
                input=parser, text=True, capture_output=True, timeout=5)

    def assert_rejected(self, diagnostic, **kwargs):
        result = self.validate(**kwargs)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(diagnostic, result.stderr)

    def test_materialized_history_has_its_own_exact_carrier(self):
        result = self.validate()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_manifest_prefetch_key_uses_the_carrier_identity(self):
        source = CASE.read_text()
        start = source.index('PORTABLE_W_KEY=')
        end = source.index('# After paused cleanup', start)
        script = ('set -euo pipefail; fail() { exit 1; }; PORTABLE_W_REF=$1;\n'
                  + source[start:end] + '\nprintf "%s" "$PORTABLE_W_KEY"')
        for suffix in ("", "@runtime:runtime.bundle:digest:" + "1" * 64):
            result = subprocess.run(["bash", "-c", script, "fixture", ref(PUBLIC) + suffix],
                                    text=True, capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, PUBLIC)
        for invalid in ("manifest://" + "g" * 64, "manifest://" + PUBLIC + "/extra", located(PUBLIC)):
            result = subprocess.run(["bash", "-c", script, "fixture", invalid],
                                    text=True, capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)

    def test_runtime_declaration_does_not_change_root_or_history_identity(self):
        annotation = "@runtime:sandbox-runtime.bundle:digest:" + "1" * 64
        for parser in (PARSER, shipped_assertion("PY_BUNDLE")):
            with self.subTest(parser=parser[:50]):
                result = self.validate(parser=parser, annotation=annotation)
                self.assertEqual(result.returncode, 0, result.stderr)
                result = self.validate(parser=parser, annotation=annotation,
                                       refs=[located(PUBLIC), ref(BASE), ref(LOWER)])
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue("own root" in result.stderr or "new immutable identity" in result.stderr,
                                result.stderr)

    def test_full_local_snapshot_cannot_impersonate_materialized_history(self):
        for key in (A, B):
            with self.subTest(key=key):
                self.assert_rejected("reused a full local Snapshot", refs=[located(key), ref(BASE), ref(LOWER)])

    def test_one_local_history_without_external_suffix(self):
        result = self.validate(refs=[located(H)], suffix=[])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_actual_history_cannot_be_masked_by_original_a(self):
        self.assert_rejected("exact single-root carrier", members=["manifest/" + A])
        self.assert_rejected("FileNotFoundError", missing_history=True)

    def test_named_publication_keeps_single_root_carriers(self):
        self.assert_rejected("exact single-root carrier", members=["manifest/" + H, "manifest/" + A])
        self.assert_rejected("exact single-root carrier", root_members=["manifest/" + PUBLIC, "manifest/" + H])

    def test_external_refs_entry_is_rejected_even_when_all_manifests_exist(self):
        self.assert_rejected("unexpectedly retained external Bundle locations", members=[
            "manifest/" + H, "bundle/refs"])

    def test_missing_and_extra_history_are_rejected(self):
        for refs in ([], [ref(BASE), ref(LOWER)], [located(H), located(A), ref(BASE), ref(LOWER)]):
            with self.subTest(refs=refs):
                self.assert_rejected("want one history plus inherited", refs=refs)

    def test_inherited_suffix_cannot_change_or_reorder(self):
        for refs in ([located(H), ref(LOWER), ref(BASE)], [located(H), ref(A), ref(LOWER)]):
            with self.subTest(refs=refs):
                self.assert_rejected("want one history plus inherited", refs=refs)

    def test_history_must_be_a_canonical_selector_at_the_published_location(self):
        for history in ("file:///old.bundle@manifest:" + H, "manifest://" + "g" * 64,
                        ref(H), located(H).replace("location:bundle", "location:other"),
                        located(H).replace(H + ".bundle", A + ".bundle"), None):
            with self.subTest(history=history):
                self.assert_rejected("not a canonical published Bundle selector", refs=[history, ref(BASE), ref(LOWER)])

    def test_self_reference_is_rejected_even_with_a_matching_member(self):
        self.assert_rejected("references its own root", refs=[located(PUBLIC), ref(BASE), ref(LOWER)])

    def test_physical_selectors_cannot_hide_in_nested_configuration(self):
        self.assert_rejected("physical Bundle selectors leaked", extra_cfg={
            "Nested": {"values": ["file:///old.bundle@manifest:" + A]}})

    def test_published_disk_selectors_are_allowed(self):
        result = self.validate(extra_cfg={"Boot": {"Root": {"Base": located("1" * 64)}}})
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_fixed_runtime_digest_ref_is_not_a_bundle_manifest_selector(self):
        result = self.validate(extra_cfg={"Boot": {"RuntimeRef": "file://sandbox-runtime.bundle@digest:" + A}})
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_c_composition_requires_new_history_and_separate_carriers(self):
        parser = shipped_assertion("PY_BUNDLE")
        result = self.validate(parser=parser)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_rejected("new immutable identity", parser=parser, refs=[located(A), ref(BASE), ref(LOWER)])
        self.assert_rejected("separate single-root", parser=parser, root_members=["manifest/" + PUBLIC, "manifest/" + A])


if __name__ == "__main__":
    unittest.main()
