"""Exercise the shipped Builder assertions with old and runtime-declared refs."""
import base64
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


LIB = Path(__file__).resolve().parent
KEY = "a" * 64
DIGEST = "b" * 64
RUNTIME = "@runtime:sandbox-runtime.bundle:digest:" + DIGEST


def template(ref, kind="snp", profile="e2b"):
    return f"{profile}-{kind}-" + base64.urlsafe_b64encode(ref.encode()).decode().rstrip("=")


class BuilderArtifact(unittest.TestCase):
    def call(self, function, *args, work=""):
        return subprocess.run(
            ["bash", "-c", 'set -euo pipefail; source "$LIB/builder_artifact.sh"; '
             'fail() { echo "$*" >&2; exit 1; }; "$@"', "fixture", function, *args],
            env={**os.environ, "LIB": str(LIB), "WORK": str(work)},
            capture_output=True, text=True, timeout=5)

    def test_persistent_refs_preserve_the_complete_declaration(self):
        bases = ["manifest://" + KEY]
        bases += [f"file://{KEY}.{extension}@{selector}:{KEY}@location:build.1"
                  for extension, selector in (("bundle", "manifest"), ("snapshot", "hmac"),
                                               ("snapshot", "digest"))]
        for profile in ("e2b", "bare"):
            for base in bases:
                for suffix in ("", RUNTIME):
                    ref = base + suffix
                    with self.subTest(ref=ref, profile=profile):
                        result = self.call("persist_ref", template(ref, profile=profile))
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertEqual(result.stdout, ref + "\n")

    def test_runtime_name_uses_the_production_utf8_byte_boundary(self):
        for name in ("runtime:v1.bundle", "x" * 248 + ".bundle", "界" * 82 + "xx.bundle"):
            ref = "manifest://" + KEY + "@runtime:" + name + ":digest:" + DIGEST
            result = self.call("persist_ref", template(ref))
            self.assertEqual(result.returncode, 0, (name, result.stderr))
            self.assertEqual(result.stdout.strip(), ref)

    def test_invalid_runtime_declarations_and_non_snapshot_kinds_are_rejected(self):
        suffixes = ["@runtime", "@runtime:", "@runtime=runtime.bundle",
                    "@runtime::digest:" + DIGEST, RUNTIME + RUNTIME,
                    RUNTIME + "@location:other", RUNTIME.replace(DIGEST, "B" * 64),
                    RUNTIME.replace(DIGEST, "b" * 63), RUNTIME.replace(":digest:", ":sha256:")]
        for name in (".bundle", "runtime.tar", "/runtime.bundle", "dir/runtime.bundle",
                     "dir\\runtime.bundle", "bad\n.bundle", "bad\x7f.bundle", "bad\x85.bundle",
                     "x" * 249 + ".bundle", "界" * 83 + ".bundle", "x@location:y.bundle",
                     "x@runtime:y.bundle", "x@digest:y.bundle"):
            suffixes.append("@runtime:" + name + ":digest:" + DIGEST)
        for suffix in suffixes:
            with self.subTest(suffix=suffix):
                self.assertNotEqual(self.call("persist_ref", template("manifest://" + KEY + suffix)).returncode, 0)
        for kind in ("img", "sbx"):
            self.assertEqual(self.call("persist_ref", template("manifest://" + KEY, kind)).returncode, 0)
            self.assertNotEqual(self.call("persist_ref", template("manifest://" + KEY + RUNTIME, kind)).returncode, 0)

    def test_canonical_id_and_base_validation_remain_enforced(self):
        valid = template("manifest://" + KEY + RUNTIME)
        for value in (valid + "=", valid + "!", valid.replace("e2b-", "unknown-"),
                      template("manifest://" + "A" * 64 + RUNTIME),
                      template(f"file://{KEY}.bundle@location:build.1" + RUNTIME),
                      template(f"file://{KEY}.bundle@manifest:{KEY}@location:../escape" + RUNTIME)):
            with self.subTest(value=value):
                self.assertNotEqual(self.call("persist_ref", value).returncode, 0)

    def test_located_carrier_path_ignores_only_runtime_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            location = "build.1"
            shard = hashlib.sha256(location.encode()).hexdigest()
            parent = work / "ref-locations" / shard[:2] / shard[2:4] / location
            parent.mkdir(parents=True)
            for ext, selector in (("bundle", "manifest"), ("snapshot", "hmac")):
                path = parent / (KEY + "." + ext)
                path.write_bytes(b"fixture")
                for suffix in ("", RUNTIME):
                    ref = f"file://{KEY}.{ext}@{selector}:{KEY}@location:{location}" + suffix
                    result = self.call("located_file_path", ref, work=work)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout.strip(), str(path))
                    result = self.call("assert_located_final", ref, "." + ext, work=work)
                    self.assertEqual(result.returncode, 0, result.stderr)
                path.unlink()
                self.assertNotEqual(self.call("assert_located_final", ref, "." + ext, work=work).returncode, 0)

    def test_manifest_root_uses_content_key_and_still_rejects_intermediate_roots(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            before = work / "before"
            before.write_text("")
            parent = work / "store/manifest/G1" / KEY[:2] / KEY[2:4]
            parent.mkdir(parents=True)
            (parent / KEY).write_bytes(b"root")
            for suffix in ("", RUNTIME):
                result = self.call("assert_only_manifest_ref_added", str(before),
                                   "manifest://" + KEY + suffix, "final", work=work)
                self.assertEqual(result.returncode, 0, result.stderr)
            (parent / DIGEST).write_bytes(b"intermediate")
            result = self.call("assert_only_manifest_ref_added", str(before),
                               "manifest://" + KEY + RUNTIME, "extra", work=work)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("more than its one final Manifest root", result.stderr)


if __name__ == "__main__":
    unittest.main()
