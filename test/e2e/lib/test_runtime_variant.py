import hashlib
import io
from pathlib import Path
import struct
import tempfile
import unittest
import zipfile

from runtime_variant import create_variant, identity


def footer(digest):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(".kuasar.digest." + digest, b"")
    return output.getvalue()


def bundle(path, *, blocks=1):
    prefix = bytearray((2 << 20) - len(footer("0" * 64)))
    struct.pack_into("<I", prefix, 1024, 0xE0F5E1E2)
    prefix[1036] = 12
    struct.pack_into("<I", prefix, 1060, blocks)
    prefix[2000:2015] = b"payload-fixture"
    path.write_bytes(prefix + footer(hashlib.sha256(prefix).hexdigest()))


class RuntimeVariantTest(unittest.TestCase):
    def test_changes_only_verified_padding_and_updates_real_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp) / "v1.bundle", Path(tmp) / "v2.bundle"
            bundle(source)
            before = source.read_bytes()
            changed = create_variant(source, target)
            self.assertEqual(source.read_bytes(), before)
            first, _, _, used, original = identity(source)
            second, _, _, _, actual = identity(target)
            self.assertEqual(changed, actual)
            self.assertNotEqual(actual, original)
            self.assertEqual(first[:used], second[:used])
            self.assertEqual([i for i, pair in enumerate(zip(first, second)) if pair[0] != pair[1]], [len(first) - 1])

    def test_no_padding_is_rejected_before_destination_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp) / "v1.bundle", Path(tmp) / "v2.bundle"
            bundle(source, blocks=512)
            with self.assertRaisesRegex(ValueError, "no verified alignment padding"):
                create_variant(source, target)
            self.assertFalse(target.exists())

    def test_forged_prefix_identity_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp) / "v1.bundle", Path(tmp) / "v2.bundle"
            bundle(source)
            raw = bytearray(source.read_bytes())
            raw[2000] ^= 1
            source.write_bytes(raw)
            with self.assertRaisesRegex(ValueError, "does not match"):
                create_variant(source, target)
            self.assertFalse(target.exists())

    def test_existing_destination_and_source_are_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp) / "v1.bundle", Path(tmp) / "v2.bundle"
            bundle(source)
            with self.assertRaises(ValueError):
                create_variant(source, source)
            target.write_bytes(b"existing")
            with self.assertRaises(FileExistsError):
                create_variant(source, target)
            self.assertEqual(target.read_bytes(), b"existing")


if __name__ == "__main__":
    unittest.main()
