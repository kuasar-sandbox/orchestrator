"""Exercise the standalone pinned-package reader on native build hosts."""

import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

from deb_extract import extract_deb


def package(path, entries):
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        for name, kind, value in entries:
            member = tarfile.TarInfo(name)
            member.mode = 0o755 if kind == "dir" else 0o644
            if kind == "file":
                member.size = len(value)
                archive.addfile(member, io.BytesIO(value))
            else:
                member.type = {"dir": tarfile.DIRTYPE, "symlink": tarfile.SYMTYPE,
                               "hardlink": tarfile.LNKTYPE, "fifo": tarfile.FIFOTYPE}[kind]
                member.linkname = value
                archive.addfile(member)
    result = bytearray(b"!<arch>\n")
    for name, content in [("debian-binary", b"2.0\n"), ("data.tar.gz", payload.getvalue())]:
        header = f"{name + '/':<16}{0:<12}{0:<6}{0:<6}{'100644':<8}{len(content):<10}`\n"
        result.extend(header.encode("ascii"))
        result.extend(content)
        if len(content) % 2:
            result.extend(b"\n")
    path.write_bytes(result)
    return hashlib.sha256(result).hexdigest()


class DebianExtraction(unittest.TestCase):
    def test_archive_links_are_materialized_without_following_host_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "package.deb"
            checksum = package(archive, [
                ("./usr/bin/alias", "symlink", "tool"),
                ("./usr/bin/absolute", "symlink", "/usr/bin/tool"),
                ("./usr/bin/hard", "hardlink", "./usr/bin/tool"),
                ("./usr/bin/tool", "file", b"fixed native payload"),
            ])
            extract_deb(archive, root / "out", checksum)
            for name in ("alias", "absolute", "hard", "tool"):
                path = root / "out/usr/bin" / name
                self.assertEqual(path.read_bytes(), b"fixed native payload")
                self.assertFalse(path.is_symlink())

    def test_hash_mismatch_creates_no_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "package.deb"
            package(archive, [("payload", "file", b"untrusted")])
            with self.assertRaisesRegex(ValueError, "SHA256"):
                extract_deb(archive, root / "out", "0" * 64)
            self.assertFalse((root / "out").exists())

    def test_rejects_unsafe_paths_links_and_types(self):
        cases = [
            [("../escape", "file", b"x")],
            [("/absolute", "file", b"x")],
            [("a", "symlink", "../../escape")],
            [("a", "symlink", "b"), ("b", "symlink", "a")],
            [("a", "file", b"x"), ("a", "file", b"y")],
            [("a", "fifo", "")],
        ]
        for entries in cases:
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                archive = root / "package.deb"
                checksum = package(archive, entries)
                with self.assertRaises(ValueError):
                    extract_deb(archive, root / "out", checksum)
                self.assertFalse((root / "escape").exists())

    def test_existing_symlink_cannot_redirect_extraction(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "out").mkdir()
            (root / "elsewhere").mkdir()
            (root / "out/usr").symlink_to(root / "elsewhere", target_is_directory=True)
            archive = root / "package.deb"
            checksum = package(archive, [("usr/payload", "file", b"x")])
            with self.assertRaisesRegex(ValueError, "escapes"):
                extract_deb(archive, root / "out", checksum)
            self.assertFalse((root / "elsewhere/payload").exists())


if __name__ == "__main__":
    unittest.main()
