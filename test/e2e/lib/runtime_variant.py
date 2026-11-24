#!/usr/bin/env python3
"""Make a different valid runtime identity by changing only alignment padding."""
import argparse
import hashlib
from pathlib import Path
import re
import struct
import zipfile


def identity(path):
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        if len(entries) != 1:
            raise ValueError("runtime must contain one digest marker")
        entry = entries[0]
        if entry.file_size != 0 or entry.compress_size != 0 or entry.compress_type != zipfile.ZIP_STORED:
            raise ValueError("runtime digest marker must be empty and uncompressed")
        if not re.fullmatch(r"\.kuasar\.digest\.[0-9a-f]{64}", entry.filename):
            raise ValueError("runtime digest marker is invalid")
        prefix_size = entry.header_offset
        marker = entry.filename.encode()
    with open(path, "rb") as stream:
        prefix = stream.read(prefix_size)
        footer = stream.read()
    if len(prefix) < 1064 or len(prefix) + len(footer) == 0 or (len(prefix) + len(footer)) % (2 << 20):
        raise ValueError("runtime is not aligned")
    if struct.unpack_from("<I", prefix, 1024)[0] != 0xE0F5E1E2:
        raise ValueError("runtime EROFS magic is missing")
    block_bits = prefix[1024 + 12]
    blocks = struct.unpack_from("<I", prefix, 1024 + 36)[0]
    if not 9 <= block_bits <= 16 or not blocks:
        raise ValueError("runtime EROFS geometry is invalid")
    filesystem_size = blocks << block_bits
    if filesystem_size >= prefix_size:
        raise ValueError("runtime has no verified alignment padding")
    digest = hashlib.sha256(prefix).hexdigest()
    if not marker.endswith(digest.encode()) or footer.count(marker) != 2:
        raise ValueError("runtime prefix does not match its declared identity")
    return prefix, footer, marker, filesystem_size, digest


def create_variant(source, target):
    source, target = Path(source), Path(target)
    if source.resolve() == target.resolve():
        raise ValueError("variant requires a separate destination")
    prefix, footer, marker, filesystem_size, old = identity(source)
    modified = bytearray(prefix)
    modified[-1] ^= 1
    new = hashlib.sha256(modified).hexdigest()
    new_marker = marker[:-64] + new.encode()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as output:
        output.write(modified)
        output.write(footer.replace(marker, new_marker))
    after, _, _, _, actual = identity(target)
    if actual == old or actual != new or after[:filesystem_size] != prefix[:filesystem_size]:
        raise ValueError("runtime variant did not preserve EROFS and change identity")
    return new


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source")
    parser.add_argument("target")
    args = parser.parse_args()
    create_variant(args.source, args.target)
    print("PASS: runtime variant preserves EROFS with a verified distinct prefix digest")


if __name__ == "__main__":
    main()
