#!/usr/bin/env python3
"""Prepare fixed FoundationDB test executables in the source-build stage only.

Prepared product cases use these declared helpers with the product's native
runtime. No compiler, download or system installation is needed by the cases.
"""

import argparse
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile

from deb_extract import extract_deb


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("cluster_native", ROOT / "scripts/cluster-native.py")
NATIVE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NATIVE)
RELEASE = "https://github.com/apple/foundationdb/releases/download/7.3.77/"
PACKAGES = {
    "x86_64": (
        ("foundationdb-server_7.3.77-1_amd64.deb", "789f1ed4cbacc0fd7043da2217c9e57516e4ec94fa7140853ec22a7e715785cc"),
        ("foundationdb-clients_7.3.77-1_amd64.deb", "b6c263723009bddbc30b6ae8cf9854340bd35cef14152abb0da8fb5af2b91254")),
    "aarch64": (
        ("foundationdb-server_7.3.77-1_aarch64.deb", "1253a0c8b92b7a5218b94e1d5c6c64813375eb8b3fe051bf99a4e6879e5cb644"),
        ("foundationdb-clients_7.3.77-1_aarch64.deb", "b3a5dd972ffd37c1d5dc0c29ffa00e2169bcac62c0186571acced8b006af7cc2")),
}
PROGRAMS = {"fdbserver-test": "usr/sbin/fdbserver", "fdbcli-test": "usr/bin/fdbcli"}


def prepare(arch, work=None):
    arch = NATIVE.architecture(arch)
    work = Path(work) if work is not None else ROOT / "build/cluster-native" / arch
    runtime, _ = NATIVE.prepare(arch, work)
    output = work / "test-server"
    output.mkdir(parents=True, exist_ok=True)
    # Re-derive the two helpers from authenticated archives. Neither stale
    # extracted files nor executable bytes supplied by the caller are trusted.
    with tempfile.TemporaryDirectory(prefix="test-assets-", dir=work) as temporary:
        package_root = Path(temporary)
        for name, checksum in PACKAGES[arch]:
            package = NATIVE.download({"url": RELEASE + name, "sha256": checksum}, work.parent / "downloads")
            extract_deb(package, package_root, checksum)
        for name, source in PROGRAMS.items():
            NATIVE.copy(package_root / source, output / name, 0o755)
    NATIVE.validate_programs(runtime, arch, [output / name for name in PROGRAMS])
    return runtime, output / "fdbserver-test", output / "fdbcli-test"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arch", required=True, type=NATIVE.architecture)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    # Helper builds consume a sealed source tree. Keep both the architecture
    # work directory and its sibling download cache outside that input tree.
    with tempfile.TemporaryDirectory(prefix="test-fdb-assets-") as temporary:
        _, server, cli = prepare(args.arch, Path(temporary) / args.arch)
        for program in (server, cli):
            NATIVE.copy(program, args.output / program.name, 0o755)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        sys.exit("test-fdb-assets: " + str(error))
