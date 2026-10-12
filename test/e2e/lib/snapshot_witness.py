"""Bounded, failure-only comparison with the retained local Snapshot.

The address is the Guest physical address observed at failure, not proof of the
mapping at capture. No plaintext memory or configuration is printed.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess

NAME = re.compile(r"[0-9a-f]{64}\.snapshot\Z")


def inspect(binary, manifest_path, journal_path):
    manifest_path, journal_path = Path(manifest_path), Path(journal_path)
    if manifest_path.stat().st_size > 64 * 1024 or journal_path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("diagnostic input exceeds limit")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("status") != "retained":
        raise ValueError("snapshot retention incomplete")
    name = manifest.get("root_name", "")
    if not NAME.fullmatch(name):
        raise ValueError("invalid retained root name")
    directory = Path(manifest["retained_directory"])
    if not directory.is_absolute() or directory.is_symlink():
        raise ValueError("invalid retained directory")
    path = directory / name
    row = next(item for item in manifest["files"] if item["name"] == name)
    before = path.lstat()
    if not path.is_file() or path.is_symlink() or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (row["device"], row["inode"], row["bytes"], row["mtime_ns"]):
        raise ValueError("retained root identity changed")
    witness = None
    for line in journal_path.read_text(errors="replace").splitlines():
        marker = "held-memory witness "
        if marker in line:
            witness = json.loads(line.split(marker, 1)[1])
    if not witness or witness.get("pagemap_status") != "pfn_visible":
        raise ValueError("no visible failure-time PFN")
    address = witness.get("guest_physical_address")
    pfn, page_size = witness.get("pfn"), witness.get("page_size")
    va = witness.get("first_bad_va")
    if any(type(x) is not int for x in (address, pfn, page_size, va)) or page_size != 4096 or pfn <= 0 or va < 0 or address != pfn * page_size + va % page_size:
        raise ValueError("inconsistent failure-time page address")
    # This case uses a one-GiB x86 Guest below the PCI hole. Do not generalize
    # GPA == memfd offset to larger or multi-region guests.
    if not 0 <= address < (1 << 30) - 16:
        raise ValueError("address outside this case's one-GiB low-memory layout")
    ref = f"file://{path}@digest:{name[:-9]}"
    process = subprocess.run([str(binary), "__e2e_snapshot_window", "-ref", ref,
                              "-offset", str(address), "-length", "16"],
                             capture_output=True, text=True, timeout=4, check=False)
    if process.returncode or len(process.stdout) > 8192:
        raise ValueError("bounded production reader could not resolve snapshot")
    result = json.loads(process.stdout)
    if result.get("memory_bytes") != 1 << 30 or result.get("offset") != address or result.get("length") != 16:
        raise ValueError("snapshot geometry mismatch")
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ValueError("snapshot changed during observation")
    return {"status": "observed", "sandbox_id": manifest["sandbox_id"],
            "address_basis": "failure-time Guest PFN; capture-time migration not excluded",
            "guest_physical_address": address, "source_nonzero_bytes": result["nonzero"],
            "source_ff_bytes": result["ff_bytes"], "source_sha256": result["sha256"],
            "source_runs": result["source_runs"], "retained_root": name,
            "payload_digest_verified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--journal", required=True)
    args = parser.parse_args()
    try:
        result = inspect(args.binary, args.manifest, args.journal)
    except Exception as error:
        result = {"status": "unavailable", "error_type": type(error).__name__}
    print("snapshot-page witness " + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
