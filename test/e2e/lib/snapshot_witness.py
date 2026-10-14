"""Bounded, failure-only comparison with the retained local Snapshot.

The address is the Guest physical address observed at failure, not proof of the
mapping at capture. No plaintext memory or configuration is printed.
"""
import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import re
import subprocess

NAME = re.compile(r"[0-9a-f]{64}\.snapshot\Z")
REASONS = frozenset((
    "no_held_memory_witness", "witness_json_invalid", "pfn_unavailable",
    "witness_geometry_invalid", "retention_invalid", "snapshot_reader_failed",
    "snapshot_output_invalid", "diagnostic_internal_error",
))


class DiagnosticUnavailable(Exception):
    def __init__(self, reason):
        if reason not in REASONS:
            raise ValueError("unknown diagnostic reason")
        self.reason = reason
        super().__init__(reason)


@contextmanager
def stage(reason):
    """Keep exception details private; publish only a fixed stage/reason."""
    try:
        yield
    except DiagnosticUnavailable:
        raise
    except Exception:
        raise DiagnosticUnavailable(reason) from None


def inspect(binary, manifest_path, journal_path):
    manifest_path, journal_path = Path(manifest_path), Path(journal_path)
    # Check the bounded journal before touching any Snapshot/retention path.
    with stage("witness_json_invalid"):
        if journal_path.stat().st_size > 2 * 1024 * 1024:
            raise ValueError()
        witness = None
        found = False
        for line in journal_path.read_text(errors="replace").splitlines():
            marker = "held-memory witness "
            if marker in line:
                found = True
                witness = json.loads(line.split(marker, 1)[1])
                if not isinstance(witness, dict):
                    raise ValueError()
    if not found:
        raise DiagnosticUnavailable("no_held_memory_witness")
    if witness.get("pagemap_status") != "pfn_visible":
        raise DiagnosticUnavailable("pfn_unavailable")
    with stage("witness_geometry_invalid"):
        address = witness.get("guest_physical_address")
        pfn, page_size = witness.get("pfn"), witness.get("page_size")
        va = witness.get("first_bad_va")
        if any(type(x) is not int for x in (address, pfn, page_size, va)) or page_size != 4096 or pfn <= 0 or va < 0 or address != pfn * page_size + va % page_size:
            raise ValueError("inconsistent failure-time page address")
        # This case uses a one-GiB x86 Guest below the PCI hole. Do not generalize
        # GPA == memfd offset to larger or multi-region guests.
        if not 0 <= address < (1 << 30) - 16:
            raise ValueError("address outside this case's one-GiB low-memory layout")
    with stage("retention_invalid"):
        if manifest_path.stat().st_size > 64 * 1024:
            raise ValueError()
        manifest = json.loads(manifest_path.read_text())
        if not isinstance(manifest["sandbox_id"], str):
            raise ValueError()
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
    ref = f"file://{path}@digest:{name[:-9]}"
    with stage("snapshot_reader_failed"):
        process = subprocess.run([str(binary), "__e2e_snapshot_window", "-ref", ref,
                                  "-offset", str(address), "-length", "16"],
                                 capture_output=True, text=True, timeout=4, check=False)
        if process.returncode:
            raise ValueError()
    with stage("snapshot_output_invalid"):
        if len(process.stdout) > 8192:
            raise ValueError()
        result = json.loads(process.stdout)
        if result.get("memory_bytes") != 1 << 30 or result.get("offset") != address or result.get("length") != 16:
            raise ValueError("snapshot geometry mismatch")
        # Validate the fields we publish; never substitute raw reader output.
        if (any(type(result.get(key)) is not int or not 0 <= result[key] <= 16
                for key in ("nonzero", "ff_bytes"))
                or result["ff_bytes"] > result["nonzero"]
                or not isinstance(result.get("sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", result["sha256"])
                or not isinstance(result.get("source_runs"), dict)):
            raise ValueError()
        for kind, counts in result["source_runs"].items():
            if (kind not in ("Data", "Zero", "Hole") or not isinstance(counts, dict)
                    or set(counts) != {"runs", "bytes", "nonzero"}
                    or any(type(counts[key]) is not int or not 0 <= counts[key] <= 16
                           for key in counts)):
                raise ValueError()
    table_args = [str(binary), "__e2e_snapshot_pagetable", "-ref", ref,
                  "-virtual-address", str(va)]
    comm, pid, release = (witness.get(key) for key in ("comm", "pid", "kernel_release"))
    if (isinstance(comm, str) and 0 < len(comm.encode("utf-8")) <= 15
            and not any(c in comm for c in "\x00\n\r")
            and type(pid) is int and pid == 1
            and isinstance(release, str) and 0 < len(release) <= 128
            and not any(c in release for c in "\x00\n\r")):
        table_args += ["-primary-comm", comm, "-namespace-pid", str(pid),
                       "-kernel-release", release]
    paging = {"status": "unavailable"}
    try:
        table = subprocess.run(table_args, capture_output=True, text=True,
                               timeout=4, check=False)
        if table.returncode == 0 and len(table.stdout) <= 32768:
            paging = json.loads(table.stdout)
    except (subprocess.TimeoutExpired, OSError, ValueError):
        pass
    with stage("retention_invalid"):
        after = path.lstat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("snapshot changed during observation")
    return {"status": "observed", "sandbox_id": manifest["sandbox_id"], "saved_paging": paging,
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
    except DiagnosticUnavailable as error:
        result = {"status": "unavailable", "reason": error.reason}
    except Exception:
        # Last-resort privacy guard; never print unexpected exception details.
        result = {"status": "unavailable", "reason": "diagnostic_internal_error"}
    print("snapshot-page witness " + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
