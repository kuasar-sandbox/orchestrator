"""Read public Checkpoint artifacts and host observations for platform E2E."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import subprocess
import tarfile
import time
import uuid
import zipfile


def checkpoint_id(raw):
    parsed = uuid.UUID(raw)
    assert parsed.version == 7 and parsed.variant == uuid.RFC_4122 and str(parsed) == raw, raw
    return raw


def fresh_checkpoint_id():
    raw = bytearray(os.urandom(16))
    raw[:6] = (time.time_ns() // 1_000_000).to_bytes(6, "big")
    raw[6] = (raw[6] & 0x0f) | 0x70
    raw[8] = (raw[8] & 0x3f) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def fence_state(database, sid, metadata):
    """The test's temporary capture must preserve the publisher's original E."""
    original = json.loads(Path(metadata).read_text())
    path = Path(original["artifact"])
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    with sqlite3.connect(database, timeout=5) as db:
        row = db.execute("select state, resume_source_kind, resume_source_ref, resume_sandbox_ref from sandboxes where id=?", (sid,)).fetchone()
    assert row and row[0] == "running", row
    return {"artifact": str(path), "sha256": digest, "resume_source": row[1:]}


def fence_refs(metadata, report_path, held_path, directory, identity, baseline_path=None):
    checkpoint_id(identity)
    original, report, held = (json.loads(Path(path).read_text()) for path in (metadata, report_path, held_path))
    assert identity != original["checkpoint_id"]
    assert isinstance(report, dict) and set(report) == {"sandboxRef", "removedRefs"} and report["removedRefs"] == [], report
    match = re.fullmatch(r"file://([0-9a-f]{64}\.sandbox)@digest:([0-9a-f]{64})", report["sandboxRef"])
    assert match, report
    path = Path(directory) / match.group(1)
    assert path.is_file() and not path.is_symlink() and path.resolve() != Path(original["artifact"]).resolve(), path
    names, payload = artifact_content(path)
    assert set(names) == {"sandbox.runtime.cfg", "checkpoint.json"}, names
    assert payload["version"] == 1 and payload["checkpoint_id"] == identity, payload
    assert payload["root"] == original["root"] and (payload.get("disks") or []) == (original.get("disks") or []), payload
    ids = [disk["diff_id"] for disk in [original["root"], *(original.get("disks") or [])]]
    assert len(ids) == len(set(ids))
    baseline = {(diff_id, original["checkpoint_id"]) for diff_id in ids}
    if baseline_path is not None:
        previous = json.loads(Path(baseline_path).read_text())
        observed = {(ref["diff_id"], ref["id"]) for ref in previous}
        assert len(observed) == len(previous) and baseline <= observed, previous
        assert all(diff_id in ids and version != identity for diff_id, version in observed), previous
        baseline = observed
    expected = baseline | {(diff_id, identity) for diff_id in ids}
    assert len(held) == len(expected) and {(ref["diff_id"], ref["id"]) for ref in held} == expected, (held, expected)
    return [{"diff_id": diff_id, "id": identity} for diff_id in ids]


class _CheckpointTarInfo(tarfile.TarInfo):
    """Keep GNU sparse logical metadata separate from the PAX stored size.

    tarfile applies generic PAX path/size after GNU.sparse.name/realsize and
    recomputes the next header from offset_data, which already skips the sparse
    map. Kuasar's canonical PAX includes both sets of fields. Preserve the
    physical boundary before that patching and retain the sparse logical view;
    do not extract/materialize potentially large Sandbox payloads.
    """

    def _proc_gnusparse_10(self, member, headers, carrier):
        start = carrier.fileobj.tell()
        stored = int(headers.get("size", member.size))
        super()._proc_gnusparse_10(member, headers, carrier)
        logical = int(headers["GNU.sparse.realsize"])
        end = packed = 0
        for offset, size in member.sparse:
            if offset < end or size < 0 or offset + size > logical:
                raise tarfile.ReadError("invalid GNU sparse extent")
            end = offset + size
            packed += size
        if carrier.fileobj.tell() - start + packed != stored:
            raise tarfile.ReadError("GNU sparse stored size does not match map and payload")
        self._sparse_layout = (headers["GNU.sparse.name"], logical,
                               start + self._block(stored))

    def _proc_pax(self, carrier):
        member = super()._proc_pax(carrier)
        if hasattr(self, "_sparse_layout"):
            member.name, member.size, carrier.offset = self._sparse_layout
        return member


def artifact_content(path):
    with tarfile.open(path, tarinfo=_CheckpointTarInfo) as carrier:
        members = [m for m in carrier if m.isfile() and not m.name.startswith(".kuasar.")]
        assert len(members) == 1, members
        member = members[0]
        with zipfile.ZipFile(carrier.extractfile(member)) as logical:
            names = logical.namelist()
            assert len(names) == len(set(names)), names
            result = json.loads(logical.read("checkpoint.json")) if "checkpoint.json" in names else None
    return names, result


def local_artifact(database, sid):
    with sqlite3.connect(database, timeout=5) as db:
        row = db.execute("select state, base_dir, resume_source_kind, resume_source_ref, resume_sandbox_ref from sandboxes where id=?", (sid,)).fetchone()
    assert row and row[0] == "paused" and row[2] in ("sandbox", "snapshot"), row
    ref = row[4] if row[2] == "snapshot" else row[3]
    assert ref.startswith("file://") and "@digest:" in ref, ref
    name = ref.removeprefix("file://").split("@", 1)[0]
    assert name == os.path.basename(name) and name.endswith(".sandbox"), ref
    path = Path(row[1], "checkpoint", name)
    return path, *artifact_content(path)


def local_metadata(database, sid):
    path, names, result = local_artifact(database, sid)
    assert set(names) == {"sandbox.runtime.cfg", "checkpoint.json"}, names
    assert result["version"] == 1, result
    checkpoint_id(result["checkpoint_id"])
    disks = [result["root"], *(result.get("disks") or [])]
    ids = [disk["diff_id"] for disk in disks]
    assert len(ids) == len(set(ids)) and all(value and value not in (".", "..") and "/" not in value and "\\" not in value for value in ids), result
    result["artifact"] = str(path)
    return result


def memory_record(directory, locator=None):
    """Read only the attempt named by the current (or captured) locator."""
    directory = Path(directory)
    if locator is None:
        locator = json.loads((directory / "current-memory.json").read_text())
    checkpoint = locator["checkpoint_id"]
    memory = locator["memory_id"]
    epoch = locator["epoch"]
    assert isinstance(checkpoint, str) and checkpoint, locator
    assert isinstance(memory, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", memory) and memory not in (".", ".."), locator
    assert type(epoch) is int and 0 < epoch < 1 << 64, locator
    digest = hashlib.sha256(checkpoint.encode()).hexdigest()
    path = directory / "memory" / digest / (memory + ".json")
    try:
        raw = path.read_text()
    except FileNotFoundError:
        # Older captures retain their original receipt path. Never select a
        # newer attempt or fall back after a present record fails validation.
        path = directory / "memory" / (digest + ".json")
        raw = path.read_text()
    record = json.loads(raw)
    assert type(record["epoch"]) is int and (record["checkpoint_id"], record["memory_id"], record["epoch"]) == (checkpoint, memory, epoch), record
    return record


def managed_memory(database, sid, node_ctl, control_socket, resource_socket, log, phase):
    """Prove the managed path, including a save that finished before observation."""
    with sqlite3.connect(database, timeout=5) as db:
        row = db.execute("select state, base_dir, instance_id, run_id, resume_source_kind, resume_source_ref from sandboxes where id=?", (sid,)).fetchone()
        owners = {run: json.loads(raw) for run, raw in db.execute("select run_id, ownership from run_pool_ownership")}
    assert row and row[0] == phase and row[4] == "snapshot", row
    directory = Path(row[1]) / "checkpoint"
    locator = json.loads((directory / "current-memory.json").read_text())
    record = memory_record(directory, locator)
    producer = record["producer"]
    assert record["committed"] and record["runtime_retired"] and not record.get("aborted") and not record.get("discarded"), record
    assert producer["sandbox_id"] == sid and producer["instance_id"] == row[2] and producer["run_id"], producer
    assert record["host_operation_id"] == record["checkpoint_id"] == locator["checkpoint_id"], record
    assert (record["memory_id"], record["epoch"]) == (locator["memory_id"], locator["epoch"]), record
    assert record["snapshot_ref"] == row[5], (record["snapshot_ref"], row[5])
    name = row[5].removeprefix("file://").split("@", 1)[0]
    assert row[5].startswith("file://") and name == Path(name).name, row[5]
    with tarfile.open(directory / name, tarinfo=_CheckpointTarInfo) as carrier:
        members = [m for m in carrier if m.isfile() and not m.name.startswith(".kuasar.")]
        assert len(members) == 1, members
        with zipfile.ZipFile(carrier.extractfile(members[0])) as logical:
            descriptor = json.loads(logical.read("memory.checkpoint.json"))
    assert descriptor["version"] == 1 and descriptor["memory_id"] == record["memory_id"] and descriptor["checkpoint_id"] == record["checkpoint_id"], descriptor
    parent_ack = False
    for line in Path(log).read_text().splitlines():
        if "checkpoint parent retirement acknowledged" not in line:
            continue
        fields = dict(word.split("=", 1) for word in shlex.split(line) if "=" in word)
        parent_ack |= all(fields.get(key) == str(value) for key, value in {
            "sid": sid, "run_id": producer["run_id"], "memory_id": record["memory_id"], "epoch": record["epoch"],
        }.items())
    assert parent_ack, "no exact parent sampling-retirement ACK (a missing unit alone is not an ACK)"
    old_owner = owners.get(producer["run_id"])
    if old_owner:
        assert old_owner.get("role") == "memory-holder" and old_owner.get("parent_retired"), old_owner
        assert old_owner["memory_id"] == record["memory_id"] and old_owner["memory_epoch"] == record["epoch"], old_owner
    else:
        assert record["ram_released"], "holder ownership disappeared while RAM was retained"

    # These are read-only public admin views. Retry the observation if a normal
    # balloon/account transition crosses the two queries; never invent R from
    # a PID, socket disappearance, or FileReady alone.
    deadline = time.monotonic() + 10
    while True:
        rows = json.loads(subprocess.check_output([node_ctl, "resource", "list", "--socket", resource_socket], text=True, timeout=5)) or []
        pressure = json.loads(subprocess.check_output([node_ctl, "resource", "pressure", "--socket", control_socket], text=True, timeout=5))
        receipts = [json.loads(path.read_text()) for path in Path(resource_socket + ".retained").glob("*.json")]
        exact = [item for item in receipts if item["sandbox_id"] == sid]
        assert len(exact) == 1, "managed capture did not retain a resource receipt"
        receipt = exact[0]
        assert receipt["producer"] == producer and receipt["checkpoint_id"] == record["checkpoint_id"], "resource producer changed"
        memory = receipt["memory"]
        assert memory["memory_id"] == record["memory_id"] and memory["epoch"] == record["epoch"], "resource memory generation changed"
        assert memory["bytes"] == receipt["source"]["reservation_memory"] > 0 and memory["source_pool"], memory
        current = [item for item in rows if item["sandbox_id"] == sid]
        if phase == "paused":
            assert not current, "paused holder still owns an active B reservation"
        else:
            assert row[3] and row[3] != producer["run_id"] and len(current) == 1, "resume did not obtain one new run and B"
            assert current[0]["peer_pid"] != record["holder"]["pid"] and current[0]["allocatable_memory"] > 0, current
        latest = memory_record(directory, locator)
        if receipt.get("settled"):
            assert latest["ram_released"], "R was released before the physical RAM fact"
        projected = sum(item["memory"]["bytes"] for item in receipts if not item.get("settled") and item["stage"] != "prepared")
        active = sum(item["allocatable_memory"] for item in rows)
        cache = sum(item.get("cache_allocated", 0) for item in pressure.get("pools", []))
        # Guest Budget and retained RAM share this pool. Host cache has its own
        # existing pool and must not be charged into reserved_memory again.
        if pressure["reserved_memory"] == active + projected and (receipt["stage"] != "prepared" or receipt.get("settled")):
            break
        assert time.monotonic() < deadline, (pressure["reserved_memory"], active, projected, cache, receipt["stage"])
        time.sleep(.05)
    return {"sid": sid, "phase": phase, "producer": producer, "current_run_id": row[3],
            "memory_id": record["memory_id"], "checkpoint_id": record["checkpoint_id"],
            "runtime_retired": True, "parent_ack": True, "file_ready": latest["file_ready"],
            "ram_released": latest["ram_released"], "resource_stage": receipt["stage"],
            "retained_bytes": 0 if receipt.get("settled") else memory["bytes"],
            "active_bytes": sum(item["allocatable_memory"] for item in current),
            "node_reserved_bytes": pressure["reserved_memory"], "cache_allocated_bytes": cache,
            "source_pool": memory["source_pool"]}


def disk_state(directory):
    root = Path(directory)
    result = []
    for path in sorted(root.glob("*.diff")):
        assert path.is_file() and not path.is_symlink(), path
        st = path.stat()
        result.append({"name": path.name, "device": st.st_dev, "inode": st.st_ino,
                       "logical_file_bytes": st.st_size, "allocated_bytes": st.st_blocks * 512})
    assert result, f"no persistent DIFF in {root}"
    return result


def live_root_disk(base_directory, run_directory, pids):
    """Require the fresh one-disk runtime to use its derived persistent DIFF."""
    base, run = Path(base_directory), Path(run_directory)
    directory = base / "checkpoint" / "diffs"
    assert base.resolve() == base and run.resolve() == run and directory.resolve() == directory, (base, run, directory)
    assert not base.is_relative_to(run) and not run.is_relative_to(base), (base, run)
    disks = disk_state(directory)
    assert len(disks) == 1, disks
    assert re.fullmatch(r"[0-9a-f]{32}\.diff", disks[0]["name"]), disks
    path = directory / disks[0]["name"]
    assert list(base.rglob("*.diff")) == [path], "unexpected writable DIFF outside the checkpoint layout"
    references = []
    for pid in pids:
        try:
            descriptors = list(Path(f"/proc/{pid}/fd").iterdir())
        except (FileNotFoundError, ProcessLookupError):
            continue
        for descriptor in descriptors:
            try:
                if os.readlink(descriptor) == str(path):
                    info = (descriptor.parent.parent / "fdinfo" / descriptor.name).read_text()
                    flags = next(int(line.split()[1], 8) for line in info.splitlines() if line.startswith("flags:"))
                    if flags & os.O_ACCMODE != os.O_RDWR:
                        continue
                    current = descriptor.stat()
                    assert (current.st_dev, current.st_ino) == (disks[0]["device"], disks[0]["inode"])
                    references.append({"pid": pid, "fd": descriptor.name})
            except (FileNotFoundError, ProcessLookupError):
                continue
    assert references, f"runtime has no open reference to {path}"
    return {"path": str(path), "disk": disks[0], "runtime_references": references}


def runtime_memory(database, sid, prefix):
    with sqlite3.connect(database, timeout=5) as db:
        row = db.execute("select run_id from sandboxes where id=?", (sid,)).fetchone()
    if not row or not row[0]:
        return {"runtime_rss_bytes": 0, "runtime_cgroup_bytes": 0, "runtime_pids": []}
    group = subprocess.check_output(["systemctl", "show", "--value", "-p", "ControlGroup", prefix + row[0] + ".service"], text=True).strip()
    if not group:
        return {"runtime_rss_bytes": 0, "runtime_cgroup_bytes": 0, "runtime_pids": []}
    root = Path("/sys/fs/cgroup", group.lstrip("/"))
    pids = set()
    for procs in root.rglob("cgroup.procs"):
        try:
            pids.update(int(pid) for pid in procs.read_text().split())
        except FileNotFoundError:
            pass
    rss, write_bytes, cancelled_write_bytes = 0, 0, 0
    for pid in pids:
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    rss += int(line.split()[1]) * 1024
            counters = dict(line.split(": ", 1) for line in Path(f"/proc/{pid}/io").read_text().splitlines())
            write_bytes += int(counters.get("write_bytes", 0))
            cancelled_write_bytes += int(counters.get("cancelled_write_bytes", 0))
        except (FileNotFoundError, ProcessLookupError):
            pass
    try:
        current = int((root / "memory.current").read_text())
    except FileNotFoundError:
        current = 0
    try:
        io_stat = {row[0]: {field.split("=", 1)[0]: int(field.split("=", 1)[1]) for field in row[1:]}
                   for row in (line.split() for line in (root / "io.stat").read_text().splitlines()) if row}
    except FileNotFoundError:
        io_stat = {}
    return {"runtime_rss_bytes": rss, "runtime_cgroup_bytes": current, "runtime_pids": sorted(pids),
            "runtime_io_write_bytes": write_bytes, "runtime_io_cancelled_write_bytes": cancelled_write_bytes,
            "runtime_cgroup_io_stat": io_stat}


def tree_allocation(directory):
    """du-like allocated bytes, deduplicating inodes and never following symlinks."""
    seen, allocated, logical = set(), 0, 0
    for root, _, files in os.walk(directory, followlinks=False):
        for name in files:
            path = Path(root, name)
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                value = path.stat()
            except FileNotFoundError:
                continue
            key = (value.st_dev, value.st_ino)
            if key not in seen:
                seen.add(key)
                allocated += value.st_blocks * 512
                logical += value.st_size
    return {"allocated_bytes": allocated, "logical_file_bytes": logical, "files": len(seen)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("new-id")
    fence = sub.add_parser("fence-state")
    for field in ("database", "sid", "metadata"):
        fence.add_argument(field)
    refs = sub.add_parser("fence-refs")
    for field in ("metadata", "report", "held", "directory", "identity"):
        refs.add_argument(field)
    refs.add_argument("--baseline")
    meta = sub.add_parser("metadata")
    meta.add_argument("database")
    meta.add_argument("sid")
    managed = sub.add_parser("managed-memory")
    for field in ("database", "sid", "node_ctl", "control_socket", "resource_socket", "log"):
        managed.add_argument(field)
    managed.add_argument("phase", choices=("paused", "running"))
    disks = sub.add_parser("disks")
    disks.add_argument("directory")
    layout = sub.add_parser("layout")
    layout.add_argument("database")
    layout.add_argument("sid")
    layout.add_argument("mode", choices=("checkpoint", "baseline-copy"))
    live = sub.add_parser("live-root-disk")
    for field in ("database", "sid", "prefix", "base_directory", "run_directory"):
        live.add_argument(field)
    measure = sub.add_parser("measure")
    for field in ("output", "database", "sid", "prefix", "directory", "phase", "started", "payload_mib"):
        measure.add_argument(field)
    measure.add_argument("--mode", choices=("checkpoint", "baseline-copy"), default="checkpoint")
    args = parser.parse_args()
    if args.command == "new-id":
        print(fresh_checkpoint_id())
    elif args.command == "fence-state":
        print(json.dumps(fence_state(args.database, args.sid, args.metadata), sort_keys=True))
    elif args.command == "fence-refs":
        print(json.dumps(fence_refs(args.metadata, args.report, args.held, args.directory, args.identity, args.baseline), sort_keys=True))
    elif args.command == "metadata":
        print(json.dumps(local_metadata(args.database, args.sid), sort_keys=True))
    elif args.command == "managed-memory":
        print(json.dumps(managed_memory(args.database, args.sid, args.node_ctl, args.control_socket, args.resource_socket, args.log, args.phase), sort_keys=True))
    elif args.command == "disks":
        print(json.dumps(disk_state(args.directory), sort_keys=True))
    elif args.command == "live-root-disk":
        pids = runtime_memory(args.database, args.sid, args.prefix)["runtime_pids"]
        print(json.dumps(live_root_disk(args.base_directory, args.run_directory, pids), sort_keys=True))
    elif args.command == "layout":
        if args.mode == "checkpoint":
            print(json.dumps(local_metadata(args.database, args.sid), sort_keys=True))
        else:
            path, names, result = local_artifact(args.database, args.sid)
            assert result is None and "checkpoint.json" not in names, names
            print(json.dumps({"artifact": str(path), "members": names}, sort_keys=True))
    else:
        base = Path(args.directory)
        diff_dir = base / "checkpoint" / "diffs"
        disks = disk_state(diff_dir) if args.mode == "checkpoint" else []
        if args.mode == "baseline-copy":
            assert not list(diff_dir.glob("*.diff")), "baseline unexpectedly uses Checkpoint DIFF"
        with sqlite3.connect(args.database, timeout=5) as db:
            run = db.execute("select run_dir from sandboxes where id=?", (args.sid,)).fetchone()
        run_dir = tree_allocation(run[0]) if run and run[0] else {"allocated_bytes": 0, "logical_file_bytes": 0, "files": 0}
        record = {"mode": args.mode, "phase": args.phase, "payload_mib": int(args.payload_mib), "sid": args.sid,
                  "elapsed_seconds": max(0, time.monotonic() - float(args.started)),
                  "time_unix": time.time(), "disks": disks,
                  "base_dir": tree_allocation(base), "run_dir": run_dir,
                  "diff_allocated_bytes": sum(disk["allocated_bytes"] for disk in disks),
                  "diff_file_bytes": sum(disk["logical_file_bytes"] for disk in disks),
                  "memory_scope": "current runtime unit and descendants; sampled, not peak",
                  "io_scope": "current live runtime /proc/PID/io and cgroup io.stat counters; resets with runtime; not a write-amplification ratio",
                  **runtime_memory(args.database, args.sid, args.prefix)}
        with open(args.output, "a", encoding="utf-8") as out:
            out.write(json.dumps(record, sort_keys=True) + "\n")
        print(json.dumps(record, sort_keys=True))


if __name__ == "__main__":
    main()
