#!/usr/bin/env python3
"""Diagnostic-only retention of a completed, local pressure-test checkpoint.

Run while the sandbox is explicitly paused and BEFORE its next Connect/Wake.
This preserves inode identity using hard links; it does not validate the logical
snapshot graph or payload digest and never substitutes for real CI acceptance.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
from typing import Any

ARTIFACT = re.compile(r"^[0-9a-f]{64}\.(snapshot|sandbox|overlay)$")
SID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _identity(st: os.stat_result) -> tuple[int, int, int, int]:
    # Link creation changes ctime, so it is deliberately excluded.
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns


def _checked_directory(path: Path, allowed: Path) -> Path:
    lexical = Path(os.path.abspath(path))
    root = allowed.resolve(strict=True)
    if lexical != root and root not in lexical.parents:
        raise ValueError("checkpoint directory is outside the allowed sandbox root")
    if lexical == root:
        raise ValueError("checkpoint must be a child directory of the sandbox root")
    for component in [lexical, *lexical.parents]:
        if component == root:
            break
        if component.is_symlink():
            raise ValueError("symlink in the checkpoint directory path")
    resolved = lexical.resolve(strict=True)
    if not resolved.is_dir() or root not in resolved.parents:
        raise ValueError("checkpoint is not a directory inside the allowed root")
    return resolved


def retain_checkpoint(
    checkpoint: Path,
    allowed_sandbox_root: Path,
    destination: Path,
    sandbox_id: str,
    *,
    max_files: int = 64,
    max_bytes: int = 2 << 30,
) -> dict[str, Any]:
    if not SID.fullmatch(sandbox_id) or sandbox_id in (".", ".."):
        raise ValueError("invalid sandbox ID")
    if max_files <= 0 or max_bytes <= 0:
        raise ValueError("retention limits must be positive")
    source = _checked_directory(checkpoint, allowed_sandbox_root)
    dest = Path(os.path.abspath(destination))
    if dest.exists() or dest.is_symlink():
        raise FileExistsError("evidence destination already exists")
    parent = dest.parent.resolve(strict=True)
    if parent == source or source in parent.parents:
        raise ValueError("evidence must not be stored inside the checkpoint being retained")
    dest = parent / dest.name
    alias = f"{sandbox_id}.snapshot"
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
    directory_fd = os.open(source, flags | os.O_DIRECTORY)
    handles: list[tuple[str, int, os.stat_result]] = []
    created = False
    report: dict[str, Any] = {
        "schema": 1, "status": "incomplete", "sandbox_id": sandbox_id,
        "source_directory": str(source), "retained_directory": str(dest),
        "method": "hardlink-no-copy-fallback", "files": [],
        "payload_digest_verified": False, "logical_parent_graph_verified": False,
        "created_unix_ns": time.time_ns(),
    }
    try:
        root_name = os.readlink(alias, dir_fd=directory_fd)
        if not ARTIFACT.fullmatch(root_name) or not root_name.endswith(".snapshot"):
            raise ValueError("snapshot alias is not a canonical same-directory snapshot name")
        names = sorted(name for name in os.listdir(directory_fd) if ARTIFACT.fullmatch(name))
        if root_name not in names:
            raise ValueError("committed snapshot root is missing")
        if len(names) > max_files:
            raise ValueError("checkpoint exceeds diagnostic file-count limit")
        total = 0
        for name in names:
            fd = os.open(name, flags, dir_fd=directory_fd)
            try:
                st = os.fstat(fd)
                if not stat.S_ISREG(st.st_mode):
                    raise ValueError("canonical artifact is not a regular file")
                total += st.st_size
                if total > max_bytes:
                    raise ValueError("checkpoint exceeds diagnostic retained-byte limit")
            except BaseException:
                os.close(fd)
                raise
            handles.append((name, fd, st))
        os.mkdir(dest, 0o700)  # Exclusive: never overwrite an earlier capture.
        created = True
        (dest / "INCOMPLETE").write_text("Evidence not complete until manifest.status=retained\n")
        for name, fd, before in handles:
            target = dest / name
            # EXDEV is reported; silently copying could change the experiment's
            # timing, IO load and storage consumption by multiple GiB.
            os.link(name, target, src_dir_fd=directory_fd, follow_symlinks=False)
            linked = target.lstat()
            current = os.fstat(fd)
            if (not stat.S_ISREG(linked.st_mode)
                    or _identity(linked) != _identity(before)
                    or _identity(current) != _identity(before)):
                raise RuntimeError("artifact identity/content metadata changed during retention")
            report["files"].append({
                "name": name, "bytes": before.st_size,
                "device": before.st_dev, "inode": before.st_ino,
                "mtime_ns": before.st_mtime_ns,
            })
        if os.readlink(alias, dir_fd=directory_fd) != root_name:
            raise RuntimeError("snapshot root alias changed during retention")
        os.symlink(root_name, dest / alias)
        report.update(status="retained", root_name=root_name,
                      retained_bytes=total, retained_file_count=len(handles))
        (dest / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")
        (dest / "INCOMPLETE").unlink()
        return report
    except BaseException as error:
        if created:
            # Preserve the partial diagnostic, but do not label it usable.
            report.update(status="incomplete", error_type=type(error).__name__,
                          error=str(error))
            (dest / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")
        raise
    finally:
        for _, fd, _ in handles:
            os.close(fd)
        os.close(directory_fd)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--checkpoint", required=True, type=Path)
    p.add_argument("--allowed-sandbox-root", required=True, type=Path)
    p.add_argument("--destination", required=True, type=Path)
    p.add_argument("--sandbox-id", required=True)
    p.add_argument("--max-files", type=int, default=64)
    p.add_argument("--max-bytes", type=int, default=2 << 30)
    a = p.parse_args()
    try:
        result = retain_checkpoint(a.checkpoint, a.allowed_sandbox_root,
                                   a.destination, a.sandbox_id,
                                   max_files=a.max_files, max_bytes=a.max_bytes)
    except Exception as error:
        print(f"checkpoint evidence unavailable: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
