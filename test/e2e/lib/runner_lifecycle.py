#!/usr/bin/env python3
"""Exact test-owned runner identities for real KVM execute acceptance.

No credentials/argv are recorded. Signals use pidfd and fresh durable
RunID/unit/process-start checks. This helper builds or starts no products.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import time


def row(work, sid):
    uri = (work / "lib/node-ctl.db").resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=5) as db:
        db.row_factory = sqlite3.Row
        value = db.execute(
            """SELECT id,state,run_id,run_dir,base_dir,vswitch_port,floatingip,
                      inner_ip,port_mac,envd_uds,ci_uds,resume_source_kind,
                      resume_source_ref,resume_sandbox_ref,sandbox_result_run_id,
                      sandbox_result_json FROM sandboxes WHERE id=?""", (sid,)
        ).fetchone()
    if value is None:
        raise ValueError("Sandbox row disappeared instead of becoming dead history")
    return dict(value)


def process(pid):
    if pid <= 1:
        raise ValueError("invalid process identity")
    base = Path("/proc") / str(pid)
    fields = (base / "stat").read_text().rsplit(")", 1)[1].split()
    cgroup = next(line[3:] for line in (base / "cgroup").read_text().splitlines() if line.startswith("0::"))
    return dict(pid=pid, ppid=int(fields[1]), start_ticks=int(fields[19]),
                cgroup=cgroup, executable=Path(os.readlink(base / "exe")).name)


def same_process(first, second):
    return all(first[key] == second[key] for key in ("pid", "ppid", "start_ticks", "cgroup", "executable"))


def userspace_members(members, proc_root=Path("/proc")):
    # KVM may place its nx-lpage-recovery kernel worker in the VMM leaf.
    # It is not another userspace VMM and has no exe link. Use the kernel's
    # task flag, never a name exemption; assert_dead still requires removal
    # of the complete delegated cgroup, including these workers.
    result = []
    for pid in members:
        # cgroup.procs emits 0 when a task has no PID in the reader's
        # namespace (for example a host KVM worker). It is not /proc/0.
        # Visible PIDs still fail closed; assert_dead checks the whole cgroup.
        if pid == "0":
            continue
        status = (proc_root / pid / "status").read_text()
        if not any(line.split() == ["Kthread:", "1"] for line in status.splitlines()):
            result.append(pid)
    return result


def runtime_root(work, configured=None):
    root = work / "run" if configured is None else Path(configured)
    if root == work / "run":
        return root
    if (re.fullmatch(r"e-[A-Za-z0-9]{6}", work.name)
            and root == Path("/tmp") / work.name / "run"
            and not root.parent.is_symlink()):
        return root
    raise ValueError("runtime root is not owned by this execute workspace")


def snapshot(work, sid, prefix, run_root=None):
    run_root = runtime_root(work, run_root)
    state = row(work, sid)
    rid = state["run_id"]
    if state["state"] != "running" or not re.fullmatch(r"sr-[0-9a-fA-F-]{36}", rid):
        raise ValueError("fault injection requires a current running RunID")
    run_dir, base_dir = run_root / "sandboxes" / sid, work / "lib/sandboxes" / sid
    if state["run_dir"] != str(run_dir) or state["base_dir"] != str(base_dir):
        raise ValueError("Sandbox directories are not owned by this execute workspace")
    unit = prefix + rid + ".service"
    props = subprocess.check_output(
        ["systemctl", "show", unit, "--property=MainPID,ControlGroup,ActiveState"], text=True
    )
    props = dict(line.split("=", 1) for line in props.splitlines() if "=" in line)
    cgroup = props["ControlGroup"]
    if not cgroup.startswith("/") or unit not in cgroup.split("/") or props["ActiveState"] != "active":
        raise ValueError("unit does not own the live test run")
    parent_pid = int((run_root / "runners" / (rid + ".pid")).read_text())
    runtime_pid = int((run_dir / (sid + ".pid")).read_text())
    if parent_pid != int(props["MainPID"]) or parent_pid != runtime_pid:
        raise ValueError("node-ctl must own both RunID and SDK runtime PID identities")
    members = userspace_members((Path("/sys/fs/cgroup") / cgroup.lstrip("/") / "vmm/cgroup.procs").read_text().split())
    if len(members) != 1:
        identities = []
        for pid in members[:16]:
            try:
                identities.append(process(int(pid)))
            except (OSError, ValueError):
                details = {"pid": pid}
                try:
                    details["status"] = [line for line in (Path("/proc") / pid / "status").read_text().splitlines()
                                         if line.startswith(("Name:", "State:", "Tgid:", "Pid:", "PPid:", "Kthread:"))]
                except OSError:
                    details["status"] = "absent"
                identities.append(details)
        raise ValueError("VMM leaf must contain exactly the current CH process: " + repr(identities))
    identities = dict(parent=process(parent_pid), runtime=process(runtime_pid), ch=process(int(members[0])))
    if identities["parent"]["executable"] != "node-ctl" or identities["runtime"]["executable"] != "node-ctl":
        raise ValueError("unexpected runner/runtime executable")
    if identities["ch"]["executable"] != "cloud-hypervisor":
        raise ValueError("unexpected VMM executable")
    if not same_process(identities["parent"], identities["runtime"]) or identities["ch"]["ppid"] != runtime_pid:
        raise ValueError("runtime/CH process parent chain does not match the exact runner")
    if any(identities[k]["cgroup"] != cgroup + "/ctl" for k in ("parent", "runtime")) or identities["ch"]["cgroup"] != cgroup + "/vmm":
        raise ValueError("runner process left its expected delegated cgroup")
    ctl_members = userspace_members((Path("/sys/fs/cgroup") / cgroup.lstrip("/") / "ctl/cgroup.procs").read_text().split())
    if ctl_members != [str(parent_pid)]:
        raise ValueError("SDK runner ctl leaf must contain only node-ctl, with no sandbox-ctl child")
    memory = {}
    for line in (Path("/proc") / str(parent_pid) / "smaps_rollup").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("Pss", "Private_Clean", "Private_Dirty"):
            memory[key + "_bytes"] = int(value.split()[0]) * 1024
    memory["fd_count"] = len(list((Path("/proc") / str(parent_pid) / "fd").iterdir()))
    return dict(sid=sid, run_id=rid, unit=unit, cgroup=cgroup,
                processes=identities, parent_measurement=memory,
                measured_monotonic=time.monotonic())


def assert_same_run(before, after):
    if any(before[key] != after[key] for key in ("sid", "run_id", "unit", "cgroup")):
        raise ValueError("restart changed the current run identity")
    for role in ("parent", "runtime", "ch"):
        if not same_process(before["processes"][role], after["processes"][role]):
            raise ValueError("restart changed the " + role + " process identity")


def verify_lease(work, observed):
    socket = work / "sandbox-resource.sock"
    lease_path = Path(str(socket) + ".leases") / (hashlib.sha256(observed["sid"].encode()).hexdigest() + ".json")
    lease = json.loads(lease_path.read_text())
    if lease["sandbox_id"] != observed["sid"] or lease["pid"] != observed["processes"]["runtime"]["pid"]:
        raise ValueError("StateSync lease did not retain the runtime PID identity")
    if lease["pid"] != observed["processes"]["parent"]["pid"]:
        raise ValueError("SDK runtime lease must belong to the resident node-ctl owner")
    if "state_sync_v1" not in lease.get("client_features", []):
        raise ValueError("runtime does not advertise StateSync")


def assert_dead(work, observed, run_root=None):
    run_root = runtime_root(work, run_root)
    state = row(work, observed["sid"])
    fields = ("run_id", "run_dir", "base_dir", "vswitch_port", "floatingip", "inner_ip", "port_mac",
              "envd_uds", "ci_uds", "resume_source_kind", "resume_source_ref", "resume_sandbox_ref")
    if state["state"] != "dead" or any(state[name] for name in fields):
        raise ValueError("automatic exit cleanup has not committed resource-free dead history")
    result = json.loads(state["sandbox_result_json"])
    if state["sandbox_result_run_id"] != observed["run_id"] or result["run_id"] != observed["run_id"] or result["sid"] != observed["sid"] or result["stage"] != "run":
        raise ValueError("dead history lost the original execution result identity")
    for subtree in (run_root / "sandboxes", work / "lib/sandboxes"):
        if (subtree / observed["sid"]).exists():
            raise ValueError("completed cleanup retained an object directory")
    if (Path("/sys/fs/cgroup") / observed["cgroup"].lstrip("/")).exists():
        raise ValueError("completed cleanup retained the delegated unit subtree")
    for expected in observed["processes"].values():
        try:
            fields = (Path("/proc") / str(expected["pid"]) / "stat").read_text().rsplit(")", 1)[1].split()
            start_ticks = int(fields[19])
        except FileNotFoundError:
            continue
        if start_ticks == expected["start_ticks"]:
            raise ValueError("automatic cleanup left an original unit process alive")


def cleanup_diagnostics(work, observed):
    """Bounded ownership facts only; never argv, environment, or credentials."""
    state = row(work, observed["sid"])
    result = {"sid": observed["sid"], "run_id": observed["run_id"],
              "state": state["state"], "processes": {}, "cgroups": {}}
    for role, expected in observed["processes"].items():
        try:
            result["processes"][role] = process(expected["pid"])
        except (OSError, ValueError):
            result["processes"][role] = "absent or exited"
    root = Path("/sys/fs/cgroup") / observed["cgroup"].lstrip("/")
    for path in (root, root / "ctl", root / "vmm"):
        fields = {}
        for name in ("cgroup.events", "cgroup.procs", "cgroup.threads", "memory.current", "memory.events"):
            try:
                fields[name] = (path / name).read_text()[:4096]
            except OSError:
                fields[name] = "absent"
        result["cgroups"][str(path)] = fields
    result["unit"] = subprocess.run(
        ["systemctl", "show", observed["unit"],
         "--property=ActiveState,SubState,Result,ControlGroup,MainPID,TasksCurrent"],
        text=True, capture_output=True, timeout=5, check=False,
    ).stdout[:4096]
    print("runner cleanup diagnostics " + json.dumps(result, sort_keys=True), flush=True)


def kill_exact(work, observed, prefix, role, run_root=None):
    expected = observed["processes"][role]
    fd = os.pidfd_open(expected["pid"])
    try:
        # Pin the PID first, then revalidate the complete durable/unit/process
        # snapshot. A changed incarnation must never receive the old signal.
        assert_same_run(observed, snapshot(work, observed["sid"], prefix, run_root))
        signal.pidfd_send_signal(fd, signal.SIGKILL)
    finally:
        os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("snapshot", "signal", "same-run", "lease", "dead"))
    parser.add_argument("work", type=Path)
    parser.add_argument("sid")
    parser.add_argument("unit_prefix")
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--role", choices=("parent", "runtime", "ch"))
    parser.add_argument("--diagnostics", action="store_true")
    parser.add_argument("--run-root", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,56}", args.sid) or not args.unit_prefix.endswith("@"):
        parser.error("invalid test sandbox or unit prefix")
    work = args.work.resolve()
    run_root = runtime_root(work, args.run_root)
    if args.operation == "snapshot":
        args.evidence.write_text(json.dumps(snapshot(work, args.sid, args.unit_prefix, run_root), indent=2) + "\n")
        return
    observed = json.loads(args.evidence.read_text())
    if observed["sid"] != args.sid or observed["unit"] != args.unit_prefix + observed["run_id"] + ".service":
        raise ValueError("evidence belongs to another execution")
    if args.operation == "dead":
        try:
            assert_dead(work, observed, run_root)
        except ValueError:
            if args.diagnostics:
                cleanup_diagnostics(work, observed)
            raise
    elif args.operation == "lease":
        verify_lease(work, observed)
    elif args.operation == "same-run":
        assert_same_run(observed, snapshot(work, args.sid, args.unit_prefix, run_root))
    else:
        if not args.role:
            parser.error("signal requires --role")
        kill_exact(work, observed, args.unit_prefix, args.role, run_root)
        print("killed exact test-owned", args.role, "for", observed["run_id"])


if __name__ == "__main__":
    main()
