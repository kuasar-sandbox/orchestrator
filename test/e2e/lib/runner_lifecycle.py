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


def snapshot(work, sid, prefix):
    state = row(work, sid)
    rid = state["run_id"]
    if state["state"] != "running" or not re.fullmatch(r"sr-[0-9a-fA-F-]{36}", rid):
        raise ValueError("fault injection requires a current running RunID")
    run_dir, base_dir = work / "run/sandboxes" / sid, work / "lib/sandboxes" / sid
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
    parent_pid = int((work / "run/runners" / (rid + ".pid")).read_text())
    runtime_pid = int((run_dir / (sid + ".pid")).read_text())
    if parent_pid != int(props["MainPID"]) or parent_pid == runtime_pid:
        raise ValueError("resident parent and runtime PID identities are not distinct")
    members = (Path("/sys/fs/cgroup") / cgroup.lstrip("/") / "vmm/cgroup.procs").read_text().split()
    if len(members) != 1:
        raise ValueError("VMM leaf must contain exactly the current CH process")
    identities = dict(parent=process(parent_pid), runtime=process(runtime_pid), ch=process(int(members[0])))
    if identities["parent"]["executable"] != "node-ctl" or identities["runtime"]["executable"] != "sandbox-ctl":
        raise ValueError("unexpected runner/runtime executable")
    if identities["ch"]["executable"] != "cloud-hypervisor":
        raise ValueError("unexpected VMM executable")
    if identities["runtime"]["ppid"] != parent_pid or identities["ch"]["ppid"] != runtime_pid:
        raise ValueError("runtime/CH process parent chain does not match the exact runner")
    if any(identities[k]["cgroup"] != cgroup + "/ctl" for k in ("parent", "runtime")) or identities["ch"]["cgroup"] != cgroup + "/vmm":
        raise ValueError("runner process left its expected delegated cgroup")
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
    if lease["pid"] == observed["processes"]["parent"]["pid"]:
        raise ValueError("resident parent masqueraded as runtime in the controller lease")
    if "state_sync_v1" not in lease.get("client_features", []):
        raise ValueError("runtime does not advertise StateSync")


def assert_dead(work, observed):
    state = row(work, observed["sid"])
    fields = ("run_id", "run_dir", "base_dir", "vswitch_port", "floatingip", "inner_ip", "port_mac",
              "envd_uds", "ci_uds", "resume_source_kind", "resume_source_ref", "resume_sandbox_ref")
    if state["state"] != "dead" or any(state[name] for name in fields):
        raise ValueError("automatic exit cleanup has not committed resource-free dead history")
    result = json.loads(state["sandbox_result_json"])
    if state["sandbox_result_run_id"] != observed["run_id"] or result["run_id"] != observed["run_id"] or result["sid"] != observed["sid"] or result["stage"] != "run":
        raise ValueError("dead history lost the original execution result identity")
    for subtree in ("run/sandboxes", "lib/sandboxes"):
        if (work / subtree / observed["sid"]).exists():
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


def kill_exact(work, observed, prefix, role):
    expected = observed["processes"][role]
    fd = os.pidfd_open(expected["pid"])
    try:
        # Pin the PID first, then revalidate the complete durable/unit/process
        # snapshot. A changed incarnation must never receive the old signal.
        assert_same_run(observed, snapshot(work, observed["sid"], prefix))
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
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,56}", args.sid) or not args.unit_prefix.endswith("@"):
        parser.error("invalid test sandbox or unit prefix")
    work = args.work.resolve()
    if args.operation == "snapshot":
        args.evidence.write_text(json.dumps(snapshot(work, args.sid, args.unit_prefix), indent=2) + "\n")
        return
    observed = json.loads(args.evidence.read_text())
    if observed["sid"] != args.sid or observed["unit"] != args.unit_prefix + observed["run_id"] + ".service":
        raise ValueError("evidence belongs to another execution")
    if args.operation == "dead":
        assert_dead(work, observed)
    elif args.operation == "lease":
        verify_lease(work, observed)
    elif args.operation == "same-run":
        assert_same_run(observed, snapshot(work, args.sid, args.unit_prefix))
    else:
        if not args.role:
            parser.error("signal requires --role")
        kill_exact(work, observed, args.unit_prefix, args.role)
        print("killed exact test-owned", args.role, "for", observed["run_id"])


if __name__ == "__main__":
    main()
