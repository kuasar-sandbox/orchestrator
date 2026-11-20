"""Find the single TCP listener of a test-owned Conductor child.

Exit 1 means the live child has not bound a listener yet. Other failures are
fatal to startup; a listener from another process is never a readiness signal.
"""
import os
from pathlib import Path
import sys


def process_identity(process, parent_pid):
    # comm can contain spaces or parentheses; fields after its final ')' start
    # at state (3), followed by ppid (4); starttime is field 22.
    fields = (process / "stat").read_text().rsplit(")", 1)[1].split()
    if fields[0] in ("Z", "X", "x"):
        raise ValueError("Conductor child has exited")
    if int(fields[1]) != parent_pid:
        raise ValueError("Conductor PID is not owned by the launching shell")
    return int(fields[19])


def listener_port(pid, parent_pid):
    if pid <= 0 or parent_pid <= 0:
        raise ValueError("PID and parent PID must be positive")
    process = Path(f"/proc/{pid}")
    identity = process_identity(process, parent_pid)
    sockets = set()
    for fd in (process / "fd").iterdir():
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue  # An unrelated descriptor can close during startup.
        if target.startswith("socket:[") and target.endswith("]"):
            sockets.add(target[8:-1])
    listeners = {}
    for family in ("tcp", "tcp6"):
        try:
            lines = (process / "net" / family).read_text().splitlines()[1:]
        except FileNotFoundError:
            if family == "tcp6":
                continue  # IPv6 need not be enabled on the test host.
            raise
        for line in lines:
            fields = line.split()
            if fields[3] == "0A" and fields[9] in sockets:
                port = int(fields[1].rsplit(":", 1)[1], 16)
                if not 0 < port <= 65535:
                    raise ValueError("invalid Conductor listener port")
                listeners[fields[9]] = port
    if process_identity(process, parent_pid) != identity:
        raise ValueError("Conductor process identity changed during discovery")
    if len(listeners) > 1:
        raise ValueError("multiple TCP listeners owned by Conductor child")
    return next(iter(listeners.values()), None)


def main():
    try:
        if len(sys.argv) != 3:
            raise ValueError("usage: conductor_listener.py PID PARENT_PID")
        port = listener_port(int(sys.argv[1]), int(sys.argv[2]))
    except (OSError, ValueError, IndexError) as error:
        print(f"Conductor listener discovery: {error}", file=sys.stderr)
        return 2
    if port is None:
        return 1
    print(port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
