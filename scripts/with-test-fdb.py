#!/usr/bin/env python3
"""Run a command against an isolated, pinned FoundationDB SSD test database.

The server is test infrastructure only. It is neither installed system-wide nor
included in the product archive. Only this invocation's process/data are removed.
"""

import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from test_fdb_assets import NATIVE, prepare


def database_diagnostics(loader, cli, cluster_file, environment):
    status = subprocess.run([*loader, str(cli), "-C", str(cluster_file), "--exec", "status json", "--timeout", "2"],
                            env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    space = os.statvfs(cluster_file.parent)
    return ("test FoundationDB filesystem: total=" + str(space.f_blocks * space.f_frsize)
            + " available=" + str(space.f_bavail * space.f_frsize)
            + " free=" + str(space.f_bfree * space.f_frsize) + " bytes\n"
            + "test FoundationDB status (exit " + str(status.returncode) + "):\n"
            + status.stdout.decode(errors="replace"))


def verify_transactions(loader, cli, cluster_file, environment):
    # An available database may still reject ordinary traffic, for example when
    # Ratekeeper stops transactions at its free-space limit. The CLI's set/get
    # commands use separate ordinary-priority transactions outside begin/commit.
    token = uuid.uuid4().hex
    key = "/orchestrator-test-readiness/" + token
    commands = "writemode on; set " + key + " " + token + "; get " + key + "; clear " + key + "; get " + key
    probe = subprocess.run([*loader, str(cli), "-C", str(cluster_file), "--exec", commands, "--timeout", "4"],
                           env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    written = ("`" + key + "' is `" + token + "'\n").encode()
    cleared = ("`" + key + "': not found\n").encode()
    if probe.returncode != 0 or written not in probe.stdout or cleared not in probe.stdout:
        raise RuntimeError("test FoundationDB ordinary read/write check failed:\n"
                           + probe.stdout.decode(errors="replace") + "\n"
                           + database_diagnostics(loader, cli, cluster_file, environment))


def main():
    command = sys.argv[1:]
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        sys.exit("usage: with-test-fdb.py -- <command> [arguments...]")
    arch = NATIVE.architecture(platform.machine())
    runtime, server, cli = prepare(arch)
    libraries = runtime / "lib/cluster-ctl"
    loader = [str(libraries / "ld.so"), "--inhibit-cache", "--library-path", str(libraries)]
    clean_env = {key: value for key, value in os.environ.items() if not key.startswith("LD_")}
    subprocess.run([*loader, str(server), "--version"], env=clean_env, check=True)
    with tempfile.TemporaryDirectory(prefix="cluster-fdb-") as temporary:
        root = Path(temporary)
        cluster_file = root / "fdb.cluster"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        address = "127.0.0.1:" + str(port)
        cluster_file.write_text("test:" + uuid.uuid4().hex + "@" + address + "\n")
        (root / "data").mkdir()
        (root / "logs").mkdir()
        with (root / "server.log").open("wb") as output:
            process = subprocess.Popen([*loader, str(server), "-C", str(cluster_file), "-p", address,
                                        "-d", str(root / "data"), "-L", str(root / "logs"),
                                        "--locality-machineid", "test-machine", "--locality-zoneid", "test-zone"],
                                       env=clean_env, stdout=output, stderr=subprocess.STDOUT)
            try:
                ready = False
                for attempt in range(3):
                    if process.poll() is not None:
                        raise RuntimeError("test FoundationDB server exited before configuration")
                    configured = subprocess.run([*loader, str(cli), "-C", str(cluster_file), "--exec", "configure new single ssd", "--timeout", "20"],
                                                env=clean_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    if attempt == 0 and configured.returncode != 0:
                        print("test FoundationDB initial configure: " + configured.stdout.decode(errors="replace"), file=sys.stderr, flush=True)
                    if configured.returncode == 0:
                        ready = True
                        break
                    time.sleep(0.1)
                if not ready:
                    raise RuntimeError("test FoundationDB configuration failed: " + configured.stdout.decode(errors="replace") + "\n" + (root / "server.log").read_text(errors="replace")[-4000:])
                available = False
                for _ in range(40):
                    status = subprocess.run([*loader, str(cli), "-C", str(cluster_file), "--exec", "status minimal", "--timeout", "2"],
                                            env=clean_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    if status.returncode == 0 and b"The database is available" in status.stdout:
                        available = True
                        break
                    time.sleep(0.1)
                if not available:
                    raise RuntimeError("test FoundationDB did not become available")
                verify_transactions(loader, cli, cluster_file, clean_env)
                print("test FoundationDB ready (7.3.77, single SSD; ordinary read/write checked)", flush=True)
                result = subprocess.run(command, env={**clean_env, "FDB_CLUSTER_FILE": str(cluster_file)})
                if result.returncode != 0:
                    print(database_diagnostics(loader, cli, cluster_file, clean_env), file=sys.stderr, flush=True)
                return result.returncode
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit("test-fdb: " + str(error))
