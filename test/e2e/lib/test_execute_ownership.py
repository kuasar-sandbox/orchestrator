"""Isolate the exact execute-test cleanup functions from host resources."""
import os
import json
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest import mock
import copy
import signal
import sys

import runner_lifecycle

SOURCE = "\n".join(Path(__file__).with_name(name).read_text()
                   for name in ("execute_contract.sh", "execute_artifact.sh"))
SOURCE += "\n" + (Path(__file__).resolve().parents[1] / "cases" / "orchestrator.lifecycle.sh").read_text()


def function(name):
    return re.search(r"(?ms)^" + name + r"\(\) \{[^\n]*\n.*?^\}", SOURCE).group()


class ExecuteOwnership(unittest.TestCase):
    def lock_command(self, directory, *command):
        script = 'set -euo pipefail\nfail() { echo "$*" >&2; exit 1; }\n'
        script += function("run_host_serialized") + '\nrun_host_serialized "$@"\n'
        return ["bash", "-c", script, "_", str(directory), *command]

    def test_parallel_case_is_refused_before_touching_host_resources(self):
        with tempfile.TemporaryDirectory() as directory:
            holder = subprocess.Popen(self.lock_command(directory, "python3", "-c",
                'print("locked", flush=True); input()'), stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                self.assertEqual(holder.stdout.readline().strip(), "locked")
                marker = Path(directory) / "must-not-run"
                contender = subprocess.run(self.lock_command(directory, "touch", str(marker)),
                                           text=True, capture_output=True, timeout=10)
                self.assertEqual(contender.returncode, 1)
                self.assertIn("another execute/MMDS case", contender.stderr)
                self.assertFalse(marker.exists())
                holder.communicate("done\n", timeout=10)
                self.assertEqual(holder.returncode, 0)
                next_case = subprocess.run(self.lock_command(directory, "sh", "-c",
                    'test "$KUASAR_EXECUTE_LOCK_HELD" = 1'), timeout=10)
                self.assertEqual(next_case.returncode, 0)
            finally:
                if holder.poll() is None:
                    holder.kill()
                holder.communicate(timeout=10)

    def test_failed_case_releases_lock_and_does_not_leave_lock_files(self):
        with tempfile.TemporaryDirectory() as directory:
            failed = subprocess.run(self.lock_command(directory, "sh", "-c", "exit 23"), timeout=10)
            self.assertEqual(failed.returncode, 23)
            repeated = subprocess.run(self.lock_command(directory, "true"), timeout=10)
            self.assertEqual(repeated.returncode, 0)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_lock_descriptor_is_not_inherited_by_test_daemons(self):
        with tempfile.TemporaryDirectory() as directory:
            program = ('import os, sys; target=os.stat(sys.argv[1]); '
                       'assert not any((st.st_dev, st.st_ino) == (target.st_dev, target.st_ino) '
                       'for fd in range(3, 256) if os.path.exists("/proc/self/fd/"+str(fd)) '
                       'for st in [os.fstat(fd)])')
            result = subprocess.run(self.lock_command(directory, "python3", "-c", program, directory),
                                    text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('flock --nonblock --exclusive --close', SOURCE)

    def wait_case(self, call, delay=0):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "run.jsonl"
            log.write_text("")
            script = "set -euo pipefail\nRUN_ARGV_LOG=$1\n"
            script += function("argv_log_count") + "\n"
            script += function("run_argv_count") + "\n"
            script += function("wait_run_argv") + "\n" + function("assert_run_source_mode") + "\n"
            if call is not None:
                # Real delayed file observation, not a timer-only success stub.
                script += '(sleep "$2"; printf "%s\\n" "$3" >> "$RUN_ARGV_LOG") &\n'
            script += "wait_run_argv 0\nassert_run_source_mode 0 fixture-sandbox from\nwait\n"
            return subprocess.run(["bash", "-c", script, "_", str(log), str(delay), json.dumps(call)],
                                  text=True, capture_output=True, timeout=20)

    def test_async_starting_waits_for_the_real_run_observation(self):
        result = self.wait_case(["run", "--sandbox-id", "fixture-sandbox", "--from", "/fixture/ready.sandbox"], 0.15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_run_observation_still_fails(self):
        result = self.wait_case(None)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("timed out waiting for run call 0", result.stderr)

    def test_observed_wrong_source_or_identity_is_not_retried_or_accepted(self):
        for call in (["run", "--sandbox-id", "fixture-sandbox", "--restore", "/fixture/ready.snapshot"],
                     ["run", "--sandbox-id", "another-sandbox", "--from", "/fixture/ready.sandbox"]):
            result = self.wait_case(call)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("timed out", result.stderr)

    def run_case(self, body, *, collision=False, fail_link=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "commands"
            script = '''set -euo pipefail
fail() { echo "$*" >&2; exit 1; }
ip() {
  printf 'ip %s\\n' "$*" >> "$COMMAND_LOG"
  if [ "$1 $2" = 'link show' ]; then [ "$COLLISION" = 1 ]; return; fi
  if [ "$1 $2" = 'link add' ] && [ "$FAIL_LINK" = 1 ]; then return 23; fi
}
systemctl() {
  printf 'systemctl %s\\n' "$*" >> "$COMMAND_LOG"
  if [ "$1" = list-units ]; then
    printf '%s\\n' sandbox-runner-fixture@run.service sandbox-builder-fixture@build.service sandbox-runner@foreign.service sandbox-builder-other@foreign.service
  fi
}
iptables() { printf 'iptables %s\\n' "$*" >> "$COMMAND_LOG"; }
sysctl() { if [ "$1" = -n ]; then echo 0; fi; }
kill() { printf 'kill %s\\n' "$*" >> "$COMMAND_LOG"; }
wait() { :; }
RUNNER_PREFIX=sandbox-runner-fixture@
BUILDER_PREFIX=sandbox-builder-fixture@
PROXY_NETNS=kuasar-fixture-no-existing-netns
PROXY_VETH_HOST=fixture-host
PROXY_VETH_NS=fixture-peer
PROXY_HOST_IP=192.0.2.1
PROXY_NS_IP=192.0.2.2
FIP_CIDR=198.51.100.0/24
SWITCH=fixture-switch
SW_NETNS=fixture-switch-ns
MMDS_ROUTES_E2E=0
E2E_KEEP=1
IMMEDIATE_DATA_PID=
SW_STARTED=
ORIG_IP_FORWARD=
PROXY_NETNS_OWNED=0
PROXY_VETH_OWNED=0
SW_NETNS_OWNED=0
FORWARD_TO_SWITCH_OWNED=0
FORWARD_FROM_SWITCH_OWNED=0
PIDS=()
OURS=()
TAGS=()
'''
            script += "\n".join(function(name) for name in
                                ("stop_owned_units", "cleanup", "setup_proxy_netns"))
            result = subprocess.run(["bash", "-c", script + "\n" + body],
                env={**os.environ, "COMMAND_LOG": str(log), "WORK": directory,
                     "COLLISION": str(int(collision)), "FAIL_LINK": str(int(fail_link))},
                capture_output=True, text=True, timeout=10)
            return result, log.read_text() if log.exists() else ""

    def test_foreign_veth_is_refused_without_cleanup(self):
        result, commands = self.run_case("trap cleanup EXIT\nsetup_proxy_netns", collision=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("refusing foreign resource", result.stderr)
        self.assertNotIn("ip link del", commands)
        self.assertNotIn("ip netns del", commands)
        self.assertNotIn("ip netns add", commands)

    def test_partial_setup_only_removes_created_namespace(self):
        result, commands = self.run_case("trap cleanup EXIT\nsetup_proxy_netns", fail_link=True)
        self.assertEqual(result.returncode, 23)
        self.assertIn("ip netns del kuasar-fixture-no-existing-netns", commands)
        self.assertNotIn("ip link del", commands)
        self.assertNotIn("ip netns del fixture-switch-ns", commands)

    def test_success_cleanup_only_removes_owned_network(self):
        result, commands = self.run_case("trap cleanup EXIT\nsetup_proxy_netns")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ip link del fixture-host", commands)
        self.assertIn("ip netns del kuasar-fixture-no-existing-netns", commands)
        self.assertNotIn("ip netns del fixture-switch-ns", commands)
        self.assertNotIn("iptables -D", commands)

    def test_unit_selector_cannot_stop_foreign_instances(self):
        result, commands = self.run_case('stop_owned_units "$RUNNER_PREFIX" "$BUILDER_PREFIX"')
        self.assertEqual(result.returncode, 0, result.stderr)
        stopped = [line for line in commands.splitlines() if line.startswith("systemctl stop ")]
        self.assertEqual(stopped, ["systemctl stop sandbox-runner-fixture@run.service",
                                   "systemctl stop sandbox-builder-fixture@build.service"])
        self.assertNotIn("systemctl reset-failed sandbox-runner@foreign", commands)

    def test_no_global_cleanup_is_encoded_in_contract(self):
        self.assertNotIn('vswitch stop "$SWITCH" --force', SOURCE)
        self.assertNotIn("systemctl stop 'sandbox-runner@*", SOURCE)
        self.assertNotIn('ip netns del "$SWITCH"', SOURCE)



class SDKShutdownJournal(unittest.TestCase):
    def check_journal(self, text):
        case = Path(__file__).resolve().parents[1] / "cases/orchestrator.resource-startup.sh"
        source = case.read_text()
        start = source.index("    # KillMode=control-group")
        end = source.index('    echo "==> PASS: 8GiB/256MiB', start)
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal"
            journal.write_text(text)
            script = 'set -euo pipefail\nfail() { echo "$*" >&2; exit 1; }\n' + source[start:end]
            return subprocess.run(["bash", "-c", script], text=True, capture_output=True,
                                  env={**os.environ, "LOW_JOURNAL": str(journal), "iteration": "1"}, timeout=5)

    def test_public_sdk_shutdown_retains_signal_and_exit_evidence(self):
        result = self.check_journal(
            "[sandbox-sdk] received terminated, requested vmm.shutdown via API\n"
            "[sandbox-sdk] CH exited code=0\n")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_signal_exit_and_escalation_remain_failures(self):
        valid = "[sandbox-sdk] received terminated\n[sandbox-sdk] CH exited code=0\n"
        for text, diagnostic in (
            ("[sandbox-sdk] CH exited code=0\n", "did not observe StopUnit"),
            ("[sandbox-sdk] received terminated\n", "did not observe CH exit"),
            (valid + "[sandbox-sdk] CH didn't exit within 10s; sending SIGKILL\n", "escalated shutdown"),
        ):
            with self.subTest(diagnostic=diagnostic):
                result = self.check_journal(text)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(diagnostic, result.stderr)


class TerminalEvidence(unittest.TestCase):
    def test_collected_runtime_keeps_bounded_result_without_private_fields(self):
        rid = "sr-00000000-0000-7000-8000-000000000001"
        state = {"state": "dead", "run_id": "", "sandbox_result_run_id": rid,
                 "sandbox_result_json": json.dumps({"stage": "run", "exit_code": 1,
                     "signal": "killed", "error": "private diagnostic", "env": "private environment"}),
                 "metadata": "private metadata", "base_dir": "private path"}
        with mock.patch.object(runner_lifecycle, "row", return_value=state):
            evidence = runner_lifecycle.terminal_evidence(Path("/fixture"), "test-run")
        self.assertEqual(evidence, {"sid": "test-run", "state": "dead", "run_id": "", "result_run_id": rid,
            "result": {"stage": "run", "exit_code": 1, "signal": "killed", "error_present": True}})
        self.assertNotIn("private", json.dumps(evidence))

    def test_unrecognized_result_content_is_not_printed(self):
        state = {"state": "dead", "run_id": "private run", "sandbox_result_run_id": "private result",
                 "sandbox_result_json": json.dumps({"stage": "private stage", "signal": "private signal",
                    "exit_code": "private code", "error": "private error"})}
        with mock.patch.object(runner_lifecycle, "row", return_value=state):
            evidence = runner_lifecycle.terminal_evidence(Path("/fixture"), "test-run")
        self.assertEqual(evidence["result"], {"error_present": True})
        self.assertNotIn("private", json.dumps(evidence))


class RunnerLifecycleIdentity(unittest.TestCase):
    def test_kernel_worker_is_not_a_second_userspace_vmm(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            for pid, status in {
                "101": "Name:\tcloud-hypervisor\nKthread:\t0\n",
                "102": "Name:\tkvm-nx-lpage-recovery\nKthread:\t1\n",
                "103": "Name:\tkvm-nx-lpage-recovery\nKthread:\t0\n",
                "104": "Name:\tkvm-nx-lpage-recovery\n",
            }.items():
                (proc / pid).mkdir()
                (proc / pid / "status").write_text(status)
            self.assertEqual(runner_lifecycle.userspace_members(["101", "102"], proc), ["101"])
            # Extra userspace tasks cannot hide behind a kernel-worker name,
            # and older status formats without Kthread retain the old check.
            self.assertEqual(runner_lifecycle.userspace_members(["101", "102", "103", "104"], proc),
                             ["101", "103", "104"])

    def test_namespace_unmapped_zero_is_not_a_local_process(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            for pid, status in {
                "101": "Kthread:\t0\n",
                "102": "Kthread:\t1\n",
                "103": "Kthread:\t0\n",
            }.items():
                (proc / pid).mkdir()
                (proc / pid / "status").write_text(status)
            # cgroup.procs uses zero for tasks outside the reader's PID
            # namespace. No /proc/0 exists; visible extra userspace remains.
            self.assertEqual(runner_lifecycle.userspace_members(
                ["0", "101", "0", "102", "103"], proc), ["101", "103"])
            # Zero alone cannot satisfy snapshot's exactly-one-CH assertion.
            self.assertEqual(runner_lifecycle.userspace_members(["0", "0"], proc), [])

    def test_zero_does_not_hide_missing_visible_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            with self.assertRaises(FileNotFoundError) as raised:
                runner_lifecycle.userspace_members(["0", "101"], proc)
            self.assertEqual(raised.exception.filename, str(proc / "101/status"))

    def test_missing_member_identity_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                runner_lifecycle.userspace_members(["101"], Path(directory))

    def observed(self, pid=101):
        return {"sid":"test-run", "run_id":"sr-old", "unit":"test@sr-old.service", "cgroup":"/test@sr-old.service",
                "processes":{role:dict(pid=pid,ppid=1,start_ticks=1,cgroup="/unit/"+role,executable=role)
                             for role in ("parent","runtime","ch")}}

    def test_exact_identity_survives_only_measurement_changes(self):
        before=self.observed()
        after=copy.deepcopy(before)
        after["parent_measurement"]={"Pss_bytes":123}
        runner_lifecycle.assert_same_run(before,after)
        for key in ("run_id","unit","cgroup"):
            changed=copy.deepcopy(before)
            changed[key]+="-new"
            with self.assertRaises(ValueError): runner_lifecycle.assert_same_run(before,changed)
        for role in ("parent","runtime","ch"):
            for key in ("pid","ppid","start_ticks"):
                changed=copy.deepcopy(before)
                changed["processes"][role][key]+=1
                with self.assertRaises(ValueError): runner_lifecycle.assert_same_run(before,changed)

    def test_stale_identity_closes_pidfd_without_sending_signal(self):
        before=self.observed()
        after=copy.deepcopy(before)
        after["run_id"]="sr-successor"
        with mock.patch.object(runner_lifecycle.os,"pidfd_open",return_value=123) as opened, \
             mock.patch.object(runner_lifecycle.os,"close") as closed, \
             mock.patch.object(runner_lifecycle.signal,"pidfd_send_signal") as sent, \
             mock.patch.object(runner_lifecycle,"snapshot",return_value=after):
            with self.assertRaises(ValueError):
                runner_lifecycle.kill_exact(Path("/test"),before,"test@","runtime")
            opened.assert_called_once_with(101)
            closed.assert_called_once_with(123)
            sent.assert_not_called()

    def test_pidfd_signal_targets_only_the_pinned_test_child(self):
        child=subprocess.Popen([sys.executable,"-c","import time; time.sleep(20)"])
        try:
            before=self.observed(child.pid)
            # Mock only the test inventory, not the actual process
            # or kernel signal. Real KVM/unit inventory is verified by execute.
            with mock.patch.object(runner_lifecycle,"snapshot",return_value=before):
                runner_lifecycle.kill_exact(Path("/test"),before,"test@","runtime")
            self.assertEqual(child.wait(timeout=5),-signal.SIGKILL)
        finally:
            if child.poll() is None: child.kill()
            child.wait(timeout=5)

    def test_real_process_identity_has_start_time_and_parent(self):
        child=subprocess.Popen([sys.executable,"-c","import time; time.sleep(20)"])
        try:
            actual=runner_lifecycle.process(child.pid)
            self.assertEqual(actual["ppid"],os.getpid())
            self.assertEqual(actual["pid"],child.pid)
            self.assertGreater(actual["start_ticks"],0)
        finally:
            child.terminate()
            child.wait(timeout=5)

    def test_lease_requires_shared_node_runtime_pid(self):
        with tempfile.TemporaryDirectory() as directory:
            work=Path(directory)
            observed=self.observed()
            observed["processes"]["runtime"]["pid"]=101
            leases=work/"sandbox-resource.sock.leases"
            leases.mkdir()
            path=leases/(runner_lifecycle.hashlib.sha256(observed["sid"].encode()).hexdigest()+".json")
            lease={"sandbox_id":observed["sid"],"pid":102,"client_features":["state_sync_v1"]}
            path.write_text(json.dumps(lease))
            with self.assertRaises(ValueError):runner_lifecycle.verify_lease(work,observed)
            lease["pid"]=101
            path.write_text(json.dumps(lease))
            runner_lifecycle.verify_lease(work,observed)

    def test_exit_matrix_is_wired_without_delete_or_manual_stop(self):
        run=function("run_runner_exit_case")
        after_signal=run.split('runner_lifecycle_check signal',1)[1]
        self.assertNotIn('req DELETE',after_signal)
        self.assertNotIn('systemctl stop',after_signal)
        self.assertNotIn('stop_orchestrator',after_signal)
        self.assertIn('wait_sandbox_state "$sid" dead',after_signal)
        self.assertIn('runner_lifecycle_check same-run',run)
        self.assertIn('runner_lifecycle_check lease',run)
        for mode in ('static','controller'):
            for role in ('ch','runtime','parent'):
                self.assertIn('run_runner_exit_case '+mode+' '+role+' ',SOURCE)

if __name__ == "__main__":
    unittest.main()
