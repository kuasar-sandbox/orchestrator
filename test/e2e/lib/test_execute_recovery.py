"""Regression coverage for cancellation-safe e2e_execute ownership recovery."""

import os
from pathlib import Path
import secrets
import stat
import subprocess
import tempfile
import unittest


LIB = Path(__file__).with_name("execute_state.sh")
E2E = Path(__file__).resolve().parents[1] / "e2e_execute.sh"
RUN_ALL = Path(__file__).resolve().parents[1] / "run_all.sh"


class ExecuteRecovery(unittest.TestCase):
    def setUp(self):
        self.fixture = tempfile.TemporaryDirectory()
        self.root = Path(self.fixture.name)
        self.state = self.root / "execute.state"
        self.unit_dir = self.root / "units"
        self.unit_dir.mkdir()
        while True:
            self.run_key = "e-" + secrets.token_hex(3)
            self.work = Path("/tmp") / self.run_key
            try:
                self.work.mkdir()
                break
            except FileExistsError:
                pass
        self.switch = "x" + self.run_key.removeprefix("e-")
        self.switch_netns = self.run_key + "-sw"
        self.proxy_netns = self.run_key + "-proxy"
        self.proxy_host = self.run_key + "h"
        self.proxy_peer = self.run_key + "p"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.log = self.root / "commands"
        self.running = self.root / "switch-running"
        self.status = self.root / "switch-status"
        self.status.write_text("0")
        connector = self.bin / "connector-ctl"
        connector.write_text(
            """#!/usr/bin/env bash
set -euo pipefail
printf 'connector-ctl %s\\n' "$*" >> "$COMMAND_LOG"
case "$1 $2" in
  'vswitch status')
    status=$(cat "$SWITCH_STATUS")
    [ "$status" = 0 ] && [ -e "$SWITCH_RUNNING" ] && exit 0
    [ "$status" = 0 ] && exit 3
    exit "$status" ;;
  'vswitch stop')
    rm -f "$SWITCH_RUNNING" "$FIXTURE/link-${3}m0" ;;
  *) exit 2 ;;
esac
"""
        )
        connector.chmod(0o755)

    def tearDown(self):
        if self.work.exists():
            for path in sorted(self.work.rglob("*"), reverse=True):
                if path.is_dir():
                    path.rmdir()
                else:
                    path.unlink()
            self.work.rmdir()
        self.fixture.cleanup()

    def env(self):
        return {
            **os.environ,
            "COMMAND_LOG": str(self.log),
            "FIXTURE": str(self.root),
            "SWITCH_RUNNING": str(self.running),
            "SWITCH_STATUS": str(self.status),
            "KUASAR_EXECUTE_STATE_PATH": str(self.state),
            "KUASAR_EXECUTE_UNIT_DIR": str(self.unit_dir),
        }

    def bash(self, body, *, check=False, extra_env=None):
        environment = self.env()
        environment.update(extra_env or {})
        result = subprocess.run(
            ["bash", "-c", 'set -euo pipefail\n. "$1"\n' + body, "_", str(LIB), str(self.bin)],
            env=environment, text=True, capture_output=True, timeout=20,
        )
        if check and result.returncode != 0:
            self.fail(f"shell failed ({result.returncode}):\nstdout={result.stdout}\nstderr={result.stderr}")
        return result

    def record(self, original_forward="0"):
        result = self.bash(
            f'execute_state_reserve {self.run_key} {self.work} {self.switch} '
            f'{self.switch_netns} {self.proxy_netns} {self.proxy_host} {self.proxy_peer} '
            f'{original_forward}\n',
            check=True,
        )
        self.assertEqual(result.stdout, "")

    def install_owned_resources(self):
        self.running.touch()
        for name in (self.switch_netns, self.proxy_netns):
            (self.root / f"netns-{name}").touch()
        for name in (self.proxy_host, self.switch + "m0"):
            (self.root / f"link-{name}").touch()
        (self.root / "rule-to").touch()
        (self.root / "rule-from").touch()
        for prefix in (f"sandbox-runner-{self.run_key}@", f"sandbox-builder-{self.run_key}@"):
            (self.unit_dir / f"{prefix}.service").touch()
            dropin = self.unit_dir / f"{prefix}.service.d"
            dropin.mkdir()
            (dropin / "override.conf").touch()
        (self.unit_dir / "sandbox-runner.slice").touch()
        (self.unit_dir / "sandbox-builder.slice").touch()
        (self.root / f"unit-sandbox-runner-{self.run_key}@run.service").touch()
        (self.root / f"unit-sandbox-builder-{self.run_key}@build.service").touch()
        (self.root / "unit-sandbox-runner@foreign.service").touch()
        (self.work / "large-staging-file").touch()

    def recovery_harness(self):
        return r'''
ip() {
  printf 'ip %s\n' "$*" >> "$COMMAND_LOG"
  case "$1 $2" in
    'netns list')
      for path in "$FIXTURE"/netns-*; do
        [ -e "$path" ] && printf '%s\n' "${path##*/netns-}"
      done
      : ;;
    'netns pids')
      [ -e "$FIXTURE/netns-$3" ] || return 1 ;;
    'netns del') rm -f "$FIXTURE/netns-$3" ;;
    'link show') [ -e "$FIXTURE/link-$3" ] ;;
    'link del') rm -f "$FIXTURE/link-$3" ;;
    *) return 2 ;;
  esac
}
iptables() {
  printf 'iptables %s\n' "$*" >> "$COMMAND_LOG"
  local rule
  case "$*" in
    *" -i $EXECUTE_STATE_PROXY_VETH_HOST -o ${EXECUTE_STATE_SWITCH}m0 "*) rule=to ;;
    *" -i ${EXECUTE_STATE_SWITCH}m0 -o $EXECUTE_STATE_PROXY_VETH_HOST "*) rule=from ;;
    *) return 2 ;;
  esac
  case "$1" in
    -C) [ -e "$FIXTURE/rule-$rule" ] ;;
    -D)
      [ "${FAIL_RULE_DELETE:-0}" != 1 ] || return 24
      rm -f "$FIXTURE/rule-$rule" ;;
    *) return 2 ;;
  esac
}
systemctl() {
  printf 'systemctl %s\n' "$*" >> "$COMMAND_LOG"
  case "$1" in
    list-units)
      local pattern="${!#}" unit
      for path in "$FIXTURE"/unit-*; do
        [ -e "$path" ] || continue
        unit="${path##*/unit-}"
        case "$unit" in $pattern) printf '%s loaded active running\n' "$unit" ;; esac
      done ;;
    stop)
      [ "${FAIL_UNIT_STOP:-0}" != 1 ] || return 23
      rm -f "$FIXTURE/unit-$2" ;;
  esac
}
sysctl() {
  printf 'sysctl %s\n' "$*" >> "$COMMAND_LOG"
  if [ "$1" = -n ]; then printf '%s\n' "${CURRENT_FORWARD:-1}"; fi
}
kill() { printf 'kill %s\n' "$*" >> "$COMMAND_LOG"; }
execute_state_recover "$2"
'''

    def test_no_state_is_a_noop_even_without_connector_binary(self):
        result = self.bash('execute_state_recover /does/not/exist\n')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_state_is_private_regular_and_strictly_parsed(self):
        self.record("-")
        self.assertTrue(stat.S_ISREG(self.state.stat().st_mode))
        self.assertEqual(stat.S_IMODE(self.state.stat().st_mode), 0o600)
        loaded = self.bash('execute_state_load\nprintf "%s\\n" "$EXECUTE_STATE_RECORD"\n', check=True)
        self.assertEqual(loaded.stdout.strip(), self.state.read_text().strip())

        self.state.write_text("v1\tbad\n")
        self.state.chmod(0o600)
        invalid = self.bash('execute_state_load\n')
        self.assertNotEqual(invalid.returncode, 0)
        self.assertIn("invalid execute recovery state schema", invalid.stderr)

    def test_symlink_or_unsafe_mode_is_rejected(self):
        target = self.root / "target"
        target.write_text("v1\n")
        self.state.symlink_to(target)
        linked = self.bash('execute_state_load\n')
        self.assertNotEqual(linked.returncode, 0)
        self.assertIn("not a regular file", linked.stderr)
        self.state.unlink()
        self.state.symlink_to(self.root / "missing-target")
        dangling = self.bash('execute_state_load\n')
        self.assertNotEqual(dangling.returncode, 0)
        self.assertIn("not a regular file", dangling.stderr)
        self.state.unlink()
        self.record()
        self.state.chmod(0o644)
        unsafe = self.bash('execute_state_load\n')
        self.assertNotEqual(unsafe.returncode, 0)
        self.assertIn("unsafe ownership or mode", unsafe.stderr)

    def test_inconsistent_switch_fails_before_any_cleanup(self):
        self.record()
        self.install_owned_resources()
        self.status.write_text("2")
        failed = self.bash(self.recovery_harness())
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("cannot validate interrupted vSwitch", failed.stderr)
        commands = self.log.read_text()
        self.assertEqual(commands.splitlines(), [f"connector-ctl vswitch status {self.switch}"])
        self.assertTrue(self.state.exists())
        self.assertTrue(self.running.exists())

    def test_cleanup_command_failure_preserves_state_and_stops_recovery(self):
        self.record()
        self.install_owned_resources()
        failed = self.bash(self.recovery_harness(), extra_env={"FAIL_UNIT_STOP": "1"})
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("cannot stop execute-owned unit", failed.stderr)
        commands = self.log.read_text()
        self.assertNotIn(f"connector-ctl vswitch stop {self.switch}", commands)
        self.assertTrue(self.state.exists())
        self.assertTrue(self.running.exists())

    def test_rule_delete_failure_returns_without_retrying_forever(self):
        self.record()
        self.install_owned_resources()
        failed = self.bash(self.recovery_harness(), extra_env={"FAIL_RULE_DELETE": "1"})
        self.assertNotEqual(failed.returncode, 0)
        commands = self.log.read_text().splitlines()
        rule_checks = [line for line in commands if line.startswith("iptables -C FORWARD")]
        rule_deletes = [line for line in commands if line.startswith("iptables -D FORWARD")]
        self.assertEqual(len(rule_checks), 1)
        self.assertEqual(len(rule_deletes), 1)
        self.assertTrue(self.state.exists())
        self.assertTrue((self.root / "rule-to").exists())

    def test_owned_interrupted_topology_is_fully_recovered(self):
        self.record()
        self.install_owned_resources()
        recovered = self.bash(self.recovery_harness(), check=True)
        self.assertIn(f"recovering interrupted e2e_execute run {self.run_key}", recovered.stderr)
        self.assertFalse(self.state.exists())
        self.assertFalse(self.work.exists())
        self.assertFalse(self.running.exists())
        for name in (self.switch_netns, self.proxy_netns, self.proxy_host, self.switch + "m0"):
            self.assertFalse((self.root / f"netns-{name}").exists())
            self.assertFalse((self.root / f"link-{name}").exists())
        commands = self.log.read_text()
        self.assertIn(f"connector-ctl vswitch stop {self.switch} --force", commands)
        self.assertIn(f"systemctl stop sandbox-runner-{self.run_key}@run.service", commands)
        self.assertIn(f"systemctl stop sandbox-builder-{self.run_key}@build.service", commands)
        self.assertNotIn("systemctl stop sandbox-runner@foreign.service", commands)
        self.assertIn(f"ip netns del {self.proxy_netns}", commands)
        self.assertIn(f"ip netns del {self.switch_netns}", commands)
        self.assertIn("sysctl -q -w net.ipv4.ip_forward=0", commands)

    def test_recovery_preserves_a_newer_forwarding_policy(self):
        self.record("1")
        self.install_owned_resources()
        recovered = self.bash(self.recovery_harness(), check=True,
                              extra_env={"CURRENT_FORWARD": "0"})
        self.assertIn("preserving newer net.ipv4.ip_forward=0", recovered.stderr)
        self.assertFalse(self.state.exists())
        commands = self.log.read_text()
        self.assertNotIn("sysctl -q -w net.ipv4.ip_forward=", commands)

    def test_normal_finish_clears_only_a_matching_clean_record(self):
        self.record("-")
        expected = self.state.read_text().strip()
        mismatched = self.bash('execute_state_finish "$2" wrong-record\n')
        self.assertNotEqual(mismatched.returncode, 0)
        self.assertIn("no longer belongs", mismatched.stderr)
        self.assertTrue(self.state.exists())

        environment = self.env()
        environment["EXPECTED"] = expected
        finished = subprocess.run(
            ["bash", "-c", r'''
set -euo pipefail
. "$1"
ip() { case "$1 $2" in 'netns list') : ;; 'link show') return 1 ;; esac; }
iptables() { return 1; }
systemctl() { [ "$1" != list-units ] || :; }
execute_state_finish "$2" "$EXPECTED"
''', "_", str(LIB), str(self.bin)],
            env=environment, text=True, capture_output=True, timeout=20,
        )
        self.assertEqual(finished.returncode, 0, finished.stderr)
        self.assertFalse(self.state.exists())
        self.assertTrue(self.work.exists())

    def test_reservation_refuses_preexisting_forwarding_rule(self):
        body = r'''
ip() { case "$1 $2" in 'netns list') : ;; 'link show') return 1 ;; esac; }
iptables() { [ "$1" = -C ]; }
execute_state_assert_targets_absent "$2" fixture fixture-sw fixture-proxy fixture-h fixture-p
'''
        # Use a status-only connector that reports the switch absent.
        connector = self.bin / "connector-ctl"
        connector.write_text("#!/usr/bin/env bash\nexit 3\n")
        connector.chmod(0o755)
        refused = self.bash(body)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn("forwarding rule already exists", refused.stderr)
        self.assertFalse(self.state.exists())

    def test_reservation_fails_closed_when_ownership_cannot_be_inspected(self):
        connector = self.bin / "connector-ctl"
        connector.write_text("#!/usr/bin/env bash\nexit 3\n")
        connector.chmod(0o755)
        namespace_error = self.bash(r'''
ip() { [ "$1 $2" != 'netns list' ] || return 2; }
iptables() { return 1; }
execute_state_assert_targets_absent "$2" fixture fixture-sw fixture-proxy fixture-h fixture-p
''')
        self.assertNotEqual(namespace_error.returncode, 0)
        self.assertIn("cannot enumerate network namespaces", namespace_error.stderr)
        self.assertFalse(self.state.exists())

        rule_error = self.bash(r'''
ip() { case "$1 $2" in 'netns list') : ;; 'link show') return 1 ;; esac; }
iptables() { return 2; }
execute_state_assert_targets_absent "$2" fixture fixture-sw fixture-proxy fixture-h fixture-p
''')
        self.assertNotEqual(rule_error.returncode, 0)
        self.assertIn("cannot inspect execute forwarding rule", rule_error.stderr)
        self.assertFalse(self.state.exists())

    def test_suite_recovers_before_fixed_name_cases_and_records_before_setup(self):
        run_all = RUN_ALL.read_text()
        execute = E2E.read_text()
        self.assertLess(run_all.index('execute_state.sh" recover'), run_all.index('cases=('))
        self.assertLess(execute.index('execute_state_recover "$BIN"'),
                        execute.index("for b in node-ctl"))
        self.assertLess(execute.index('execute_state_recover "$BIN"'),
                        execute.index('docker image inspect "$E2E_IMAGE"'))
        self.assertLess(execute.index("execute_state_assert_targets_absent"),
                        execute.index("execute_state_assert_units_absent"))
        self.assertLess(execute.index("execute_state_assert_units_absent"),
                        execute.index("execute_state_reserve"))
        self.assertLess(execute.index("execute_state_reserve"), execute.index("setup_proxy_netns"))
        self.assertLess(execute.index("execute_state_remember_forwarding"),
                        execute.index("sysctl -q -w net.ipv4.ip_forward=1"))
        self.assertLess(execute.index('execute_state_restore_forwarding "$ORIG_IP_FORWARD"'),
                        execute.index('execute_state_finish "$BIN"'))


if __name__ == "__main__":
    unittest.main()
