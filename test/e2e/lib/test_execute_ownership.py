"""Isolate the exact execute-test cleanup functions from host resources."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

SOURCE = (Path(__file__).resolve().parents[1] / "e2e_execute.sh").read_text()


def function(name):
    return re.search(r"(?ms)^" + name + r"\(\) \{\n.*?^\}", SOURCE).group()


class ExecuteOwnership(unittest.TestCase):
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

    def test_no_stale_switch_adoption_or_global_stop(self):
        self.assertNotIn('vswitch stop "$SWITCH" --force', SOURCE)
        self.assertNotIn("systemctl stop 'sandbox-runner@*", SOURCE)
        self.assertNotIn('ip netns del "$SWITCH"', SOURCE)
        self.assertIn('[ "$switch_status" -eq 3 ] || fail', SOURCE)
        self.assertIn('SWITCH="${SWITCH:-x${RUN_KEY#e-}}"', SOURCE)


if __name__ == "__main__":
    unittest.main()
