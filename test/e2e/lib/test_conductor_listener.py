"""Real child sockets exercise the exact opt-in Conductor startup path."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import unittest

import conductor_listener


ROOT = Path(__file__).parent
CHILD = """
import json, socket, sys
sockets = []
ipv6 = sys.argv[2] == '6'
for _ in range(int(sys.argv[1])):
    listener = socket.socket(socket.AF_INET6 if ipv6 else socket.AF_INET)
    listener.bind(('::' if ipv6 else '0.0.0.0', 0))
    listener.listen()
    sockets.append(listener)
print(json.dumps([s.getsockname()[1] for s in sockets]), flush=True)
sys.stdin.read()
"""
HTTP_CHILD = """#!/usr/bin/env python3
import http.server, json, pathlib, socket, sys
config = json.loads(pathlib.Path(sys.argv[-1]).read_text())
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(204)
        self.end_headers()
    def log_message(self, *args):
        pass
if 'socket_fd' in config:
    server = http.server.HTTPServer(('0.0.0.0', config['port']), Handler,
                                   bind_and_activate=False)
    server.socket.close()
    server.socket = socket.socket(fileno=config['socket_fd'])
else:
    server = http.server.HTTPServer(('0.0.0.0', config['port']), Handler)
pathlib.Path(config['result']).write_text(str(server.socket.getsockname()[1]))
server.serve_forever()
"""


@contextmanager
def child_listeners(count, ipv6=False):
    process = subprocess.Popen([sys.executable, "-u", "-c", CHILD, str(count), "6" if ipv6 else "4"],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        ports = json.loads(process.stdout.readline())
        yield process, ports
    finally:
        process.communicate(timeout=5)


class ConductorListenerTest(unittest.TestCase):
    def test_only_owned_child_listener_is_selected(self):
        with child_listeners(1) as (foreign, foreign_ports), child_listeners(1) as (child, ports):
            self.assertNotEqual(ports, foreign_ports)
            self.assertEqual(conductor_listener.listener_port(child.pid, os.getpid()), ports[0])
            with socket.create_connection(("127.0.0.1", ports[0]), timeout=2):
                pass

    def test_foreign_listener_does_not_make_child_ready(self):
        with child_listeners(1), child_listeners(0) as (child, _):
            self.assertIsNone(conductor_listener.listener_port(child.pid, os.getpid()))

    @unittest.skipUnless(socket.has_ipv6, "Python has no IPv6 support")
    def test_owned_ipv6_wildcard_listener_is_selected(self):
        with child_listeners(1, ipv6=True) as (child, ports):
            self.assertEqual(conductor_listener.listener_port(child.pid, os.getpid()), ports[0])
            with socket.create_connection(("::1", ports[0]), timeout=2):
                pass

    def test_multiple_owned_listeners_are_rejected(self):
        with child_listeners(2) as (child, _):
            with self.assertRaisesRegex(ValueError, "multiple TCP listeners"):
                conductor_listener.listener_port(child.pid, os.getpid())

    def test_wrong_parent_and_exited_child_are_rejected(self):
        with child_listeners(1) as (child, _):
            with self.assertRaisesRegex(ValueError, "not owned"):
                conductor_listener.listener_port(child.pid, child.pid)
        with self.assertRaises(FileNotFoundError):
            conductor_listener.listener_port(child.pid, os.getpid())

    def test_cli_distinguishes_pending_startup_from_fatal_ambiguity(self):
        for count, status in ((0, 1), (2, 2)):
            with self.subTest(listeners=count), child_listeners(count) as (child, _):
                result = subprocess.run([sys.executable, str(ROOT / "conductor_listener.py"),
                                         str(child.pid), str(os.getpid())],
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, status)
                self.assertEqual(result.stdout, "")
                if status == 2:
                    self.assertIn("multiple TCP listeners", result.stderr)

    def start_conductor(self, listener=None):
        source = (ROOT / "execute_process.sh").read_text()
        function = re.search(r"(?ms)^start_conductor\(\) \{.*?^\}", source).group()
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            node = work / "node-ctl"
            node.write_text(HTTP_CHILD)
            node.chmod(0o755)
            port = listener.getsockname()[1] if listener else 0
            config = {"port": port, "result": str(work / "bound-port")}
            if listener:
                config["socket_fd"] = listener.fileno()
            (work / "config.yaml").write_text(json.dumps(config))
            # An invalid helper path proves that explicit PORT never uses
            # discovery; the inherited socket holds that fixed port throughout.
            environment = os.environ | {
                "WORK": directory, "ORCH_BIN_DIR": directory,
                "SCRIPT_DIR": str(ROOT) if not listener else "/no-discovery-for-explicit-port",
                "PORT": str(port), "DOMAIN": "fixture.invalid",
            }
            script = """set -euo pipefail
declare -a PIDS=()
ORCH_PID=""
fail() { echo "$*" >&2; exit 1; }
finish() {
    if [ -n "$ORCH_PID" ]; then
        kill "$ORCH_PID" 2>/dev/null || true
        wait "$ORCH_PID" 2>/dev/null || true
    fi
}
trap finish EXIT
""" + function + "\nstart_conductor \"$WORK/orch.log\"\nprintf '%s\\n' \"$PORT\"\n"
            result = subprocess.run(["bash", "-c", script], env=environment,
                                    capture_output=True, text=True, timeout=20,
                                    pass_fds=(listener.fileno(),) if listener else ())
            self.assertEqual(result.returncode, 0, result.stderr)
            actual = int(result.stdout.strip())
            self.assertEqual(actual, int((work / "bound-port").read_text()))
            self.assertGreater(actual, 0)
            return actual

    def test_zero_port_uses_actual_child_listener_and_existing_health_check(self):
        with child_listeners(1):
            self.start_conductor()

    def test_explicit_port_stays_fixed_without_discovery(self):
        with socket.socket() as listener:
            listener.bind(("0.0.0.0", 0))
            listener.listen()
            self.assertEqual(self.start_conductor(listener), listener.getsockname()[1])


if __name__ == "__main__":
    unittest.main()
