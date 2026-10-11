"""Real Unix socket and evidence ownership checks for prepared case workspaces."""
import ast
import importlib.util
import inspect
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock
import time


HELPER = Path(__file__).with_name("case_workspace.sh")


class CaseWorkspace(unittest.TestCase):
    def run_workspace(self, output, body, *args, keep=False):
        env = dict(os.environ, WORK=str(output), OUT=str(output))
        env.pop("E2E_KEEP", None)
        if keep:
            env["E2E_KEEP"] = "1"
        result = subprocess.run(
            ["bash", "-c", 'set -euo pipefail; . "$1"; shift; '
             'case_workspace_init; trap case_workspace_cleanup EXIT; ' + body,
             "_", str(HELPER), *args], env=env, text=True,
            capture_output=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def test_long_runner_path_allows_maximum_sandbox_socket(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / ("runner-" + "x" * 80) / "orchestrator.resource-reservation.sh"
            probe = '''import json, pathlib, socket, sys
work = pathlib.Path(sys.argv[1])
path = work / "run" / "sandboxes" / ("s" * 57) / "vsock.sock_5000"
path.parent.mkdir(parents=True)
with socket.socket(socket.AF_UNIX) as server:
    server.bind(str(path))
    print(json.dumps({"work": str(work), "socket_bytes": len(str(path).encode())}))
'''
            observed = json.loads(self.run_workspace(output, 'python3 -c "$1" "$WORK"', probe))
            self.assertLessEqual(observed["socket_bytes"], 107)
            self.assertFalse(Path(observed["work"]).exists())
            self.assertTrue(output.is_dir())

    def test_cleanup_keeps_logs_and_preserves_runner_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / "prepared.input").write_text("prepared")
            runtime = self.run_workspace(output,
                'printf "%s\\n" "$WORK"; printf evidence > "$WORK/node.log"; '
                'printf private > "$WORK/config.yaml"').strip()
            self.assertFalse(Path(runtime).exists())
            self.assertEqual((output / "prepared.input").read_text(), "prepared")
            self.assertEqual((output / "runtime/node.log").read_text(), "evidence")
            self.assertFalse((output / "runtime/config.yaml").exists())

    def test_explicit_keep_retains_only_this_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            output = self.run_workspace(Path(directory), 'printf "%s\\n" "$WORK"', keep=True)
            runtime = Path(output.splitlines()[0])
            try:
                self.assertTrue(runtime.is_dir())
                self.assertIn("kept runtime directory: " + str(runtime), output)
            finally:
                shutil.rmtree(runtime)


class SplitExecuteWorkspace(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.key = 'e-' + secrets.token_hex(3)
        self.work = Path(self.temporary.name).resolve() / 'disk' / self.key
        self.work.mkdir(parents=True)
        self.runtime = Path('/tmp') / self.key
        self.runtime.mkdir(mode=0o700)
        self.addCleanup(lambda: shutil.rmtree(self.runtime, ignore_errors=True))
        self.run_root = self.runtime / 'run'
        self.sid = 'test-sandbox'
        self.rid = 'sr-00000000-0000-0000-0000-000000000001'
        self.prefix = 'sandbox-runner-' + self.key + '@'
        self.unit = self.prefix + self.rid + '.service'
        self.cgroup = '/fixture-' + self.key + '/' + self.unit
        spec = importlib.util.spec_from_file_location('runner_lifecycle_tested',
            HELPER.with_name('runner_lifecycle.py'))
        self.helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.helper)

    def test_runtime_root_is_explicit_and_bounded_by_workspace_identity(self):
        self.assertEqual(self.helper.runtime_root(self.work), self.work / 'run')
        self.assertEqual(self.helper.runtime_root(self.work, self.run_root), self.run_root)
        for root in (self.work / 'foreign', Path('/tmp/e-foreign/run'), self.run_root / '..'):
            with self.subTest(root=root), self.assertRaisesRegex(ValueError, 'runtime root'):
                self.helper.runtime_root(self.work, root)

    def test_snapshot_reads_short_runtime_pid_files_and_disk_state(self):
        directory = self.run_root / 'sandboxes' / self.sid
        directory.mkdir(parents=True)
        (directory / (self.sid + '.pid')).write_text('101')
        (self.run_root / 'runners').mkdir()
        (self.run_root / 'runners' / (self.rid + '.pid')).write_text('101')
        state = dict(state='running', run_id=self.rid, run_dir=str(directory),
                     base_dir=str(self.work / 'lib/sandboxes' / self.sid))
        props = f'MainPID=101\nControlGroup={self.cgroup}\nActiveState=active\n'
        identities = {
            101: dict(pid=101, ppid=1, start_ticks=1, cgroup=self.cgroup+'/ctl', executable='node-ctl'),
            102: dict(pid=102, ppid=101, start_ticks=2, cgroup=self.cgroup+'/ctl', executable='sandbox-ctl'),
            103: dict(pid=103, ppid=101, start_ticks=3, cgroup=self.cgroup+'/vmm', executable='cloud-hypervisor'),
        }
        synthetic = {
            '/sys/fs/cgroup' + self.cgroup + '/ctl/cgroup.procs': '101\n',
            '/proc/101/status': 'Kthread:\t0\n',
            '/proc/102/status': 'Kthread:\t0\n',
            '/sys/fs/cgroup' + self.cgroup + '/vmm/cgroup.procs': '103\n',
            '/proc/103/status': 'Kthread:\t0\n',
            '/proc/101/smaps_rollup': 'Pss: 1 kB\nPrivate_Clean: 0 kB\nPrivate_Dirty: 1 kB\n',
        }
        read_text, iterdir = Path.read_text, Path.iterdir
        def read(path, *args, **kwargs):
            return synthetic[str(path)] if str(path) in synthetic else read_text(path, *args, **kwargs)
        def children(path):
            return iter(()) if str(path) == '/proc/101/fd' else iterdir(path)
        with mock.patch.object(self.helper, 'row', return_value=state), \
             mock.patch.object(self.helper.subprocess, 'check_output', return_value=props), \
             mock.patch.object(self.helper, 'process', side_effect=lambda pid: identities[pid]), \
             mock.patch.object(Path, 'read_text', read), mock.patch.object(Path, 'iterdir', children):
            kwargs = {'run_root': self.run_root} if 'run_root' in inspect.signature(self.helper.snapshot).parameters else {}
            observed = self.helper.snapshot(self.work, self.sid, self.prefix, **kwargs)
            # Reject the retired child-process ownership model, not just accept
            # the new shared RunID/runtime PID in the happy-path fixture.
            with self.subTest(invalid='separate runtime PID'):
                (directory / (self.sid + '.pid')).write_text('102')
                try:
                    with self.assertRaisesRegex(ValueError, 'node-ctl must own both'):
                        self.helper.snapshot(self.work, self.sid, self.prefix, **kwargs)
                finally:
                    (directory / (self.sid + '.pid')).write_text('101')
            with self.subTest(invalid='CH parent'), mock.patch.dict(identities[103], ppid=102):
                with self.assertRaisesRegex(ValueError, 'process parent chain'):
                    self.helper.snapshot(self.work, self.sid, self.prefix, **kwargs)
            with self.subTest(invalid='extra ctl process'), mock.patch.dict(synthetic, {
                    '/sys/fs/cgroup' + self.cgroup + '/ctl/cgroup.procs': '101\n102\n'}):
                with self.assertRaisesRegex(ValueError, 'ctl leaf must contain only node-ctl'):
                    self.helper.snapshot(self.work, self.sid, self.prefix, **kwargs)
        self.assertEqual(observed['processes'], dict(parent=identities[101], runtime=identities[101], ch=identities[103]))

    def test_dead_check_rejects_a_retained_short_runtime_directory(self):
        (self.run_root / 'sandboxes' / self.sid).mkdir(parents=True)
        observed = dict(sid=self.sid, run_id=self.rid, cgroup=self.cgroup, processes={})
        fields = ('run_id','run_dir','base_dir','vswitch_port','floatingip','inner_ip','port_mac',
                  'envd_uds','ci_uds','resume_source_kind','resume_source_ref','resume_sandbox_ref')
        state = {name: '' for name in fields}
        state.update(state='dead', sandbox_result_run_id=self.rid,
            sandbox_result_json=json.dumps(dict(run_id=self.rid, sid=self.sid, stage='run')))
        kwargs = {'run_root': self.run_root} if 'run_root' in inspect.signature(self.helper.assert_dead).parameters else {}
        with mock.patch.object(self.helper, 'row', return_value=state), \
             self.assertRaisesRegex(ValueError, 'retained an object directory'):
            self.helper.assert_dead(self.work, observed, **kwargs)

    def test_signal_revalidates_the_same_declared_runtime_root(self):
        proc = dict(pid=12345, ppid=1, start_ticks=99, cgroup=self.cgroup, executable='fixture')
        observed = dict(sid=self.sid, run_id=self.rid, unit=self.unit, cgroup=self.cgroup,
                        processes={role: proc for role in ('parent','runtime','ch')})
        with mock.patch.object(self.helper, 'snapshot', return_value=observed) as snapshot, \
             mock.patch.object(self.helper.os, 'pidfd_open', return_value=42), \
             mock.patch.object(self.helper.os, 'close'), \
             mock.patch.object(self.helper.signal, 'pidfd_send_signal') as send:
            self.helper.kill_exact(self.work, observed, self.prefix, 'runtime', self.run_root)
        snapshot.assert_called_once_with(self.work, self.sid, self.prefix, self.run_root)
        send.assert_called_once()

    def test_lifecycle_wrapper_passes_runtime_root_for_every_operation(self):
        case = HELPER.parent.parent / 'cases/orchestrator.lifecycle.sh'
        line = next(line for line in case.read_text().splitlines() if 'python3 ' in line and 'runner_lifecycle.py' in line)
        env = dict(os.environ, E2E_LIB='/fixture', WORK=str(self.work),
                   EXECUTE_RUN_ROOT=str(self.run_root), RUNNER_PREFIX=self.prefix)
        for operation in ('snapshot','signal','same-run','dead'):
            script = 'python3() { printf "%s\\n" "$@"; }; check() { ' + line.strip() + '; }; check "$1" sid evidence'
            result = subprocess.run(['bash','-c',script,'_',operation], env=env, text=True, capture_output=True, check=True)
            args = result.stdout.splitlines()
            self.assertIn('--run-root', args)
            self.assertEqual(args[args.index('--run-root')+1], str(self.run_root))
            self.assertIn(operation, args)

    def test_pressure_embedded_python_uses_the_declared_short_root(self):
        text = (HELPER.parent.parent / 'cases/orchestrator.proxy-wake.sh').read_text()
        (self.work / 'pressure-before.json').write_text(json.dumps({'nonce':'fixture-only'}))
        (self.work / 'pressure-second-before.json').write_text(json.dumps({'nonce':'second-fixture'}))
        env = dict(os.environ, BIN='/fixture/bin', WORK=str(self.work),
                   EXECUTE_RUN_ROOT=str(self.run_root), FIRST=self.sid, SECOND='second-sandbox')
        for marker, expected_count in [('PY_DIAG',1),('PY_SATISFIED',2),('PY_POLICY',1)]:
            with self.subTest(marker=marker):
                header = re.search(r'^\s*python3 - .*<<\'' + marker + r'\'.*$', text, re.M)
                self.assertIsNotNone(header)
                source = text[header.end()+1:].split('\n'+marker,1)[0]
                tree = ast.parse(source)
                command = 'python3() { printf "%s\\n" "$@"; }; fail() { return 1; }; ' + header[0] + '\n' + marker + '\n'
                result = subprocess.run(['bash','-c',command], env=env, text=True, capture_output=True, check=True)
                args = result.stdout.splitlines()
                self.assertIn(str(self.run_root), args)
                # Run actual setup assignments with the arguments carried by the
                # Bash header. No product command or pressure loop is executed.
                prefix = []
                for node in tree.body:
                    if isinstance(node,(ast.Import,ast.ImportFrom)):
                        continue
                    if not isinstance(node,ast.Assign):
                        break
                    prefix.append(node)
                namespace = dict(Path=Path, json=json, time=time, sys=SimpleNamespace(argv=args))
                exec(compile(ast.Module(body=prefix,type_ignores=[]),str(case_path(marker)),'exec'), namespace)
                observations = []
                for node in ast.walk(tree):
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'glob':
                        observations.append((node.func.value, self.run_root/'sandboxes'))
                    if not isinstance(node, ast.List):
                        continue
                    for index, element in enumerate(node.elts[:-1]):
                        if isinstance(element, ast.Constant) and element.value in ('--run-root','--unix-socket'):
                            expected = self.run_root/'sandboxes'
                            if element.value == '--unix-socket': expected = expected/self.sid/'ch.sock'
                            observations.append((node.elts[index+1], expected))
                self.assertEqual(len(observations), expected_count)
                namespace['sid'] = self.sid
                for expression, expected in observations:
                    value = eval(compile(ast.Expression(expression),marker,'eval'),namespace)
                    self.assertEqual(Path(value), expected)


def case_path(marker):
    return HELPER.parent.parent / ('cases/orchestrator.proxy-wake.sh:' + marker)


if __name__ == "__main__":
    unittest.main()
