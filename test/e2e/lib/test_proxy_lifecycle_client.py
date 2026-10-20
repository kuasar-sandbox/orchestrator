import contextlib
import errno
import io
import json
import subprocess
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from proxy_lifecycle_client import (Probe, cgroup_processes, owned_process_observations,
                                    wait_exec_shutdown_observations)


class CgroupProcessesTest(unittest.TestCase):
    def test_reads_processes(self):
        with patch.object(Path, 'read_text', return_value='12\n34\n'):
            self.assertEqual(cgroup_processes(Path('/fixture')), ['12', '34'])

    def test_removed_cgroup_is_empty(self):
        for code in (errno.ENOENT, errno.ENODEV):
            with self.subTest(errno=code):
                with patch.object(Path, 'read_text', side_effect=OSError(code, 'removed')):
                    self.assertEqual(cgroup_processes(Path('/fixture')), [])

    def test_other_errors_propagate(self):
        for code in (errno.EACCES, errno.EIO):
            with self.subTest(errno=code):
                error = OSError(code, 'unexpected')
                with patch.object(Path, 'read_text', side_effect=error):
                    with self.assertRaises(OSError) as raised:
                        cgroup_processes(Path('/fixture'))
                self.assertIs(raised.exception, error)


class ExecShutdownObservationTest(unittest.TestCase):
    def run_wait(self, cgroup_values, exit_values, pid_values, clock_values):
        exit_record = Mock()
        exit_record.exists.side_effect = exit_values
        with patch('proxy_lifecycle_client.cgroup_processes', side_effect=cgroup_values) as groups:
            with patch.object(Path, 'exists', side_effect=pid_values) as pids:
                with patch('proxy_lifecycle_client.time.monotonic', side_effect=clock_values) as clock:
                    with patch('proxy_lifecycle_client.time.sleep') as sleep:
                        wait_exec_shutdown_observations(Path('/fixture'), exit_record, ['12'], 100)
        return groups, pids, clock, sleep

    def test_empty_cgroup_still_waits_for_owned_pid(self):
        _, pids, _, sleep = self.run_wait([[], []], [True, True], [True, False], [100])
        self.assertEqual(pids.call_count, 2)
        sleep.assert_called_once_with(.02)

    def test_owned_pid_cannot_extend_original_deadline(self):
        _, pids, clock, sleep = self.run_wait([[], []], [True, True], [True, True], [100, 118])
        self.assertEqual(pids.call_count, 2)
        self.assertEqual(clock.call_count, 2)
        sleep.assert_called_once_with(.02)
        # The caller's unchanged final /proc assertion must still reject it.

    def test_existing_cgroup_observation_is_still_required(self):
        _, _, _, sleep = self.run_wait([['12'], []], [True], [False], [100])
        sleep.assert_called_once_with(.02)

    def test_exit_record_is_still_required(self):
        _, _, _, sleep = self.run_wait([[], []], [False, True], [False], [100])
        sleep.assert_called_once_with(.02)

    def test_complete_observations_do_not_sleep(self):
        _, _, clock, sleep = self.run_wait([[]], [True], [False], [])
        clock.assert_not_called()
        sleep.assert_not_called()

    def test_read_errors_are_not_suppressed(self):
        error = PermissionError(errno.EACCES, 'denied')
        with patch('proxy_lifecycle_client.cgroup_processes', side_effect=error):
            with self.assertRaises(PermissionError) as raised:
                wait_exec_shutdown_observations(Path('/fixture'), Mock(), ['12'], 100)
        self.assertIs(raised.exception, error)


class OwnedProcessObservationTest(unittest.TestCase):
    def test_reports_only_process_state_fields(self):
        with patch.object(Path, 'read_text', return_value='State: Z (zombie)\nPPid: 1\nTracerPid: 0\nThreads: 1\nNSpid: 12\nName: ignored\n'):
            self.assertEqual(owned_process_observations(['12']), {
                '12': {'State': 'Z (zombie)', 'PPid': '1', 'TracerPid': '0',
                       'Threads': '1', 'NSpid': '12'}})

    def test_disappearance_and_denied_observation_are_distinct(self):
        with patch.object(Path, 'read_text', side_effect=[
                FileNotFoundError(errno.ENOENT, 'gone'), PermissionError(errno.EACCES, 'denied')]):
            self.assertEqual(owned_process_observations(['12', '13']), {
                '12': {'gone_during_observation': True}, '13': {'observation_errno': errno.EACCES}})


class FullShutdownFailureJournalTest(unittest.TestCase):
    invocation = '1234567890abcdef1234567890abcdef12'
    unit = 'e-test01-custom-bothfd.service'

    def run_shutdown(self, result, journal=None, main_pid=0, stderr=None):
        probe = Mock()
        probe.config = dict(unit=self.unit, socket_units='fixture.socket',
                            exit_record='/fixture/exit.json')
        probe.cgroup = Path('/fixture/cgroup')
        probe.main_pid.return_value = main_pid
        output = io.StringIO() if stderr is None else stderr
        calls = []

        def run(args, **kwargs):
            calls.append((args, kwargs))
            if args[0] == 'systemctl':
                return subprocess.CompletedProcess(args, 0)
            if isinstance(journal, Exception):
                raise journal
            return journal or subprocess.CompletedProcess(args, 0, b'{"MESSAGE":"custom proxy error"}\n')

        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(Path, 'read_text', side_effect=['', json.dumps(result)]))
            stack.enter_context(patch.object(Path, 'exists', return_value=True))
            stack.enter_context(patch.object(Path, 'unlink'))
            stack.enter_context(patch('proxy_lifecycle_client.cgroup_processes', return_value=[]))
            stack.enter_context(patch('proxy_lifecycle_client.time.monotonic', side_effect=[100, 115, 115]))
            stack.enter_context(patch('proxy_lifecycle_client.subprocess.check_output',
                                     side_effect=[self.invocation, 'Result=success\nSendSIGKILL=yes\n']))
            stack.enter_context(patch('proxy_lifecycle_client.subprocess.run', side_effect=run))
            stack.enter_context(contextlib.redirect_stderr(output))
            failure = None
            try:
                Probe.full_shutdown(probe)
            except AssertionError as error:
                failure = error
        return failure, calls, output.getvalue() if stderr is None else ''

    def exit_record(self, **changes):
        result = dict(SERVICE_RESULT='success', EXIT_CODE='exited',
                      EXIT_STATUS='0', INVOCATION_ID=self.invocation)
        result.update(changes)
        return result

    def test_success_has_no_journal_query(self):
        failure, calls, output = self.run_shutdown(self.exit_record())
        self.assertIsNone(failure)
        self.assertEqual([args[0] for args, _ in calls], ['systemctl'])
        self.assertEqual(output, '')

    def test_failure_uses_captured_invocation_and_keeps_original_assertion(self):
        result = self.exit_record(EXIT_STATUS='1', INVOCATION_ID='different-record')
        failure, calls, output = self.run_shutdown(result)
        self.assertEqual(failure.args, (result,))
        self.assertEqual(calls[0][0], ['systemctl', 'stop', '--no-block', self.unit, 'fixture.socket'])
        args, kwargs = calls[1]
        self.assertIn('_SYSTEMD_UNIT=' + self.unit, args)
        self.assertIn('_SYSTEMD_INVOCATION_ID=' + self.invocation, args)
        self.assertNotIn('_SYSTEMD_INVOCATION_ID=different-record', args)
        self.assertIn('--lines=80', args)
        self.assertIn('--output-fields=MESSAGE,PRIORITY,_PID', args)
        self.assertNotIn('--all', args)
        self.assertEqual(kwargs['timeout'], 5)
        self.assertEqual(kwargs['stderr'], subprocess.DEVNULL)
        self.assertIn('custom proxy error', json.loads(output)['journal_tail'])

    def test_query_errors_do_not_mask_exit_failure(self):
        result = self.exit_record(SERVICE_RESULT='exit-code', EXIT_STATUS='1')
        for error in (FileNotFoundError('journalctl unavailable'),
                      subprocess.TimeoutExpired('journalctl', 5)):
            with self.subTest(error=type(error).__name__):
                failure, _, output = self.run_shutdown(result, journal=error)
                self.assertEqual(failure.args, (result,))
                self.assertEqual(json.loads(output)['journal_error'], type(error).__name__)

    def test_nonzero_query_and_output_cap_keep_exit_failure(self):
        result = self.exit_record(EXIT_STATUS='1')
        message = b'x' * 20000 + b'\xff\n::error::diagnostic\n'
        failure, _, output = self.run_shutdown(
            result, journal=subprocess.CompletedProcess([], 1, message))
        self.assertEqual(failure.args, (result,))
        diagnostic = json.loads(output)
        self.assertEqual(diagnostic['returncode'], 1)
        self.assertEqual(diagnostic['journal_tail'], message[-16384:].decode('utf-8', 'replace'))
        self.assertEqual(len(output.splitlines()), 1)

    def test_legacy_commands_are_escaped_without_changing_json_content(self):
        result = self.exit_record(EXIT_STATUS='1')
        message = b'prefix ##[error]diagnostic ##[add-mask]fixture-value\n::error::diagnostic\n'
        failure, _, output = self.run_shutdown(
            result, journal=subprocess.CompletedProcess([], 0, message))
        self.assertEqual(failure.args, (result,))
        self.assertEqual(json.loads(output)['journal_tail'], message.decode())
        self.assertNotIn('##[', output)
        self.assertEqual(len(output.splitlines()), 1)

    def test_output_errors_do_not_mask_exit_failure(self):
        result = self.exit_record(EXIT_STATUS='1')
        broken = Mock()
        broken.write.side_effect = OSError(errno.EIO, 'stderr unavailable')
        closed = io.StringIO()
        closed.close()
        for label, stream in (('OSError', broken), ('closed-stream ValueError', closed)):
            with self.subTest(error=label):
                failure, _, _ = self.run_shutdown(result, stderr=stream)
                self.assertEqual(failure.args, (result,))

    def test_earlier_shutdown_deadline_failure_does_not_query_journal(self):
        failure, calls, output = self.run_shutdown(self.exit_record(), main_pid=42)
        self.assertEqual(failure.args, ('application did not finish its own bounded shutdown',))
        self.assertEqual([args[0] for args, _ in calls], ['systemctl'])
        self.assertEqual(output, '')

class LifecycleManagementTest(unittest.TestCase):
    def test_unix_socket_path_is_not_the_http_host(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'stats.sock')
            with socket.socket(socket.AF_UNIX) as server:
                server.bind(path)
                server.listen(1)
                server.settimeout(3)
                received = []

                def respond():
                    with server.accept()[0] as connection:
                        connection.settimeout(3)
                        request = b''
                        while b'\r\n\r\n' not in request:
                            chunk = connection.recv(4096)
                            if not chunk:
                                return
                            request += chunk
                        received.append(request)
                        connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}')

                worker = threading.Thread(target=respond)
                worker.start()
                try:
                    probe = Probe.__new__(Probe)
                    probe.stats = path
                    self.assertEqual(probe.management()[::2], (200, b'{}'))
                finally:
                    worker.join(4)
                self.assertFalse(worker.is_alive())
                self.assertEqual(len(received), 1)
                self.assertIn(b'GET /status HTTP/1.1\r\n', received[0])
                self.assertIn(b'Host: localhost\r\n', received[0])
                self.assertNotIn(path.encode(), received[0])


if __name__ == '__main__':
    unittest.main()
