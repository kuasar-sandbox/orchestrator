import errno
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
