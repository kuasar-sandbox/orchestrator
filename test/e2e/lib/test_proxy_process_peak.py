import contextlib
import errno
import io
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from proxy_lifecycle_client import Probe, report_process_limit


class ProcessLimitEvidenceTest(unittest.TestCase):
    config = dict(entry='app', source='bothfd', unit='fixture.service')

    def report(self, pids, stream=None):
        output = io.StringIO() if stream is None else stream
        with contextlib.redirect_stderr(output):
            report_process_limit(pids, self.config, Path('/fixture/cgroup'), 12)
        return output.getvalue() if stream is None else ''

    def test_raw_duplicates_and_identity_are_preserved(self):
        pids = ['12', '13', '14', '15', '16', '17', '17']
        with patch('proxy_lifecycle_client.owned_process_observations',
                   return_value={pid: {'PPid': '12', 'State': 'S (sleeping)'} for pid in pids}):
            with patch('proxy_lifecycle_client.os.readlink', return_value='/fixture/proxy'):
                result = json.loads(self.report(pids))
        self.assertEqual(result['pids'], pids)
        self.assertEqual((result['observed_count'], result['unique_count']), (7, 6))
        self.assertEqual(result['main_pid'], 12)
        self.assertEqual(result['processes']['17']['exe'], '/fixture/proxy')
        self.assertFalse(result['truncated'])

    def test_snapshot_is_bounded_and_does_not_read_command_arguments(self):
        pids = [str(pid) for pid in range(100, 200)]
        with patch.object(Path, 'read_text', autospec=True, return_value='State: S\nPPid: 1\n') as reads:
            with patch('proxy_lifecycle_client.os.readlink', return_value='x' * 1000) as links:
                result = json.loads(self.report(pids))
        self.assertEqual(result['pids'], pids[:32])
        self.assertEqual(result['observed_count'], 100)
        self.assertTrue(result['truncated'])
        self.assertEqual(reads.call_count, 32)
        self.assertEqual(links.call_count, 32)
        self.assertEqual([str(call.args[0]) for call in reads.call_args_list],
                         ['/proc/' + pid + '/status' for pid in pids[:32]])
        self.assertEqual([call.args[0] for call in links.call_args_list],
                         ['/proc/' + pid + '/exe' for pid in pids[:32]])
        self.assertEqual(len(result['processes']['100']['exe']), 512)

    def test_process_disappearance_is_explicit(self):
        with patch.object(Path, 'read_text', side_effect=FileNotFoundError(errno.ENOENT, 'gone')):
            with patch('proxy_lifecycle_client.os.readlink', side_effect=FileNotFoundError(errno.ENOENT, 'gone')):
                result = json.loads(self.report(['12']))
        self.assertEqual(result['processes']['12'],
                         {'gone_during_observation': True, 'exe_errno': errno.ENOENT})

    def test_workflow_commands_are_escaped(self):
        with patch('proxy_lifecycle_client.owned_process_observations', return_value={'12': {}}):
            with patch('proxy_lifecycle_client.os.readlink', return_value='##[error]x\n::error::x'):
                output = self.report(['12'])
        self.assertEqual(len(output.splitlines()), 1)
        self.assertNotIn('##[', output)
        self.assertEqual(json.loads(output)['processes']['12']['exe'], '##[error]x\n::error::x')

    def test_stderr_failure_is_not_fatal(self):
        broken = Mock()
        broken.write.side_effect = OSError(errno.EIO, 'unavailable')
        closed = io.StringIO()
        closed.close()
        for stream in (broken, closed):
            with self.subTest(stream=type(stream).__name__):
                with patch('proxy_lifecycle_client.owned_process_observations', return_value={'12': {}}):
                    with patch('proxy_lifecycle_client.os.readlink', return_value='/fixture/proxy'):
                        self.report(['12'], stream)


class ProcessLimitMonitorTest(unittest.TestCase):
    def test_only_first_violation_is_reported_and_raw_peaks_are_unchanged(self):
        probe = Probe.__new__(Probe)
        probe.cgroup = Path('/fixture/cgroup')
        probe.pid = 12
        probe.config = ProcessLimitEvidenceTest.config
        probe.peaks = dict(processes=0, rss_kib=0, fds=0)
        probe.process_limit_reported = False
        probe.stop = Mock()
        probe.stop.is_set.side_effect = [False, False, False, False, True]
        probe.log = Mock()
        samples = ['1 2 3 4 5 6', '1 2 3 4 5 6 6', '1 2 3 4 5 6 7 8', '1 2 3']
        def read(path):
            return samples.pop(0) if path.name == 'cgroup.procs' else 'VmRSS: 4 kB\n'
        with patch.object(Path, 'read_text', autospec=True, side_effect=read):
            with patch.object(Path, 'iterdir', return_value=iter(())):
                with patch('proxy_lifecycle_client.report_process_limit') as report:
                    probe.resources()
        report.assert_called_once_with(['1', '2', '3', '4', '5', '6', '6'],
                                       probe.config, probe.cgroup, probe.pid)
        self.assertEqual(probe.peaks['processes'], 8)
        self.assertEqual(probe.stop.wait.call_count, 4)
        self.assertEqual(probe.log.call_count, 4)

    def test_original_run_assertion_still_rejects_seven_processes(self):
        probe = Mock()
        probe.config = dict(entry='app', source='address', target='/fixture/target')
        probe.pid = 12
        probe.initial_pid = 12
        probe.stable_fds = [4, 4]
        probe.requests = 1
        probe.errors = []
        probe.peaks = dict(processes=7, rss_kib=100, fds=4)
        with patch('proxy_lifecycle_client.digest', side_effect=['old', 'new', 'old', 'new']):
            with self.assertRaises(AssertionError) as raised:
                Probe.run(probe)
        self.assertEqual(raised.exception.args, (probe.peaks,))
        probe.operation.assert_any_call(0)
        probe.operation.assert_any_call(1)
        probe.operation.assert_any_call(2)
        probe.full_shutdown.assert_called_once()
        probe.monitor.join.assert_called_once()


if __name__ == '__main__':
    unittest.main()
