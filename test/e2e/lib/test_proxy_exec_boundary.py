"""Deterministic wait-event coverage; no ptrace, processes or host mutation."""
from contextlib import ExitStack
import errno
import io
import signal
import unittest
from unittest.mock import Mock, patch

from proxy_exec_boundary import ExecBoundary


MAIN = 100
SIBLING = 101


def stopped(sig=signal.SIGTRAP, event=0):
    return (event << 16) | (sig << 8) | 0x7f


class ExecBoundaryWaitTest(unittest.TestCase):
    def setUp(self):
        self.contexts = ExitStack()
        self.addCleanup(self.contexts.close)
        self.trace = ExecBoundary.__new__(ExecBoundary)
        self.trace.pid = MAIN
        self.trace.traced = {MAIN, SIBLING}
        self.trace.call = Mock()
        self.stderr = io.StringIO()
        self.contexts.enter_context(patch('sys.stderr', self.stderr))

    def wait_events(self, events, times=None):
        waiter = self.contexts.enter_context(patch('proxy_exec_boundary.os.waitpid', side_effect=events))
        clock = self.contexts.enter_context(patch('proxy_exec_boundary.time.monotonic',
                                       side_effect=times or [0] * (len(events) + 1)))
        self.contexts.enter_context(patch('proxy_exec_boundary.time.sleep'))
        self.trace.wait()
        return waiter, clock

    def test_sibling_esrch_requires_original_pid_exec(self):
        self.trace.call.side_effect = ProcessLookupError(errno.ESRCH, 'gone')
        waiter, _ = self.wait_events([(SIBLING, stopped()), (MAIN, stopped(event=4))])
        self.assertEqual(waiter.call_count, 2)
        self.trace.call.assert_called_once_with(7, SIBLING, 0)
        self.assertEqual(self.trace.traced, {MAIN})
        self.assertIn('main_pid=100 tid=101 status=0x57f signal=0 errno=3', self.stderr.getvalue())

    def test_sibling_esrch_retains_ownership_until_terminal_event(self):
        def events(*_):
            if events.index == 0:
                events.index += 1
                return SIBLING, stopped()
            self.assertIn(SIBLING, self.trace.traced)
            raise ChildProcessError(errno.ECHILD, 'no children')
        events.index = 0
        self.trace.call.side_effect = ProcessLookupError(errno.ESRCH, 'gone')
        with patch('proxy_exec_boundary.os.waitpid', side_effect=events), \
                patch('proxy_exec_boundary.time.monotonic', return_value=0):
            with self.assertRaises(ChildProcessError):
                self.trace.wait()

    def test_sibling_esrch_then_sibling_exit_then_exec(self):
        self.trace.call.side_effect = ProcessLookupError(errno.ESRCH, 'gone')
        self.wait_events([(SIBLING, stopped()), (SIBLING, 0), (MAIN, stopped(event=4))])
        self.assertEqual(self.trace.traced, {MAIN})

    def test_leader_exit_after_sibling_esrch_fails(self):
        for terminal in (0, 1 << 8, signal.SIGKILL):
            with self.subTest(status=terminal):
                self.trace.call.side_effect = ProcessLookupError(errno.ESRCH, 'gone')
                with self.assertRaisesRegex(AssertionError, 'main exited before exec'):
                    self.wait_events([(SIBLING, stopped()), (MAIN, terminal)])

    def test_leader_esrch_is_not_suppressed(self):
        self.trace.call.side_effect = ProcessLookupError(errno.ESRCH, 'gone')
        with self.assertRaises(ProcessLookupError):
            self.wait_events([(MAIN, stopped())])
        self.assertIn('main_pid=100 tid=100', self.stderr.getvalue())

    def test_non_esrch_is_not_suppressed(self):
        self.trace.call.side_effect = PermissionError(errno.EPERM, 'denied')
        with self.assertRaises(PermissionError):
            self.wait_events([(SIBLING, stopped())])

    def test_echild_after_esrch_fails(self):
        self.trace.call.side_effect = ProcessLookupError(errno.ESRCH, 'gone')
        with self.assertRaises(ChildProcessError):
            self.wait_events([(SIBLING, stopped()), ChildProcessError(errno.ECHILD, 'no children')])

    def test_repeated_esrch_does_not_extend_original_deadline(self):
        self.trace.call.side_effect = ProcessLookupError(errno.ESRCH, 'gone')
        with self.assertRaisesRegex(AssertionError, 'main did not reach exec'):
            self.wait_events([(SIBLING, stopped()), (102, stopped()), (103, stopped())],
                             times=[10, 11, 20, 44.999, 45])
        self.assertEqual(self.trace.call.call_count, 3)

    def test_wrong_pid_exec_still_fails(self):
        with self.assertRaises(AssertionError):
            self.wait_events([(SIBLING, stopped(event=4))])

    def test_signal_delivery_is_preserved(self):
        self.wait_events([(SIBLING, stopped(signal.SIGUSR2)), (MAIN, stopped(event=4))])
        self.trace.call.assert_called_once_with(7, SIBLING, signal.SIGUSR2)

    def test_trap_and_stop_are_not_delivered(self):
        self.wait_events([(SIBLING, stopped(signal.SIGSTOP)),
                          (SIBLING, stopped(signal.SIGTRAP)), (MAIN, stopped(event=4))])
        self.assertEqual([call.args for call in self.trace.call.call_args_list],
                         [(7, SIBLING, 0), (7, SIBLING, 0)])


if __name__ == '__main__':
    unittest.main()
