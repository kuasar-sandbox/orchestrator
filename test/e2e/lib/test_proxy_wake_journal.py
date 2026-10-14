"""Synthetic Go fatal fixtures; no guest, journal service or KVM required."""
from pathlib import Path
import subprocess
import sys
import unittest

import proxy_wake_journal as journal


FATAL = """SIGSEGV: segmentation violation
PC=0x123 m=0 sigcode=1
fatal error: unexpected signal during runtime execution
runtime stack:
runtime.throw({0xdead, 0xbeef})
	/private/toolchain/src/runtime/panic.go:1067 +0x48 fp=0xsecret sp=0xsecret pc=0xsecret
runtime.(*mheap).allocSpan(0xprivate, nested(private), {private})
	/private/toolchain/src/runtime/mheap.go:1342 +0x1a2 fp=0xsecret
runtime.goexit({private})
	/private/toolchain/src/runtime/asm_amd64.s:1700 +0x1
goroutine 1 gp=0xsecret m=0 mp=0xsecret [running]:
runtime.mallocgc(0xprivate, private, private)
	runtime/malloc.go:1045 +0x55
main.workload(private)
github.example/private/workload.Call(private)
created by main.workload in goroutine 2
	/private/application/workload.go:99 +0xaa
rax 0xsecret
RAX: 0xsecret
RIP: 0xsecret
Code: private
goroutine 2 gp=0xsecret [runnable]:
runtime.second(private)
	/private/toolchain/src/runtime/second.go:2 +0xbb
"""


class JournalProjectionTests(unittest.TestCase):
    def project(self, text):
        return journal.project(text.splitlines())

    def test_sigsegv_runtime_frames_are_projected(self):
        result = self.project(FATAL)
        self.assertEqual(result, [
            'SIGSEGV: [redacted]', 'fatal error: [redacted]', 'runtime stack:',
            'runtime.throw([redacted])', '\tpanic.go:1067 +0x48',
            'runtime.(*mheap).allocSpan([redacted])', '\tmheap.go:1342 +0x1a2',
            'runtime.goexit([redacted])', '\tasm_amd64.s:1700 +0x1',
            'goroutine 1 [running]:', 'runtime.mallocgc([redacted])',
            '\tmalloc.go:1045 +0x55',
        ])
        for private in ('private', 'secret', '/toolchain', 'main.', '0xdead', '0xbeef', 'second'):
            self.assertNotIn(private, '\n'.join(result))

    def test_fatal_with_legacy_goroutine_header(self):
        result = self.project('fatal error: private\ngoroutine 17 [chan receive, 2 minutes]:\n'
                              'runtime.gopark(private)\n\t/usr/local/go/src/runtime/proc.go:460 +0xce\n')
        self.assertEqual(result, ['fatal error: [redacted]', 'goroutine 17 [chan receive]:',
                                  'runtime.gopark([redacted])', '\tproc.go:460 +0xce'])

    def test_only_immediately_associated_runtime_source(self):
        result = self.project('fatal error: private\nruntime.throw(private)\n'
                              '\t/private/not_runtime/secret.go:99 +0x1\n'
                              '\t/private/runtime/orphan.go:9 +0x2\n'
                              'main.workload(private)\n\t/private/runtime/wrong.go:4 +0x3\n'
                              'runtime.(*mheap).allocSpan(private)\n'
                              '\t/private/runtime/mheap.go:1 +0x2\n')
        self.assertEqual(result, ['fatal error: [redacted]', 'runtime.throw([redacted])',
                                  'runtime.(*mheap).allocSpan([redacted])', '\tmheap.go:1 +0x2'])

    def test_unknown_state_and_unparseable_calls_do_not_leak(self):
        result = self.project('panic: private\ngoroutine 9 gp=private [private]:\n'
                              'runtime.bad/private(secret)\n'
                              'runtime.throw(private) trailing private\n')
        self.assertEqual(result, ['panic: [redacted]', 'goroutine 9 [redacted]:'])

    def test_fatal_reserved_before_event_tail_and_second_fatal(self):
        lines = FATAL.splitlines() + ['mem_report fixture=' + str(i) for i in range(200)]
        lines += ['fatal error: second', 'runtime.later(private)', '\t/runtime/later.go:1 +0x2']
        result = journal.project(lines)
        self.assertEqual(len(result), 120)
        self.assertIn('runtime.(*mheap).allocSpan([redacted])', result)
        self.assertEqual(result[-1], 'mem_report fixture=199')
        self.assertNotIn('runtime.later([redacted])', result)
        self.assertEqual(result.count('fatal error: [redacted]'), 1)

    def test_input_limit_does_not_consume_line_801(self):
        def lines():
            yield from ['mem_report fixture\n'] * 800
            raise AssertionError('read beyond input limit')
        self.assertEqual(len(journal.project(lines())), 120)
        self.assertNotIn('fatal error: [redacted]',
                         journal.project(['unrelated'] * 800 + FATAL.splitlines()))

    def test_large_first_stack_respects_output_bound_and_whole_pairs(self):
        result = self.project('fatal error: fixture\nruntime stack:\n' +
                              ('runtime.throw(private)\n\t/runtime/panic.go:1 +0x2\n' * 400))
        self.assertEqual(len(result), 120)
        self.assertEqual(result[-2:], ['runtime.throw([redacted])', '\tpanic.go:1 +0x2'])

    def test_existing_events_and_cli_integration(self):
        events = ['mem_report fixture', 'restore BudgetAtSnapshot reserved=1',
                  'runtime ready', 'runtime exit', 'received terminated',
                  'held-memory witness {}', 'CH exited code=1']
        result = subprocess.run([sys.executable, str(Path(journal.__file__))],
                                input='\n'.join(events + ['unrelated']), text=True,
                                capture_output=True, check=True)
        self.assertEqual(result.stdout.splitlines(), events)
        self.assertEqual(result.stderr, '')
        case = Path(__file__).resolve().parents[1] / 'cases/orchestrator.proxy-wake.sh'
        self.assertIn('tee "$WORK/pressure-$sid.journal" |\n'
                      '            python3 "$SCRIPT_DIR/proxy_wake_journal.py"', case.read_text())


if __name__ == '__main__':
    unittest.main()
