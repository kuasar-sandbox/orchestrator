#!/usr/bin/env python3
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import resource_shutdown as subject


BOOT = '0123456789abcdef0123456789abcdef'


def identity():
    return {'schema': 1, 'run_id': 'sr-current', 'sandbox_id': 'current-sandbox', 'boot_id': BOOT,
            'runtime': {'pid': 111}, 'ch': {'pid': 112}, 'control_group': '/owned.service'}


def rows(outcome='<nil>', code=0, after_reap=False):
    messages = ['[sandbox-ctl] CH started pid=112', '[sandbox-ctl] received terminated, requesting vmm.shutdown',
                '[sandbox-ctl] CH process reaped: ' + outcome, f'[sandbox-ctl] CH exited code={code}']
    if after_reap:
        messages[1], messages[2] = messages[2], messages[1]
    return [dict(subject.journal_fields(identity()), MESSAGE=message) for message in messages]


def encode(values):
    return b''.join(json.dumps(row).encode() + b'\n' for row in values)


class ProcessIdentity(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name); self.proc = self.root / 'proc'; self.cgroup = self.root / 'cgroup'
        self.group = '/sandbox-runner.slice/owned.service'
        (self.proc / 'sys/kernel/random').mkdir(parents=True)
        (self.proc / 'sys/kernel/random/boot_id').write_text(BOOT)
        self.binaries = {}
        for name in ('parent', 'sandbox-ctl', 'cloud-hypervisor'):
            path = self.root / name; path.write_text(name); self.binaries[name] = path
        self.write_process(110, 1, 'parent', '/ctl')
        self.write_process(111, 110, 'sandbox-ctl', '/ctl')
        self.write_process(112, 111, 'cloud-hypervisor', '/vmm')
        self.members = self.cgroup / self.group.lstrip('/') / 'vmm/cgroup.procs'
        self.members.parent.mkdir(parents=True); self.members.write_text('112\n')

    def write_process(self, pid, ppid, executable, suffix, start=100, state='S'):
        directory = self.proc / str(pid); directory.mkdir(exist_ok=True)
        fields = [state, str(ppid)] + ['0'] * 17 + [str(start)]
        (directory / 'stat').write_text(f'{pid} (name with ) parentheses) ' + ' '.join(fields))
        (directory / 'cgroup').write_text('0::' + self.group + suffix + '\n')
        link = directory / 'exe'
        if link.is_symlink():
            link.unlink()
        link.symlink_to(self.binaries[executable])

    def capture(self):
        return subject.capture_identity('sr-current', 'current-sandbox', self.group, 110,
            self.binaries['sandbox-ctl'], self.binaries['cloud-hypervisor'], proc=self.proc, cgroup_root=self.cgroup)

    def test_binds_prepared_binaries_parent_child_cgroups_and_boot(self):
        result = self.capture()
        self.assertEqual(result['boot_id'], BOOT)
        self.assertEqual(result['runtime']['ppid'], result['parent']['pid'])
        self.assertEqual(result['ch']['ppid'], result['runtime']['pid'])
        self.assertEqual(result['ch']['exe_inode'], self.binaries['cloud-hypervisor'].stat().st_ino)

    def test_rejects_wrong_executable_parent_cgroup_or_dead_process(self):
        for executable, ppid, suffix, state in [('parent', 110, '/ctl', 'S'),
                ('sandbox-ctl', 999, '/ctl', 'S'), ('sandbox-ctl', 110, '/foreign', 'S'),
                ('sandbox-ctl', 110, '/ctl', 'Z')]:
            with self.subTest(executable=executable, ppid=ppid, suffix=suffix, state=state):
                self.write_process(111, ppid, executable, suffix, state=state)
                with self.assertRaises(ValueError): self.capture()

    def test_rejects_multiple_prepared_ch_processes(self):
        self.write_process(113, 111, 'cloud-hypervisor', '/vmm')
        self.members.write_text('112\n113\n')
        with self.assertRaisesRegex(ValueError, 'one prepared CH'): self.capture()

    def write_worker(self, creator=112, executable=None):
        directory = self.proc / '113'; directory.mkdir(exist_ok=True)
        (directory / 'comm').write_text(f'kvm-nx-lpage-recovery-{creator}\n')
        thread = self.proc / '112/task/112'; thread.mkdir(parents=True, exist_ok=True)
        (thread / 'exe').symlink_to(self.binaries[executable or 'cloud-hypervisor'])
        self.members.write_text('0\n112\n113\n')

    def test_accepts_invisible_kernel_task_and_associated_nx_worker(self):
        self.write_worker()
        result = self.capture()
        self.assertEqual(result['vmm_members'], [0, 112, 113])
        self.assertEqual(result['nx_workers'], [(113, 112)])
        self.assertEqual(result['ch']['pid'], 112)

    def test_rejects_unexpected_userspace_or_unassociated_kernel_task(self):
        self.write_process(113, 111, 'parent', '/vmm')
        self.members.write_text('0\n112\n113\n')
        with self.assertRaisesRegex(ValueError, 'unexpected VMM userspace'): self.capture()
        (self.proc / '113/exe').unlink()
        self.write_worker(creator=999)
        with self.assertRaisesRegex(ValueError, 'not associated'): self.capture()
        (self.proc / '113/comm').write_text('unexpected-kernel-task\n')
        with self.assertRaisesRegex(ValueError, 'unrecognized'): self.capture()

    def test_detects_pid_start_tick_change_during_capture(self):
        original = subject.process_identity
        changed = False
        def read(proc, pid):
            nonlocal changed
            result = original(proc, pid)
            if pid == 112 and not changed:
                changed = True
                self.write_process(112, 111, 'cloud-hypervisor', '/vmm', start=101)
            return result
        with patch.object(subject, 'process_identity', side_effect=read), self.assertRaisesRegex(ValueError, 'identity changed'):
            self.capture()


class JournalProof(unittest.TestCase):
    def test_positive_normal_sigterm_and_ordinary_nonzero_outcomes(self):
        for outcome, code in [('<nil>', 0), ('signal: terminated', -1), ('exit status 1', 1), ('exit status 255', 255)]:
            for after_reap in (False, True):
                with self.subTest(outcome=outcome, after_reap=after_reap):
                    result = subject.inspect_journal(encode(rows(outcome, code, after_reap)), identity())
                    self.assertTrue(result['complete']); self.assertEqual(result['exit_code'], code)

    def test_rejects_wrong_sender_run_sandbox_boot_transport_and_tag(self):
        for field, value in [('_PID', '999'), ('KUASAR_RUN_ID', 'sr-stale'), ('KUASAR_SANDBOX_ID', 'stale'),
                             ('_BOOT_ID', 'f' * 32), ('_TRANSPORT', 'stdout'), ('SYSLOG_IDENTIFIER', 'guest')]:
            sample = rows(); sample[-1][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'identity'):
                subject.inspect_journal(encode(sample), identity())

    def test_rejects_wrong_ch_start_pid(self):
        sample = rows(); sample[0]['MESSAGE'] = '[sandbox-ctl] CH started pid=999'
        with self.assertRaisesRegex(ValueError, 'different CH PID'): subject.inspect_journal(encode(sample), identity())

    def test_missing_start_reap_terminal_or_stop_is_not_success(self):
        for missing in range(4):
            sample = rows(); del sample[missing]
            with self.subTest(missing=missing):
                self.assertFalse(subject.inspect_journal(encode(sample), identity())['complete'])

    def test_killed_unknown_or_invalid_wait_results_fail(self):
        for outcome in ('signal: killed', 'signal: aborted (core dumped)', 'context canceled',
                        'waitid: no child processes', 'exit status 0', 'exit status 256', 'exit status -1', ''):
            with self.subTest(outcome=outcome), self.assertRaises(ValueError):
                subject.inspect_journal(encode(rows(outcome, -1)), identity())

    def test_rejects_contradictory_duplicate_and_reordered_terminal_records(self):
        samples = [rows('<nil>', -1)]
        for at in (0, 2, 3):
            sample = rows(); sample.append(sample[at]); samples.append(sample)
        sample = rows(); sample[2], sample[3] = sample[3], sample[2]; samples.append(sample)
        sample = rows(); sample[0], sample[2] = sample[2], sample[0]; samples.append(sample)
        for sample in samples:
            with self.subTest(sample=sample), self.assertRaises(ValueError):
                subject.inspect_journal(encode(sample), identity())

    def test_all_current_and_legacy_escalations_fail_even_with_normal_terminal(self):
        for phrase in subject.ESCALATIONS:
            sample = rows(); sample.insert(2, dict(sample[0], MESSAGE='[sandbox-ctl] ' + phrase))
            with self.subTest(phrase=phrase), self.assertRaisesRegex(ValueError, 'SIGKILL'):
                subject.inspect_journal(encode(sample), identity())

    def test_malformed_or_excessive_journal_fails(self):
        for raw in (b'not-json\n', b'{}\n', encode([rows()[0]] * subject.JOURNAL_LIMIT)):
            with self.subTest(raw=raw[:30]), self.assertRaises(ValueError):
                subject.inspect_journal(raw, identity())


class BoundedObservation(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name); self.journal = self.root / 'runtime.journal'; self.proof = self.root / 'proof.log'
        self.clock = 0.0; self.calls = []

    def sleep(self, seconds): self.clock += seconds

    def observe(self, run, timeout=.1, **kwargs):
        return subject.observe(identity(), self.journal, self.proof, timeout, run=run,
                               monotonic=lambda: self.clock, sleep=self.sleep,
                               cgroup_root=self.root / 'cgroups', **kwargs)

    def producer(self, outputs, status=0):
        def run(command, **options):
            self.calls.append((command, options['timeout']))
            raw = outputs[min(len(self.calls) - 1, len(outputs) - 1)]
            return subprocess.CompletedProcess(command, status, raw, b'fixture stderr\n')
        return run

    def test_only_missing_records_are_retried_and_initial_evidence_is_retained(self):
        partial = encode(rows()[:2]); complete = encode(rows())
        result = self.observe(self.producer([partial, complete]))
        self.assertEqual(result['conclusion'], 'success'); self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.journal.read_bytes(), complete)
        self.assertEqual(Path(str(self.journal) + '.initial.journal').read_bytes(), partial)
        for field, value in subject.journal_fields(identity()).items():
            self.assertIn(field + '=' + value, self.calls[0][0])
        self.assertNotIn('-u', self.calls[0][0]); self.assertLess(self.calls[1][1], self.calls[0][1])

    def test_missing_proof_times_out_without_accepting_an_inactive_unit(self):
        with self.assertRaisesRegex(ValueError, 'did not become visible'):
            self.observe(self.producer([encode(rows()[:2])]))
        result = json.loads(self.proof.read_text())
        self.assertEqual(result['conclusion'], 'failure'); self.assertFalse(result['observation']['complete'])

    def test_wrong_visible_outcome_and_journal_failure_are_not_retried(self):
        for sample, status in [(encode(rows('signal: killed', -1)), 0), (encode(rows()), 1)]:
            self.calls = []; self.clock = 0
            with self.subTest(status=status), self.assertRaises(ValueError):
                self.observe(self.producer([sample], status))
            self.assertEqual(len(self.calls), 1)
            self.assertEqual(json.loads(self.proof.read_text())['conclusion'], 'failure')

    def test_journal_process_timeout_preserves_partial_output_and_fails(self):
        def run(command, **options):
            raise subprocess.TimeoutExpired(command, options['timeout'], output=b'partial native journal', stderr=b'fixture timeout')
        with self.assertRaises(subprocess.TimeoutExpired): self.observe(run)
        self.assertEqual(self.journal.read_bytes(), b'partial native journal')
        self.assertEqual(json.loads(self.proof.read_text())['error']['type'], 'TimeoutExpired')

    def test_ready_journal_waits_for_cgroup_within_same_deadline(self):
        vmm = self.root / 'cgroups/owned.service/vmm'; vmm.mkdir(parents=True)
        producer = self.producer([encode(rows())])
        def run(command, **options):
            if self.clock >= .05:
                vmm.rmdir()
            return producer(command, **options)
        self.assertEqual(self.observe(run)['conclusion'], 'success')
        self.assertEqual(self.clock, .05)
        vmm.mkdir(); self.calls = []; self.clock = 0
        with self.assertRaisesRegex(ValueError, 'vmm cgroup remained after StopUnit'):
            self.observe(self.producer([encode(rows())]))
        self.assertEqual(self.clock, .1)

    def test_absent_cgroup_does_not_extend_deadline_for_late_journal(self):
        self.clock = .08
        with self.assertRaisesRegex(ValueError, 'did not become visible'):
            self.observe(self.producer([encode(rows()[:2]), encode(rows())]), deadline=.1)
        self.assertEqual(self.clock, .1)
        self.assertEqual(len(self.calls), 1)
        self.assertAlmostEqual(self.calls[0][1], .02)
        self.clock = .01; self.calls = []
        self.assertEqual(self.observe(self.producer([encode(rows()[:2]), encode(rows())]),
                                     deadline=.1)['conclusion'], 'success')
        self.assertAlmostEqual(self.clock, .06)

    def test_complete_result_after_shared_deadline_is_rejected(self):
        producer = self.producer([encode(rows())])
        def run(command, **options):
            self.clock = .11
            return producer(command, **options)
        with self.assertRaisesRegex(ValueError, 'did not become visible'):
            self.observe(run)
        self.assertEqual(json.loads(self.proof.read_text())['conclusion'], 'failure')


if __name__ == '__main__':
    unittest.main()
