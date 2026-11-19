"""Exercise failure-only page evidence with a real DB and child processes."""
import copy
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import re
import select
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import lifecycle_diagnostics as diagnostic


def trace_record(event='fault', seq=None, version=1, **changes):
    record = dict(v=version, page=diagnostic.TRACE_PAGE,
                  seq=seq if seq is not None else (33 if event == 'summary' else 1), event=event)
    if version == 2:
        record.update(elapsed_ns=123456789, pid=1234, handler=1)
    if event == 'summary':
        record.update(recorded=32, dropped=0)
    else:
        record.update(start=diagnostic.TRACE_PAGE, length=4096, state='absent')
        if event == 'source':
            record['kind'] = 'data'
        elif event in ('copy', 'zero'):
            record.update(sha256=hashlib.sha256(bytes(4096)).hexdigest(),
                          non_ff=4096, done=4096, errno=0)
        elif event == 'remove' and version == 2:
            record['kernel_event'] = 'remove'
        elif event == 'register':
            record.update(start=0, length=1 << 30, uffd_fd=11, backend_va=0x7F0000000000,
                          memfd_fd=12, memfd_dev=1, memfd_ino=123, memfd_size=1 << 30,
                          kernel_release='7.0.0-1017-azure', ch_va=0x7E0000000000, ch_pid=5678,
                          peer_errno=0, memfd_errno=0, kernel_errno=0, maps_errno=0,
                          maps_va=0x7F0000000000, maps_size=1 << 30, maps_offset=0,
                          maps_dev=1, maps_ino=123, maps_match=True)
        elif event == 'observe':
            record.update(phase='before-ack', mincore_errno=0, resident=False,
                          seek_data_offset=diagnostic.TRACE_PAGE,
                          seek_hole_offset=diagnostic.TRACE_PAGE + 4096,
                          seek_data_errno=0, seek_hole_errno=0, extent_data=True)
    record.update(changes)
    return record


def guard_record(event='armed', **changes):
    attempt = event == 'attempt'
    record = dict(v=1, seq=1 if attempt else 3 if event == 'closed' else 2,
                  event=event, reason='close' if event == 'closed' else 'success',
                  pid=1234, page=diagnostic.TRACE_PAGE, target_valid=True,
                  backend_va=0x7F0000000000, target_va=0x7F0000000000 + diagnostic.TRACE_PAGE,
                  length=4096, host_page_size=4096, memfd_fd=12,
                  memfd_dev=0 if attempt else 1, memfd_ino=0 if attempt else 123,
                  memfd_size=1 << 30, guard_fd=-1 if attempt else 13,
                  errno=0, elapsed_ns=1 if attempt else 3 if event == 'closed' else 2)
    if event == 'failed':
        record.update(reason='create', errno=1, guard_fd=-1)
    record.update(changes)
    return record


def shmem_record(event='armed', **changes):
    record = dict(v=1, seq=1, event=event, pid=1234, page=diagnostic.TRACE_PAGE,
                  memfd_dev=1, memfd_ino=123)
    if event == 'armed':
        record.update(window_start=diagnostic.TRACE_PAGE - 8192,
                      window_end=diagnostic.TRACE_PAGE + 8192, page_size=4096, max_records=16)
    elif event == 'failed':
        record.update(stage='btf', errno=2)
    elif event == 'insert':
        record.update(seq=2, inserter_pid=5678, inserter_tid=5679, pid_scope='initial', comm='cloud-hypervisor',
                      index=diagnostic.TRACE_PAGE // 4096, nr_pages=1, ktime_ns=123456789,
                      result=0, stack_errno=0, frames=['shmem_add_to_page_cache+0x12',
                                                       'shmem_get_folio_gfp+0x34'])
    elif event == 'closed':
        record.update(seq=3, matched=1, emitted=1, dropped=0, pending=0, nested=0,
                      entry_missed=0, return_missed=0, errno=0)
    record.update(changes)
    return record


class LifecycleDiagnostics(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.db, self.log = self.root / 'state.db', self.root / 'conductor.log'
        self.journal_sequence = 0
        self.log.write_text('')
        repo = Path(__file__).resolve().parents[3]
        schema = re.search(r'const schema = `([^`]+)`',
                           (repo / 'internal/store/store.go').read_text()).group(1)
        with sqlite3.connect(self.db) as db:
            db.executescript(schema)
            required = {row[1]: (0 if row[2] == 'INTEGER' else 'unprojected')
                        for row in db.execute('PRAGMA table_info(sandboxes)')
                        if row[3] and row[4] is None}
            for sid in ('target-a', 'target-b'):
                record = dict(required, id=sid, state='dead', run_id='',
                              dead_run_id='current-' + sid,
                              sandbox_result_run_id='current-' + sid,
                              resume_source_kind='snapshot',
                              resume_source_ref='file://saved.snapshot@digest:' + 'a' * 64,
                              sandbox_result_json='{}')
                db.execute('INSERT INTO sandboxes (' + ','.join(record) + ') VALUES ('
                           + ','.join('?' for _ in record) + ')', tuple(record.values()))
        self.fault = dict(page_size=4096,
                          pagemap=dict(present=True, swapped=False, pfn=0x100001),
                          page=dict(offset_in_page=16, length=4080,
                                    sha256=hashlib.sha256(bytes(4080)).hexdigest()))
        self.payload = dict(source_identity='digest:' + 'a' * 64,
                            gpa=0x100001000, memory_offset=12288, start=16, length=4080,
                            sha256=hashlib.sha256(b'\xff' * 4080).hexdigest(),
                            non_ff=0, bad_points=[],
                            layers=[dict(identity='digest:' + 'a' * 64, kind='Data')],
                            private='DO_NOT_EMIT_HELPER_FIELDS')
        (self.root / 'payload.json').write_text(json.dumps(self.payload))
        journal = self.root / 'journalctl'
        journal.write_text('#!' + sys.executable + '\n' + r'''
import json, os, re, sys, time
from pathlib import Path
root = Path(os.environ['PAGE_TEST_ROOT'])
args = sys.argv[1:]
with (root / 'journal-calls.jsonl').open('a') as output:
    print(json.dumps(args), file=output)
if os.environ.get('PAGE_TEST_JOURNAL_MODE') == 'guard-timeout' and any(arg.startswith('--grep=uffd-backend-guard') for arg in args):
    (root / 'journal.pid').write_text(str(os.getpid()))
    time.sleep(30)
if os.environ.get('PAGE_TEST_JOURNAL_MODE') == 'shmem-timeout' and any(arg.startswith('--grep=uffd-shmem-allocation') for arg in args):
    (root / 'journal.pid').write_text(str(os.getpid()))
    time.sleep(30)
entries = [json.loads(line) for line in (root / 'journal.jsonl').read_text().splitlines()]
if not os.environ.get('PAGE_TEST_JOURNAL_UNFILTERED'):
    cursor = next((arg.split('=', 1)[1] for arg in args if arg.startswith('--after-cursor=')), None)
    if cursor:
        offset = next((index + 1 for index, entry in enumerate(entries) if entry.get('__CURSOR') == cursor), len(entries))
        entries = entries[offset:]
    for arg in args:
        if '=' in arg and not arg.startswith('-'):
            key, value = arg.split('=', 1)
            entries = [entry for entry in entries if entry.get(key) == value]
    pattern = next((arg.split('=', 1)[1] for arg in args if arg.startswith('--grep=')), None)
    if pattern is not None:
        entries = [entry for entry in entries if isinstance(entry.get('MESSAGE'), str) and re.search(pattern, entry['MESSAGE'])]
    count = args[args.index('-n') + 1]
    entries = entries[:int(count)] if count.startswith('+') else entries[-int(count):]
    if pattern is not None and not entries:
        raise SystemExit(1)
for entry in entries:
    print(json.dumps(entry))
''')
        helper = self.root / 'memorycompare'
        helper.write_text('#!' + sys.executable + '\n' + r'''
import json, os, sys, time
from pathlib import Path
root = Path(os.environ['PAGE_TEST_ROOT'])
with (root / 'calls.jsonl').open('a') as output:
    print(json.dumps(sys.argv[1:]), file=output)
(root / 'helper.pid').write_text(str(os.getpid()))
print('DO_NOT_EMIT_HELPER_STDERR', file=sys.stderr)
mode = os.environ.get('PAGE_TEST_MODE', '')
if mode == 'timeout': time.sleep(30)
if mode == 'exit': sys.exit(7)
if mode == 'invalid': print('DO_NOT_EMIT_INVALID_JSON'); sys.exit(0)
if mode == 'large': print('x' * 20000); sys.exit(0)
print((root / 'payload.json').read_text())
''')
        journal.chmod(0o755)
        helper.chmod(0o755)
        self.probe = dict(helper=str(helper), manifest_config=str(self.root / 'manifest.yaml'),
                          checkpoint_root=str(self.root / 'sandboxes'))
        self.environment = mock.patch.dict(os.environ, dict(
            PATH=str(self.root) + os.pathsep + os.environ['PATH'], PAGE_TEST_ROOT=str(self.root),
            PAGE_TEST_MODE='', PAGE_TEST_JOURNAL_MODE='', PAGE_TEST_JOURNAL_UNFILTERED=''))
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.write_journal([self.entry('target-a'), self.entry('target-b')])

    def entry(self, sid, run=None, fault=None):
        return dict(KUASAR_SANDBOX_ID=sid, KUASAR_RUN_ID=run or 'current-' + sid,
                    MESSAGE='RuntimeError: held-memory contents changed: '
                    + json.dumps(self.fault if fault is None else fault))

    def write_journal(self, entries):
        (self.root / 'journal.jsonl').write_text('\n'.join(map(json.dumps, entries)))

    def run_diagnostic(self, probe=True, stream=None):
        stream = io.StringIO() if stream is None else stream
        diagnostic.emit_failure_evidence(self.db, self.log, ['target-a', 'target-b'],
                                         stream=stream, memory_probe=self.probe if probe else None)
        return [json.loads(line) for line in stream.getvalue().splitlines()]

    def calls(self):
        path = self.root / 'calls.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_exact_current_run_reads_one_page_once_without_changing_db(self):
        self.write_journal([self.entry('target-a', 'obsolete'), self.entry('foreign'),
                            self.entry('target-a'), self.entry('target-b')])
        before = self.db.read_bytes()
        records = self.run_diagnostic()
        pages = [row for row in records if row['lifecycle_diagnostic'] == 'snapshot_page']
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]['sid'], 'target-a')
        self.assertEqual(pages[0]['run_id'], 'current-target-a')
        self.assertEqual(pages[0]['memory_offset'], 12288)
        self.assertEqual(pages[0]['source_identity'], 'digest:' + 'a' * 64)
        self.assertFalse(pages[0]['same_bytes_at_reported_pfn'])
        self.assertEqual(pages[0]['pfn_observation'], 'after_restore_failure')
        self.assertEqual(self.calls(), [[
            'page', '--source-ref', 'file://saved.snapshot@digest:' + 'a' * 64,
            '--source-dir', str(self.root / 'sandboxes/target-a/checkpoint'),
            '--manifest-config', str(self.root / 'manifest.yaml'),
            '--gpa', '4294971392', '--start', '16', '--length', '4080']])
        self.assertEqual(self.db.read_bytes(), before)
        self.assertNotIn('DO_NOT_EMIT', json.dumps(records))

    def test_old_run_foreign_sid_masked_pfn_and_absent_helper_never_invoke(self):
        for pfn in (None, 0, True, -1, 1 << 52):
            with self.subTest(pfn=pfn):
                fault = copy.deepcopy(self.fault)
                fault['pagemap']['pfn'] = pfn
                self.write_journal([self.entry('target-a', 'obsolete'), self.entry('foreign'),
                                    self.entry('target-a', fault=fault)])
                records = self.run_diagnostic()
                self.assertEqual(self.calls(), [])
                self.assertTrue(any(row.get('unavailable') == 'ValueError' for row in records))
        self.write_journal([self.entry('target-a')])
        self.run_diagnostic(probe=False)
        self.assertEqual(self.calls(), [])

    def test_bad_helper_output_and_exit_preserve_original_failure(self):
        cases = [('exit', None), ('invalid', None), ('large', None),
                 ('', dict(gpa=4096)), ('', dict(non_ff=True)),
                 ('', dict(memory_offset=17)), ('', dict(source_identity='digest:' + 'b' * 64)),
                 ('', dict(non_ff=1, bad_points=[dict(offset=15, actual=0)]))]
        for mode, changes in cases:
            with self.subTest(mode=mode, changes=changes):
                os.environ['PAGE_TEST_MODE'] = mode
                (self.root / 'calls.jsonl').unlink(missing_ok=True)
                value = dict(self.payload, **(changes or {}))
                (self.root / 'payload.json').write_text(json.dumps(value))
                stream = io.StringIO()
                with self.assertRaisesRegex(RuntimeError, '^original business failure$'):
                    try:
                        raise RuntimeError('original business failure')
                    finally:
                        self.run_diagnostic(stream=stream)
                records = [json.loads(line) for line in stream.getvalue().splitlines()]
                pages = [row for row in records if row['lifecycle_diagnostic'] == 'snapshot_page']
                self.assertEqual(len(self.calls()), 1)
                self.assertEqual(len(pages), 1)
                self.assertIn('unavailable', pages[0])
                self.assertNotIn('DO_NOT_EMIT', json.dumps(records))

    def test_real_helper_timeout_kills_child_and_does_not_retry(self):
        os.environ['PAGE_TEST_MODE'] = 'timeout'
        started = time.monotonic()
        records = self.run_diagnostic()
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(len(self.calls()), 1)
        self.assertTrue(any(row.get('unavailable') == 'TimeoutExpired' for row in records))
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.root / 'helper.pid').read_text()), 0)

    def test_cli_environment_wires_page_probe_without_emitting_configuration(self):
        env = dict(os.environ, KUASAR_E2E_MEMORY_COMPARE_BIN=self.probe['helper'],
                   KUASAR_E2E_MEMORY_CONFIG=self.probe['manifest_config'],
                   KUASAR_E2E_MEMORY_CHECKPOINT_ROOT=self.probe['checkpoint_root'],
                   MANIFEST_KEY='DO_NOT_EMIT_KEY')
        result = subprocess.run([sys.executable, diagnostic.__file__, str(self.db), str(self.log),
                                 'target-a'], env=env, capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.calls()), 1)
        records = [json.loads(line) for line in result.stderr.splitlines()]
        self.assertTrue(any(row['lifecycle_diagnostic'] == 'snapshot_page' for row in records))
        self.assertNotIn('DO_NOT_EMIT', result.stdout + result.stderr)

    def trace_entry(self, sid, event='fault', seq=None, run=None, version=1, **changes):
        trace = trace_record(event, seq, version, **changes)
        return dict(KUASAR_SANDBOX_ID=sid, KUASAR_RUN_ID=run or 'current-' + sid,
                    MESSAGE=diagnostic.TRACE_PREFIX + json.dumps(trace))

    def runtime_entry(self, message, sid='target-a', **changes):
        self.journal_sequence += 1
        entry = dict(MESSAGE=message, _PID='1234', _BOOT_ID='a' * 32,
                     _SYSTEMD_UNIT='runner@current-' + sid + '.service', _STREAM_ID='b' * 32,
                     _TRANSPORT='stdout', __CURSOR='s=' + 'a' * 32 + ';i=' + hex(self.journal_sequence)[2:])
        entry.update(changes)
        return entry

    def guard_entry(self, sid='target-a', event='armed', run=None, journal=None, **changes):
        entry = self.runtime_entry(diagnostic.GUARD_PREFIX + json.dumps(guard_record(event, **changes)), sid,
                                   KUASAR_SANDBOX_ID=sid, KUASAR_RUN_ID=run or 'current-' + sid,
                                   SYSLOG_IDENTIFIER='sandbox-ctl', _TRANSPORT='journal')
        entry.pop('_STREAM_ID')
        entry.update(journal or {})
        return entry

    def shmem_entry(self, sid='target-a', event='armed', run=None, journal=None, **changes):
        entry = self.runtime_entry(diagnostic.SHMEM_PREFIX + json.dumps(shmem_record(event, **changes)), sid,
                                   KUASAR_SANDBOX_ID=sid, KUASAR_RUN_ID=run or 'current-' + sid,
                                   SYSLOG_IDENTIFIER='sandbox-ctl', _TRANSPORT='journal')
        entry.pop('_STREAM_ID')
        entry.update(journal or {})
        return entry

    def guard_fault_entries(self, sid='target-a', **changes):
        messages = [
            'unexpected fault address 0x7f0000400008', 'fatal error: fault',
            '[signal SIGBUS: bus error code=0x2 addr=0x7f0000400008 pc=0x401234]',
            'goroutine 27 gp=0xc00010 m=2 mp=0xc00020 [running]:',
            'runtime.throw({DO_NOT_EMIT_ARGUMENTS})',
            '\t/private/DO_NOT_EMIT_PATH/runtime/panic.go:110 +0x1 fp=0xc001 sp=0xc002 pc=0x401235',
            'github.com/kuasar-sandbox/sandboxer/pkg/memory.(*Memory).Read({DO_NOT_EMIT_BYTES})',
            '\t/private/DO_NOT_EMIT_PATH/memory_linux.go:42 +0x2',
            'goroutine 28 [waiting]:', 'private.DO_NOT_EMIT_OTHER_GOROUTINE(0x1)',
            '\t/private/other.go:12 +0x3']
        return [self.runtime_entry(message, sid, **changes) for message in messages]

    def test_trace_identity_and_quota_preserve_fault_and_business_evidence(self):
        for version in (1, 2):
            with self.subTest(version=version):
                (self.root / 'calls.jsonl').unlink(missing_ok=True)
                entries = [self.trace_entry('foreign', version=version),
                           self.trace_entry('target-a', run='obsolete', version=version),
                           self.trace_entry('target-a', run='current-target-b', version=version),
                           self.entry('target-a')]
                events = ('seed', 'fault', 'source', 'copy', 'zero', 'remove', 'load-hit')
                if version == 2:
                    events += ('register', 'observe')
                for sid in ('target-a', 'target-b'):
                    entries.extend(self.trace_entry(sid, events[(seq - 1) % len(events)], seq,
                                                    version=version) for seq in range(1, 33))
                    entries.append(self.trace_entry(sid, 'summary', dropped=9, seq=42, version=version))
                entries.extend(dict(KUASAR_SANDBOX_ID='target-a', KUASAR_RUN_ID='current-target-a',
                                    MESSAGE=f'exit diagnostic {index}') for index in range(11))
                self.write_journal(entries)
                self.log.write_text('\n'.join(json.dumps(dict(sid='target-a', msg='pause capture completed',
                                                              generation=index)) for index in range(80)))
                records = self.run_diagnostic()
                traces = [row for row in records if row['lifecycle_diagnostic'] == 'uffd_page_trace']
                business = [row for row in records if row['lifecycle_diagnostic'] != 'uffd_page_trace']
                self.assertEqual(len(traces), 33)
                self.assertEqual([row['seq'] for row in traces], [*range(1, 33), 42])
                self.assertTrue(all(row['sid'] == 'target-a' and row['run_id'] == 'current-target-a'
                                    and row['v'] == version for row in traces))
                self.assertEqual(len(business), diagnostic.MAX_RECORDS)
                self.assertEqual(len(self.calls()), 1)
                self.assertTrue(any('held-memory contents changed' in row.get('MESSAGE', '')
                                    for row in business))
                self.assertEqual(sum(row['lifecycle_diagnostic'] == 'snapshot_page' for row in business), 1)

    def test_trace_rejects_malformed_fields_without_raw_message_fallback(self):
        malformed = [dict(v=True), dict(v=2), dict(page=0), dict(seq=0), dict(seq=33),
                     dict(event='error_DO_NOT_EMIT'), dict(start=diagnostic.TRACE_PAGE + 1),
                     dict(length=4095), dict(start=0, length=1 << 64), dict(state='DO_NOT_EMIT'),
                     dict(error='DO_NOT_EMIT_INJECTED_FIELD'), dict(token='DO_NOT_EMIT_TOKEN')]
        entries = [self.trace_entry('target-a', **changes) for changes in malformed]
        entries += [self.trace_entry('target-a', 'source', kind='DO_NOT_EMIT'),
                    self.trace_entry('target-a', 'copy', non_ff=True),
                    self.trace_entry('target-a', 'copy', non_ff=4097),
                    self.trace_entry('target-a', 'copy', done=1 << 63),
                    self.trace_entry('target-a', 'copy', errno=-2),
                    self.trace_entry('target-a', 'copy', sha256='x' * 3000),
                    self.trace_entry('target-a', 'summary', recorded=33),
                    self.trace_entry('target-a', 'summary', seq=100)]
        entries.append(dict(KUASAR_SANDBOX_ID='target-a', KUASAR_RUN_ID='current-target-a',
                            MESSAGE='uffd-page-trace error DO_NOT_EMIT_INVALID_JSON'))
        entries += [self.trace_entry('target-a', version=2, token='DO_NOT_EMIT_V2_TOKEN'),
                    self.trace_entry('target-a', 'register', version=2,
                                     kernel_release='DO_NOT_EMIT_UNAVAILABLE_KERNEL', kernel_errno=1),
                    self.trace_entry('target-a', 'observe', version=2, mincore_errno=1,
                                     error='DO_NOT_EMIT_V2_ERROR')]
        # A new current run wins over the retained dead/result identities.
        with sqlite3.connect(self.db) as db:
            db.execute("UPDATE sandboxes SET run_id='active-target-b' WHERE id='target-b'")
        entries += [self.trace_entry('target-b'),
                    self.trace_entry('target-b', 'copy', run='active-target-b', done=-14, errno=14),
                    self.trace_entry('target-b', 'zero', seq=2, run='active-target-b', done=-1, errno=-1)]
        self.write_journal(entries + [self.entry('target-a')])
        records = self.run_diagnostic()
        traces = [row for row in records if row['lifecycle_diagnostic'] == 'uffd_page_trace']
        self.assertEqual([(row['sid'], row['run_id'], row['done'], row['errno']) for row in traces],
                         [('target-b', 'active-target-b', -14, 14),
                          ('target-b', 'active-target-b', -1, -1)])
        self.assertNotIn('DO_NOT_EMIT', json.dumps(records))
        self.assertEqual(len(self.calls()), 1)

    def test_guard_recovers_first_fault_beyond_legacy_tail_without_arguments(self):
        entries = [self.guard_entry(event='attempt'), self.guard_entry()]
        entries += self.guard_fault_entries()
        # More than both the 200-entry and 2 MiB legacy tail; the guard and
        # first fault stack must still be found by bounded independent queries.
        entries += [self.runtime_entry('goroutine ' + str(index) + ' [waiting]:') for index in range(500)]
        entries += [dict(KUASAR_SANDBOX_ID='target-a', KUASAR_RUN_ID='current-target-a',
                         MESSAGE='heartbeat ' + 'x' * 11000) for _ in range(201)]
        self.write_journal(entries)
        before = self.db.read_bytes()
        records = self.run_diagnostic(probe=False)
        guards = [row for row in records if row['lifecycle_diagnostic'].startswith('uffd_backend_')]
        self.assertEqual(len(guards), 3)
        fault = guards[-1]
        self.assertEqual((fault['status'], fault['reason']), ('sigbus', 'captured'))
        self.assertEqual((fault['fault_address'], fault['pc'], fault['signal_code']),
                         (0x7F0000400008, 0x401234, 2))
        self.assertTrue(fault['target_match'])
        self.assertEqual(fault['frames'], [
            dict(function='runtime.throw', file='panic.go', line=110),
            dict(function='github.com/kuasar-sandbox/sandboxer/pkg/memory.(*Memory).Read',
                 file='memory_linux.go', line=42)])
        self.assertNotIn('DO_NOT_EMIT', json.dumps(records))
        self.assertEqual(before, self.db.read_bytes())
        queries = [json.loads(line) for line in (self.root / 'journal-calls.jsonl').read_text().splitlines()]
        stack_query = next(query for query in queries if '_STREAM_ID=' + 'b' * 32 in query)
        self.assertEqual(stack_query[stack_query.index('-n') + 1], '+80')
        self.assertIn('_PID=1234', stack_query)
        self.assertIn('_BOOT_ID=' + 'a' * 32, stack_query)

    def test_guard_quotas_do_not_evict_business_trace_summary_or_workload_fault(self):
        entries = [self.entry('target-a'), self.guard_entry(event='attempt'), self.guard_entry()]
        entries += self.guard_fault_entries()
        entries += [self.trace_entry('target-a', seq=seq, version=2) for seq in range(1, 33)]
        entries.append(self.trace_entry('target-a', 'summary', version=2))
        entries += [self.guard_entry('target-b', event=event) for event in ('attempt', 'armed', 'closed')]
        self.write_journal(entries)
        self.log.write_text('\n'.join(json.dumps(dict(sid='target-a', msg='pause capture completed',
                                                      generation=index)) for index in range(80)))
        records = self.run_diagnostic()
        guards = [row for row in records if row['lifecycle_diagnostic'].startswith('uffd_backend_')]
        traces = [row for row in records if row['lifecycle_diagnostic'] == 'uffd_page_trace']
        business = [row for row in records if row not in guards and row not in traces]
        self.assertLessEqual(len(guards), diagnostic.MAX_GUARD_RECORDS)
        self.assertEqual(len(guards), 3)
        self.assertEqual(len(traces), 33)
        self.assertEqual(len(business), diagnostic.MAX_RECORDS)
        self.assertEqual(sum(row['lifecycle_diagnostic'] == 'snapshot_page' for row in business), 1)
        self.assertTrue(any('held-memory contents changed' in row.get('MESSAGE', '') for row in business))
        self.assertEqual(len(self.calls()), 1)

    def test_guard_prefers_later_fault_over_healthy_closed_first_sid(self):
        entries = [self.guard_entry(event=event) for event in ('attempt', 'armed', 'closed')]
        entries += [self.guard_entry('target-b', event=event) for event in ('attempt', 'armed')]
        entries += self.guard_fault_entries('target-b')
        self.write_journal(entries)
        guards = [row for row in self.run_diagnostic(probe=False)
                  if row['lifecycle_diagnostic'].startswith('uffd_backend_')]
        self.assertEqual(len(guards), 3)
        self.assertTrue(all(row['sid'] == 'target-b' for row in guards))
        self.assertEqual(guards[-1]['status'], 'sigbus')

    def test_guard_rejects_old_run_foreign_sid_pid_and_malformed_without_raw_fallback(self):
        os.environ['PAGE_TEST_JOURNAL_UNFILTERED'] = '1'
        cases = [self.guard_entry(run='obsolete'), self.guard_entry('foreign'),
                 self.guard_entry(journal=dict(_PID='4321')),
                 self.guard_entry(journal=dict(SYSLOG_IDENTIFIER='console')),
                 self.guard_entry(journal=dict(_TRANSPORT='stdout')),
                 self.guard_entry(reason='fatal DO_NOT_EMIT_REASON'),
                 self.guard_entry(error='failed DO_NOT_EMIT_ERROR'),
                 self.guard_entry(pid=True)]
        for entry in cases:
            with self.subTest(message=entry['MESSAGE'][:80]):
                self.write_journal([entry])
                records = self.run_diagnostic(probe=False)
                self.assertFalse(any(row['lifecycle_diagnostic'].startswith('uffd_backend_') for row in records))
                self.assertNotIn('DO_NOT_EMIT', json.dumps(records))

    def test_guard_fault_requires_exact_boot_unit_pid_stream_and_optional_sid_run(self):
        # The child fixture applies the same journal field matches as the real
        # subprocess. A shared stderr stream alone never identifies a backend.
        for changes in (dict(_PID='4321'), dict(_BOOT_ID='c' * 32),
                        dict(_SYSTEMD_UNIT='runner@another.service'),
                        dict(KUASAR_SANDBOX_ID='foreign'), dict(KUASAR_RUN_ID='obsolete'),
                        dict(_TRANSPORT='journal'), dict(_STREAM_ID='invalid')):
            with self.subTest(changes=changes):
                self.write_journal([self.guard_entry(event='attempt'), self.guard_entry()]
                                   + self.guard_fault_entries(**changes))
                faults = [row for row in self.run_diagnostic(probe=False)
                          if row['lifecycle_diagnostic'] == 'uffd_backend_fault']
                self.assertEqual(len(faults), 1)
                self.assertEqual(faults[0]['status'], 'unavailable')
                self.assertEqual(faults[0]['frames'], [])

    def test_guard_stack_is_capped_and_does_not_cross_stream(self):
        entries = [self.guard_entry(event='attempt'), self.guard_entry()]
        entries += self.guard_fault_entries()[:4]
        for index in range(30):
            entries += [self.runtime_entry('private.frame' + str(index) + '(DO_NOT_EMIT_ARGUMENT)'),
                        self.runtime_entry('\t/private/frame.go:' + str(index + 1) + ' +0x1')]
        self.write_journal(entries)
        fault = next(row for row in self.run_diagnostic(probe=False)
                     if row['lifecycle_diagnostic'] == 'uffd_backend_fault')
        self.assertEqual(len(fault['frames']), diagnostic.MAX_FAULT_FRAMES)
        self.assertEqual(fault['frames'][-1]['function'], 'private.frame11')
        self.assertNotIn('DO_NOT_EMIT', json.dumps(fault))

        entries = [self.guard_entry(event='attempt'), self.guard_entry()]
        entries += self.guard_fault_entries()[:3]
        entries += self.guard_fault_entries(_STREAM_ID='c' * 32)[3:]
        self.write_journal(entries)
        fault = next(row for row in self.run_diagnostic(probe=False)
                     if row['lifecycle_diagnostic'] == 'uffd_backend_fault')
        self.assertEqual((fault['status'], fault['reason'], fault['frames']),
                         ('sigbus', 'stack_missing', []))

    def test_guard_query_timeout_reaps_child_and_preserves_business_budget(self):
        os.environ['PAGE_TEST_JOURNAL_MODE'] = 'guard-timeout'
        self.log.write_text(json.dumps(dict(sid='target-a', msg='pause capture completed')))
        stream = io.StringIO()
        started = time.monotonic()
        diagnostic.emit_failure_evidence(self.db, self.log, ['target-a'], stream=stream)
        self.assertLess(time.monotonic() - started, 5)
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.root / 'journal.pid').read_text()), 0)
        records = [json.loads(line) for line in stream.getvalue().splitlines()]
        self.assertTrue(any(row['lifecycle_diagnostic'] == 'conductor' for row in records))
        self.assertTrue(any(row['lifecycle_diagnostic'] == 'state' for row in records))


    def test_shmem_dedicated_query_recovers_all_records_beyond_business_tail(self):
        entries = [self.shmem_entry()]
        entries += [self.shmem_entry(event='insert', seq=seq, ktime_ns=seq) for seq in range(2, 18)]
        entries += [self.shmem_entry(event='closed', seq=18, matched=21, emitted=16, dropped=5,
                                     pending=2, nested=3, entry_missed=-1, return_missed=4)]
        entries += [dict(KUASAR_SANDBOX_ID='target-a', KUASAR_RUN_ID='current-target-a',
                         MESSAGE='heartbeat ' + 'x' * 11000) for _ in range(201)]
        self.write_journal(entries)
        before = self.db.read_bytes()
        records = self.run_diagnostic(probe=False)
        allocations = [row for row in records if row['lifecycle_diagnostic'] == 'uffd_shmem_allocation']
        self.assertEqual([row['seq'] for row in allocations], list(range(1, 19)))
        self.assertEqual(allocations[-1]['dropped'], 5)
        self.assertEqual(allocations[-1]['pending'], 2)
        self.assertEqual(allocations[-1]['entry_missed'], -1)
        self.assertTrue(all(len(json.dumps(row, separators=(',', ':')).encode()) < 4096 for row in allocations))
        self.assertEqual(self.db.read_bytes(), before)
        queries = [json.loads(line) for line in (self.root / 'journal-calls.jsonl').read_text().splitlines()]
        query = next(query for query in queries if '--grep=' + diagnostic.SHMEM_PREFIX in query)
        self.assertIn('KUASAR_SANDBOX_ID=target-a', query)
        self.assertIn('KUASAR_RUN_ID=current-target-a', query)
        self.assertIn('SYSLOG_IDENTIFIER=sandbox-ctl', query)
        self.assertEqual(query[query.index('-n') + 1], '+18')
        self.assertIn('_PID', next(arg for arg in query if arg.startswith('--output-fields=')))

    def test_shmem_quota_is_independent_and_failure_run_beats_live_secondary(self):
        entries = [self.entry('target-a'), self.shmem_entry()]
        entries += [self.shmem_entry(event='insert', seq=seq) for seq in range(2, 18)]
        entries += [self.shmem_entry(event='closed', seq=18, matched=16, emitted=16)]
        entries += [self.guard_entry(event=event) for event in ('attempt', 'armed', 'closed')]
        entries += [self.trace_entry('target-a', seq=seq, version=2) for seq in range(1, 33)]
        entries += [self.trace_entry('target-a', event='summary', version=2)]
        entries += [self.shmem_entry('target-b'), self.shmem_entry('target-b', event='insert')]
        entries += [self.guard_entry('target-b', event=event) for event in ('attempt', 'armed')]
        # A missing guard fault in an active SECOND used to outrank the closed
        # backend from FIRST, even though FIRST reported the corrupt held page.
        self.write_journal(entries)
        self.log.write_text('\n'.join(json.dumps(dict(sid='target-a', msg='pause capture completed',
                                                      generation=index)) for index in range(80)))
        records = self.run_diagnostic()
        allocations = [row for row in records if row['lifecycle_diagnostic'] == 'uffd_shmem_allocation']
        guards = [row for row in records if row['lifecycle_diagnostic'].startswith('uffd_backend_')]
        traces = [row for row in records if row['lifecycle_diagnostic'] == 'uffd_page_trace']
        business = [row for row in records if row not in allocations and row not in guards and row not in traces]
        self.assertEqual(len(allocations), 18)
        self.assertEqual(len(guards), 3)
        self.assertTrue(all(row['sid'] == 'target-a' for row in allocations + guards))
        self.assertEqual(guards[-1]['event'], 'closed')
        self.assertEqual(len(traces), 33)
        self.assertEqual(len(business), diagnostic.MAX_RECORDS)
        self.assertTrue(any('held-memory contents changed' in row.get('MESSAGE', '') for row in business))
        self.assertEqual(sum(row['lifecycle_diagnostic'] == 'snapshot_page' for row in business), 1)

    def test_shmem_selects_known_failed_run_without_borrowing_new_run_snapshot(self):
        with sqlite3.connect(self.db) as db:
            db.execute("UPDATE sandboxes SET state='running', run_id='active-target-a' WHERE id='target-a'")
        entries = [self.entry('target-a'), self.shmem_entry(), self.shmem_entry(event='insert'),
                   self.shmem_entry(event='closed'), self.shmem_entry(run='active-target-a')]
        entries += [self.guard_entry(event=event) for event in ('attempt', 'armed', 'closed')]
        self.write_journal(entries)
        records = self.run_diagnostic()
        diagnostics = [row for row in records if row['lifecycle_diagnostic'] == 'uffd_shmem_allocation'
                       or row['lifecycle_diagnostic'].startswith('uffd_backend_')]
        self.assertEqual(len(diagnostics), 6)
        self.assertTrue(all(row['run_id'] == 'current-target-a' for row in diagnostics))
        self.assertEqual(self.calls(), [])

    def test_shmem_rejects_untrusted_sources_and_raw_fallback(self):
        os.environ['PAGE_TEST_JOURNAL_UNFILTERED'] = '1'
        cases = [self.shmem_entry('foreign'), self.shmem_entry(run='obsolete'),
                 self.shmem_entry(run='current-target-b'), self.shmem_entry(journal=dict(_PID='4321')),
                 self.shmem_entry(journal=dict(_PID=1234)),
                 self.shmem_entry(journal=dict(SYSLOG_IDENTIFIER='console')),
                 self.shmem_entry(journal=dict(_TRANSPORT='stdout')),
                 self.shmem_entry(journal=dict(_BOOT_ID='invalid')),
                 self.shmem_entry(journal=dict(_SYSTEMD_UNIT='unit\nDO_NOT_EMIT')),
                 self.shmem_entry(journal=dict(__CURSOR='invalid cursor')),
                 self.shmem_entry(page=diagnostic.TRACE_PAGE + 4096),
                 self.shmem_entry(error='failed DO_NOT_EMIT_ERROR')]
        for entry in cases:
            with self.subTest(entry=entry):
                self.write_journal([entry])
                records = self.run_diagnostic(probe=False)
                self.assertFalse(any(row['lifecycle_diagnostic'] == 'uffd_shmem_allocation' for row in records))
                self.assertNotIn('DO_NOT_EMIT', json.dumps(records))

    def test_shmem_requires_same_producer_identity_and_window(self):
        os.environ['PAGE_TEST_JOURNAL_UNFILTERED'] = '1'
        invalid = [dict(pid=4321), dict(memfd_dev=2), dict(memfd_ino=456),
                   dict(index=diagnostic.TRACE_PAGE // 4096 + 2),
                   dict(index=diagnostic.TRACE_PAGE // 4096 - 3),
                   dict(frames=['0xffff800000001234']), dict(frames=['failed DO_NOT_EMIT']),
                   dict(frames=['kernel_fn(']), dict(frames=['kernel_fn'] * 33)]
        for changes in invalid:
            with self.subTest(changes=changes):
                self.write_journal([self.shmem_entry(), self.shmem_entry(event='insert', **changes)])
                allocations = [row for row in self.run_diagnostic(probe=False)
                               if row['lifecycle_diagnostic'] == 'uffd_shmem_allocation']
                self.assertEqual([row['event'] for row in allocations], ['armed'])
        for journal in (dict(_BOOT_ID='c' * 32), dict(_SYSTEMD_UNIT='runner@other.service')):
            with self.subTest(journal=journal):
                self.write_journal([self.shmem_entry(), self.shmem_entry(event='insert', journal=journal)])
                allocations = [row for row in self.run_diagnostic(probe=False)
                               if row['lifecycle_diagnostic'] == 'uffd_shmem_allocation']
                self.assertEqual([row['event'] for row in allocations], ['armed'])
        self.write_journal([self.shmem_entry(), self.shmem_entry(event='insert', seq=2),
                            self.shmem_entry(event='insert', seq=2),
                            self.shmem_entry(event='insert', seq=3, index=1021, nr_pages=2,
                                             inserter_pid=0, inserter_tid=0, comm='swapper/0'),
                            self.shmem_entry(event='closed', seq=4, emitted=2, matched=2),
                            self.shmem_entry(event='insert', seq=4)])
        allocations = [row for row in self.run_diagnostic(probe=False)
                       if row['lifecycle_diagnostic'] == 'uffd_shmem_allocation']
        self.assertEqual([row['seq'] for row in allocations], [1, 2, 3, 4])

    def test_shmem_explicit_failure_is_preserved_without_claiming_no_allocation(self):
        self.write_journal([self.entry('target-a'), self.shmem_entry(event='failed', stage='btf', errno=2),
                            self.shmem_entry('target-b'), self.shmem_entry('target-b', event='insert')])
        allocations = [row for row in self.run_diagnostic(probe=False)
                       if row['lifecycle_diagnostic'] == 'uffd_shmem_allocation']
        self.assertEqual(len(allocations), 1)
        self.assertEqual((allocations[0]['sid'], allocations[0]['event'], allocations[0]['stage']),
                         ('target-a', 'failed', 'btf'))
        self.assertNotIn('complete', allocations[0])

    def test_shmem_capture_preserves_frame_boundary_and_rejects_oversized_duplicate_json(self):
        valid = self.shmem_entry(event='insert', frames=['f' * 96] * 32)
        self.assertLess(len(valid['MESSAGE'].encode()), diagnostic.MAX_SHMEM_MESSAGE)
        duplicate = dict(valid, MESSAGE=valid['MESSAGE'][:-1] + ', "pid": 1234}')
        oversized = dict(valid, MESSAGE='x' * (4096 - len(valid['MESSAGE'])) + valid['MESSAGE'])
        malformed = dict(valid, MESSAGE=diagnostic.SHMEM_PREFIX + 'failed DO_NOT_EMIT_INVALID_JSON')
        self.write_journal([self.shmem_entry(), duplicate, oversized, malformed,
                            self.shmem_entry(event='insert', frames=['f' * 96] * 33), valid,
                            self.shmem_entry(event='closed')])
        records = self.run_diagnostic(probe=False)
        allocations = [row for row in records if row['lifecycle_diagnostic'] == 'uffd_shmem_allocation']
        self.assertEqual([row['event'] for row in allocations], ['armed', 'insert', 'closed'])
        self.assertEqual(allocations[1]['frames'], ['f' * 96] * 32)
        self.assertNotIn('DO_NOT_EMIT', json.dumps(records))

    def test_missing_failure_run_diagnostics_do_not_select_an_unrelated_live_run(self):
        self.write_journal([self.entry('target-a'), self.shmem_entry('target-b'),
                            self.shmem_entry('target-b', event='insert'), self.guard_entry('target-b')])
        records = self.run_diagnostic(probe=False)
        self.assertFalse(any(row['lifecycle_diagnostic'] == 'uffd_shmem_allocation'
                             or row['lifecycle_diagnostic'].startswith('uffd_backend_') for row in records))

    def test_shmem_query_timeout_reaps_child_and_preserves_business_evidence(self):
        os.environ['PAGE_TEST_JOURNAL_MODE'] = 'shmem-timeout'
        self.log.write_text(json.dumps(dict(sid='target-a', msg='pause capture completed')))
        stream = io.StringIO()
        started = time.monotonic()
        diagnostic.emit_failure_evidence(self.db, self.log, ['target-a'], stream=stream)
        self.assertLess(time.monotonic() - started, 5)
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.root / 'journal.pid').read_text()), 0)
        records = [json.loads(line) for line in stream.getvalue().splitlines()]
        self.assertTrue(any(row['lifecycle_diagnostic'] == 'state' for row in records))
        self.assertTrue(any(row['lifecycle_diagnostic'] == 'conductor' for row in records))
        self.assertTrue(any('held-memory contents changed' in row.get('MESSAGE', '') for row in records))


class BackendGuardSchema(unittest.TestCase):
    def parse(self, record):
        return diagnostic.memory_backend_guard(diagnostic.GUARD_PREFIX + json.dumps(record))

    def test_events_have_exact_fixed_fields_types_and_bounds(self):
        for event in ('attempt', 'armed', 'failed', 'closed'):
            record = guard_record(event)
            self.assertEqual(self.parse(record), record)
            for key in record:
                with self.subTest(event=event, missing=key):
                    reduced = dict(record)
                    del reduced[key]
                    self.assertIsNone(self.parse(reduced))
                if type(record[key]) is int:
                    for invalid in (True, False, '1', None, 1.5, -(1 << 65), 1 << 65):
                        with self.subTest(event=event, key=key, invalid=invalid):
                            self.assertIsNone(self.parse(dict(record, **{key: invalid})))
            self.assertIsNone(self.parse(dict(record, token='DO_NOT_EMIT')))
        self.assertIsNotNone(self.parse(guard_record('failed', reason='target', errno=22,
                                                    page=0, target_va=0, target_valid=False)))
        self.assertIsNotNone(self.parse(guard_record('failed', errno=-1)))
        for changes in (dict(v=2), dict(seq=1), dict(page=1), dict(target_valid=1),
                        dict(target_va=1), dict(backend_va=0), dict(length=8192),
                        dict(guard_fd=-1), dict(host_page_size=65536), dict(errno=1),
                        dict(event='fault'), dict(reason='DO_NOT_EMIT')):
            self.assertIsNone(self.parse(guard_record(**changes)))

    def test_duplicate_json_encoded_size_and_malformed_values_are_rejected(self):
        valid = diagnostic.GUARD_PREFIX + json.dumps(guard_record())
        self.assertIsNone(diagnostic.memory_backend_guard(valid[:-1] + ', "pid": 1234}'))
        for message in (None, 1, diagnostic.GUARD_PREFIX + 'null', diagnostic.GUARD_PREFIX + '[]',
                        diagnostic.GUARD_PREFIX + '{', diagnostic.GUARD_PREFIX + '[' * 1000,
                        'x' * 2048 + valid, '\u00e9' * 900 + valid):
            self.assertIsNone(diagnostic.memory_backend_guard(message))


class MemoryPageTraceSchema(unittest.TestCase):
    events = ('seed', 'fault', 'source', 'copy', 'zero', 'remove', 'load-hit', 'summary')

    def parse(self, record):
        return diagnostic.memory_page_trace(diagnostic.TRACE_PREFIX + json.dumps(record))

    def test_all_versioned_events_and_missing_or_unknown_fields(self):
        for version in (1, 2):
            events = self.events + (('register', 'observe') if version == 2 else ())
            for event in events:
                with self.subTest(version=version, event=event):
                    record = trace_record(event, version=version)
                    self.assertEqual(self.parse(record), record)
                    for key in record:
                        with self.subTest(missing=key):
                            incomplete = dict(record)
                            del incomplete[key]
                            self.assertIsNone(self.parse(incomplete))
                    for key in ('token', 'path', 'error', 'authorization', 'source_ref'):
                        self.assertIsNone(self.parse(dict(record, **{key: 'DO_NOT_EMIT'})))
        for event in ('register', 'observe'):
            self.assertIsNone(self.parse(trace_record(event, version=1)))
        self.assertEqual(self.parse(trace_record('remove', version=2, kernel_event='unmap'))['kernel_event'],
                         'unmap')

    def test_v2_integer_fields_reject_nonintegers_and_out_of_range_values(self):
        maximum, signed = (1 << 64) - 1, (1 << 63) - 1
        common = {'elapsed_ns': (0, maximum), 'pid': (1, (1 << 31) - 1), 'handler': (1, maximum)}
        fields = {
            'fault': dict(common, v=(1, 2), page=(diagnostic.TRACE_PAGE, diagnostic.TRACE_PAGE),
                          seq=(1, 32), start=(0, diagnostic.TRACE_PAGE), length=(1, maximum)),
            'register': dict(common, uffd_fd=(1, (1 << 31) - 1), memfd_fd=(1, (1 << 31) - 1),
                             ch_pid=(0, (1 << 31) - 1), **{key: (0, maximum) for key in (
                                 'backend_va', 'memfd_dev', 'memfd_ino', 'memfd_size', 'ch_va',
                                 'maps_va', 'maps_size', 'maps_offset', 'maps_dev', 'maps_ino')},
                             **{key: (-1, 4095) for key in (
                                 'peer_errno', 'memfd_errno', 'kernel_errno', 'maps_errno')}),
            'observe': dict(common, **{key: (-1, signed) for key in (
                'seek_data_offset', 'seek_hole_offset')}, **{key: (-1, 4095) for key in (
                'mincore_errno', 'seek_data_errno', 'seek_hole_errno')}),
            'copy': dict(common, non_ff=(0, 4096), done=(-(1 << 63), signed), errno=(-1, 4095)),
            'summary': dict(common, recorded=(0, 32), dropped=(0, maximum)),
        }
        for event, limits in fields.items():
            for key, (low, high) in limits.items():
                for invalid in (True, False, None, '0', 1.0, [], {}, low - 1, high + 1):
                    with self.subTest(event=event, key=key, invalid=invalid):
                        self.assertIsNone(self.parse(dict(trace_record(event, version=2), **{key: invalid})))
        record = trace_record('fault', version=2, elapsed_ns=maximum, handler=maximum,
                              pid=(1 << 31) - 1, start=0, length=maximum)
        self.assertEqual(self.parse(record), record)
        summary = trace_record('summary', version=2, seq=maximum, dropped=maximum - 33)
        self.assertEqual(self.parse(summary), summary)

    def test_register_unavailable_is_distinct_from_maps_mismatch(self):
        record = trace_record('register', version=2, maps_match=False)
        self.assertEqual(self.parse(record), record)
        maps_fields = ('maps_va', 'maps_size', 'maps_offset', 'maps_dev', 'maps_ino', 'maps_match')
        unavailable = dict(record, maps_errno=errno.ENOENT, peer_errno=errno.EBADF, ch_pid=0,
                           memfd_errno=-1, memfd_dev=0, memfd_ino=0, memfd_size=0,
                           kernel_errno=-1, kernel_release='')
        for key in maps_fields:
            del unavailable[key]
        self.assertEqual(self.parse(unavailable), unavailable)
        for key in maps_fields:
            with self.subTest(unavailable_field=key):
                self.assertIsNone(self.parse(dict(unavailable, **{key: record[key]})))
        for changes in (dict(kernel_release=''), dict(kernel_release='x' * 65),
                        dict(kernel_release=None), dict(kernel_errno=1), dict(maps_match=1)):
            with self.subTest(changes=changes):
                self.assertIsNone(self.parse(dict(record, **changes)))
        maximum_release = dict(record, kernel_release='x' * 64)
        self.assertEqual(self.parse(maximum_release), maximum_release)

    def test_observe_conditional_results_and_both_phases(self):
        combinations = ((0, 0), (errno.ENXIO, 0), (errno.EBADF, 0),
                        (0, errno.EBADF), (errno.ENXIO, errno.EBADF), (-1, -1))
        for phase in ('before-ack', 'close'):
            for mincore_error in (0, errno.EPERM, -1):
                for data_error, hole_error in combinations:
                    with self.subTest(phase=phase, mincore=mincore_error,
                                      data=data_error, hole=hole_error):
                        record = trace_record('observe', version=2, phase=phase,
                                              mincore_errno=mincore_error,
                                              seek_data_errno=data_error, seek_hole_errno=hole_error,
                                              extent_data=data_error == 0)
                        if mincore_error:
                            del record['resident']
                        if data_error:
                            record['seek_data_offset'] = -1
                        if hole_error:
                            record['seek_hole_offset'] = -1
                        if hole_error or data_error not in (0, errno.ENXIO):
                            del record['extent_data']
                        self.assertEqual(self.parse(record), record)
                        for key in ('resident', 'extent_data'):
                            malformed = dict(record)
                            if key in malformed:
                                del malformed[key]
                            else:
                                malformed[key] = False
                            self.assertIsNone(self.parse(malformed))
        for changes in (dict(start=0, length=diagnostic.TRACE_PAGE + 4096), dict(length=8192),
                        dict(phase='poll'), dict(resident=0), dict(extent_data=1),
                        dict(seek_data_offset=-1), dict(seek_hole_offset=-1),
                        dict(seek_data_errno=errno.ENXIO), dict(seek_hole_errno=errno.EBADF)):
            with self.subTest(changes=changes):
                self.assertIsNone(self.parse(trace_record('observe', version=2, **changes)))

    def test_event_specific_fields_do_not_cross_schemas(self):
        invalid = [('fault', dict(kernel_event='remove')), ('remove', dict(kernel_event='remap')),
                   ('source', dict(kind='Data')), ('register', dict(resident=True)),
                   ('observe', dict(maps_match=True)), ('summary', dict(start=0)),
                   ('summary', dict(recorded=32, dropped=1)), ('copy', dict(sha256='A' * 64)),
                   ('fault', dict(state='unknown')), ('fault', dict(event='unknown'))]
        for event, changes in invalid:
            with self.subTest(event=event, changes=changes):
                self.assertIsNone(self.parse(dict(trace_record(event, version=2), **changes)))

    def test_json_and_encoded_message_size_are_strictly_bounded(self):
        record = trace_record('fault', version=2)
        message = diagnostic.TRACE_PREFIX + json.dumps(record)
        self.assertEqual(diagnostic.memory_page_trace('x' * (2047 - len(message)) + message), record)
        self.assertIsNone(diagnostic.memory_page_trace('x' * (2048 - len(message)) + message))
        # The writer bounds bytes, not Unicode code points.
        self.assertIsNone(diagnostic.memory_page_trace('\u00e9' * 1024 + message))
        self.assertIsNone(diagnostic.memory_page_trace('\ud800' + message))
        duplicate = diagnostic.TRACE_PREFIX + json.dumps(record)[:-1] + ', "v": 2}'
        for malformed in (duplicate, diagnostic.TRACE_PREFIX + '{}',
                          diagnostic.TRACE_PREFIX + '[]', diagnostic.TRACE_PREFIX + 'null',
                          diagnostic.TRACE_PREFIX + '{',
                          diagnostic.TRACE_PREFIX + '[' * 1000 + ']' * 1000, None, 12):
            with self.subTest(malformed=malformed):
                self.assertIsNone(diagnostic.memory_page_trace(malformed))


class ShmemAllocationSchema(unittest.TestCase):
    def parse(self, record):
        return diagnostic.memory_shmem_allocation(diagnostic.SHMEM_PREFIX + json.dumps(record))

    def test_all_events_and_collection_failure_stages(self):
        for event in ('armed', 'failed', 'insert', 'closed'):
            with self.subTest(event=event):
                record = shmem_record(event)
                self.assertEqual(self.parse(record), record)
                for field in record:
                    missing = dict(record)
                    del missing[field]
                    self.assertIsNone(self.parse(missing))
                self.assertIsNone(self.parse(dict(record, error='DO_NOT_EMIT')))
        for stage in ('parse', 'identity', 'btf', 'maps', 'program', 'attach_return', 'attach_entry', 'reader'):
            self.assertIsNotNone(self.parse(shmem_record('failed', stage=stage, page=0,
                                                        memfd_dev=0, memfd_ino=0, errno=-1)))
        self.assertIsNone(self.parse(shmem_record('failed', stage='failed DO_NOT_EMIT')))
        self.assertIsNone(self.parse(shmem_record('failed', page=1)))
        self.assertIsNotNone(self.parse(shmem_record(window_end=diagnostic.TRACE_PAGE + 4096)))
        self.assertIsNone(self.parse(shmem_record(window_end=diagnostic.TRACE_PAGE + 6144)))
        self.assertIsNone(self.parse(shmem_record(window_start=diagnostic.TRACE_PAGE - 4096)))

    def test_numeric_fields_reject_boolean_wrong_types_and_overflow(self):
        maximum = (1 << 64) - 1
        common = dict(v=(1, 1), pid=(1, (1 << 31) - 1), memfd_dev=(0, maximum), memfd_ino=(1, maximum))
        bounds = {
            'armed': dict(common, seq=(1, 1), page=(diagnostic.TRACE_PAGE, diagnostic.TRACE_PAGE),
                          page_size=(4096, 4096), max_records=(16, 16)),
            'failed': dict(errno=(-1, 4095)),
            'insert': dict(seq=(2, 17), inserter_pid=(0, (1 << 31) - 1),
                           inserter_tid=(0, (1 << 31) - 1), index=(0, maximum // 4096),
                           nr_pages=(1, 1048576), ktime_ns=(0, maximum),
                           result=(-(1 << 31), (1 << 31) - 1), stack_errno=(-4095, 0)),
            'closed': dict(matched=(0, maximum), emitted=(0, 16), dropped=(0, maximum),
                           pending=(0, 64), nested=(0, maximum), entry_missed=(-1, maximum),
                           return_missed=(-1, maximum), errno=(-1, 4095)),
        }
        for event, fields in bounds.items():
            for field, (low, high) in fields.items():
                for invalid in (True, False, 1.0, '1', None, low - 1, high + 1):
                    with self.subTest(event=event, field=field, invalid=invalid):
                        self.assertIsNone(self.parse(shmem_record(event, **{field: invalid})))
        self.assertIsNone(self.parse(shmem_record('insert', index=maximum // 4096, nr_pages=2)))
        self.assertIsNotNone(self.parse(shmem_record('insert', index=maximum // 4096,
                                                    ktime_ns=maximum, result=-(1 << 31))))
        self.assertIsNotNone(self.parse(shmem_record('insert', nr_pages=1048576,
                                                    result=(1 << 31) - 1, stack_errno=-4095, frames=[])))
        self.assertIsNotNone(self.parse(shmem_record('closed', seq=18, emitted=16, matched=maximum,
                                                    dropped=maximum, pending=64, nested=maximum,
                                                    entry_missed=maximum, return_missed=-1)))
        self.assertIsNone(self.parse(shmem_record('closed', seq=4, emitted=1)))

    def test_frames_are_bounded_symbols_without_addresses_or_arguments(self):
        record = shmem_record('insert', frames=['kernel_fn.constprop.0+0xff'] * 32, comm='c' * 16)
        self.assertEqual(self.parse(record), record)
        self.assertIsNotNone(self.parse(shmem_record('insert', frames=['f' * 96, 'unknown'], comm='')))
        for frames in (None, {}, 'kernel_fn', [1], [''], ['f' * 97], ['kernel_fn'] * 33,
                       ['0xffff800000001234'], ['0xffff800000001234+0x1'],
                       ['kernel_fn(0x1234)'], ['kernel_fn\nDO_NOT_EMIT'], ['kernel_\u00e9']):
            with self.subTest(frames=frames):
                self.assertIsNone(self.parse(shmem_record('insert', frames=frames)))
        for comm in ('c' * 17, 'worker\n', 'worker\x00', '\u00e9', None, 1):
            self.assertIsNone(self.parse(shmem_record('insert', comm=comm)))
        self.assertIsNone(self.parse(shmem_record('insert', stack_errno=-1)))
        self.assertIsNotNone(self.parse(shmem_record('insert', stack_errno=-1, frames=[])))
        self.assertIsNone(self.parse(shmem_record('insert', pid_scope='backend')))

    def test_exact_byte_limit_duplicate_fields_and_non_objects(self):
        record = shmem_record('insert')
        message = diagnostic.SHMEM_PREFIX + json.dumps(record)
        self.assertEqual(diagnostic.memory_shmem_allocation('x' * (4095 - len(message)) + message), record)
        self.assertIsNone(diagnostic.memory_shmem_allocation('x' * (4096 - len(message)) + message))
        self.assertIsNone(diagnostic.memory_shmem_allocation('\u00e9' * 2048 + message))
        self.assertIsNone(diagnostic.memory_shmem_allocation('\ud800' + message))
        duplicate = diagnostic.SHMEM_PREFIX + json.dumps(record)[:-1] + ', "pid": 1234}'
        for value in (duplicate, diagnostic.SHMEM_PREFIX + '{}', diagnostic.SHMEM_PREFIX + '[]',
                      diagnostic.SHMEM_PREFIX + 'null', diagnostic.SHMEM_PREFIX + '{',
                      diagnostic.SHMEM_PREFIX + '[' * 1000 + ']' * 1000, None, 1):
            self.assertIsNone(diagnostic.memory_shmem_allocation(value))
        for event in ([], {}, 1, None, 'unknown'):
            self.assertIsNone(self.parse(dict(shmem_record('insert'), event=event)))


class ExecuteMemoryTraceWrapper(unittest.TestCase):
    def test_actual_wrapper_preserves_argv_exit_and_readiness_for_all_marker_modes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / 'bin'
            binary.mkdir()
            (binary / 'node-ctl').write_text('fixture')
            backend = binary / 'sandbox-ctl'
            backend.write_text('#!' + sys.executable + '\n' + r'''
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
ready = next(int(arg.split('=', 1)[1]) for arg in args if arg.startswith('--ready-fd='))
Path(os.environ['WORK'], 'observed.json').write_text(json.dumps({
    'argv': args, 'trace': os.environ.get('SANDBOX_UFFD_TRACE_MEMFD_PAGE'),
    'guard': os.environ.get('SANDBOX_UFFD_GUARD_BACKEND_PAGE'),
    'shmem': os.environ.get('SANDBOX_UFFD_TRACE_SHMEM_PAGE'), 'pid': os.getpid()}))
os.write(ready, b'control_ready\nready\n')
os.close(ready)
os.read(int(os.environ['FIXTURE_RELEASE_FD']), 1)
raise SystemExit(37)
''')
            backend.chmod(0o755)
            env = dict(os.environ, WORK=str(root), BIN=str(binary),
                       SANDBOX_UFFD_TRACE_MEMFD_PAGE='inherited-must-not-enable',
                       SANDBOX_UFFD_GUARD_BACKEND_PAGE='inherited-must-not-enable',
                       SANDBOX_UFFD_TRACE_SHMEM_PAGE='inherited-must-not-enable')
            instrument = Path(diagnostic.__file__).with_name('execute_instrument.sh')
            result = subprocess.run(['bash', str(instrument)], env=env, capture_output=True,
                                    text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            marker = root / 'trace-memory-page'
            cases = [
                ('absent', None, ['run', '--restore', 'file://saved'], None),
                ('cold', b'4194304\n', ['run', '--config', 'cold.yaml'], None),
                ('restore', b'4194304\n', ['run', '--restore', 'file://saved'], '4194304'),
                ('restore-equals', b'4194304\n', ['run', '--restore=file://saved'], '4194304'),
                ('non-run', b'4194304\n', ['exec', '--restore=file://saved'], None),
                ('after-delimiter', b'4194304\n', ['run', '--', '--restore=file://saved'], None),
                ('malformed', b'4194304; echo injected\n', ['run', '--restore=file://saved'], None),
                ('binary', b'4194304\x00\n', ['run', '--restore=file://saved'], None),
                ('large', b'4194304\n' + b'x' * 100000, ['run', '--restore=file://saved'], None),
                ('empty', b'', ['run', '--restore=file://saved'], None),
                ('symlink', b'4194304\n', ['run', '--restore=file://saved'], None),
            ]
            for name, contents, args, expected in cases:
                with self.subTest(name=name):
                    marker.unlink(missing_ok=True)
                    if contents is not None:
                        if name == 'symlink':
                            target = root / 'marker-target'
                            target.write_bytes(contents)
                            marker.symlink_to(target)
                        else:
                            marker.write_bytes(contents)
                    ready_read, ready_write = os.pipe()
                    release_read, release_write = os.pipe()
                    forwarded = [*args, f'--ready-fd={ready_write}', 'space argument', '']
                    process = subprocess.Popen([str(root / 'orch-bin/sandbox-ctl'), *forwarded],
                                               env=dict(env, FIXTURE_RELEASE_FD=str(release_read)),
                                               pass_fds=(ready_write, release_read),
                                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    os.close(ready_write)
                    os.close(release_read)
                    try:
                        readiness = b''
                        deadline = time.monotonic() + 3
                        while True:
                            remaining = max(0, deadline - time.monotonic())
                            self.assertTrue(select.select([ready_read], [], [], remaining)[0],
                                            'wrapper retained or changed readiness fd')
                            chunk = os.read(ready_read, 1024)
                            if not chunk:
                                break
                            readiness += chunk
                        self.assertEqual(readiness, b'control_ready\nready\n')
                        self.assertIsNone(process.poll())
                        observed = json.loads((root / 'observed.json').read_text())
                        self.assertEqual(observed, dict(argv=forwarded, trace=expected, guard=None,
                                                        shmem=expected, pid=process.pid))
                    finally:
                        os.close(ready_read)
                        os.write(release_write, b'x')
                        os.close(release_write)
                        try:
                            stdout, stderr = process.communicate(timeout=3)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.communicate(timeout=3)
                            raise
                    self.assertEqual(process.returncode, 37, stderr.decode())
                    self.assertEqual(stdout, b'')


if __name__ == '__main__':
    unittest.main()
