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


class LifecycleDiagnostics(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.db, self.log = self.root / 'state.db', self.root / 'conductor.log'
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
        journal.write_text('#!' + sys.executable + '\n'
                           'import os\nfrom pathlib import Path\n'
                           'print(Path(os.environ["PAGE_TEST_ROOT"], "journal.jsonl").read_text())\n')
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
            PAGE_TEST_MODE=''))
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


class ExecuteProductionWrapper(unittest.TestCase):
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
                ('restore', b'4194304\n', ['run', '--restore', 'file://saved'], None),
                ('restore-equals', b'4194304\n', ['run', '--restore=file://saved'], None),
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
