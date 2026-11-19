"""Exercise failure-only page evidence with a real DB and child processes."""
import copy
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

    def trace_entry(self, sid, event='fault', seq=None, run=None, **changes):
        trace = dict(v=1, page=diagnostic.TRACE_PAGE,
                     seq=seq if seq is not None else (33 if event == 'summary' else 1), event=event)
        if event == 'summary':
            trace.update(recorded=32, dropped=0)
        else:
            trace.update(start=diagnostic.TRACE_PAGE, length=4096, state='absent')
            if event == 'source':
                trace['kind'] = 'data'
            elif event in ('copy', 'zero'):
                trace.update(sha256=hashlib.sha256(bytes(4096)).hexdigest(),
                             non_ff=4096, done=4096, errno=0)
        trace.update(changes)
        return dict(KUASAR_SANDBOX_ID=sid, KUASAR_RUN_ID=run or 'current-' + sid,
                    MESSAGE=diagnostic.TRACE_PREFIX + json.dumps(trace))

    def test_trace_identity_and_quota_preserve_fault_and_business_evidence(self):
        entries = [self.trace_entry('foreign'), self.trace_entry('target-a', run='obsolete'),
                   self.trace_entry('target-a', run='current-target-b'), self.entry('target-a')]
        events = ('seed', 'fault', 'source', 'copy', 'zero', 'remove', 'load-hit')
        for sid in ('target-a', 'target-b'):
            entries.extend(self.trace_entry(sid, events[(seq - 1) % len(events)], seq)
                           for seq in range(1, 33))
            entries.append(self.trace_entry(sid, 'summary', dropped=9, seq=42))
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
                            for row in traces))
        self.assertEqual(len(business), diagnostic.MAX_RECORDS)
        self.assertEqual(len(self.calls()), 1)
        self.assertTrue(any('held-memory contents changed' in row.get('MESSAGE', '') for row in business))
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
    'argv': args, 'trace': os.environ.get('SANDBOX_UFFD_TRACE_MEMFD_PAGE'), 'pid': os.getpid()}))
os.write(ready, b'control_ready\nready\n')
os.close(ready)
os.read(int(os.environ['FIXTURE_RELEASE_FD']), 1)
raise SystemExit(37)
''')
            backend.chmod(0o755)
            env = dict(os.environ, WORK=str(root), BIN=str(binary),
                       SANDBOX_UFFD_TRACE_MEMFD_PAGE='inherited-must-not-enable')
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
            ]
            for name, contents, args, expected in cases:
                with self.subTest(name=name):
                    marker.unlink(missing_ok=True)
                    if contents is not None:
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
                        self.assertEqual(observed, dict(argv=forwarded, trace=expected, pid=process.pid))
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
