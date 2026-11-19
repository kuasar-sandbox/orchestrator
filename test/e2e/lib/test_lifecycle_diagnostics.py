"""Exercise failure-only page evidence with a real DB and child processes."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
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


if __name__ == '__main__':
    unittest.main()
