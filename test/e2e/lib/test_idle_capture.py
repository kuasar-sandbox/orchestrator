"""Offline controls for the real idle capture barrier's two completion roles."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

from idle_timeout import capture_barrier_fact, verify_capture_parent_ack


class IdleCaptureBarrier(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='idle-capture-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.database = self.root / 'node.db'
        self.base = self.root / 'sandbox'
        self.directory = self.base / 'checkpoint'
        self.directory.mkdir(parents=True)
        self.record = dict(version=1, producer=dict(sandbox_id='sid', instance_id='instance', run_id='run'),
                           checkpoint_id='checkpoint', host_operation_id='operation', memory_id='memory', epoch=2,
                           committed=True, runtime_retired=True, snapshot_ref='snapshot', sandbox_ref='sandbox')
        self.owner = dict(task_id='sid', run_id='run', retirement_instance_id='instance', retirement_base_dir=str(self.base),
                          retirement_checkpoint_id='checkpoint', retirement_operation_id='operation')
        with sqlite3.connect(self.database) as db:
            db.execute('create table sandboxes(id,state,instance_id,run_id,base_dir,sandbox_result_run_id,sandbox_result_json)')
            db.execute('insert into sandboxes values(?,?,?,?,?,?,?)', ('sid','pausing','instance','run',str(self.base),'',''))
            db.execute('create table run_pool_ownership(run_id,ownership)')
            db.execute('insert into run_pool_ownership values(?,?)', ('run',json.dumps(self.owner)))
        self.write_record()

    def write_record(self):
        locator = {key:self.record[key] for key in ('checkpoint_id','memory_id','epoch')}
        (self.directory/'current-memory.json').write_text(json.dumps(locator))
        path = self.directory/'memory'/hashlib.sha256(locator['checkpoint_id'].encode()).hexdigest()/(locator['memory_id']+'.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.record))

    def write_owner(self):
        with sqlite3.connect(self.database) as db:
            db.execute('update run_pool_ownership set ownership=?', (json.dumps(self.owner),))

    def result(self):
        with sqlite3.connect(self.database) as db:
            db.execute('update sandboxes set sandbox_result_run_id=?,sandbox_result_json=?', ('run',json.dumps(dict(run_id='run',exit_code=0))))

    def test_holder_fact_requires_no_ordinary_result(self):
        fact = capture_barrier_fact(self.database,'sid')
        self.assertEqual((fact['completion'],fact['memory_id'],fact['epoch']), ('runtime-retired','memory',2))
        self.result()
        with self.assertRaisesRegex(AssertionError,'ordinary runner result'):
            capture_barrier_fact(self.database,'sid')

    def test_retirement_and_exact_producer_are_mandatory(self):
        for field in ('sandbox_id','instance_id','run_id'):
            with self.subTest(field=field):
                old = self.record['producer'][field]
                self.record['producer'][field] = 'other'
                self.write_record()
                with self.assertRaises(AssertionError):
                    capture_barrier_fact(self.database,'sid')
                self.record['producer'][field] = old
        for field,value in [('committed',False),('runtime_retired',False),('aborted',True),('discarded',True),('host_operation_id','other')]:
            with self.subTest(field=field):
                old = self.record.get(field)
                self.record[field] = value
                self.write_record()
                with self.assertRaises(AssertionError):
                    capture_barrier_fact(self.database,'sid')
                if old is None:
                    del self.record[field]
                else:
                    self.record[field] = old

    def test_pending_role_and_exact_intent_are_mandatory(self):
        for field,value in [('task_id','other'),('run_id','other'),('retirement_instance_id','other'),
                            ('retirement_base_dir','other'),('retirement_checkpoint_id','other'),
                            ('role','memory-holder'),('parent_retired',True)]:
            with self.subTest(field=field):
                old = self.owner.get(field)
                self.owner[field] = value
                self.write_owner()
                with self.assertRaises(AssertionError):
                    capture_barrier_fact(self.database,'sid')
                if old is None:
                    del self.owner[field]
                else:
                    self.owner[field] = old

    def test_missing_record_never_downgrades_prepared_intent(self):
        (self.directory/'current-memory.json').unlink()
        self.result()
        with self.assertRaises(FileNotFoundError):
            capture_barrier_fact(self.database,'sid')

    def test_legacy_branch_still_requires_durable_ordinary_result(self):
        self.owner = {}
        self.write_owner()
        self.assertIsNone(capture_barrier_fact(self.database,'sid'))
        self.result()
        fact = capture_barrier_fact(self.database,'sid')
        self.assertEqual(fact['completion'],'runner-result')
        verify_capture_parent_ack(self.database,self.root/'absent.log','sid',fact)

    def test_exact_parent_ack_and_no_ordinary_result_after_commit(self):
        fact = capture_barrier_fact(self.database,'sid')
        self.owner.update(role='memory-holder',parent_retired=True,memory_id='memory',memory_epoch=2)
        self.write_owner()
        log = self.root/'orch.log'
        event = dict(msg='checkpoint parent retirement acknowledged',sid='sid',run_id='run',memory_id='memory',epoch=2)
        log.write_text(json.dumps(event)+'\n')
        verify_capture_parent_ack(self.database,log,'sid',fact)
        log.write_text('')
        for field in ('task_id','run_id','retirement_operation_id','retirement_checkpoint_id','memory_id','memory_epoch'):
            with self.subTest(owner_field=field):
                old = self.owner[field]
                self.owner[field] = 'other'
                self.write_owner()
                with mock.patch('idle_timeout.time.sleep', side_effect=RuntimeError('invalid owner must fail before waiting')):
                    with self.assertRaises(AssertionError):
                        verify_capture_parent_ack(self.database,log,'sid',fact)
                self.owner[field] = old
                self.write_owner()
        for field in ('run_id','memory_id','epoch'):
            with self.subTest(field=field):
                bad = dict(event, **{field:'other'})
                log.write_text(json.dumps(bad)+'\n')
                with mock.patch('idle_timeout.time.monotonic', side_effect=[0, 6]):
                    with self.assertRaisesRegex(AssertionError,'ACK missing'):
                        verify_capture_parent_ack(self.database,log,'sid',fact)
        log.write_text(json.dumps(event)+'\n'+json.dumps(dict(msg='sandbox execution result accepted',sid='sid',run_id='run'))+'\n')
        with self.assertRaisesRegex(AssertionError,'ordinary runner result'):
            verify_capture_parent_ack(self.database,log,'sid',fact)
        log.write_text(json.dumps(event)+'\n')
        self.result()
        with self.assertRaisesRegex(AssertionError,'ordinary runner result'):
            verify_capture_parent_ack(self.database,log,'sid',fact)


    def test_durable_ack_can_precede_its_log_entry(self):
        fact = capture_barrier_fact(self.database,'sid')
        self.owner.update(role='memory-holder',parent_retired=True,memory_id='memory',memory_epoch=2)
        self.write_owner()
        log = self.root/'orch.log'
        log.write_text('')
        done, errors = threading.Event(), []
        def check():
            try:
                verify_capture_parent_ack(self.database,log,'sid',fact)
            except Exception as error:
                errors.append(error)
            finally:
                done.set()
        thread = threading.Thread(target=check)
        thread.start()
        try:
            self.assertFalse(done.wait(.05), 'durable ACK was mistaken for a flushed log')
        finally:
            log.write_text('msg="checkpoint parent retirement acknowledged" sid=sid run_id=run memory_id=memory epoch=2\n')
            thread.join(2)
        self.assertTrue(done.is_set())
        self.assertFalse(errors, errors)


if __name__ == '__main__':
    unittest.main()
