"""Bounded, read-only evidence for failed idle/pressure Pause/Wake cases."""
from collections import deque
import json
from pathlib import Path
import re
import shlex
import sqlite3
import subprocess
import sys
import tempfile


MAX_RECORDS = 64
MAX_FIELD = 2048
MAX_INPUT = 2 * 1024 * 1024
STATE_FIELDS = (
    'id', 'instance_id', 'state', 'run_id', 'sandbox_result_run_id',
    'dead_unix', 'dead_run_id', 'launch_mode', 'resume_source_kind',
    'resume_source_ref', 'resume_sandbox_ref', 'pause_reason',
    'resource_obligation', 'pressure_version', 'running_since_ns',
)
RESULT_FIELDS = ('sid', 'run_id', 'stage', 'exit_code', 'signal', 'error')
EVENT_FIELDS = (
    'time', 'level', 'msg', 'sid', 'run_id', 'stage', 'generation',
    'capture_kind', 'mode', 'capture_ms', 'elapsed_ms', 'handshake_ms',
    'retry', 'pressure_version', 'err', 'error',
)
EVENTS = {
    'idle pause attempt', 'idle pause result', 'pause proxy activity',
    'pause capture resolved', 'pause capture completed', 'pause rollback incomplete',
    'pause: ownership cleanup incomplete',
    'paused sandbox ownership cleanup incomplete; retrying',
    'paused sandbox cleanup worker retirement failed; retrying',
    'sandbox execution result accepted',
    'sandbox execution result accepted from disconnected run',
    'sandbox run end check incomplete; retaining retry ownership',
    'node pressure operation blocked', 'resource pause adopted',
}
JOURNAL_ERROR = re.compile(
    r'error|failed|fatal|exit|closed|shutdown|reboot|quiesce|restore|oom|out of memory|killed|signal', re.I)
CREDENTIAL = re.compile(
    r'(?i)((?<![\w-])["\']?[\w-]{0,64}(?:token|secret|password|authorization|api[_-]?key)'
    r'[\w-]{0,64}["\']?\s*[=:]\s*)'
    r'(?:"[^"\n]*"|\'[^\'\n]*\'|[^\s,;]+)')


def project(record, fields):
    result = {}
    for key in fields:
        value = record.get(key)
        if isinstance(value, str):
            value = re.sub(r'(?i)\bBearer\s+[^\s,;]+', 'Bearer [redacted]', value)
            value = CREDENTIAL.sub(r'\1[redacted]', value)
            if len(value) > MAX_FIELD:
                value = value[:MAX_FIELD // 2] + '...[truncated]...' + value[-MAX_FIELD // 2 + 17:]
        if key in record and (value is None or isinstance(value, (str, int, float, bool))):
            result[key] = value
    return result


def tail_lines(stream):
    stream.seek(0, 2)
    offset = max(0, stream.tell() - MAX_INPUT)
    stream.seek(offset)
    data = stream.read(MAX_INPUT)
    if offset:
        data = data.partition(b'\n')[2]
    return data.decode('utf-8', errors='replace').splitlines()


def parse_line(line):
    try:
        value = json.loads(line)
        return value if isinstance(value, dict) else {}
    except ValueError:
        try:
            return dict(word.split('=', 1) for word in shlex.split(line) if '=' in word)
        except ValueError:
            return {}


def emit_failure_evidence(db_path, log_path, sandbox_ids, facts_path=None, stream=None):
    """Never replace the case's original failure, including if a source is absent."""
    stream = sys.stderr if stream is None else stream
    # These cases own at most three identities (PRESSURE_SID usually duplicates
    # SECOND). Keep journal queries bounded even if a caller supplies more.
    ids = tuple(dict.fromkeys(sid for sid in sandbox_ids if sid))[:3]
    if not ids:
        return
    emitted, run_ids = 0, set()

    def emit(source, record):
        nonlocal emitted
        if emitted < MAX_RECORDS:
            try:
                print(json.dumps(dict(lifecycle_diagnostic=source, **record), sort_keys=True),
                      file=stream, flush=True)
            except Exception:
                pass
            emitted += 1

    def unavailable(source, error, **fields):
        emit(source, dict(unavailable=type(error).__name__, **fields))

    try:
        uri = Path(db_path).resolve().as_uri() + '?mode=ro'
        with sqlite3.connect(uri, uri=True, timeout=1) as db:
            db.row_factory = sqlite3.Row
            columns = ','.join((*STATE_FIELDS, 'sandbox_result_json'))
            placeholders = ','.join('?' for _ in ids)
            rows = db.execute(f'SELECT {columns} FROM sandboxes WHERE id IN ({placeholders})', ids).fetchall()
        for row in rows:
            record = dict(row)
            run_ids.update(record[key] for key in ('run_id', 'sandbox_result_run_id', 'dead_run_id') if record[key])
            out = project(record, STATE_FIELDS)
            try:
                result = json.loads(record['sandbox_result_json'] or '{}')
                out['result'] = project(result, RESULT_FIELDS) if isinstance(result, dict) else {'invalid': True}
            except ValueError:
                out['result'] = {'invalid': True}
            emit('state', out)
        for sid in ids:
            if not any(row['id'] == sid for row in rows):
                emit('state', dict(id=sid, absent=True))
    except Exception as error:
        unavailable('state', error)

    if facts_path is not None:
        try:
            with Path(facts_path).open('rb') as source:
                facts = deque(maxlen=8)
                for line in tail_lines(source):
                    fact = parse_line(line)
                    result = fact.get('result')
                    if isinstance(result, dict) and result.get('sid') in ids:
                        out = project(fact, ('idle_timeout', 'state', 'run_id', 'snapshot_succeeded', 'pause_commit_held'))
                        out['result'] = project(result, RESULT_FIELDS)
                        facts.append(out)
                for fact in facts:
                    emit('capture_fact', fact)
        except Exception as error:
            unavailable('capture_fact', error)

    # Capture state before journal queries. A slow/unavailable journal cannot
    # hide the durable result. Temporary files bound memory even for long entries.
    for sid in ids:
        try:
            with tempfile.TemporaryFile() as output:
                result = subprocess.run(
                    ['journalctl', f'KUASAR_SANDBOX_ID={sid}', '--no-pager', '-n', '200', '-o', 'json'],
                    stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.DEVNULL, timeout=3)
                if result.returncode:
                    emit('journal', dict(sid=sid, unavailable='exit', exit_code=result.returncode))
                    continue
                entries = deque(maxlen=12)
                for line in tail_lines(output):
                    entry = parse_line(line)
                    message = entry.get('MESSAGE')
                    if (entry.get('KUASAR_SANDBOX_ID') == sid and isinstance(message, str)
                            and JOURNAL_ERROR.search(message)):
                        entries.append(project(entry, ('__REALTIME_TIMESTAMP', 'MESSAGE')))
                for entry in entries:
                    emit('journal', dict(sid=sid, **entry))
        except Exception as error:
            unavailable('journal', error, sid=sid)

    try:
        events = deque(maxlen=MAX_RECORDS)
        with Path(log_path).open('rb') as source:
            for line in tail_lines(source):
                event = parse_line(line)
                matches = event.get('sid') in ids if event.get('sid') else event.get('run_id') in run_ids
                if matches and event.get('msg') in EVENTS:
                    events.append(project(event, EVENT_FIELDS))
        for event in list(events)[-(MAX_RECORDS - emitted):] if emitted < MAX_RECORDS else ():
            emit('conductor', event)
    except Exception as error:
        unavailable('conductor', error)


if __name__ == '__main__':
    try:
        emit_failure_evidence(sys.argv[1], sys.argv[2], sys.argv[3:])
    except Exception as error:
        # The shell caller also preserves its original failure status.
        print('lifecycle diagnostics unavailable: ' + type(error).__name__, file=sys.stderr)
