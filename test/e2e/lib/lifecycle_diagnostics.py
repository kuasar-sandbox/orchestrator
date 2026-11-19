"""Bounded, read-only evidence for failed idle/pressure Pause/Wake cases."""
from collections import deque
import json
import os
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
TRACE_PAGE = 4194304
MAX_TRACE_RECORDS = 32
TRACE_PREFIX = 'uffd-page-trace '
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


def held_memory_fault(message):
    """Read only the bounded JSON emitted by the hold workload's failure path."""
    _, marker, payload = message.partition('held-memory contents changed')
    payload = payload.lstrip(': ').strip()
    if not marker or not payload or len(payload) > MAX_FIELD:
        return None
    try:
        value = json.loads(payload)
        return value if isinstance(value, dict) else None
    except ValueError:
        return None


def memory_page_trace(message):
    """Accept only the one-page trace schema, never arbitrary runtime fields."""
    _, marker, payload = message.partition(TRACE_PREFIX)
    if not marker or len(message) >= MAX_FIELD:
        return None
    try:
        value = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(value, dict):
        return None
    event = value.get('event')
    fields = {'v', 'page', 'seq', 'event'}
    if event == 'summary':
        fields |= {'recorded', 'dropped'}
    elif event in ('seed', 'fault', 'source', 'copy', 'zero', 'remove', 'load-hit'):
        fields |= {'start', 'length', 'state'}
        if event == 'source':
            fields.add('kind')
        elif event in ('copy', 'zero'):
            fields |= {'sha256', 'non_ff', 'done', 'errno'}
    else:
        return None
    if set(value) != fields:
        return None

    def integer(key, low, high):
        return type(value.get(key)) is int and low <= value[key] <= high

    maximum = (1 << 64) - 1
    if (not integer('v', 1, 1) or not integer('page', TRACE_PAGE, TRACE_PAGE)
            or not integer('seq', 1, maximum)):
        return None
    if event == 'summary':
        if (not integer('recorded', 0, MAX_TRACE_RECORDS) or not integer('dropped', 0, maximum)
                or value['seq'] != value['recorded'] + value['dropped'] + 1):
            return None
    else:
        if (not integer('seq', 1, MAX_TRACE_RECORDS) or not integer('start', 0, TRACE_PAGE)
                or not integer('length', 1, maximum)
                or not TRACE_PAGE + 4096 <= value['start'] + value['length'] <= 1 << 64
                or value['state'] not in ('absent', 'loaded', 'released')):
            return None
        if event == 'source' and value['kind'] not in ('data', 'hole', 'zero'):
            return None
        if event in ('copy', 'zero') and (
                not isinstance(value['sha256'], str)
                or re.fullmatch(r'[0-9a-f]{64}', value['sha256']) is None
                or not integer('non_ff', 0, 4096)
                or not integer('done', -(1 << 63), (1 << 63) - 1)
                or not integer('errno', -1, 4095)):
            return None
    return value


def memory_page_request(fault):
    """A masked/unavailable PFN is not a usable physical address."""
    if fault.get('page_size') != 4096:
        raise ValueError('unsupported guest page size')
    mapping, page = fault.get('pagemap'), fault.get('page')
    if not isinstance(mapping, dict) or not isinstance(page, dict):
        raise ValueError('missing page evidence')
    pfn = mapping.get('pfn')
    if (mapping.get('present') is not True or mapping.get('swapped') is not False
            or type(pfn) is not int or not 0 < pfn < (1 << 52)):
        raise ValueError('guest PFN is unavailable')
    start, length = page.get('offset_in_page'), page.get('length')
    if (type(start) is not int or type(length) is not int
            or not 0 <= start < 4096 or not 0 < length <= 4096 - start):
        raise ValueError('invalid held-page intersection')
    digest = page.get('sha256')
    if not isinstance(digest, str) or re.fullmatch(r'[0-9a-f]{64}', digest) is None:
        raise ValueError('invalid guest page digest')
    return pfn * 4096, start, length, digest


def saved_memory_page(probe, record, request):
    """Read one saved page through the native fixture, with no VM side effects."""
    gpa, start, length, guest_digest = request
    if record.get('resume_source_kind') != 'snapshot':
        raise ValueError('no saved snapshot')
    sid = record['id']
    if re.fullmatch(r'[a-zA-Z0-9_-]{1,128}', sid) is None:
        raise ValueError('invalid sandbox identity')
    source_ref = record.get('resume_source_ref')
    if not isinstance(source_ref, str) or not source_ref.startswith('file://'):
        raise ValueError('unsupported saved source')
    command = [probe['helper'], 'page', '--source-ref', source_ref,
               '--source-dir', str(Path(probe['checkpoint_root']) / sid / 'checkpoint'),
               '--manifest-config', probe['manifest_config'], '--gpa', str(gpa),
               '--start', str(start), '--length', str(length)]
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=output,
                                stderr=subprocess.DEVNULL, timeout=3)
        if result.returncode:
            return dict(unavailable='helper_exit', exit_code=result.returncode)
        if output.seek(0, 2) > 16384:
            raise ValueError('oversized page evidence')
        output.seek(0)
        value = json.loads(output.read(16384))
    if not isinstance(value, dict):
        raise ValueError('invalid page evidence')
    # Only the fixture's numeric/digest evidence is forwarded. Paths, arbitrary
    # helper output and local encryption configuration never enter the journal.
    out = project(value, ('source_identity', 'gpa', 'memory_offset', 'start', 'length', 'sha256', 'non_ff'))
    if (any(type(out.get(field)) is not int for field in ('gpa', 'start', 'length'))
            or (out['gpa'], out['start'], out['length']) != (gpa, start, length)):
        raise ValueError('page evidence does not match request')
    offset, non_ff = out.get('memory_offset'), out.get('non_ff')
    if (type(offset) is not int or not 0 <= offset <= (1 << 64) - 4096 or offset % 4096
            or type(non_ff) is not int or not 0 <= non_ff <= length):
        raise ValueError('invalid saved page counters')
    digest = out.get('sha256')
    if not isinstance(digest, str) or re.fullmatch(r'[0-9a-f]{64}', digest) is None:
        raise ValueError('invalid saved page digest')
    points = value.get('bad_points', [])
    layers = value.get('layers', [])
    if (not isinstance(points, list) or len(points) != min(non_ff, 8)
            or not isinstance(layers, list) or not 1 <= len(layers) <= 64):
        raise ValueError('unbounded page evidence')
    previous = start - 1
    for point in points:
        if (not isinstance(point, dict) or type(point.get('offset')) is not int
                or not previous < point['offset'] < start + length
                or type(point.get('actual')) is not int or not 0 <= point['actual'] < 255):
            raise ValueError('invalid saved page sample')
        previous = point['offset']
    for layer in layers:
        if (not isinstance(layer, dict) or not isinstance(layer.get('identity'), str)
                or re.fullmatch(r'(digest|hmac):[0-9a-f]{64}', layer['identity']) is None
                or layer.get('kind') not in ('Data', 'Zero', 'Hole')):
            raise ValueError('invalid saved page layer')
    if out.get('source_identity') != layers[0]['identity']:
        raise ValueError('saved page identity does not match top layer')
    # Native offsets are within the physical page; workload offsets are within
    # held. The two digests already cover the same start:length intersection.
    out['bad_points'] = [project(point, ('offset', 'actual')) for point in points]
    out['layers'] = [project(layer, ('identity', 'kind')) for layer in layers]
    out['guest_sha256'] = guest_digest
    out['same_bytes_at_reported_pfn'] = digest == guest_digest
    # A PFN sampled after restore is not proof that Linux never migrated that
    # virtual page since capture. Preserve this boundary in the emitted record.
    out['pfn_observation'] = 'after_restore_failure'
    return out


def emit_failure_evidence(db_path, log_path, sandbox_ids, facts_path=None, stream=None,
                          memory_probe=None):
    """Never replace the case's original failure, including if a source is absent."""
    stream = sys.stderr if stream is None else stream
    # These cases own at most three identities (PRESSURE_SID usually duplicates
    # SECOND). Keep journal queries bounded even if a caller supplies more.
    ids = tuple(dict.fromkeys(sid for sid in sandbox_ids if sid))[:3]
    if not ids:
        return
    emitted, run_ids = 0, set()
    trace_emitted, trace_summary_emitted = 0, False
    states, page_probe_used = {}, False

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

    def emit_trace(record):
        nonlocal trace_emitted, trace_summary_emitted
        if record['event'] == 'summary':
            if trace_summary_emitted:
                return
            trace_summary_emitted = True
        else:
            if trace_emitted >= MAX_TRACE_RECORDS:
                return
            trace_emitted += 1
        try:
            print(json.dumps(dict(lifecycle_diagnostic='uffd_page_trace', **record), sort_keys=True),
                  file=stream, flush=True)
        except Exception:
            pass

    try:
        uri = Path(db_path).resolve().as_uri() + '?mode=ro'
        with sqlite3.connect(uri, uri=True, timeout=1) as db:
            db.row_factory = sqlite3.Row
            columns = ','.join((*STATE_FIELDS, 'sandbox_result_json'))
            placeholders = ','.join('?' for _ in ids)
            rows = db.execute(f'SELECT {columns} FROM sandboxes WHERE id IN ({placeholders})', ids).fetchall()
        for row in rows:
            record = dict(row)
            states[record['id']] = record
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
                fault_entry = None
                record = states.get(sid, {})
                current_run = (record.get('run_id') or record.get('dead_run_id')
                               or record.get('sandbox_result_run_id'))
                for line in tail_lines(output):
                    entry = parse_line(line)
                    message = entry.get('MESSAGE')
                    if entry.get('KUASAR_SANDBOX_ID') != sid or not isinstance(message, str):
                        continue
                    if 'uffd-page-trace' in message:
                        # Trace messages have their own quota and cannot evict
                        # the workload fault from the last twelve error lines.
                        trace = memory_page_trace(message)
                        if trace is not None and current_run and entry.get('KUASAR_RUN_ID') == current_run:
                            emit_trace(dict(sid=sid, run_id=current_run, **trace))
                        continue
                    if JOURNAL_ERROR.search(message):
                        entries.append(project(entry, ('__REALTIME_TIMESTAMP', 'KUASAR_RUN_ID', 'MESSAGE')))
                        fault = held_memory_fault(message)
                        if fault is not None and current_run and entry.get('KUASAR_RUN_ID') == current_run:
                            fault_entry = (current_run, fault)
                for entry in entries:
                    emit('journal', dict(sid=sid, **entry))
                if memory_probe and not page_probe_used and fault_entry:
                    run_id, fault = fault_entry
                    try:
                        request = memory_page_request(fault)
                        page_probe_used = True
                        page = saved_memory_page(memory_probe, states[sid], request)
                        emit('snapshot_page', dict(sid=sid, run_id=run_id, **page))
                    except Exception as error:
                        unavailable('snapshot_page', error, sid=sid, run_id=run_id)
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
        probe = {name: os.environ.get(variable, '') for name, variable in (
            ('helper', 'KUASAR_E2E_MEMORY_COMPARE_BIN'),
            ('manifest_config', 'KUASAR_E2E_MEMORY_CONFIG'),
            ('checkpoint_root', 'KUASAR_E2E_MEMORY_CHECKPOINT_ROOT'))}
        emit_failure_evidence(sys.argv[1], sys.argv[2], sys.argv[3:],
                              memory_probe=probe if all(probe.values()) else None)
    except Exception as error:
        # The shell caller also preserves its original failure status.
        print('lifecycle diagnostics unavailable: ' + type(error).__name__, file=sys.stderr)
