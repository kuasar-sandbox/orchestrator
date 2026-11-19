"""Bounded, read-only evidence for failed idle/pressure Pause/Wake cases."""
from collections import deque
import errno
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import time


MAX_RECORDS = 64
MAX_FIELD = 2048
MAX_INPUT = 2 * 1024 * 1024
TRACE_PAGE = 4194304
MAX_TRACE_RECORDS = 32
TRACE_PREFIX = 'uffd-page-trace '
GUARD_PREFIX = 'uffd-backend-guard '
MAX_GUARD_RECORDS = 4
SHMEM_PREFIX = 'uffd-shmem-allocation '
MAX_SHMEM_RECORDS = 16
MAX_SHMEM_LIFECYCLE = 2
MAX_SHMEM_MESSAGE = 4096
MAX_SHMEM_FRAMES = 32
MAX_FAULT_LINES = 80
MAX_FAULT_FRAMES = 12
GUARD_JOURNAL_FIELDS = ','.join((
    'MESSAGE', 'SYSLOG_IDENTIFIER', 'KUASAR_SANDBOX_ID', 'KUASAR_RUN_ID',
    '_PID', '_BOOT_ID', '_SYSTEMD_UNIT', '_STREAM_ID', '_TRANSPORT', '__CURSOR'))
SIGBUS_LINE = re.compile(
    r'^\[signal SIGBUS: bus error code=(0x[0-9a-fA-F]+|[0-9]+) '
    r'addr=(0x[0-9a-fA-F]+) pc=(0x[0-9a-fA-F]+)\]$')
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


def memory_backend_guard(message):
    """Accept only the immediate, fixed-field guard records; never raw errors."""
    if not isinstance(message, str) or len(message) >= MAX_FIELD:
        return None
    _, marker, payload = message.partition(GUARD_PREFIX)
    if not marker:
        return None

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate guard field')
            result[key] = value
        return result

    try:
        if len(message.encode('utf-8')) >= MAX_FIELD:
            return None
        value = json.loads(payload, object_pairs_hook=unique_object)
    except (ValueError, UnicodeError, RecursionError):
        return None
    fields = {'v', 'seq', 'event', 'reason', 'pid', 'page', 'target_valid',
              'backend_va', 'target_va', 'length', 'host_page_size', 'memfd_fd',
              'memfd_dev', 'memfd_ino', 'memfd_size', 'guard_fd', 'errno', 'elapsed_ns'}
    if not isinstance(value, dict) or set(value) != fields:
        return None
    maximum = (1 << 64) - 1
    bounds = {key: (0, maximum) for key in
              ('page', 'backend_va', 'target_va', 'memfd_dev', 'memfd_ino', 'elapsed_ns')}
    bounds.update(v=(1, 1), seq=(1, 3), pid=(1, (1 << 31) - 1), length=(4096, 4096),
                  host_page_size=(1, (1 << 31) - 1), memfd_fd=(-1, (1 << 31) - 1),
                  memfd_size=(-(1 << 63), (1 << 63) - 1), guard_fd=(-1, (1 << 31) - 1),
                  errno=(-1, 4095))
    if any(type(value[key]) is not int or not low <= value[key] <= high
           for key, (low, high) in bounds.items()):
        return None
    if type(value['target_valid']) is not bool:
        return None
    if value['target_valid']:
        if (value['page'] % 4096 or not value['backend_va'] or value['backend_va'] % 4096
                or value['target_va'] != value['backend_va'] + value['page']
                or value['target_va'] + 4096 > maximum
                or value['page'] + 4096 > value['memfd_size']):
            return None
    elif value['page'] != 0 or value['target_va'] != 0:
        return None
    event = value['event']
    if not isinstance(event, str) or not isinstance(value['reason'], str):
        return None
    if event == 'attempt':
        valid = (value['seq'] == 1 and value['reason'] == 'success' and value['errno'] == 0
                 and value['guard_fd'] == -1 and value['memfd_dev'] == value['memfd_ino'] == 0)
    elif event == 'armed':
        valid = (value['seq'] == 2 and value['reason'] == 'success' and value['errno'] == 0
                 and value['target_valid'] and value['guard_fd'] >= 0 and value['host_page_size'] == 4096)
    elif event == 'failed':
        valid = (value['seq'] == 2 and value['errno'] != 0 and value['reason'] in
                 {'target', 'page_size', 'address', 'stat', 'create', 'api', 'features', 'register'})
    elif event == 'closed':
        valid = (value['seq'] == 3 and value['reason'] == 'close' and value['target_valid']
                 and value['guard_fd'] >= 0 and value['host_page_size'] == 4096)
    else:
        valid = False
    return value if valid else None


def guard_journal(arguments, deadline, count):
    """Read a bounded prefix, sharing the original per-SID journal deadline."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise subprocess.TimeoutExpired('journalctl', 3)
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        result = subprocess.run(
            ['journalctl', *arguments, '--no-pager', '-o', 'json', '-n', '+' + str(count),
             '--output-fields=' + GUARD_JOURNAL_FIELDS], stdin=subprocess.DEVNULL,
            stdout=output, stderr=errors, timeout=remaining)
        output.seek(0)
        # A malformed long entry cannot displace the start of the fault stack.
        data = output.read(256 * 1024)
        errors.seek(0)
        if result.returncode and not (result.returncode == 1 and not data and not errors.read(1)):
            raise RuntimeError('journal query failed')
    if data and not data.endswith(b'\n'):
        data = data.rpartition(b'\n')[0]
    return [parse_line(line) for line in data.decode('utf-8', errors='replace').splitlines()[:count]
            if len(line) < 16384]


def memory_shmem_allocation(message):
    """Validate the bounded kernel-allocation schema without forwarding raw text."""
    if not isinstance(message, str) or len(message) >= MAX_SHMEM_MESSAGE:
        return None
    _, marker, payload = message.partition(SHMEM_PREFIX)
    if not marker:
        return None

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate allocation field')
            result[key] = value
        return result

    try:
        if len(message.encode('utf-8')) >= MAX_SHMEM_MESSAGE:
            return None
        value = json.loads(payload, object_pairs_hook=unique_object)
    except (ValueError, UnicodeError, RecursionError):
        return None
    if not isinstance(value, dict):
        return None
    event = value.get('event')
    fields = {'v', 'seq', 'event', 'pid', 'page', 'memfd_dev', 'memfd_ino'}
    event_fields = {
        'armed': {'window_start', 'window_end', 'page_size', 'max_records'},
        'failed': {'stage', 'errno'},
        'insert': {'inserter_pid', 'inserter_tid', 'pid_scope', 'comm', 'index', 'nr_pages',
                   'ktime_ns', 'result', 'stack_errno', 'frames'},
        'closed': {'matched', 'emitted', 'dropped', 'pending', 'nested',
                   'entry_missed', 'return_missed', 'errno'},
    }
    if not isinstance(event, str) or event not in event_fields or set(value) != fields | event_fields[event]:
        return None

    def integer(key, low, high):
        return type(value.get(key)) is int and low <= value[key] <= high

    maximum = (1 << 64) - 1
    if (not integer('v', 1, 1) or not integer('seq', 1, MAX_SHMEM_RECORDS + MAX_SHMEM_LIFECYCLE)
            or not integer('pid', 1, (1 << 31) - 1)
            or any(not integer(key, 0, maximum) for key in ('page', 'memfd_dev', 'memfd_ino'))):
        return None
    if event == 'failed':
        if (value['seq'] != 1 or not isinstance(value['stage'], str)
                or value['stage'] not in {'parse', 'identity', 'btf', 'maps', 'program',
                                           'attach_return', 'attach_entry', 'reader'}
                or not integer('errno', -1, 4095)
                or value['page'] not in (0, TRACE_PAGE)):
            return None
        return value
    if value['page'] != TRACE_PAGE or value['memfd_ino'] == 0:
        return None
    if event == 'armed':
        if (value['seq'] != 1 or not integer('page_size', 4096, 4096)
                or not integer('max_records', MAX_SHMEM_RECORDS, MAX_SHMEM_RECORDS)
                or not integer('window_start', max(0, TRACE_PAGE - 8192), max(0, TRACE_PAGE - 8192))
                or not integer('window_end', TRACE_PAGE + 4096, TRACE_PAGE + 8192)
                or value['window_end'] % 4096):
            return None
    elif event == 'insert':
        if (not integer('seq', 2, MAX_SHMEM_RECORDS + 1)
                or any(not integer(key, 0, (1 << 31) - 1) for key in ('inserter_pid', 'inserter_tid'))
                or value['pid_scope'] != 'initial'
                or not isinstance(value['comm'], str)
                or re.fullmatch(r'[\x20-\x7e]{0,16}', value['comm']) is None
                or not integer('index', 0, maximum // 4096)
                or not integer('nr_pages', 1, 1048576)
                or value['index'] + value['nr_pages'] > 1 << 52
                or not integer('ktime_ns', 0, maximum)
                or not integer('result', -(1 << 31), (1 << 31) - 1)
                or not integer('stack_errno', -4095, 0)):
            return None
        frames = value['frames']
        if (not isinstance(frames, list) or len(frames) > MAX_SHMEM_FRAMES
                or (value['stack_errno'] != 0 and frames)
                or any(not isinstance(frame, str) or not 1 <= len(frame) <= 96
                       or re.fullmatch(r'[A-Za-z0-9_.$:/-]+(?:\+0x[0-9a-fA-F]+)?', frame) is None
                       or re.fullmatch(r'0x[0-9a-fA-F]+(?:\+0x[0-9a-fA-F]+)?', frame) is not None
                       for frame in frames)):
            return None
    else:
        if (not integer('emitted', 0, MAX_SHMEM_RECORDS)
                or value['seq'] != value['emitted'] + 2
                or any(not integer(key, 0, maximum) for key in ('matched', 'dropped', 'nested'))
                or not integer('pending', 0, 64)
                or any(not integer(key, -1, maximum) for key in ('entry_missed', 'return_missed'))
                or not integer('errno', -1, 4095)):
            return None
    return value


def shmem_allocation_evidence(sid, run_id, deadline):
    """One authenticated backend identity, at most 16 insertions and two lifecycle records."""
    if not run_id:
        return []
    entries = guard_journal([
        'KUASAR_SANDBOX_ID=' + sid, 'KUASAR_RUN_ID=' + run_id,
        'SYSLOG_IDENTIFIER=sandbox-ctl', '--grep=' + SHMEM_PREFIX], deadline,
        MAX_SHMEM_RECORDS + MAX_SHMEM_LIFECYCLE)
    records, identity, armed, last_sequence = [], None, None, 0
    for entry in entries:
        if (entry.get('KUASAR_SANDBOX_ID') != sid or entry.get('KUASAR_RUN_ID') != run_id
                or entry.get('SYSLOG_IDENTIFIER') != 'sandbox-ctl' or entry.get('_TRANSPORT') != 'journal'):
            continue
        record = memory_shmem_allocation(entry.get('MESSAGE'))
        if record is None:
            continue
        origin = guard_origin(entry, record['pid'])
        if origin is None:
            continue
        candidate = (*origin, *(record[key] for key in ('pid', 'page', 'memfd_dev', 'memfd_ino')))
        if identity is None:
            if record['event'] not in ('armed', 'failed'):
                continue
            identity = candidate
        if candidate != identity or record['seq'] <= last_sequence:
            continue
        if record['event'] == 'insert':
            start, end = record['index'] * 4096, (record['index'] + record['nr_pages']) * 4096
            if armed is None or start >= armed['window_end'] or end <= armed['window_start']:
                continue
        elif record['event'] == 'closed' and armed is None:
            continue
        records.append(dict(lifecycle_diagnostic='uffd_shmem_allocation', sid=sid, run_id=run_id, **record))
        last_sequence = record['seq']
        if record['event'] == 'armed':
            armed = record
        if record['event'] in ('failed', 'closed'):
            break
    return records


def guard_origin(entry, pid):
    """Only trusted journal metadata can bind a backend's native log to fd 2."""
    boot, unit, cursor = (entry.get(key) for key in ('_BOOT_ID', '_SYSTEMD_UNIT', '__CURSOR'))
    if (entry.get('_PID') != str(pid) or not isinstance(boot, str)
            or not re.fullmatch(r'[0-9a-f]{32}', boot) or not isinstance(unit, str)
            or not re.fullmatch(r'[A-Za-z0-9@_.:\\-]{1,256}', unit)
            or not isinstance(cursor, str) or not re.fullmatch(r'[A-Za-z0-9=;_-]{1,512}', cursor)):
        return None
    return (boot, unit)


def fault_stack(entries, origin, pid, stream_id, sid, run_id):
    """Project the first Go stack only: symbols and source basenames, no args."""
    frames, started, pending, lines = [], False, None, 0
    for entry in entries:
        if (guard_origin(entry, pid) != origin or entry.get('_STREAM_ID') != stream_id
                or entry.get('_TRANSPORT') != 'stdout'
                or entry.get('KUASAR_SANDBOX_ID', sid) != sid
                or entry.get('KUASAR_RUN_ID', run_id) != run_id):
            continue
        message = entry.get('MESSAGE')
        if not isinstance(message, str) or len(message) >= MAX_FIELD:
            continue
        for line in message.splitlines():
            lines += 1
            if lines > MAX_FAULT_LINES:
                return frames
            if re.fullmatch(r'goroutine [0-9]+(?: [^\r\n]{0,200})? \[[^\]\r\n]{1,80}\]:', line):
                if started:
                    return frames
                started = True
                continue
            if line == 'runtime stack:':
                if started:
                    return frames
                started = True
                continue
            if not started:
                continue
            if line.startswith('created by '):
                return frames
            location = re.fullmatch(
                r'\s+(?:[A-Za-z0-9_./@-]+/)?([A-Za-z0-9_.-]+\.(?:go|s)):([0-9]{1,10})'
                r'(?: \+0x[0-9a-fA-F]+)?(?: (?:fp|sp|pc)=0x[0-9a-fA-F]{1,16}){0,3}', line)
            if pending and location:
                frames.append(dict(function=pending, file=location[1][:80], line=int(location[2])))
                pending = None
                if len(frames) >= MAX_FAULT_FRAMES:
                    return frames
                continue
            symbol = re.match(
                r'^([A-Za-z0-9_/.$\[\],*-]+(?:\(\*?[A-Za-z0-9_/.\[\],*-]+\)[A-Za-z0-9_/.$\[\],*-]+)?)\(', line)
            pending = symbol[1][:128] if symbol else None
    return frames


def backend_guard_evidence(sid, run_id, deadline):
    """One backend identity, three producer records and at most one fault record."""
    if not run_id:
        return []
    entries = guard_journal([
        'KUASAR_SANDBOX_ID=' + sid, 'KUASAR_RUN_ID=' + run_id,
        'SYSLOG_IDENTIFIER=sandbox-ctl', '--grep=' + GUARD_PREFIX], deadline, MAX_GUARD_RECORDS)
    records, selected, seen, armed, closed = [], None, set(), None, False
    for entry in entries:
        if (entry.get('KUASAR_SANDBOX_ID') != sid or entry.get('KUASAR_RUN_ID') != run_id
                or entry.get('SYSLOG_IDENTIFIER') != 'sandbox-ctl' or entry.get('_TRANSPORT') != 'journal'):
            continue
        record = memory_backend_guard(entry.get('MESSAGE'))
        if record is None or entry.get('_PID') != str(record['pid']):
            continue
        if record['target_valid'] and record['page'] != TRACE_PAGE:
            continue
        identity = tuple(record[key] for key in
                         ('pid', 'page', 'backend_va', 'target_va', 'target_valid', 'memfd_fd', 'memfd_size'))
        identity += (entry.get('_BOOT_ID'), entry.get('_SYSTEMD_UNIT'))
        if selected is None:
            selected = identity
        if identity != selected or record['seq'] in seen:
            continue
        if record['event'] == 'closed' and armed and any(
                record[key] != armed[1][key] for key in ('memfd_dev', 'memfd_ino', 'guard_fd')):
            continue
        seen.add(record['seq'])
        records.append(dict(lifecycle_diagnostic='uffd_backend_guard', sid=sid, run_id=run_id, **record))
        if record['event'] == 'armed':
            armed = (entry, record)
        closed |= record['event'] == 'closed'
    if armed is None or closed:
        return records
    entry, record = armed
    fault = dict(lifecycle_diagnostic='uffd_backend_fault', sid=sid, run_id=run_id,
                 pid=record['pid'], page=record['page'], target_va=record['target_va'],
                 status='unavailable', reason='identity', frames=[])
    records.append(fault)
    origin = guard_origin(entry, record['pid'])
    if origin is None:
        return records
    matches = ['_BOOT_ID=' + origin[0], '_SYSTEMD_UNIT=' + origin[1], '_PID=' + str(record['pid'])]
    try:
        signals = guard_journal([*matches, '--after-cursor=' + entry['__CURSOR'],
                                 '--grep=' + SIGBUS_LINE.pattern], deadline, 1)
        fault['reason'] = 'missing'
        for signal in signals:
            message, stream_id = signal.get('MESSAGE'), signal.get('_STREAM_ID')
            match = SIGBUS_LINE.fullmatch(message) if isinstance(message, str) else None
            if (not match or guard_origin(signal, record['pid']) != origin
                    or signal.get('_TRANSPORT') != 'stdout' or not isinstance(stream_id, str)
                    or not re.fullmatch(r'[0-9a-f]{32}', stream_id)
                    or signal.get('KUASAR_SANDBOX_ID', sid) != sid
                    or signal.get('KUASAR_RUN_ID', run_id) != run_id):
                continue
            code, address, pc = (int(part, 16 if part.startswith('0x') else 10) for part in match.groups())
            if code > (1 << 32) - 1 or address >= 1 << 64 or pc >= 1 << 64:
                continue
            fault.update(status='sigbus', reason='stack_missing', signal_code=code,
                         fault_address=address, pc=pc,
                         target_match=record['target_va'] <= address < record['target_va'] + 4096)
            stack = guard_journal([*matches, '_STREAM_ID=' + stream_id,
                                   '--after-cursor=' + signal['__CURSOR']], deadline, MAX_FAULT_LINES)
            fault['frames'] = fault_stack(stack, origin, record['pid'], stream_id, sid, run_id)
            if fault['frames']:
                fault['reason'] = 'captured'
    except Exception as error:
        fault['reason'] = 'timeout' if isinstance(error, subprocess.TimeoutExpired) else 'query'
    return records


def memory_page_trace(message):
    """Accept only the one-page trace schema, never arbitrary runtime fields."""
    if not isinstance(message, str):
        return None
    _, marker, payload = message.partition(TRACE_PREFIX)
    if not marker or len(message) >= MAX_FIELD:
        return None

    def unique_object(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError('duplicate trace field')
            result[key] = item
        return result

    try:
        if len(message.encode('utf-8')) >= MAX_FIELD:
            return None
        value = json.loads(payload, object_pairs_hook=unique_object)
    except (ValueError, UnicodeError, RecursionError):
        return None
    if not isinstance(value, dict):
        return None
    version = value.get('v')
    if type(version) is not int or version not in (1, 2):
        return None
    event = value.get('event')
    fields = {'v', 'page', 'seq', 'event'}
    if version == 2:
        fields |= {'elapsed_ns', 'pid', 'handler'}
    if event == 'summary':
        fields |= {'recorded', 'dropped'}
    elif event in ('seed', 'fault', 'source', 'copy', 'zero', 'remove', 'load-hit'):
        fields |= {'start', 'length', 'state'}
        if event == 'source':
            fields.add('kind')
        elif event in ('copy', 'zero'):
            fields |= {'sha256', 'non_ff', 'done', 'errno'}
        elif event == 'remove' and version == 2:
            fields.add('kernel_event')
    elif event == 'register' and version == 2:
        fields |= {'start', 'length', 'state', 'uffd_fd', 'backend_va', 'memfd_fd',
                   'memfd_dev', 'memfd_ino', 'memfd_size', 'kernel_release',
                   'ch_va', 'ch_pid', 'peer_errno', 'memfd_errno', 'kernel_errno', 'maps_errno'}
        if value.get('maps_errno') == 0:
            fields |= {'maps_va', 'maps_size', 'maps_offset', 'maps_dev', 'maps_ino', 'maps_match'}
    elif event == 'observe' and version == 2:
        fields |= {'start', 'length', 'state', 'phase', 'mincore_errno',
                   'seek_data_offset', 'seek_hole_offset', 'seek_data_errno', 'seek_hole_errno'}
        if value.get('mincore_errno') == 0:
            fields.add('resident')
        if (value.get('seek_hole_errno') == 0
                and value.get('seek_data_errno') in (0, errno.ENXIO)):
            fields.add('extent_data')
    else:
        return None
    if set(value) != fields:
        return None

    def integer(key, low, high):
        return type(value.get(key)) is int and low <= value[key] <= high

    maximum = (1 << 64) - 1
    if not integer('page', TRACE_PAGE, TRACE_PAGE) or not integer('seq', 1, maximum):
        return None
    if version == 2 and (not integer('elapsed_ns', 0, maximum)
                         or not integer('pid', 1, (1 << 31) - 1)
                         or not integer('handler', 1, maximum)):
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
        if event == 'remove' and version == 2 and value['kernel_event'] not in ('remove', 'unmap'):
            return None
        if event == 'register':
            if (not integer('uffd_fd', 1, (1 << 31) - 1)
                    or not integer('memfd_fd', 1, (1 << 31) - 1)
                    or not integer('ch_pid', 0, (1 << 31) - 1)
                    or any(not integer(key, 0, maximum) for key in (
                        'backend_va', 'memfd_dev', 'memfd_ino', 'memfd_size', 'ch_va'))
                    or any(not integer(key, -1, 4095) for key in (
                        'peer_errno', 'memfd_errno', 'kernel_errno', 'maps_errno'))):
                return None
            release = value['kernel_release']
            if (not isinstance(release, str) or len(release) > 64
                    or (value['kernel_errno'] == 0 and not release)
                    or (value['kernel_errno'] != 0 and release != '')):
                return None
            if value['maps_errno'] == 0 and (
                    any(not integer(key, 0, maximum) for key in (
                        'maps_va', 'maps_size', 'maps_offset', 'maps_dev', 'maps_ino'))
                    or type(value['maps_match']) is not bool):
                return None
        if event == 'observe':
            if (value['start'] != TRACE_PAGE or value['length'] != 4096
                    or value['phase'] not in ('before-ack', 'close')
                    or any(not integer(key, -1, 4095) for key in (
                        'mincore_errno', 'seek_data_errno', 'seek_hole_errno'))
                    or any(not integer(key, -1, (1 << 63) - 1) for key in (
                        'seek_data_offset', 'seek_hole_offset'))):
                return None
            if value['mincore_errno'] == 0 and type(value['resident']) is not bool:
                return None
            if 'extent_data' in value and type(value['extent_data']) is not bool:
                return None
            for operation in ('data', 'hole'):
                offset, error = value[f'seek_{operation}_offset'], value[f'seek_{operation}_errno']
                if (error == 0 and offset < 0) or (error != 0 and offset != -1):
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
    guard_emitted = 0
    guard_candidate, guard_priority = [], (-1, -1)
    shmem_emitted, shmem_lifecycle_emitted = 0, 0
    shmem_candidate, shmem_priority = [], (-1, -1)
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

    def emit_guard(record):
        nonlocal guard_emitted
        if guard_emitted >= MAX_GUARD_RECORDS:
            return
        try:
            print(json.dumps(record, sort_keys=True), file=stream, flush=True)
        except Exception:
            pass
        guard_emitted += 1

    def emit_shmem(record):
        nonlocal shmem_emitted, shmem_lifecycle_emitted
        insertion = record['event'] == 'insert'
        if ((insertion and shmem_emitted >= MAX_SHMEM_RECORDS)
                or (not insertion and shmem_lifecycle_emitted >= MAX_SHMEM_LIFECYCLE)):
            return
        try:
            encoded = json.dumps(record, sort_keys=True, separators=(',', ':'))
            if len(encoded.encode('utf-8')) >= MAX_SHMEM_MESSAGE:
                return
            print(encoded, file=stream, flush=True)
        except Exception:
            pass
        if insertion:
            shmem_emitted += 1
        else:
            shmem_lifecycle_emitted += 1

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
            journal_deadline = time.monotonic() + 3
            with tempfile.TemporaryFile() as output:
                result = subprocess.run(
                    ['journalctl', f'KUASAR_SANDBOX_ID={sid}', '--no-pager', '-n', '200', '-o', 'json'],
                    stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.DEVNULL, timeout=3)
                if result.returncode:
                    emit('journal', dict(sid=sid, unavailable='exit', exit_code=result.returncode))
                    continue
                entries = deque(maxlen=12)
                faults = {}
                record = states.get(sid, {})
                current_run = (record.get('run_id') or record.get('dead_run_id')
                               or record.get('sandbox_result_run_id'))
                known_runs = tuple(dict.fromkeys(run for run in
                                  (current_run, record.get('dead_run_id'), record.get('sandbox_result_run_id'))
                                  if run))
                for line in tail_lines(output):
                    entry = parse_line(line)
                    message = entry.get('MESSAGE')
                    if entry.get('KUASAR_SANDBOX_ID') != sid or not isinstance(message, str):
                        continue
                    if 'uffd-backend-guard' in message or 'uffd-shmem-allocation' in message:
                        # Immediate evidence has its own queries and quotas;
                        # even malformed records must not leak through raw errors.
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
                        if fault is not None and entry.get('KUASAR_RUN_ID') in known_runs:
                            faults[entry['KUASAR_RUN_ID']] = fault
                for entry in entries:
                    emit('journal', dict(sid=sid, **entry))
                fault_entry = next(((run, faults[run]) for run in known_runs if run in faults), None)
                evidence_run = fault_entry[0] if fault_entry else current_run
                allocations = []
                try:
                    allocations = shmem_allocation_evidence(sid, evidence_run, journal_deadline)
                except Exception:
                    # Diagnostic queries share this SID's original deadline;
                    # they cannot consume the business-record budget.
                    pass
                priority = (int(fault_entry is not None),
                            2 if any(row['event'] == 'insert' for row in allocations)
                            else 1 if allocations else 0)
                if priority > shmem_priority:
                    shmem_candidate, shmem_priority = allocations, priority
                guards = []
                try:
                    guards = backend_guard_evidence(sid, evidence_run, journal_deadline)
                except Exception:
                    pass
                priority = (int(fault_entry is not None),
                            3 if any(row.get('status') == 'sigbus' for row in guards)
                            else 2 if any(row.get('event') == 'armed' for row in guards)
                            and not any(row.get('event') == 'closed' for row in guards)
                            else 1 if guards else 0)
                if priority > guard_priority:
                    guard_candidate, guard_priority = guards, priority
                # A previous failed run can identify the diagnostic producer,
                # but its snapshot source is not necessarily the current one.
                if (memory_probe and not page_probe_used and fault_entry
                        and fault_entry[0] == current_run):
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

    # Select after all owned SIDs: a live secondary run must not displace the
    # closed backend belonging to the actual held-memory failure.
    for allocation in shmem_candidate:
        emit_shmem(allocation)
    for guard in guard_candidate:
        emit_guard(guard)

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
