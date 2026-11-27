#!/usr/bin/env python3
"""Observe the exact low-headroom runner's CH shutdown without unit-name attribution."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid


JOURNAL_LIMIT = 512
ESCALATIONS = (
    "CH reached the shutdown deadline, sending SIGKILL",
    "while shutdown in progress, sending SIGKILL now",
    "memory.high lifecycle lock remained busy; sending SIGKILL to fence potential capture",
    "CH didn't exit within",
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def process_identity(proc, pid):
    require(type(pid) is int and pid > 1, "invalid process PID")
    directory = Path(proc) / str(pid)
    raw = (directory / 'stat').read_text()
    fields = raw[raw.rindex(')') + 2:].split()
    require(len(fields) >= 20 and fields[0] != 'Z', "process is not live")
    unified = [line[3:] for line in (directory / 'cgroup').read_text().splitlines()
               if line.startswith('0::')]
    require(len(unified) == 1, "missing unified process cgroup")
    executable = (directory / 'exe').stat()
    return {'pid': pid, 'ppid': int(fields[1]), 'start_ticks': int(fields[19]),
            'exe_device': executable.st_dev, 'exe_inode': executable.st_ino,
            'cgroup': unified[0]}


def same_executable(identity, path):
    executable = Path(path).stat()
    return (identity['exe_device'], identity['exe_inode']) == (executable.st_dev, executable.st_ino)


def vmm_members(proc, members_file, ch_binary):
    members = {int(value) for value in members_file.read_text().split()}
    require(0 < len(members) <= 256 and min(members) >= 0, 'invalid VMM member set')
    candidates, workers = [], []
    for pid in sorted(members):
        if pid == 0:
            # A kernel task outside this PID namespace has no visible /proc entry.
            continue
        directory = Path(proc) / str(pid)
        try:
            (directory / 'exe').stat()
        except FileNotFoundError:
            worker = re.fullmatch(r'kvm-nx-lpage-recovery-([1-9][0-9]*)',
                                 (directory / 'comm').read_text().strip())
            require(worker, 'unrecognized VMM kernel process')
            workers.append((pid, int(worker[1])))
        else:
            row = process_identity(proc, pid)
            require(same_executable(row, ch_binary), 'unexpected VMM userspace process')
            candidates.append(row)
    require(len(candidates) == 1, 'expected one prepared CH process in the VMM cgroup')
    ch = candidates[0]
    for pid, creator_tid in workers:
        thread = Path(proc) / str(ch['pid']) / 'task' / str(creator_tid) / 'exe'
        require(thread.exists() and os.path.samefile(thread, ch_binary),
                'VMM NX worker is not associated with the prepared CH')
    return members, ch, workers


def capture_identity(run_id, sandbox_id, control_group, parent_pid, sandbox_binary, ch_binary,
                     *, proc=Path('/proc'), cgroup_root=Path('/sys/fs/cgroup')):
    for value in (run_id, sandbox_id):
        require(isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', value),
                'invalid run or sandbox identity')
    require(control_group.startswith('/') and control_group != '/' and
            '..' not in Path(control_group).parts, 'invalid runner cgroup')
    members_file = Path(cgroup_root) / control_group.lstrip('/') / 'vmm/cgroup.procs'
    members, ch, workers = vmm_members(proc, members_file, ch_binary)
    runtime = process_identity(proc, ch['ppid'])
    parent = process_identity(proc, parent_pid)
    require(same_executable(runtime, sandbox_binary), 'CH parent is not the prepared sandbox-ctl')
    require(runtime['ppid'] == parent_pid, 'sandbox-ctl does not belong to the runner parent')
    require(ch['cgroup'] == control_group + '/vmm' and
            runtime['cgroup'] == parent['cgroup'] == control_group + '/ctl',
            'runner, sandbox-ctl or CH cgroup identity differs')
    for row in (parent, runtime, ch):
        require(process_identity(proc, row['pid']) == row, 'process identity changed during capture')
    require(vmm_members(proc, members_file, ch_binary) == (members, ch, workers),
            'VMM members or identities changed during capture')
    return {'schema': 1, 'run_id': run_id, 'sandbox_id': sandbox_id,
            'boot_id': uuid.UUID((Path(proc) / 'sys/kernel/random/boot_id').read_text().strip()).hex,
            'control_group': control_group, 'parent': parent, 'runtime': runtime, 'ch': ch,
            'vmm_members': sorted(members), 'nx_workers': workers}


def journal_fields(identity):
    require(identity.get('schema') == 1, 'invalid captured identity schema')
    require(re.fullmatch(r'[0-9a-f]{32}', identity.get('boot_id', '')), 'invalid captured boot identity')
    for field in ('run_id', 'sandbox_id'):
        require(re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', identity.get(field, '')), 'invalid captured task identity')
    runtime_pid = identity['runtime']['pid']
    ch_pid = identity['ch']['pid']
    require(type(runtime_pid) is int and runtime_pid > 1 and type(ch_pid) is int and ch_pid > 1,
            'invalid captured process identity')
    return {'_BOOT_ID': identity['boot_id'], '_TRANSPORT': 'journal', 'SYSLOG_IDENTIFIER': 'sandbox-ctl',
            'KUASAR_RUN_ID': identity['run_id'], 'KUASAR_SANDBOX_ID': identity['sandbox_id'],
            '_PID': str(runtime_pid)}


def inspect_journal(raw, identity):
    expected = journal_fields(identity)
    lines = [line for line in raw.splitlines() if line.strip()]
    require(len(lines) < JOURNAL_LIMIT, 'journal observation reached its row bound')
    started, reaped, terminal, terminated = [], [], [], []
    for position, line in enumerate(lines):
        row = json.loads(line)
        require(isinstance(row, dict) and all(row.get(key) == value for key, value in expected.items()),
                'journal row differs from the captured sender/task/boot identity')
        message = row.get('MESSAGE')
        require(isinstance(message, str), 'invalid journal message')
        require(not any(phrase in message for phrase in ESCALATIONS), 'CH shutdown escalated to SIGKILL')
        if '[sandbox-ctl] received terminated' in message:
            terminated.append(position)
        match = re.search(r'\[sandbox-ctl\] CH started pid=([0-9]+)$', message)
        require('[sandbox-ctl] CH started pid=' not in message or match, 'malformed CH startup record')
        if match:
            require(int(match[1]) == identity['ch']['pid'], 'journal started a different CH PID')
            started.append(position)
        match = re.search(r'\[sandbox-ctl\] CH process reaped: (.+)$', message)
        require('[sandbox-ctl] CH process reaped:' not in message or match, 'malformed CH reaping record')
        if match:
            outcome = match[1]
            if outcome == '<nil>':
                code = 0
            elif outcome == 'signal: terminated':
                code = -1
            elif status := re.fullmatch(r'exit status ([0-9]+)', outcome):
                code = int(status[1])
                require(1 <= code <= 255, 'invalid ordinary CH exit status')
            else:
                raise ValueError('CH reaping was killed or has an unknown outcome')
            reaped.append((position, code, outcome))
        match = re.search(r'\[sandbox-ctl\] CH exited code=(-?[0-9]+)$', message)
        require('[sandbox-ctl] CH exited code=' not in message or match, 'malformed CH terminal record')
        if match:
            terminal.append((position, int(match[1])))
    require(len(started) <= 1 and len(reaped) <= 1 and len(terminal) <= 1,
            'duplicate CH lifecycle records')
    if reaped and terminal:
        require(reaped[0][1] == terminal[0][1], 'CH reaped and terminal exit codes disagree')
        require(reaped[0][0] < terminal[0][0], 'CH terminal record precedes reaping')
    if started and reaped:
        require(started[0] < reaped[0][0], 'CH reaping precedes its captured startup')
    complete = bool(started and reaped and terminal and terminated)
    return {'complete': complete, 'rows': len(lines), 'started': bool(started),
            'reaped': reaped[0][2] if reaped else None, 'exit_code': terminal[0][1] if terminal else None,
            'terminated': bool(terminated)}


def observe(identity, journal_path, proof_path, timeout=5, *, deadline=None,
            cgroup_root=Path('/sys/fs/cgroup'), run=subprocess.run,
            monotonic=time.monotonic, sleep=time.sleep):
    require(0 < timeout <= 5, 'invalid journal observation budget')
    start = monotonic()
    deadline = start + timeout if deadline is None else deadline
    require(deadline <= start + timeout, 'shutdown deadline exceeds the original budget')
    fields = journal_fields(identity)
    group = identity['control_group']
    require(group.startswith('/') and group != '/' and '..' not in Path(group).parts,
            'invalid captured runner cgroup')
    vmm = Path(cgroup_root) / group.lstrip('/') / 'vmm'
    command = ['journalctl', '--no-pager', '--quiet', '--output=json', f'--lines={JOURNAL_LIMIT}',
               *(f'{key}={value}' for key, value in fields.items())]
    proof = {'schema': 1, 'identity': identity, 'conclusion': 'failure', 'journal_reads': 0,
             'shutdown_deadline_monotonic': deadline, 'remaining_budget_at_entry': deadline - start}
    raw = b''
    try:
        while True:
            remaining = deadline - monotonic()
            proof['vmm_cgroup_absent'] = not vmm.exists()
            require(remaining > 0 or proof['vmm_cgroup_absent'], 'vmm cgroup remained after StopUnit')
            require(remaining > 0, 'required CH shutdown evidence did not become visible')
            proof['journal_reads'] += 1
            try:
                completed = run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=remaining)
            except subprocess.TimeoutExpired as error:
                raw = error.output or b''
                Path(journal_path).write_bytes(raw)
                Path(str(journal_path) + '.stderr.log').write_bytes(error.stderr or b'')
                raise
            raw = completed.stdout
            Path(journal_path).write_bytes(raw)
            Path(str(journal_path) + '.stderr.log').write_bytes(completed.stderr)
            if proof['journal_reads'] == 1:
                Path(str(journal_path) + '.initial.journal').write_bytes(raw)
                proof['initial_journal_sha256'] = hashlib.sha256(raw).hexdigest()
            require(completed.returncode == 0, 'journalctl failed')
            proof['observation'] = None
            proof['observation'] = inspect_journal(raw, identity)
            proof['vmm_cgroup_absent'] = not vmm.exists()
            remaining = deadline - monotonic()
            if proof['observation']['complete'] and proof['vmm_cgroup_absent'] and remaining > 0:
                proof['conclusion'] = 'success'
                return proof
            if remaining > 0:
                sleep(min(.05, remaining))
    except Exception as error:
        proof['error'] = {'type': type(error).__name__, 'message': str(error)[:1024]}
        raise
    finally:
        proof['wall_seconds'] = monotonic() - start
        proof['journal_sha256'] = hashlib.sha256(raw).hexdigest()
        Path(proof_path).write_text(json.dumps(proof, sort_keys=True) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    capture = sub.add_parser('capture')
    for name in ('run-id', 'sandbox-id', 'control-group', 'sandbox-binary', 'ch-binary', 'identity'):
        capture.add_argument('--' + name, required=True)
    capture.add_argument('--parent-pid', required=True, type=int)
    check = sub.add_parser('observe')
    for name in ('identity', 'journal', 'proof'):
        check.add_argument('--' + name, required=True, type=Path)
    check.add_argument('--deadline', type=float)
    args = parser.parse_args()
    if args.action == 'capture':
        identity = capture_identity(args.run_id, args.sandbox_id, args.control_group, args.parent_pid,
                                    args.sandbox_binary, args.ch_binary)
        Path(args.identity).write_text(json.dumps(identity, sort_keys=True) + '\n')
    else:
        observe(json.loads(args.identity.read_text()), args.journal, args.proof, deadline=args.deadline)


if __name__ == '__main__':
    main()
