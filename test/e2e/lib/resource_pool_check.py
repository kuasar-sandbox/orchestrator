import json
import pathlib
import sqlite3
import subprocess
import sys

mode, root, *args = sys.argv[1:]
root = pathlib.Path(root)
with sqlite3.connect(root / 'lib/node-ctl.db', timeout=5) as db:
    owners = [json.loads(x[0]) for x in db.execute('select ownership from run_pool_ownership')]
    rows = {x[0]: x for x in db.execute('select id,state,run_id,resource_obligation from sandboxes')}
if mode == 'owner':
    sid = args[0]
    row = rows[sid]
    owner = next(x for x in owners if x['run_id'] == row[2])
    print(owner['resource_pool'], owner['run_id'], owner['unit'])
elif mode == 'ledger':
    want = dict(zip(('numa0', 'numa1'), map(int, args)))
    status = json.loads(subprocess.check_output([str(root / 'orch-bin/node-ctl'), 'resource', 'pressure', '--socket', str(root / 'node-ctl.socket')]))
    for pool in status['pools']:
        n = want[pool['name']]
        assert pool['scheduled'] == {'memory_bytes': n * 2560 * 1024**2, 'cpu_milli': n * 2000}, pool
        assert pool['Reserved']['cpu_milli'] == n * 2000, pool
        assert pool['ReservationCount'] == n, pool
        assert pool['ProvisionalCount'] == 0, pool
    for sid, row in rows.items():
        if row[1] == 'running':
            assert row[3] == 0, row
            owner = next(x for x in owners if x['run_id'] == row[2])
            assert owner['task_id'] == sid, owner
    print(json.dumps(status, indent=2))
elif mode == 'placement':
    sid, node, cpus = args
    owner = next(x for x in owners if x['run_id'] == rows[sid][2])
    unit = owner['unit']
    cg = subprocess.check_output(['systemctl', 'show', '--value', '-p', 'ControlGroup', unit], text=True).strip()
    pids = (pathlib.Path('/sys/fs/cgroup' + cg) / 'vmm/cgroup.procs').read_text().split()
    found = False
    for pid in pids:
        # A cgroup namespace may report PID 0 for tasks outside this PID namespace.
        if pid == '0':
            continue
        proc = pathlib.Path('/proc') / pid
        try:
            comm = (proc / 'comm').read_text()
        except FileNotFoundError:
            continue  # exited between cgroup and proc snapshots
        if 'cloud-hyperviso' not in comm:
            continue
        fields = dict(line.split(':', 1) for line in (proc / 'status').read_text().splitlines())
        def cpu_set(value):
            result = set()
            for part in value.strip().split(','):
                a, _, b = part.partition('-')
                result.update(range(int(a), int(b or a) + 1))
            return result
        assert cpu_set(fields['Cpus_allowed_list']) == cpu_set(cpus), fields['Cpus_allowed_list']
        assert fields['Mems_allowed_list'].strip() == node, fields['Mems_allowed_list']
        maps = (proc / 'numa_maps').read_text()
        assert any(('bind:' + node + ' ') in line and ('N' + node + '=') in line for line in maps.splitlines()), maps
        anonymous_pages = {}
        for line in maps.splitlines():
            if ' anon=' not in line:
                continue
            for field in line.split():
                key, sep, value = field.partition('=')
                if sep and key.startswith('N') and key[1:].isdigit():
                    anonymous_pages[key] = anonymous_pages.get(key, 0) + int(value)
        assert anonymous_pages.get('N' + node, 0) > 256, anonymous_pages
        print(json.dumps({'sandbox': sid, 'unit': unit, 'vmm_pid': pid, 'cpu_mask': cpus, 'numa_node': node, 'anonymous_pages': anonymous_pages}))
        found = True
    assert found, f'no Cloud Hypervisor in {cg}/vmm'
else:
    raise SystemExit(mode)
