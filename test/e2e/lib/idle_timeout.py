"""Real VM idle Pause/Wake assertions and bounded latency/resource evidence."""
import base64
import concurrent.futures
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import sqlite3
import shlex
import statistics
import subprocess
import threading
import time


def run():
    env = os.environ
    sid, work = env['SID'], Path(env['WORK'])
    timeout = int(env['IDLE_TIMEOUT'])
    output = Path(env['CASE_OUT']) / f'idle-timeout-{timeout}.json'
    db_path = work / 'lib/node-ctl.db'
    samples, resources, transitions = [], [], []
    business_passed = False
    stop = threading.Event()
    watched = [int(env['CONDUCTOR_PID']), int(env['PROXY_MASTER_PID'])]

    def state():
        with sqlite3.connect(db_path, timeout=5) as db:
            row = db.execute('select state,run_id,resume_source_kind,resource_obligation,idle_timeout,vswitch_port,run_dir,envd_uds,ci_uds,floatingip,inner_ip,port_mac,base_dir from sandboxes where id=?', (sid,)).fetchone()
        assert row and row[4] == timeout, row
        assert row[3] == 0, 'idle Pause acquired resource responsibility'
        return row

    def await_paused(ended):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            row = state()
            if (row[0] == 'paused' and row[1] == '' and all(value == '' for value in row[5:12])
                    and not (Path(env['EXECUTE_RUN_ROOT'])/'sandboxes'/sid).exists()):
                assert row[12] == str(work/'lib/sandboxes'/sid) and Path(row[12]).is_dir(), row
                assert row[2] == 'snapshot', row  # autoPauseMemory=false is TTL-only.
                return time.monotonic() - ended
            assert row[0] in ('running', 'pausing', 'starting', 'paused'), row
            time.sleep(.02)
        raise AssertionError('automatic Pause did not complete')

    def await_running():
        deadline = time.monotonic() + 10
        while state()[0] != 'running':
            assert time.monotonic() < deadline, 'request did not activate before park deadline'
            time.sleep(.02)

    def monitor():
        previous = None
        while not stop.wait(.1):
            entries = []
            # Restrict evidence to this case's VM and Proxy worker children.
            roles = {watched[0]:'conductor', watched[1]:'proxy-master'}
            for path in Path('/proc').glob('[0-9]*/comm'):
                try:
                    pid = int(path.parent.name)
                    command = path.read_text().strip()
                    raw = (path.parent/'cmdline').read_bytes()
                    if command.startswith('cloud-hyperviso') and (
                            env['EXECUTE_RUN_ROOT'].encode() in raw
                            or str((path.parent/'cwd').resolve()).startswith(env['EXECUTE_RUN_ROOT']+'/')):
                        roles[pid] = 'vm'
                    elif b'proxy' in raw and b'--worker' in raw:
                        stat = (path.parent/'stat').read_text().rsplit(')', 1)[1].split()
                        if int(stat[1]) == watched[1]:
                            roles[pid] = 'proxy-worker'
                except (OSError, ValueError):
                    pass
            try:
                current = state()[0]
                if current != previous:
                    transitions.append(dict(monotonic=time.monotonic(), state=current))
                    previous = current
            except (OSError, sqlite3.Error):
                pass
            for pid, role in roles.items():
                try:
                    stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
                    status = Path(f'/proc/{pid}/status').read_text().splitlines()
                    rss = next((int(x.split()[1]) for x in status if x.startswith('VmRSS:')), 0)
                    entries.append(dict(pid=pid, role=role, cpu_ticks=int(stat[11])+int(stat[12]), rss_kib=rss))
                except (OSError, ValueError):
                    pass
            resources.append(dict(monotonic=time.monotonic(), processes=entries))

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    counter, secret = 0, None
    exec_base = [str(Path(env['BIN']) / 'sandbox-ctl'), 'exec', '--proxy', f"http://127.0.0.1:{env['PROXY_PORT']}",
                 '--proxy-header', f'E2b-Sandbox-Id: {sid}', '--proxy-header', 'E2b-Sandbox-Service: exec',
                 '--proxy-header', 'X-Access-Token: ' + env['EXEC_TOKEN']]

    def execute(script):
        out, err = work / 'idle-exec.stdout.out', work / 'idle-exec.stderr.out'
        # sandbox-ctl's file targets append; each invocation needs fresh sinks.
        out.write_bytes(b''); err.write_bytes(b'')
        result = subprocess.run(exec_base + ['--stdout-to', str(out), '--stderr-to', str(err), '--', '/bin/sh', '-c', script],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        assert result.returncode == 0, f'Native Exec failed ({result.returncode})'
        return out.read_bytes(), err.read_bytes()

    def http(path='/', expected=200):
        conn = HTTPConnection('127.0.0.1', int(env['PROXY_PORT']), timeout=60)
        try:
            conn.request('GET', path, headers={'Host': f"8001-{sid}.{env['DOMAIN']}", 'X-Access-Token': env['FORWARD_TOKEN'], 'Connection': 'close'})
            response = conn.getresponse()
            data = response.read()
            assert response.status == expected, (response.status, data)
            return json.loads(data) if expected == 200 else data
        finally:
            conn.close()

    def verify(value):
        nonlocal counter, secret
        counter += 1
        assert value['count'] == counter, (value['count'], counter)
        if secret is None:
            secret = value['memory']
        assert value['memory'] == secret, 'in-memory identity changed across Pause/Wake'

    # This random identity and request counter exist only in the server process.
    # Restarting the script or replaying any delivered business changes assertions.
    server = r"""
import json,time,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import threading
identity=uuid.uuid4().hex
count=0
lock=threading.Lock()
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  global count
  if self.path=='/health': payload={'ready':True}
  else:
   if self.path=='/slow': time.sleep(4)
   with lock:
    count+=1
    payload={'memory':identity,'count':count}
  body=json.dumps(payload).encode()
  self.send_response(200);self.send_header('Content-Length',str(len(body)));self.send_header('Connection','close');self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
ThreadingHTTPServer(('0.0.0.0',8001),Handler).serve_forever()
"""
    encoded = base64.b64encode(server.encode()).decode()
    try:
        install = f"python3 -c \"import base64;open('/tmp/idle-server.py','wb').write(base64.b64decode('{encoded}'))\"; nohup python3 /tmp/idle-server.py </dev/null >/tmp/idle-server.log 2>&1 &\n" + "python3 - <<'PY'\nimport time,urllib.request\nfor i in range(100):\n try:\n  urllib.request.urlopen('http://127.0.0.1:8001/health').read();break\n except OSError:time.sleep(.02)\nelse:raise RuntimeError('server startup')\nprint('installed')\nPY"
        stdout, stderr = execute(install)
        assert stdout == b'installed\n' and not stderr
        await_paused(time.monotonic())
        for mode in ('http', 'exec'):
            for _ in range(5):
                started = time.monotonic()
                if mode == 'http':
                    value = http()
                else:
                    stdout, stderr = execute("python3 - <<'PY'\nimport urllib.request,sys\nprint(urllib.request.urlopen('http://127.0.0.1:8001/').read().decode())\nsys.stdout.write('x'*262144+'\\nEND\\n');sys.stderr.write('stderr-complete\\n')\nPY")
                    first, rest = stdout.split(b'\n', 1)
                    assert rest == b'x'*262144+b'\nEND\n', f'relay output truncated/replayed: length={len(stdout)}, first={first[:80]!r}, tail={rest[-40:]!r}'
                    assert stderr == b'stderr-complete\n'
                    value = json.loads(first)
                responded = time.monotonic()
                verify(value)
                pause = await_paused(responded)
                samples.append(dict(kind=mode, wake_response_seconds=responded-started, end_to_paused_seconds=pause))
        # Concurrent long HTTP flows stay busy across multiple idle deadlines.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(http, '/slow') for _ in range(2)]
            await_running()
            time.sleep(2.5)
            assert state()[0] == 'running', 'in-flight HTTP was paused'
            values = sorted((f.result() for f in futures), key=lambda v: v['count'])
            for value in values:
                verify(value)
        await_paused(time.monotonic())
        # Native Exec remains connected through full relay, including slow output.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(execute, "python3 - <<'PY'\nimport time,sys\nfor i in range(6):\n print(i,flush=True);time.sleep(1)\nsys.stderr.write('done\\n')\nPY")
            await_running()
            time.sleep(3)
            assert state()[0] == 'running', 'streaming Exec was paused'
            stdout, stderr = future.result()
            assert stdout == b'0\n1\n2\n3\n4\n5\n' and stderr == b'done\n'
        await_paused(time.monotonic())
        # Probe once after all long flows: exactly 13 delivered counter updates.
        verify(http())
        await_paused(time.monotonic())
        assert counter == 13
        target, reached, release = (Path(env[k]) for k in ('PAUSE_BARRIER_TARGET','PAUSE_BARRIER_REACHED','PAUSE_BARRIER_RELEASE'))
        def hold_capture():
            reached.unlink(missing_ok=True); release.unlink(missing_ok=True)
            target.write_text(sid+'\n')
            verify(http())
            deadline=time.monotonic()+30
            while not reached.exists():
                assert time.monotonic()<deadline, 'automatic capture barrier not reached'
                time.sleep(.02)
            assert state()[0]=='pausing'
        try:
            hold_capture()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future=pool.submit(http)
                time.sleep(.3)
                assert not future.done(), 'request bypassed admitted capture'
                target.unlink(); release.touch()
                verify(future.result())
            await_paused(time.monotonic())
            hold_capture()
            started=time.monotonic()
            http(expected=502)
            parked=time.monotonic()-started
            assert 9 <= parked < 15, ('original park deadline changed', parked)
            target.unlink(); release.touch()
            await_paused(time.monotonic())
            verify(http())  # Timed-out business was never delivered or replayed.
            await_paused(time.monotonic())
            assert counter == 17
            business_passed = True
        finally:
            target.unlink(missing_ok=True); release.touch()
    finally:
        stop.set();thread.join(timeout=2)
        result = dict(business_passed=business_passed, capture_timing_includes_test_barriers=True, idle_timeout=timeout, samples=samples, resources=resources, transitions=transitions, verified_requests=counter)
        # Product logs expose the timing components without adding test hooks.
        log_path = work/'orch.log'
        log = log_path.read_text(errors='replace') if log_path.exists() else ''
        events=[]
        for line in log.splitlines():
            try:
                event=json.loads(line)
            except ValueError:
                try:
                    event=dict(word.split('=',1) for word in shlex.split(line) if '=' in word)
                    for key in ('retry','capture_ms','handshake_ms','elapsed_ms','observation_lag_ms','idle_wait_ms'):
                        if key in event: event[key]=float(event[key])
                except ValueError:
                    continue
            if event.get('sid') == sid and event.get('msg') in ('idle pause attempt','idle pause result','pause proxy activity','pause capture completed'):
                events.append(event)
        result['pause_events']=events
        result['capture_count']=sum(e.get('msg')=='pause capture completed' for e in events) if events else None
        attempts=[e for e in events if e.get('msg')=='idle pause attempt']
        result['retry_attempts']=sum(e.get('retry',0)>0 for e in attempts) if attempts else None
        for role in sorted({p['role'] for sample in resources for p in sample['processes']}):
            points=[p for sample in resources for p in sample['processes'] if p['role']==role]
            cpu_ticks=0
            for pid in {p['pid'] for p in points}:
                ticks=[p['cpu_ticks'] for p in points if p['pid']==pid]
                cpu_ticks+=max(ticks)-min(ticks)
            result.setdefault('process_summary',{})[role]={'rss_kib_max':max(p['rss_kib'] for p in points),'samples':len(points),'observed_cpu_seconds':cpu_ticks/os.sysconf('SC_CLK_TCK')}
        # State samples bound Resume startup separately from park/request latency.
        starts=[transitions[i+1]['monotonic']-point['monotonic'] for i,point in enumerate(transitions[:-1]) if point['state']=='starting' and transitions[i+1]['state']=='running']
        result['resume_starting_seconds']=starts
        for field in ('wake_response_seconds','end_to_paused_seconds'):
            values=sorted(s[field] for s in samples)
            if values:
                result[field]=dict(n=len(values),min=values[0],p50=statistics.median(values),p95=values[max(0,round(.95*len(values))-1)],max=values[-1])
        output.write_text(json.dumps(result,indent=2)+'\n')
        if business_passed:
            assert attempts and result['capture_count'] >= len(samples), 'incomplete Pause log evidence'
            assert {'conductor','proxy-master','proxy-worker','vm'} <= result.get('process_summary',{}).keys(), 'incomplete resource evidence'
    print(json.dumps({k:v for k,v in result.items() if k not in ('samples','resources','transitions','pause_events')}))


if __name__ == '__main__':
    run()
