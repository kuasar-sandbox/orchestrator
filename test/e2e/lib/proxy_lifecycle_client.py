#!/usr/bin/env python3
"""Observe real Proxy images, service actions and unretried Guest business."""
import base64
import errno
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import websocket_probe as ws
from proxy_exec_boundary import ExecBoundary


def digest(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def record_service_exit(path):
    # ExecStopPost runs before systemd can garbage-collect the inactive unit's
    # execution status. Record manager-supplied values, never inferred success.
    result = {key: os.environ.get(key) for key in
              ('SERVICE_RESULT', 'EXIT_CODE', 'EXIT_STATUS', 'INVOCATION_ID')}
    destination = Path(path)
    temporary = destination.with_suffix('.next')
    temporary.write_text(json.dumps(result) + '\n')
    temporary.replace(destination)


def cgroup_processes(cgroup):
    try:
        return (cgroup / 'cgroup.procs').read_text().split()
    except OSError as error:
        # Removal can race either opening the path or reading an open cgroup.
        if error.errno in (errno.ENOENT, errno.ENODEV):
            return []
        raise


def wait_exec_shutdown_observations(cgroup, exit_record, owned, started):
    # cgroup.procs can be empty before the last owned PID is reaped. Wait for
    # every existing final assertion within the same original 18-second budget.
    while (cgroup_processes(cgroup) or not exit_record.exists() or
           any(Path('/proc', pid).exists() for pid in owned)) and time.monotonic() - started < 18:
        time.sleep(.02)


def owned_process_observations(owned):
    result = {}
    for pid in owned:
        try:
            lines = Path('/proc', pid, 'status').read_text().splitlines()
            result[pid] = {key: value.strip() for key, value in
                           (line.split(':', 1) for line in lines if ':' in line)
                           if key in ('State', 'PPid', 'TracerPid', 'Threads', 'NSpid')}
        except FileNotFoundError:
            result[pid] = {'gone_during_observation': True}
        except OSError as error:
            result[pid] = {'observation_errno': error.errno}
    return result


def report_process_limit(pids, config, cgroup, main_pid):
    # Preserve the raw cgroup sample, including duplicates. These subsequent
    # /proc reads are observations, not an atomic process snapshot.
    sample = pids[:32]
    processes = owned_process_observations(sample)
    for pid in sample:
        try:
            processes[pid]['exe'] = os.readlink('/proc/' + pid + '/exe')[:512]
        except OSError as error:
            processes[pid]['exe_errno'] = error.errno
    observation = dict(kind='process_limit_observation', time=time.time(),
                       entry=config['entry'], source=config['source'],
                       unit=config['unit'], cgroup=str(cgroup), main_pid=main_pid,
                       observed_count=len(pids), unique_count=len(set(pids)),
                       pids=sample, truncated=len(pids) > len(sample),
                       processes=processes)
    try:
        # A diagnostic must not become a workflow command or hide the original
        # process-count assertion if stderr is unavailable.
        print(json.dumps(observation).replace('#', r'\u0023'),
              file=sys.stderr, flush=True)
    except (OSError, ValueError):
        pass


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, socket_path, **kwargs):
        super().__init__('localhost', **kwargs)
        self.socket_path = socket_path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.socket_path)


class Probe:
    def __init__(self, config):
        self.config = config
        self.stats = config['stats']
        self.pid = self.main_pid()
        self.initial_pid = self.pid
        self.stop = threading.Event()
        self.stop_traffic = threading.Event()
        self.errors = []
        self.requests = 0
        self.peaks = dict(processes=0, rss_kib=0, fds=0)
        self.process_limit_reported = False
        self.stable_fds = []
        self.out = open(config['output'], 'x', buffering=1)
        self.output_lock = threading.Lock()
        self.cgroup = Path('/sys/fs/cgroup' + subprocess.check_output(
            ['systemctl', 'show', '--value', '-p', 'ControlGroup', config['unit']], text=True).strip())
        self.load = threading.Thread(target=self.traffic)
        self.monitor = threading.Thread(target=self.resources)

    def log(self, **fields):
        with self.output_lock:
            self.out.write(json.dumps(dict(time=time.time(), **fields)) + '\n')

    def main_pid(self):
        return int(subprocess.check_output(['systemctl', 'show', '--value', '-p', 'MainPID', self.config['unit']], text=True))

    def check_pid_admission(self):
        admitted = Path(self.config['plugin_pidfile']).read_text().split()
        assert admitted == [str(self.pid)], admitted
        self.log(kind='pid_admission', main_pid=self.pid, admitted=admitted)

    def management(self, path='/status', method='GET', endpoint=None):
        connection = UnixHTTP(endpoint or self.stats, timeout=2)
        try:
            connection.request(method, path)
            response = connection.getresponse()
            body = response.read()
            return response.status, dict(response.getheaders()), body
        finally:
            connection.close()

    def wait_status(self, predicate, endpoint=None, timeout=35):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                code, _, body = self.management(endpoint=endpoint)
                assert code == 200, (code, body)
                last = json.loads(body)
                assert len(last['instances']) <= 2, last
                assert last['coordinator_pid'] == self.pid, last
                self.log(kind='status', status=last)
                if predicate(last):
                    return last
            except (OSError, http.client.HTTPException):
                pass  # Only management may have the documented exec gap.
            time.sleep(.02)
        raise AssertionError(f'lifecycle state timeout: {last}')

    def check_ready(self):
        code, headers, body = self.management('/readyz')
        status = json.loads(body)
        assert code == 200 and status['ready'] and status['pid'] == self.pid, status
        assert headers['Cache-Control'] == 'no-store'
        assert headers['Content-Type'] == 'application/json'
        assert status['workers'] == {'ready': 2, 'expected': 2}
        for path in ('/readyz', '/status'):
            assert self.management(path, 'POST')[0] == 405

    def connection(self, port=8002):
        connection = http.client.HTTPConnection('127.0.0.1', self.config['port'], timeout=30)
        headers = {'Host': f"{port}-{self.config['sid']}.{self.config['domain']}",
                   'X-Access-Token': self.config['token'], 'Connection': 'close'}
        return connection, headers

    def get(self, path):
        connection, headers = self.connection()
        try:
            connection.request('GET', path, headers=headers)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def traffic(self):
        while not self.stop_traffic.is_set():
            started = time.time()
            try:
                code, body = self.get('/health')
                assert code == 200 and body == b'healthy\n', (code, body)
                self.requests += 1
                self.log(kind='business', start=started, status=code)
            except Exception as error:
                self.errors.append(str(error))
                self.log(kind='business', start=started, error=str(error))
            self.stop_traffic.wait(.005)

    def resources(self):
        while not self.stop.is_set():
            try:
                pids = (self.cgroup / 'cgroup.procs').read_text().split()
            except FileNotFoundError:
                pids = []
            rss, fds = 0, 0
            for pid in pids:
                try:
                    rss += int(next(line.split()[1] for line in Path('/proc', pid, 'status').read_text().splitlines() if line.startswith('VmRSS:')))
                    fds += len(list(Path('/proc', pid, 'fd').iterdir()))
                except (FileNotFoundError, ProcessLookupError, StopIteration):
                    pass
            for name, value in dict(processes=len(pids), rss_kib=rss, fds=fds).items():
                self.peaks[name] = max(value, self.peaks[name])
            self.log(kind='resources', pids=pids, rss_kib=rss, fds=fds)
            if len(pids) > 6 and not self.process_limit_reported:
                self.process_limit_reported = True
                report_process_limit(pids, self.config, self.cgroup, self.pid)
            self.stop.wait(.02)

    def finite(self, key):
        # One Guest serves every entry/source combination. Never reuse a released
        # request key from a previous mode or accepted operation.
        key = f'{self.initial_pid}-{time.monotonic_ns()}-{key}'
        connection, headers = self.connection()
        connection.request('GET', '/finite?id=' + key, headers=headers)
        response = connection.getresponse()
        assert response.status == 200 and response.read(6) == b'begin\n'
        # An existing CONNECT remains usable after complete service shutdown
        # has stopped new data admission. It releases the Guest barrier without
        # depending on a new request through a replacement instance.
        control = http.client.HTTPConnection('guest', timeout=10)
        control.sock = self.open_tunnel()

        def complete():
            try:
                control.request('GET', '/release?id=' + key, headers={'Connection': 'close'})
                released = control.getresponse()
                assert released.status == 204
                released.read()
                body = response.read()
            finally:
                control.close()
                connection.close()
            assert body == b'complete\n', body
            self.log(kind='finite_complete', request=key, sid=self.config['sid'])
        return complete

    def metadata(self):
        code, body = self.get('/metadata')
        assert code == 200 and body == b'healthy\n', (code, body)
        self.log(kind='guest_mmds_complete', sid=self.config['sid'])

    def websocket(self):
        connection = socket.create_connection(('127.0.0.1', self.config['port']), timeout=15)
        key = base64.b64encode(os.urandom(16)).decode()
        request = (f"GET /ws HTTP/1.1\r\nHost: 8001-{self.config['sid']}.{self.config['domain']}\r\n"
                   f"X-Access-Token: {self.config['token']}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
                   f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: test\r\n\r\n")
        connection.sendall(request.encode())
        reader = connection.makefile('rb')
        assert reader.readline().split()[1] == b'101'
        http.client.parse_headers(reader)
        assert ws.read_frame(reader) == (1, b'ready')
        assert ws.read_frame(reader) == (2, b'\x00\xff\x7f')

        def complete():
            connection.sendall(ws.frame(9, b'during-drain', True))
            assert ws.read_frame(reader) == (10, b'during-drain')
            connection.sendall(ws.frame(8, b'\x03\xe8', True))
            connection.shutdown(socket.SHUT_WR)
            assert ws.read_frame(reader) == (8, b'\x03\xe8') and reader.read() == b''
            reader.close()
            connection.close()
            self.log(kind='websocket_complete', half_close=True)
        return complete

    def open_tunnel(self):
        connection = socket.create_connection(('127.0.0.1', self.config['port']), timeout=15)
        request = (f"CONNECT guest HTTP/1.1\r\nHost: guest\r\nE2b-Sandbox-Id: {self.config['sid']}\r\n"
                   f"E2b-Sandbox-Port: 8002\r\nX-Access-Token: {self.config['token']}\r\n\r\n")
        connection.sendall(request.encode())
        response = http.client.HTTPResponse(connection)
        response.begin()
        assert response.status == 200
        response.close()
        return connection

    def tunnel(self):
        connection = self.open_tunnel()

        def complete():
            connection.sendall(b'GET /health HTTP/1.1\r\nHost: guest\r\nConnection: close\r\n\r\n')
            response = http.client.HTTPResponse(connection)
            response.begin()
            assert response.status == 200 and response.read() == b'healthy\n'
            response.close()
            connection.close()
            self.log(kind='connect_complete')
        return complete

    def trigger(self, index):
        if self.config['entry'] == 'app' and index == 0:
            code, _, body = self.management('/test/restart', 'POST')
            assert code == 200, (code, body)
        else:
            os.kill(self.pid, signal.SIGUSR1 if self.config['entry'] == 'app' else signal.SIGUSR2)

    def check_restart_flag(self, expected):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            connection = http.client.HTTPConnection('127.0.0.1', int(self.config['api_port']), timeout=2)
            try:
                connection.request('GET', f"/sandboxes/{self.config['sid']}/stats/traffic",
                                   headers={'Host': 'api.' + self.config['domain'], 'X-API-KEY': self.config['api_key']})
                response = connection.getresponse()
                body = json.loads(response.read())
                self.log(kind='restart_flag_observation', status=response.status, traffic=body)
                if response.status == 200 and body.get('inplaceRestart', False) == expected:
                    return
            finally:
                connection.close()
            time.sleep(.02)
        raise AssertionError(f'conductor did not report inplaceRestart={expected}')

    def stable_resources(self):
        # Stop only between completed operations. Each takeover retains the
        # continuous no-retry request stream, and every started request joins.
        self.stop_traffic.set()
        self.load.join()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            connection = http.client.HTTPConnection('127.0.0.1', int(self.config['api_port']), timeout=2)
            try:
                connection.request('GET', f"/sandboxes/{self.config['sid']}/stats/traffic",
                                   headers={'Host': 'api.' + self.config['domain'], 'X-API-KEY': self.config['api_key']})
                response = connection.getresponse()
                body = json.loads(response.read())
                assert response.status == 200, body
                if body['inflight']['connected'] == 0:
                    break
            finally:
                connection.close()
            time.sleep(.02)
        else:
            raise AssertionError('business did not become idle between operations')
        samples = []
        for _ in range(25):
            pids = (self.cgroup / 'cgroup.procs').read_text().split()
            assert len(pids) == 3, pids
            samples.append(sum(len(list(Path('/proc', pid, 'fd').iterdir())) for pid in pids))
            time.sleep(.02)
        assert len(set(samples[-5:])) == 1, samples
        self.stable_fds.append(samples[-1])
        self.stop_traffic.clear()
        self.load = threading.Thread(target=self.traffic)
        self.load.start()

    def deploy(self, source):
        destination = Path(self.config['deployment'])
        temporary = destination.with_suffix('.next')
        shutil.copyfile(source, temporary)
        temporary.chmod(0o700)
        temporary.replace(destination)

    def operation(self, index):
        config_path = Path(self.config['proxy_config'])
        original_config = config_path.read_bytes()
        self.deploy(self.config['target'])
        target_digest = digest(self.config['target'])
        completions_a = [self.finite(f'{index}-a'), self.websocket(), self.tunnel()]
        previous = self.wait_status(lambda s: s['ready'])['inplace_restart']
        self.trigger(index)
        prepared = self.wait_status(lambda s: s['inplace_restart'] and
            (not previous or s['inplace_restart']['id'] != previous['id']) and
            s['inplace_restart']['phase'] == 'draining_a' and s['workers']['ready'] == 0)
        operation = prepared['inplace_restart']['id']
        candidate = prepared['instances'][1]
        assert candidate['ready'] and candidate['pid'] != self.pid
        self.check_pid_admission()
        assert digest(f"/proc/{candidate['pid']}/exe") == target_digest
        assert self.management('/readyz')[0] == 503
        completions_b = [self.finite(f'{index}-b'), self.websocket(), self.tunnel()]
        self.check_restart_flag(True)
        self.metadata()
        if index == 0:
            # An accepted target/input must survive later deployment changes.
            self.deploy(self.config['initial'])
            config_path.write_text('invalid: [unclosed\n')
        elif index == 1:
            Path(self.config['deployment']).chmod(0o600)  # Real execve EACCES.
        else:
            Path(self.stats).with_name('fail-successor').touch(exist_ok=False)
        for complete in reversed(completions_a):
            complete()
        if index == 1:
            recovery = self.wait_status(lambda s: s['inplace_restart']['id'] == operation and
                s['inplace_restart']['attempts'] >= 4 and s['inplace_restart'].get('next_attempt'), candidate['stats_socket'])
            assert recovery['ready'] and recovery['service_ready']
            assert 'permission denied' in recovery['inplace_restart']['last_error']
            Path(self.config['deployment']).chmod(0o700)
        elif index == 2:
            recovery = self.wait_status(lambda s: s['inplace_restart']['id'] == operation and
                s['inplace_restart']['attempts'] >= 4 and s['inplace_restart'].get('next_attempt'), candidate['stats_socket'])
            assert recovery['ready'] and recovery['service_ready']
            assert 'test-only successor initialization failure' in recovery['inplace_restart']['last_error']
            Path(self.stats).with_name('fail-successor').unlink()
        successor = self.wait_status(lambda s: s['ready'] and s['inplace_restart']['id'] == operation and
            s['inplace_restart']['phase'] == 'draining_b')
        assert successor['pid'] == self.initial_pid == self.main_pid()
        assert digest(f'/proc/{self.pid}/exe') == target_digest
        self.metadata()
        for complete in reversed(completions_b):
            complete()
        result = self.wait_status(lambda s: s['inplace_restart']['id'] == operation and s['inplace_restart'].get('result'))
        assert result['inplace_restart']['result'] == 'completed', result
        assert result['inplace_restart']['forced_close_count'] == 0 and len(result['instances']) == 1
        assert not Path(f"/proc/{candidate['pid']}").exists(), 'B not reaped'
        assert not Path(candidate['stats_socket']).exists(), 'B stats socket remains'
        candidate_shm = Path(self.stats).parent / ('.proxy-' + candidate['boot_id'] + '.shm')
        assert not candidate_shm.exists(), 'B route SHM remains'
        config_path.write_bytes(original_config)
        self.deploy(self.config['target'])
        self.check_ready()
        self.check_restart_flag(False)
        self.stable_resources()
        self.log(kind='operation_complete', id=operation, target_sha256=target_digest,
                 main_pid=self.pid, result=result['inplace_restart'], stable_fds=self.stable_fds[-1])

    def full_shutdown(self):
        completions = [self.finite('full-stop'), self.websocket(), self.tunnel()]
        self.stop_traffic.set()
        self.load.join()
        owned = (self.cgroup / 'cgroup.procs').read_text().split()
        invocation = subprocess.check_output(['systemctl', 'show', '--value',
            '-p', 'InvocationID', self.config['unit']], text=True).strip()
        assert invocation, 'missing service invocation identity'
        exit_record = Path(self.config['exit_record'])
        exit_record.unlink(missing_ok=True)
        started = time.monotonic()
        units = [self.config['unit']] + self.config['socket_units'].split()
        subprocess.run(['systemctl', 'stop', '--no-block', *units], check=True)
        self.wait_status(lambda s: not s['ready'] and s['workers']['ready'] == 0 and s['data'] == 'draining', timeout=5)
        for complete in reversed(completions):
            complete()
        while (self.main_pid() or not exit_record.exists()) and time.monotonic() - started < 15:
            time.sleep(.02)
        assert self.main_pid() == 0, 'application did not finish its own bounded shutdown'
        result = json.loads(exit_record.read_text())
        try:
            assert result == dict(SERVICE_RESULT='success', EXIT_CODE='exited',
                                  EXIT_STATUS='0', INVOCATION_ID=invocation), result
        except AssertionError:
            # Query this invocation before cleanup removes the failed unit.
            # JSON keeps large fields bounded by journalctl's default limit;
            # do not request --all or include environment/command-line fields.
            diagnostic = dict(kind='full_shutdown_journal', unit=self.config['unit'],
                              invocation=invocation)
            try:
                journal = subprocess.run(
                    ['journalctl', '--no-pager', '--quiet', '--output=json',
                     '--output-fields=MESSAGE,PRIORITY,_PID', '--lines=80',
                     '_SYSTEMD_UNIT=' + self.config['unit'],
                     '_SYSTEMD_INVOCATION_ID=' + invocation],
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    timeout=5)
                diagnostic.update(returncode=journal.returncode,
                                  journal_tail=journal.stdout[-16384:].decode('utf-8', 'replace'))
            except (OSError, subprocess.TimeoutExpired) as error:
                diagnostic['journal_error'] = type(error).__name__
            try:
                # The runner also recognizes legacy commands inside a line.
                print(json.dumps(diagnostic).replace('##[', r'\u0023\u0023['),
                      file=sys.stderr, flush=True)
            except (OSError, ValueError):
                pass  # Diagnostic output must not replace the original failure.
            raise
        # ExecStopPost must also have ended before checking the complete cgroup.
        while cgroup_processes(self.cgroup) and time.monotonic() - started < 15:
            time.sleep(.02)
        remaining = cgroup_processes(self.cgroup)
        assert not remaining and all(not Path('/proc', pid).exists() for pid in owned), remaining
        properties = subprocess.check_output(['systemctl', 'show', self.config['unit'],
            '-p', 'Result', '-p', 'SendSIGKILL'], text=True)
        values = dict(line.split('=', 1) for line in properties.splitlines())
        assert values == dict(Result='success', SendSIGKILL='yes'), values
        self.log(kind='full_shutdown', elapsed=time.monotonic() - started, owned_pids=owned,
                 result=result, systemd=values, finite_completed=True, worker_cleanup='application')

    def worker_signal_shutdown(self):
        # Stop generating new requests while all workers independently stop
        # admission. Every already accepted response/tunnel must still finish.
        self.stop_traffic.set()
        self.load.join()
        completions = [self.finite('worker-term'), self.websocket(), self.tunnel()]
        # Go can create a worker from any OS thread; Linux lists each child's
        # creating thread in that thread's children file, not only the leader's.
        children = set()
        for task in Path(f'/proc/{self.pid}/task').iterdir():
            try:
                children.update((task / 'children').read_text().split())
            except FileNotFoundError:
                pass  # An unrelated runtime thread can exit during the scan.
        children = sorted(children, key=int)
        assert len(children) == 2, children
        for child in children:
            assert digest(f'/proc/{child}/exe') == digest(f'/proc/{self.pid}/exe')
            os.kill(int(child), signal.SIGTERM)
        self.wait_status(lambda s: not s['ready'], timeout=5)
        repeated = 0
        for child in children:
            try:
                os.kill(int(child), signal.SIGTERM)  # The busy worker retains its first deadline.
                repeated += 1
            except ProcessLookupError:
                pass  # An idle worker can already have exited and been replaced.
        assert repeated, 'no busy worker remained to drain'
        for complete in reversed(completions):
            complete()
        self.wait_status(lambda s: s['ready'], timeout=10)
        assert self.pid == self.main_pid()
        assert all(not Path('/proc', child).exists() for child in children), children
        self.metadata()
        self.log(kind='worker_term_complete', main_pid=self.pid, old_workers=children,
                 finite_completed=True, websocket_completed=True, connect_completed=True)

    def exec_window_shutdown(self, forced):
        # Main has no Go signal handler at this kernel stop. Only ready B and
        # its real workers can handle the Guest requests established below.
        self.deploy(self.config['initial'])
        subprocess.run(['systemctl', 'start', self.config['unit']], check=True)
        self.pid = self.main_pid()
        self.wait_status(lambda s: s['ready'])
        self.check_pid_admission()
        self.deploy(self.config['target'])
        invocation = subprocess.check_output(['systemctl', 'show', '--value',
            '-p', 'InvocationID', self.config['unit']], text=True).strip()
        exit_record = Path(self.config['exit_record'])
        exit_record.unlink(missing_ok=True)
        trace = ExecBoundary(self.pid)
        tunnel = None
        try:
            trace.attach()
            os.kill(self.pid, signal.SIGUSR2)
            trace.wait()
            candidates = list(Path(self.stats).parent.glob('.proxy-*.sock'))
            assert len(candidates) == 1, candidates
            candidate = self.wait_status(lambda s: s['ready'], str(candidates[0]))
            assert candidate['pid'] != self.pid
            assert digest(f'/proc/{self.pid}/exe') == digest(f"/proc/{candidate['pid']}/exe") == digest(self.config['target'])
            self.metadata()
            if forced:
                tunnel = self.open_tunnel()
                tunnel.settimeout(20)
            else:
                complete = self.finite('exec-stop')
            owned = cgroup_processes(self.cgroup)
            started = time.monotonic()
            subprocess.run(['systemctl', 'stop', '--no-block', self.config['unit'],
                            *self.config['socket_units'].split()], check=True)
            deadline = time.monotonic() + 5
            while True:
                masks = [int(line.split()[1], 16) for line in Path(f'/proc/{self.pid}/status').read_text().splitlines()
                         if line.startswith(('SigPnd:', 'ShdPnd:'))]
                if any(mask & (1 << (signal.SIGTERM - 1)) for mask in masks):
                    break
                assert time.monotonic() < deadline, 'systemd did not queue TERM at exec'
                time.sleep(.005)
            self.log(kind='exec_stop_term_pending', main_pid=self.pid, candidate=candidate, forced=forced)
            trace.detach()
            self.wait_status(lambda s: not s['ready'] and s['data'] == 'draining', str(candidates[0]), timeout=5)
            if forced:
                assert tunnel.recv(1) == b'', 'tunnel survived application deadline'
            else:
                complete()
            wait_exec_shutdown_observations(self.cgroup, exit_record, owned, started)
            remaining_owned = [pid for pid in owned if Path('/proc', pid).exists()]
            if remaining_owned:
                observation = dict(elapsed=time.monotonic() - started,
                                   pids=owned_process_observations(remaining_owned))
                self.log(kind='exec_stop_incomplete', **observation)
                print('exec-stop cleanup incomplete: ' + json.dumps(observation), file=sys.stderr, flush=True)
            assert not cgroup_processes(self.cgroup), 'B/workers survived their own drain budget'
            assert all(not Path('/proc', pid).exists() for pid in owned), owned
            result = json.loads(exit_record.read_text())
            assert result == dict(SERVICE_RESULT='success', EXIT_CODE='killed',
                                  EXIT_STATUS='TERM', INVOCATION_ID=invocation), result
            journal = subprocess.check_output(['journalctl', '--no-pager', '-o', 'cat',
                '_SYSTEMD_INVOCATION_ID=' + invocation], text=True)
            outcome = 'completed_with_forced_close' if forced else 'completed'
            lines = [line for line in journal.splitlines() if 'proxy candidate shutdown' in line]
            assert len(lines) == 1 and outcome in lines[0], journal
            if not forced:
                assert 'completed_with_forced_close' not in lines[0], lines
            assert "State 'stop-sigterm' timed out" not in journal, journal
            assert not Path(self.stats).exists()
            assert not Path(self.stats).with_name('proxy-routes.shm').exists()
            assert not list(Path(self.stats).parent.glob('.proxy-*'))
            self.log(kind='exec_stop_complete', forced=forced, elapsed=time.monotonic()-started,
                     result=result, candidate_outcome=lines[0], owned_pids=owned,
                     business='expected_tunnel_deadline' if forced else 'finite_complete', paths_removed=True)
        finally:
            trace.detach()
            if tunnel is not None:
                tunnel.close()

    def run(self):
        self.wait_status(lambda s: s['ready'])
        self.check_pid_admission()
        self.check_ready()
        self.log(kind='start', entry=self.config['entry'], source=self.config['source'],
                 initial_sha256=digest(f'/proc/{self.pid}/exe'), target_sha256=digest(self.config['target']))
        assert digest(f'/proc/{self.pid}/exe') != digest(self.config['target'])
        self.load.start()
        self.monitor.start()
        try:
            for index in range(3 if self.config['entry'] == 'app' else 2):
                self.operation(index)
            if self.config['source'] in ('datafd', 'bothfd'):
                previous = self.pid
                subprocess.run(['systemctl', 'restart', self.config['unit']], check=True)
                self.pid = self.main_pid()
                assert self.pid != previous
                self.wait_status(lambda s: s['ready'])
                self.check_pid_admission()
                self.check_ready()
                self.log(kind='ordinary_service_restart', previous=previous, current=self.pid)
            assert self.stable_fds[-1] <= self.stable_fds[0], self.stable_fds
            if self.config['source'] == 'bothfd' and self.config['entry'] in ('builtin', 'custom'):
                self.worker_signal_shutdown()
            self.full_shutdown()
            if self.config['source'] == 'bothfd' and self.config['entry'] in ('builtin', 'custom'):
                self.exec_window_shutdown(False)
                self.exec_window_shutdown(True)
        finally:
            self.stop_traffic.set()
            self.stop.set()
            self.load.join()
            self.monitor.join()
            self.log(kind='business_summary', requests=self.requests, failures=self.errors, peaks=self.peaks)
            self.out.close()
        assert self.requests > 0 and not self.errors, self.errors
        assert self.peaks['processes'] <= 6, self.peaks


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == 'record-stop':
        record_service_exit(sys.argv[2])
    else:
        Probe(json.loads(Path(sys.argv[1]).read_text())).run()
