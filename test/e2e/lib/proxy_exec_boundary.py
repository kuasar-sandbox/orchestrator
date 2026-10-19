"""Test-only ptrace stop after exec, before the new Go image executes."""
import ctypes
import errno
import os
from pathlib import Path
import signal
import sys
import time


class ExecBoundary:
    def __init__(self, pid):
        self.pid = pid
        self.traced = set()
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.libc.ptrace.restype = ctypes.c_long

    def call(self, request, pid, data=0):
        if self.libc.ptrace(request, pid, ctypes.c_void_p(), ctypes.c_void_p(data)) == -1:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))

    def attach(self):
        # Stop every current thread before allowing any to clone/exec again.
        while True:
            added = False
            for path in Path(f'/proc/{self.pid}/task').iterdir():
                tid = int(path.name)
                if tid in self.traced:
                    continue
                try:
                    self.call(16, tid)  # PTRACE_ATTACH
                except OSError as error:
                    if error.errno == errno.ESRCH:
                        continue
                    raise
                self.traced.add(tid)
                _, status = os.waitpid(tid, 0x40000000)  # __WALL
                assert os.WIFSTOPPED(status), status
                self.call(0x4200, tid, 0x18)  # SETOPTIONS: TRACECLONE | TRACEEXEC
                added = True
            if not added:
                break
        for tid in self.traced:
            self.call(7, tid)  # PTRACE_CONT

    def wait(self, timeout=35):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            tid, status = os.waitpid(-1, 0x40000000 | os.WNOHANG)
            if not tid:
                time.sleep(.005)
                continue
            if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                self.traced.discard(tid)
                assert tid != self.pid, ('main exited before exec', status)
                continue
            assert os.WIFSTOPPED(status), status
            self.traced.add(tid)
            if status >> 16 == 4:  # PTRACE_EVENT_EXEC; single thread now.
                assert tid == self.pid
                self.traced = {tid}
                return
            deliver = os.WSTOPSIG(status)
            if deliver in (signal.SIGTRAP, signal.SIGSTOP):
                deliver = 0
            try:
                self.call(7, tid, deliver)
            except OSError as error:
                print(f'exec boundary CONT failed: main_pid={self.pid} '
                      f'tid={tid} status={status:#x} signal={deliver} '
                      f'errno={error.errno}', file=sys.stderr, flush=True)
                if error.errno != errno.ESRCH or tid == self.pid:
                    raise
                # Another thread can exec after this thread's stop was reaped,
                # destroying the stopped sibling before CONT. ESRCH is not
                # proof of death: retain ownership until an exit/exec event.
                # Only the original PID's EXEC event above succeeds; keep the
                # original deadline and all main-exit/waitpid failures.
        raise AssertionError('main did not reach exec')

    def detach(self):
        for tid in list(self.traced):
            try:
                self.call(17, tid)  # PTRACE_DETACH at the exec stop.
            except OSError as error:
                if error.errno != errno.ESRCH:
                    raise
            self.traced.discard(tid)
