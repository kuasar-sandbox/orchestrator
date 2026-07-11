package main

// Shared scaffold for the in-unit sandbox launcher: lock the task pidfile
// (double-start guard), fetch a LaunchSpec over the config-socket, then exec-replace
// into the target so it inherits this PID (the unit's main pid + cgroup).

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// launchTask locks+writes the pidfile, fetches the LaunchSpec for
// "sandbox:<sid>" over the config-socket, applies its workdir/env, and
// exec-replaces into the target (which inherits this PID and the unit cgroup).
func launchTask(socket, configID, pidfile string) error {
	if pidfile != "" {
		if err := lockPidfile(pidfile); err != nil {
			return err
		}
	}
	spec, err := configsock.FetchLaunchSpec(socket, configID)
	if err != nil {
		return fmt.Errorf("fetch launch spec: %w", err)
	}
	if spec.Exec == "" {
		return fmt.Errorf("launch spec has no exec")
	}
	if spec.Workdir != "" {
		if err := os.Chdir(spec.Workdir); err != nil {
			return fmt.Errorf("chdir %s: %w", spec.Workdir, err)
		}
	}
	argv := append([]string{spec.Exec}, spec.Args...)
	return syscall.Exec(spec.Exec, argv, taskEnv(spec.Env))
}

func envDefault(p *string, key string) {
	if *p == "" {
		*p = os.Getenv(key)
	}
}

// lockPidfile opens path, takes a non-blocking exclusive POSIX lock (fails if another
// instance already holds it — the double-start guard), writes our pid, and clears
// FD_CLOEXEC so the lock survives execve into the target (the fd stays open in the
// same-PID process; the lock releases on process exit). It is never closed and never
// unlinked — the launcher does not clean up the pidfile.
func lockPidfile(path string) error {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT, 0o600)
	if err != nil {
		return fmt.Errorf("open pidfile %s: %w", path, err)
	}
	lk := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	if err := unix.FcntlFlock(uintptr(fd), unix.F_SETLK, &lk); err != nil {
		unix.Close(fd)
		return fmt.Errorf("pidfile %s locked (task already running?): %w", path, err)
	}
	if err := unix.Ftruncate(fd, 0); err != nil {
		return fmt.Errorf("truncate pidfile: %w", err)
	}
	if _, err := unix.Pwrite(fd, []byte(strconv.Itoa(os.Getpid())+"\n"), 0); err != nil {
		return fmt.Errorf("write pidfile: %w", err)
	}
	if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
		_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags&^unix.FD_CLOEXEC)
	}
	return nil
}

// taskEnv returns the child environment: the inherited env minus the TASK_* bootstrap
// vars, plus the spec's added env (secrets like MANIFEST_KEY).
func taskEnv(add map[string]string) []string {
	skip := map[string]bool{
		"TASK_PIDFILE": true, "TASK_CONFIG_SOCKET": true,
		"TASK_RUN_ID": true, "TASK_SANDBOX_ID": true, "TASK_BUILD_ID": true,
	}
	out := make([]string, 0, len(os.Environ())+len(add))
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i >= 0 && skip[kv[:i]] {
			continue
		}
		out = append(out, kv)
	}
	for k, v := range add {
		out = append(out, k+"="+v)
	}
	return out
}
