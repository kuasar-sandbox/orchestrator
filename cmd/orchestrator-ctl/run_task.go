package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/configsock"
)

// runTask is the universal sub-task launcher — the ExecStart of every sandbox /
// build unit. It locks+writes its pidfile (double-start guard), fetches a generic
// LaunchSpec for its config-id over the config-socket, then exec-replaces into the
// target so the target inherits this PID (the unit's main pid + cgroup). Flags
// fall back to the TASK_* env (systemd %i wiring); TASK_* are stripped from the
// child environment, and the spec's env (secrets like MANIFEST_KEY) is added.
//
//	orchestrator-ctl run-task --pidfile=<f> --config-socket=<uds> --config-id=<id>
func runTask(args []string, _ *slog.Logger) error {
	fs := flag.NewFlagSet("run-task", flag.ExitOnError)
	pidfile := fs.String("pidfile", "", "pidfile to lock+write (TASK_PIDFILE)")
	socket := fs.String("config-socket", "", "config-socket UDS (TASK_CONFIG_SOCKET)")
	configID := fs.String("config-id", "", "task config id, e.g. sandbox:<sid> (TASK_CONFIG_ID)")
	_ = fs.Parse(args)
	envDefault(pidfile, "TASK_PIDFILE")
	envDefault(socket, "TASK_CONFIG_SOCKET")
	envDefault(configID, "TASK_CONFIG_ID")
	if *socket == "" || *configID == "" {
		return fmt.Errorf("run-task: --config-socket and --config-id required")
	}

	// Lock + write the pidfile, then keep the fd open across exec (clear
	// FD_CLOEXEC) so the POSIX lock is held by the exec'd target process.
	if *pidfile != "" {
		if err := lockPidfile(*pidfile); err != nil {
			return err
		}
	}

	spec, err := configsock.FetchLaunchSpec(*socket, *configID)
	if err != nil {
		return fmt.Errorf("run-task: fetch launch spec: %w", err)
	}
	if spec.Exec == "" {
		return fmt.Errorf("run-task: launch spec has no exec")
	}
	if spec.Workdir != "" {
		if err := os.Chdir(spec.Workdir); err != nil {
			return fmt.Errorf("run-task: chdir %s: %w", spec.Workdir, err)
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

// lockPidfile opens path, takes a non-blocking exclusive POSIX lock (fails if
// another instance already holds it — the double-start guard), writes our pid,
// and clears FD_CLOEXEC so the lock survives execve into the target (the fd stays
// open in the same-PID process; the lock releases on process exit). It is never
// closed and never unlinked — run-task does not clean up the pidfile.
func lockPidfile(path string) error {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT, 0o600)
	if err != nil {
		return fmt.Errorf("run-task: open pidfile %s: %w", path, err)
	}
	lk := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	if err := unix.FcntlFlock(uintptr(fd), unix.F_SETLK, &lk); err != nil {
		unix.Close(fd)
		return fmt.Errorf("run-task: pidfile %s locked (task already running?): %w", path, err)
	}
	if err := unix.Ftruncate(fd, 0); err != nil {
		return fmt.Errorf("run-task: truncate pidfile: %w", err)
	}
	if _, err := unix.Pwrite(fd, []byte(strconv.Itoa(os.Getpid())+"\n"), 0); err != nil {
		return fmt.Errorf("run-task: write pidfile: %w", err)
	}
	if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
		_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags&^unix.FD_CLOEXEC)
	}
	return nil
}

// taskEnv returns the child environment: the inherited env minus the TASK_*
// bootstrap vars, plus the spec's added env (secrets).
func taskEnv(add map[string]string) []string {
	skip := map[string]bool{"TASK_PIDFILE": true, "TASK_CONFIG_SOCKET": true, "TASK_CONFIG_ID": true}
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
