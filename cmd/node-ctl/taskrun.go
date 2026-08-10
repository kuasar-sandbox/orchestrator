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
// exec-replaces into the target, passing the node-owned VMM cgroup capability.
func launchTask(socket, configID, pidfile string, ready, vmmCgroup *os.File) error {
	return launchTaskWith(socket, configID, pidfile, ready, vmmCgroup, taskLaunchOps{
		lockPidfile: lockPidfile,
		fetchSpec:   configsock.FetchLaunchSpec,
		chdir:       os.Chdir,
		exec:        syscall.Exec,
	})
}

type taskLaunchOps struct {
	lockPidfile func(string) error
	fetchSpec   func(string, string) (*configsock.LaunchSpec, error)
	chdir       func(string) error
	exec        func(string, []string, []string) error
}

func launchTaskWith(socket, configID, pidfile string, ready, vmmCgroup *os.File, ops taskLaunchOps) error {
	if pidfile != "" {
		if err := ops.lockPidfile(pidfile); err != nil {
			return err
		}
	}
	spec, err := ops.fetchSpec(socket, configID)
	if err != nil {
		return fmt.Errorf("fetch launch spec: %w", err)
	}
	if spec.Exec == "" {
		return fmt.Errorf("launch spec has no exec")
	}
	if vmmCgroup == nil || vmmCgroup.Fd() < 3 {
		return fmt.Errorf("launch task has no inheritable vmm cgroup descriptor")
	}
	if arg, ok := launchSpecCgroupArg(spec.Args); ok {
		return fmt.Errorf("launch spec must not set node-owned cgroup argument %q", arg)
	}
	if len(spec.Args) == 0 || spec.Args[0] != "run" {
		return fmt.Errorf("launch spec must invoke sandbox-ctl run")
	}
	if spec.Workdir != "" {
		if err := ops.chdir(spec.Workdir); err != nil {
			return fmt.Errorf("chdir %s: %w", spec.Workdir, err)
		}
	}
	argv := []string{spec.Exec, "run", fmt.Sprintf("--cgroup-path=fd=%d", vmmCgroup.Fd())}
	if ready != nil {
		fd := int(ready.Fd())
		if fd < 3 {
			return fmt.Errorf("readiness fd %d is not inheritable", fd)
		}
		argv = append(argv, fmt.Sprintf("--ready-fd=%d", fd))
	}
	argv = append(argv, spec.Args[1:]...)
	env := taskEnv(spec.Env)
	// These are the last fallible operations before exec. If exec itself fails,
	// runAssignedSandbox's defers close the now-inheritable descriptors.
	if err := clearCloseOnExec(vmmCgroup); err != nil {
		return fmt.Errorf("make vmm cgroup descriptor inheritable: %w", err)
	}
	if ready != nil {
		if err := clearCloseOnExec(ready); err != nil {
			return err
		}
	}
	return ops.exec(spec.Exec, argv, env)
}

func launchSpecCgroupArg(args []string) (string, bool) {
	for _, arg := range args {
		if arg == "--cgroup-path" || strings.HasPrefix(arg, "--cgroup-path=") ||
			arg == "--cgroup-adopt" || strings.HasPrefix(arg, "--cgroup-adopt=") {
			return arg, true
		}
	}
	return "", false
}

func clearCloseOnExec(f *os.File) error {
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("get descriptor flags: %w", err)
	}
	if _, err := unix.FcntlInt(f.Fd(), unix.F_SETFD, flags&^unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("clear descriptor close-on-exec: %w", err)
	}
	return nil
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
