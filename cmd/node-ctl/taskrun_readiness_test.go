package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"golang.org/x/sys/unix"
)

func TestRunAssignedSandboxReadinessFDOrderingAndExecArg(t *testing.T) {
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	flags, err := unix.FcntlInt(readyW.Fd(), unix.F_GETFD, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(readyW.Fd(), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		t.Fatal(err)
	}

	var order []string
	assertCloseOnExec := func(stage string, want bool) {
		t.Helper()
		flags, err := unix.FcntlInt(readyW.Fd(), unix.F_GETFD, 0)
		if err != nil {
			t.Fatalf("%s: F_GETFD: %v", stage, err)
		}
		if got := flags&unix.FD_CLOEXEC != 0; got != want {
			t.Fatalf("%s: close-on-exec=%t, want %t", stage, got, want)
		}
	}

	runRoot := filepath.Join(t.TempDir(), "run")
	runPidfile := filepath.Join(runRoot, "runs", "run-1.pid")
	execErr := errors.New("exec failed")
	err = runAssignedSandbox(runPidfile, "/config.sock", "run-1", runSandboxOps{
		lockPidfile: func(path string) error {
			order = append(order, "run pidfile")
			if path != runPidfile {
				t.Fatalf("run pidfile = %q", path)
			}
			return nil
		},
		waitAssignment: func(_ context.Context, socket, kind, runID string) (string, error) {
			order = append(order, "assignment")
			if socket != "/config.sock" || kind != "sandbox" || runID != "run-1" {
				t.Fatalf("WaitAssignment(%q, %q, %q)", socket, kind, runID)
			}
			return "sid-1", nil
		},
		connectReady: func(path string) (*os.File, error) {
			order = append(order, "connect ready")
			if want := configsock.ReadinessSocketPath(runRoot, "sid-1"); path != want {
				t.Fatalf("ready path = %q, want %q", path, want)
			}
			assertCloseOnExec("connect", true)
			return readyW, nil
		},
		launchTask: func(socket, configID, pidfile string, ready *os.File) error {
			order = append(order, "launch task")
			if socket != "/config.sock" || configID != "sandbox:sid-1" ||
				pidfile != filepath.Join(runRoot, "sid-1", "sid-1.pid") || ready != readyW {
				t.Fatalf("launchTask(%q, %q, %q, %v)", socket, configID, pidfile, ready)
			}
			return launchTaskWith(socket, configID, pidfile, ready, taskLaunchOps{
				lockPidfile: func(string) error {
					order = append(order, "sandbox pidfile")
					assertCloseOnExec("sandbox pidfile", true)
					return nil
				},
				fetchSpec: func(string, string) (*configsock.LaunchSpec, error) {
					order = append(order, "fetch spec")
					assertCloseOnExec("fetch spec", true)
					return &configsock.LaunchSpec{
						Exec: "/bin/sandbox-ctl", Args: []string{"run"}, Workdir: "/work",
					}, nil
				},
				chdir: func(path string) error {
					order = append(order, "chdir")
					assertCloseOnExec("chdir", true)
					if path != "/work" {
						t.Fatalf("chdir = %q", path)
					}
					return nil
				},
				exec: func(path string, argv, _ []string) error {
					order = append(order, "exec")
					assertCloseOnExec("exec", false)
					wantArg := "--ready-fd=" + strconv.Itoa(int(readyW.Fd()))
					if path != "/bin/sandbox-ctl" || len(argv) != 3 || argv[2] != wantArg {
						t.Fatalf("exec path=%q argv=%q, want final %q", path, argv, wantArg)
					}
					return execErr
				},
			})
		},
	})
	if !errors.Is(err, execErr) {
		t.Fatalf("runAssignedSandbox error = %v", err)
	}
	wantOrder := []string{
		"run pidfile", "assignment", "connect ready", "launch task",
		"sandbox pidfile", "fetch spec", "chdir", "exec",
	}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("order = %q, want %q", order, wantOrder)
	}
	if _, err := readyW.Write([]byte("x")); err == nil {
		t.Fatal("ready fd remained open after exec failure")
	}
}

func TestRunAssignedSandboxPreExecFailureClosesReadinessFD(t *testing.T) {
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	wantErr := errors.New("fetch failed")
	err = runAssignedSandbox("/run/sandbox/runs/run.pid", "/config.sock", "run", runSandboxOps{
		lockPidfile: func(string) error { return nil },
		waitAssignment: func(context.Context, string, string, string) (string, error) {
			return "sid", nil
		},
		connectReady: func(string) (*os.File, error) { return readyW, nil },
		launchTask: func(socket, configID, pidfile string, ready *os.File) error {
			return launchTaskWith(socket, configID, pidfile, ready, taskLaunchOps{
				lockPidfile: func(string) error { return nil },
				fetchSpec:   func(string, string) (*configsock.LaunchSpec, error) { return nil, wantErr },
				chdir:       func(string) error { return nil },
				exec:        func(string, []string, []string) error { t.Fatal("exec called"); return nil },
			})
		},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	buf := make([]byte, 1)
	if n, err := readyR.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read after pre-exec failure = %d, %v; want EOF", n, err)
	}
}

func TestConnectReadinessSocketKeepsCloseOnExec(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready.sock")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan *net.UnixConn, 1)
	go func() {
		conn, _ := l.AcceptUnix()
		accepted <- conn
	}()

	f, err := connectReadinessSocket(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	conn := <-accepted
	defer conn.Close()
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if err != nil {
		t.Fatal(err)
	}
	if flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("connected readiness fd does not have FD_CLOEXEC")
	}
}
