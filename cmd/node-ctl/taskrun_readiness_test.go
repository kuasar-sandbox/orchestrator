package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"golang.org/x/sys/unix"
)

func TestRunAssignedSandboxReadinessFDOrderingAndExecArg(t *testing.T) {
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	vmmCgroup, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
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
	assertCgroupCloseOnExec := func(stage string, want bool) {
		t.Helper()
		flags, err := unix.FcntlInt(vmmCgroup.Fd(), unix.F_GETFD, 0)
		if err != nil {
			t.Fatalf("%s: cgroup F_GETFD: %v", stage, err)
		}
		if got := flags&unix.FD_CLOEXEC != 0; got != want {
			t.Fatalf("%s: cgroup close-on-exec=%t, want %t", stage, got, want)
		}
	}

	runRoot := filepath.Join(t.TempDir(), "run")
	runPidfile := nodepath.RunnerPID(runRoot, "run-1")
	execErr := errors.New("exec failed")
	err = runAssignedSandbox(runPidfile, "/config.sock", "run-1", runSandboxOps{
		lockPidfile: func(path string) error {
			order = append(order, "run pidfile")
			if path != runPidfile {
				t.Fatalf("run pidfile = %q", path)
			}
			return nil
		},
		prepareCgroup: func() (*os.File, error) {
			order = append(order, "prepare cgroup")
			assertCgroupCloseOnExec("prepare cgroup", true)
			return vmmCgroup, nil
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
		launchTask: func(socket, sid, runID string, ready, cgroup *os.File) error {
			order = append(order, "launch task")
			if socket != "/config.sock" || sid != "sid-1" || runID != "run-1" || ready != readyW || cgroup != vmmCgroup {
				t.Fatalf("launchTask(%q, %q, %q, %v, %v)", socket, sid, runID, ready, cgroup)
			}
			return launchTaskWith(context.Background(), func() { order = append(order, "stop context") }, socket, sid, runID, ready, cgroup, taskLaunchOps{
				fetchBootstrap: func(_ context.Context, socket, gotSID, gotRunID string) (*configsock.SandboxTaskSpec, error) {
					order = append(order, "fetch spec")
					assertCloseOnExec("fetch spec", true)
					assertCgroupCloseOnExec("fetch spec", true)
					if socket != "/config.sock" || gotSID != "sid-1" || gotRunID != "run-1" {
						t.Fatalf("bootstrap identity = %q/%q/%q", socket, gotSID, gotRunID)
					}
					return &configsock.SandboxTaskSpec{
						SandboxID: gotSID, RunID: gotRunID,
						Env:   map[string]string{"MANIFEST_KEY": "task-key"},
						Final: &configsock.LaunchSpec{Exec: "/bin/sandbox-ctl", Args: []string{"run"}, Workdir: "/work"},
					}, nil
				},
				setenv: func(string, string) error { return nil },
				chdir: func(path string) error {
					order = append(order, "chdir")
					assertCloseOnExec("chdir", true)
					assertCgroupCloseOnExec("chdir", true)
					if path != "/work" {
						t.Fatalf("chdir = %q", path)
					}
					return nil
				},
				exec: func(path string, argv, _ []string) error {
					order = append(order, "exec")
					assertCloseOnExec("exec", false)
					assertCgroupCloseOnExec("exec", false)
					wantArg := "--ready-fd=" + strconv.Itoa(int(readyW.Fd()))
					wantCgroup := "--cgroup-path=fd=" + strconv.Itoa(int(vmmCgroup.Fd()))
					if path != "/bin/sandbox-ctl" || len(argv) != 4 || argv[2] != wantCgroup || argv[3] != wantArg {
						t.Fatalf("exec path=%q argv=%q, want injected %q, %q", path, argv, wantCgroup, wantArg)
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
		"run pidfile", "prepare cgroup", "assignment", "connect ready",
		"launch task", "fetch spec", "chdir", "stop context", "exec",
	}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("order = %q, want %q", order, wantOrder)
	}
	if _, err := readyW.Write([]byte("x")); err == nil {
		t.Fatal("ready fd remained open after exec failure")
	}
}

func TestRunAssignedSandboxSessionCoversAssignmentAndLaunch(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "config.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	sessionAcked := make(chan struct{})
	sessionClosed := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != configsock.PathRunSession || r.Method != http.MethodPut {
			t.Fatalf("unexpected session request %s %s", r.Method, r.URL.Path)
		}
		var req configsock.RunSessionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode session request: %v", err)
			return
		}
		if req.Kind != "sandbox" || req.RunID != "run-1" {
			t.Errorf("session identity = %+v", req)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(configsock.RunSessionResponse{Kind: req.Kind, RunID: req.RunID}); err != nil {
			t.Errorf("write session ack: %v", err)
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(sessionAcked)
		<-r.Context().Done()
		close(sessionClosed)
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		<-done
	}()

	vmmCgroup, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer vmmCgroup.Close()
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	launchErr := errors.New("launch returned")
	err = runAssignedSandbox(nodepath.RunnerPID(filepath.Join(dir, "run"), "run-1"), socket, "run-1", runSandboxOps{
		lockPidfile:   func(string) error { return nil },
		prepareCgroup: func() (*os.File, error) { return vmmCgroup, nil },
		openSession:   configsock.OpenRunSession,
		waitAssignment: func(context.Context, string, string, string) (string, error) {
			select {
			case <-sessionAcked:
			default:
				t.Fatal("assignment started before run session was acknowledged")
			}
			select {
			case <-sessionClosed:
				t.Fatal("session closed before assignment")
			default:
			}
			return "sid", nil
		},
		connectReady: func(string) (*os.File, error) { return readyW, nil },
		launchTask: func(string, string, string, *os.File, *os.File) error {
			select {
			case <-sessionClosed:
				t.Fatal("session closed before launch returned")
			default:
			}
			return launchErr
		},
	})
	if !errors.Is(err, launchErr) {
		t.Fatalf("runAssignedSandbox error = %v, want %v", err, launchErr)
	}
	select {
	case <-sessionClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("run session was not closed after launch returned")
	}
}

func TestLaunchTaskTwoStageUsesAuthoritativeEnvAndLocalLocations(t *testing.T) {
	t.Setenv("MANIFEST_KEY", "inherited-wrong-key")
	t.Setenv("KUASAR_RUN_ID", "inherited-run")
	t.Setenv("TASK_SANDBOX_ID", "bootstrap-only")
	vmm, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer vmm.Close()
	execErr := errors.New("exec intercepted")
	var fetches, reads, completions int
	stopped := false
	err = launchTaskWith(context.Background(), func() { stopped = true }, "/config.sock", "sid", "run-1", nil, vmm, taskLaunchOps{
		fetchBootstrap: func(context.Context, string, string, string) (*configsock.SandboxTaskSpec, error) {
			fetches++
			return &configsock.SandboxTaskSpec{
				SandboxID: "sid", RunID: "run-1", Workdir: "/task-work",
				Env: map[string]string{"MANIFEST_KEY": "authoritative-key", "KUASAR_RUN_ID": "run-1"},
				Prepare: &configsock.ArtifactPrepareSpec{RunID: "run-1",
					RootRef: "manifest://root", AbsoluteDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano(),
				},
			}, nil
		},
		setenv: os.Setenv,
		prepareArtifact: func(_ context.Context, spec configsock.ArtifactPrepareSpec) (*taskartifact.Result, error) {
			reads++
			if os.Getenv("MANIFEST_KEY") != "authoritative-key" || spec.RootRef != "manifest://root" {
				t.Fatalf("prepare environment/root = %q/%q", os.Getenv("MANIFEST_KEY"), spec.RootRef)
			}
			return &taskartifact.Result{
				PreparedSource: types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: "manifest://root"},
				Summary: configsock.ArtifactPrepareSummary{
					SchemaVersion: configsock.ArtifactPrepareSchemaVersion, PreparedSourceKind: string(types.ResumeSourceSnapshot),
					ResolutionDigest: strings.Repeat("1", 64), RequiredRefCount: 2,
				},
				RefLocationURIs: map[string]string{"z-location": "file:///z", "a-location": "file:///a"},
			}, nil
		},
		completePrepare: func(_ context.Context, socket, sid, runID string, summary configsock.ArtifactPrepareSummary) (*configsock.LaunchSpec, error) {
			completions++
			if socket != "/config.sock" || sid != "sid" || runID != "run-1" || summary.RequiredRefCount != 2 {
				t.Fatalf("completion = %q/%q/%q %+v", socket, sid, runID, summary)
			}
			return &configsock.LaunchSpec{
				Exec: "/bin/sandbox-ctl", Args: []string{"run"},
				Env: map[string]string{"MANIFEST_KEY": "must-not-win", "FINAL_ONLY": "yes"},
			}, nil
		},
		chdir: func(path string) error {
			if path != "/task-work" {
				t.Fatalf("workdir = %q", path)
			}
			return nil
		},
		exec: func(path string, argv, env []string) error {
			if !stopped {
				t.Fatal("task cancellation resources were not stopped before exec")
			}
			wantSuffix := []string{
				"--restore", "manifest://root",
				"--ref-location", "a-location=file:///a",
				"--ref-location", "z-location=file:///z",
			}
			if path != "/bin/sandbox-ctl" || len(argv) < len(wantSuffix) || !reflect.DeepEqual(argv[len(argv)-len(wantSuffix):], wantSuffix) {
				t.Fatalf("exec path/argv = %q, %q", path, argv)
			}
			manifestEntries := 0
			for _, entry := range env {
				switch {
				case entry == "MANIFEST_KEY=authoritative-key":
					manifestEntries++
				case strings.HasPrefix(entry, "MANIFEST_KEY="):
					t.Fatalf("non-authoritative manifest key in exec env: %q", entry)
				case strings.HasPrefix(entry, "TASK_"):
					t.Fatalf("bootstrap variable leaked into exec env: %q", entry)
				}
			}
			if manifestEntries != 1 {
				t.Fatalf("authoritative MANIFEST_KEY entries = %d", manifestEntries)
			}
			return execErr
		},
	})
	if !errors.Is(err, execErr) {
		t.Fatalf("launchTaskWith error = %v", err)
	}
	if fetches != 1 || reads != 1 || completions != 1 {
		t.Fatalf("calls fetch/read/complete = %d/%d/%d, want 1/1/1", fetches, reads, completions)
	}
}

func TestLaunchTaskColdFastPathUsesOneBootstrapOnly(t *testing.T) {
	vmm, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer vmm.Close()
	execErr := errors.New("exec intercepted")
	var fetches int
	err = launchTaskWith(context.Background(), func() {}, "/config.sock", "sid", "run", nil, vmm, taskLaunchOps{
		fetchBootstrap: func(context.Context, string, string, string) (*configsock.SandboxTaskSpec, error) {
			fetches++
			return &configsock.SandboxTaskSpec{
				SandboxID: "sid", RunID: "run", Env: map[string]string{"MANIFEST_KEY": "key"},
				Final: &configsock.LaunchSpec{Exec: "/bin/sandbox-ctl", Args: []string{"run"}},
			}, nil
		},
		prepareArtifact: func(context.Context, configsock.ArtifactPrepareSpec) (*taskartifact.Result, error) {
			t.Fatal("cold path prepared a snapshot")
			return nil, nil
		},
		completePrepare: func(context.Context, string, string, string, configsock.ArtifactPrepareSummary) (*configsock.LaunchSpec, error) {
			t.Fatal("cold path used a second RPC")
			return nil, nil
		},
		setenv: func(string, string) error { return nil },
		chdir:  func(string) error { return nil },
		exec:   func(string, []string, []string) error { return execErr },
	})
	if !errors.Is(err, execErr) || fetches != 1 {
		t.Fatalf("cold launch = %v, fetches=%d", err, fetches)
	}
}

func TestRunAssignedSandboxPreExecFailureClosesReadinessFD(t *testing.T) {
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	vmmCgroup, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("fetch failed")
	err = runAssignedSandbox("/run/sandbox/runners/run.pid", "/config.sock", "run", runSandboxOps{
		lockPidfile:   func(string) error { return nil },
		prepareCgroup: func() (*os.File, error) { return vmmCgroup, nil },
		waitAssignment: func(context.Context, string, string, string) (string, error) {
			return "sid", nil
		},
		connectReady: func(string) (*os.File, error) { return readyW, nil },
		launchTask: func(socket, sid, runID string, ready, cgroup *os.File) error {
			return launchTaskWith(context.Background(), func() {}, socket, sid, runID, ready, cgroup, taskLaunchOps{
				fetchBootstrap: func(context.Context, string, string, string) (*configsock.SandboxTaskSpec, error) {
					return nil, wantErr
				},
				setenv: func(string, string) error { return nil },
				chdir:  func(string) error { return nil },
				exec:   func(string, []string, []string) error { t.Fatal("exec called"); return nil },
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

func TestLaunchTaskRejectsLaunchSpecCgroupOverride(t *testing.T) {
	for _, arg := range []string{"--cgroup-path", "--cgroup-path=/foreign", "--cgroup-adopt", "--cgroup-adopt=false"} {
		t.Run(arg, func(t *testing.T) {
			vmm, err := os.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer vmm.Close()
			err = launchTaskWith(context.Background(), func() {}, "/config.sock", "sid", "run", nil, vmm, taskLaunchOps{
				fetchBootstrap: func(context.Context, string, string, string) (*configsock.SandboxTaskSpec, error) {
					return &configsock.SandboxTaskSpec{
						SandboxID: "sid", RunID: "run",
						Env:   map[string]string{"MANIFEST_KEY": "task-key"},
						Final: &configsock.LaunchSpec{Exec: "/bin/sandbox-ctl", Args: []string{"run", arg}},
					}, nil
				},
				setenv: func(string, string) error { return nil },
				chdir:  func(string) error { return nil },
				exec: func(string, []string, []string) error {
					t.Fatal("exec called")
					return nil
				},
			})
			if err == nil || !strings.Contains(err.Error(), "must not set") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
