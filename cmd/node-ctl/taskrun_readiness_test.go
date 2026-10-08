package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxproc"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxsdk"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"golang.org/x/sys/unix"
)

func TestWaitSandboxAssignmentRetriesInterruptedResponse(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var attempts atomic.Int32
	sid, err := waitSandboxAssignmentWithRetry(ctx, missing, "run-1", func(callCtx context.Context, socket, kind, runID string) (string, error) {
		if attempts.Add(1) < 3 {
			_, callErr := configsock.WaitAssignment(callCtx, missing, kind, runID)
			return "", callErr
		}
		return "sid-replayed", nil
	})
	if err != nil || sid != "sid-replayed" || attempts.Load() != 3 {
		t.Fatalf("assignment retry = %q, %v after %d attempts", sid, err, attempts.Load())
	}
}

func TestRunAssignedSandboxReadinessFDOrderingAndSDKExecution(t *testing.T) {
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
	startErr := errors.New("SDK start failed")
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

				execute: func(ctx context.Context, spec configsock.LaunchSpec, source types.ResumeSource, locations, env map[string]string, vmm, ready *os.File) error {
					order = append(order, "execute SDK")
					assertCloseOnExec("execute SDK", true)
					assertCgroupCloseOnExec("execute SDK", true)
					if spec.Exec != "/bin/sandbox-ctl" || spec.Workdir != "/work" || !reflect.DeepEqual(spec.Args, []string{"run"}) || vmm != vmmCgroup || ready != readyW || ctx.Err() != nil {
						t.Fatalf("SDK input mismatch: %+v", spec)
					}
					return startErr
				},
			})
		},
	})
	if !errors.Is(err, startErr) {
		t.Fatalf("runAssignedSandbox error = %v", err)
	}
	wantOrder := []string{
		"run pidfile", "prepare cgroup", "assignment", "connect ready",
		"launch task", "fetch spec", "execute SDK", "stop context",
	}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("order = %q, want %q", order, wantOrder)
	}
	if _, err := readyW.Write([]byte("x")); err == nil {
		t.Fatal("ready fd remained open after SDK start failure")
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
		// Mark the server-side admission decision before emitting the response.
		// OpenRunSession still cannot return until it has decoded that response,
		// so WaitAssignment is ordered after both admission and the protocol ACK.
		// Closing this after Flush is scheduler-racy: the client may decode the
		// bytes and enter WaitAssignment before this handler runs its next line.
		close(sessionAcked)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(configsock.RunSessionResponse{Kind: req.Kind, RunID: req.RunID}); err != nil {
			t.Errorf("write session ack: %v", err)
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
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
		openSession:   openRunSessionKeeper,
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

func startRunSessionOnlyServer(t *testing.T, socket string, sessionAck chan<- int, sessionClosed chan<- int, sessionCount *atomic.Int32) (*http.Server, <-chan error) {
	t.Helper()
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != configsock.PathRunSession || r.Method != http.MethodPut {
			t.Errorf("unexpected session request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
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
		n := int(sessionCount.Add(1))
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(configsock.RunSessionResponse{Kind: req.Kind, RunID: req.RunID}); err != nil {
			t.Errorf("write session ack: %v", err)
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case sessionAck <- n:
		default:
		}
		<-r.Context().Done()
		select {
		case sessionClosed <- n:
		default:
		}
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	return server, done
}

func TestRunAssignedSandboxSessionReconnectDoesNotDuplicateAssignmentOrLaunch(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "config.sock")
	sessionAck := make(chan int, 8)
	sessionClosed := make(chan int, 8)
	var sessionCount atomic.Int32
	server1, done1 := startRunSessionOnlyServer(t, socket, sessionAck, sessionClosed, &sessionCount)
	var server2 *http.Server
	var done2 <-chan error
	defer func() {
		if server2 != nil {
			_ = server2.Close()
			<-done2
		}
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
	var assignments atomic.Int32
	var launches atomic.Int32
	launchErr := errors.New("launch returned")
	err = runAssignedSandbox(nodepath.RunnerPID(filepath.Join(dir, "run"), "run-1"), socket, "run-1", runSandboxOps{
		lockPidfile:   func(string) error { return nil },
		prepareCgroup: func() (*os.File, error) { return vmmCgroup, nil },
		openSession:   openRunSessionKeeper,
		waitAssignment: func(context.Context, string, string, string) (string, error) {
			if assignments.Add(1) != 1 {
				t.Fatal("assignment called more than once")
			}
			select {
			case got := <-sessionAck:
				if got != 1 {
					t.Fatalf("first session ack = %d", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("first session was not acknowledged before assignment")
			}
			_ = server1.Close()
			select {
			case err := <-done1:
				if err != http.ErrServerClosed {
					t.Fatalf("first server returned %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("first server did not stop")
			}
			server2, done2 = startRunSessionOnlyServer(t, socket, sessionAck, sessionClosed, &sessionCount)
			select {
			case got := <-sessionAck:
				if got != 2 {
					t.Fatalf("reconnected session ack = %d", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("run session did not reconnect after server restart")
			}
			return "sid", nil
		},
		connectReady: func(string) (*os.File, error) { return readyW, nil },
		launchTask: func(string, string, string, *os.File, *os.File) error {
			if launches.Add(1) != 1 {
				t.Fatal("launch called more than once")
			}
			return launchErr
		},
	})
	if !errors.Is(err, launchErr) {
		t.Fatalf("runAssignedSandbox error = %v, want %v", err, launchErr)
	}
	if got := assignments.Load(); got != 1 {
		t.Fatalf("assignments = %d, want 1", got)
	}
	if got := launches.Load(); got != 1 {
		t.Fatalf("launches = %d, want 1", got)
	}
	select {
	case got := <-sessionClosed:
		if got != 1 && got != 2 {
			t.Fatalf("closed session = %d", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no session close observed")
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
	startErr := errors.New("SDK execution intercepted")
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
		execute: func(ctx context.Context, spec configsock.LaunchSpec, source types.ResumeSource, locations, env map[string]string, _, _ *os.File) error {
			if stopped || ctx.Err() != nil {
				t.Fatal("runtime lifetime stopped before SDK execution")
			}
			if spec.Workdir != "/task-work" || spec.Exec != "/bin/sandbox-ctl" || !reflect.DeepEqual(spec.Args, []string{"run"}) {
				t.Fatalf("SDK launch spec = %+v", spec)
			}
			if source.Kind != types.ResumeSourceSnapshot || source.Ref != "manifest://root" || source.SandboxRef == "" {
				t.Fatalf("source = %+v", source)
			}
			if !reflect.DeepEqual(locations, map[string]string{"a-location": "file:///a", "z-location": "file:///z"}) {
				t.Fatalf("locations=%v", locations)
			}
			if env["MANIFEST_KEY"] != "authoritative-key" || env["FINAL_ONLY"] != "yes" || env["KUASAR_RUN_ID"] != "run-1" {
				t.Fatal("SDK authoritative environment mismatch")
			}
			for key := range env {
				if strings.HasPrefix(key, "TASK_") {
					t.Fatal("bootstrap environment leaked")
				}
			}

			return startErr
		},
	})
	if !errors.Is(err, startErr) {
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
	startErr := errors.New("SDK execution intercepted")
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

		execute: func(context.Context, configsock.LaunchSpec, types.ResumeSource, map[string]string, map[string]string, *os.File, *os.File) error {
			return startErr
		},
	})
	if !errors.Is(err, startErr) || fetches != 1 {
		t.Fatalf("cold launch = %v, fetches=%d", err, fetches)
	}
}

func TestRunAssignedSandboxPrepareFailureClosesReadinessFD(t *testing.T) {
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

				execute: func(context.Context, configsock.LaunchSpec, types.ResumeSource, map[string]string, map[string]string, *os.File, *os.File) error {
					t.Fatal("SDK execution called")
					return nil
				},
			})
		},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	buf := make([]byte, 1)
	if n, err := readyR.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read after prepare failure = %d, %v; want EOF", n, err)
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

				execute: func(context.Context, configsock.LaunchSpec, types.ResumeSource, map[string]string, map[string]string, *os.File, *os.File) error {
					t.Fatal("SDK execution called")
					return nil
				},
			})
			if err == nil || !strings.Contains(err.Error(), "must not set") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestSDKReportFailureKeepsObservedResult(t *testing.T) {
	for _, tc := range []struct {
		name    string
		invalid bool
		code    int
		runErr  error
		stage   types.SandboxExecutionStage
	}{
		{name: "adapter", invalid: true, stage: types.SandboxResultStart},
		{name: "terminal", code: 7, stage: types.SandboxResultRun},
		{name: "usage", code: 2, runErr: errors.New("invalid environment default"), stage: types.SandboxResultRun},
		{name: "startup", runErr: errors.New("SDK startup failed"), stage: types.SandboxResultRun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := sdkLaunchSpec(t)
			if tc.invalid {
				spec.Exec = "/bin/sh"
			}
			readyR, readyW, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer readyR.Close()
			var got configsock.SandboxExecutionResult
			cleaned := false
			err = executeSandboxAndReportWith(context.Background(), "sid", "run-1", spec, types.ResumeSource{}, nil, map[string]string{"MANIFEST_KEY": ""}, nil, readyW,
				func(ctx context.Context, in *sandboxsdk.Input, cg *os.File, ready *sandboxsdk.Readiness) (int, error) {
					if tc.invalid {
						t.Fatal("executed rejected input")
					}
					cleaned = true
					return tc.code, tc.runErr
				}, func(result configsock.SandboxExecutionResult) error {
					got = result
					if !tc.invalid && !cleaned {
						t.Fatal("published before SDK cleanup")
					}
					_ = readyR.SetReadDeadline(time.Now().Add(time.Second))
					wire, err := io.ReadAll(readyR)
					if err != nil || len(wire) != 0 {
						t.Fatalf("readiness not EOF before result: %q %v", wire, err)
					}
					return errors.New("report rejected")
				})
			var reported sandboxRunReportedError
			if !errors.As(err, &reported) || !strings.Contains(err.Error(), "report rejected") {
				t.Fatalf("reported error=%v", err)
			}
			if got.Stage != tc.stage || got.SID != "sid" || got.RunID != "run-1" {
				t.Fatalf("result=%+v", got)
			}
			if !tc.invalid {
				want := tc.code
				if tc.runErr != nil && want <= 0 {
					want = 1
				}
				if got.ExitCode == nil || *got.ExitCode != want {
					t.Fatalf("exit=%v want %d", got.ExitCode, want)
				}
			}
		})
	}
}

func sdkLaunchSpec(t *testing.T) configsock.LaunchSpec {
	t.Helper()
	dir := t.TempDir()
	return configsock.LaunchSpec{Exec: "/opt/bin/sandbox-ctl", Workdir: dir, Args: []string{"run", "--sandbox-id", "sid", "--path-id", "sid", "--config", dir + "/sandbox.yaml", "--manifest-config", "", "--run-root", dir + "/run", "--base-root", dir + "/base", "--log-to", "default", "--stdout-to", dir + "/stdout", "--stderr-to", dir + "/stderr", "--console", "off"}}
}

func TestLaunchTaskPrepareFailureClosesReadinessBeforeReportACK(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "config.sock")
	resultReceived := make(chan struct{})
	releaseResult := make(chan struct{})
	server, done := startSandboxLaunchResultServer(t, socket, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case configsock.PathTaskSandboxBootstrap:
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(configsock.SandboxTaskSpec{Error: "bootstrap failed"})
		case configsock.PathRunSandboxResult:
			var req configsock.SandboxResultRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode result: %v", err)
				return
			}
			if req.Result.Stage != types.SandboxResultPrepare {
				t.Errorf("prepare failure report stage = %s", req.Result.Stage)
			}
			close(resultReceived)
			<-releaseResult
			_ = json.NewEncoder(w).Encode(configsock.SandboxResultResponse{})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	defer closeTestHTTPServer(t, server, done)

	vmm, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer vmm.Close()
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	errCh := make(chan error, 1)
	go func() { errCh <- launchTask(context.Background(), func() {}, socket, "sid", "run-1", readyW, vmm, nil) }()
	select {
	case <-resultReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("prepare result was not posted")
	}
	if err := readyR.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if n, err := readyR.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("readiness before report ACK = %d, %v; want EOF", n, err)
	}
	close(releaseResult)
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "bootstrap failed") {
			t.Fatalf("launchTask error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("launchTask did not return after report ACK")
	}
}

func TestParentRunPidfilesAndSessionFDsAreNotInheritedBySandboxprocChild(t *testing.T) {
	dir := t.TempDir()
	runPidfile := filepath.Join(dir, "runners", "sr-test.pid")
	buildPidfile := filepath.Join(dir, "builds", "bid", "builder.pid")
	for _, path := range []string{runPidfile, buildPidfile} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := lockPidfile(path); err != nil {
			t.Fatalf("lockPidfile(%s): %v", path, err)
		}
	}

	socket := filepath.Join(dir, "config.sock")
	sessionAck := make(chan struct{})
	sessionRelease := make(chan struct{})
	server, done := startSandboxLaunchResultServer(t, socket, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != configsock.PathRunSession {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(configsock.RunSessionResponse{Kind: "sandbox", RunID: "sr-test"})
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(sessionAck)
		<-sessionRelease
	})
	defer close(sessionRelease)
	defer closeTestHTTPServer(t, server, done)
	keeper, err := configsock.OpenRunSessionKeeper(context.Background(), socket, "sandbox", "sr-test")
	if err != nil {
		t.Fatal(err)
	}
	defer keeper.Close()
	select {
	case <-sessionAck:
	case <-time.After(2 * time.Second):
		t.Fatal("session was not established")
	}

	vmm, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer vmm.Close()
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNodeCtlFDInspectionChild$", "--")
	cmd.Env = append(os.Environ(),
		"NODE_CTL_FD_INSPECTION_CHILD=1",
		"NODE_CTL_FORBIDDEN_FD_PATHS="+runPidfile+string(os.PathListSeparator)+buildPidfile,
		"NODE_CTL_FORBID_SOCKET_FDS=1",
	)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := sandboxproc.Start(cmd, vmm, readyW); err != nil {
		t.Fatal(err)
	}
	if err := readyR.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	wire, err := io.ReadAll(readyR)
	if err != nil {
		t.Fatal(err)
	}
	if string(wire) != "ready\n" {
		t.Fatalf("child readiness = %q", wire)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestNodeCtlFDInspectionChild(t *testing.T) {
	if os.Getenv("NODE_CTL_FD_INSPECTION_CHILD") != "1" {
		return
	}
	forbidden := map[string]bool{}
	for _, path := range filepath.SplitList(os.Getenv("NODE_CTL_FORBIDDEN_FD_PATHS")) {
		if path != "" {
			forbidden[path] = true
		}
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		fdPath := filepath.Join("/proc/self/fd", entry.Name())
		target, err := os.Readlink(fdPath)
		if err != nil {
			continue
		}
		if forbidden[target] {
			t.Fatalf("inherited forbidden parent pidfile fd %s -> %s", entry.Name(), target)
		}
		if os.Getenv("NODE_CTL_FORBID_SOCKET_FDS") == "1" && strings.HasPrefix(target, "socket:[") {
			t.Fatalf("inherited parent socket fd %s -> %s", entry.Name(), target)
		}
	}
	readyFD := -1
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "--ready-fd=") {
			readyFD, _ = strconv.Atoi(strings.TrimPrefix(arg, "--ready-fd="))
		}
	}
	if readyFD < 0 {
		t.Fatal("missing ready fd")
	}
	ready := os.NewFile(uintptr(readyFD), "ready")
	if _, err := io.WriteString(ready, "ready\n"); err != nil {
		t.Fatal(err)
	}
	if err := ready.Close(); err != nil {
		t.Fatal(err)
	}
}

func startSandboxLaunchResultServer(t *testing.T, socket string, handler http.HandlerFunc) (*http.Server, <-chan error) {
	t.Helper()
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	return server, done
}

func closeTestHTTPServer(t *testing.T, server *http.Server, done <-chan error) {
	t.Helper()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}
}

func intPtr(v int) *int { return &v }

func TestLaunchTaskUsesLibraryWithoutSandboxCtlProcess(t *testing.T) {
	spec := sdkLaunchSpec(t)
	marker := filepath.Join(spec.Workdir, "child-started")
	spec.Exec = filepath.Join(spec.Workdir, "sandbox-ctl")
	if err := os.WriteFile(spec.Exec, []byte("#!/bin/sh\ntouch '"+marker+"'\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(spec.Workdir, "config.sock")
	var got configsock.SandboxExecutionResult
	server, done := startSandboxLaunchResultServer(t, socket, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case configsock.PathTaskSandboxBootstrap:
			_ = json.NewEncoder(w).Encode(configsock.SandboxTaskSpec{SandboxID: "sid", RunID: "run", Env: map[string]string{"MANIFEST_KEY": ""}, Final: &spec})
		case configsock.PathRunSandboxResult:
			var req configsock.SandboxResultRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			got = req.Result
			_ = json.NewEncoder(w).Encode(configsock.SandboxResultResponse{})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	defer closeTestHTTPServer(t, server, done)
	cg, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cg.Close()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	err = launchTask(context.Background(), func() {}, socket, "sid", "run", w, cg, nil)
	if err == nil || err.Error() != "exit status 1" {
		t.Fatalf("expected CLI-equivalent SDK config failure, got %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("sandbox-ctl child was executed")
	}
	if got.Stage != types.SandboxResultRun || got.ExitCode == nil || *got.ExitCode != 1 || got.Error != "exit status 1" {
		t.Fatalf("library failure result=%+v", got)
	}
	_ = r.SetReadDeadline(time.Now().Add(time.Second))
	if wire, err := io.ReadAll(r); err != nil || len(wire) != 0 {
		t.Fatalf("readiness=%q %v", wire, err)
	}
}

func TestSDKExecutionLifetimeAndCleanupPrecedeResult(t *testing.T) {
	spec := sdkLaunchSpec(t)
	signals := make(chan os.Signal, 4)
	ctx, stop := sandbox.RunSignalContext(context.Background(), signals)
	defer stop()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	running := make(chan struct{})
	cleanup := make(chan struct{})
	reported := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- executeSandboxAndReportWith(ctx, "sid", "run", spec, types.ResumeSource{}, nil, map[string]string{"MANIFEST_KEY": ""}, nil, w,
			func(ctx context.Context, _ *sandboxsdk.Input, _ *os.File, ready *sandboxsdk.Readiness) (int, error) {
				ready.Notify(sandbox.ReadinessControlReady)
				ready.Notify(sandbox.ReadinessReady)
				close(running)
				<-ctx.Done()
				close(cleanup)
				return 0, nil
			}, func(result configsock.SandboxExecutionResult) error {
				select {
				case <-cleanup:
				default:
					t.Error("result before cleanup")
				}
				if result.ExitCode == nil || *result.ExitCode != 0 || result.RunID != "run" {
					t.Errorf("result=%+v", result)
				}
				close(reported)
				return nil
			})
	}()
	<-running
	_ = r.SetReadDeadline(time.Now().Add(time.Second))
	wire, err := io.ReadAll(r)
	if err != nil || string(wire) != "control_ready\nready\n" {
		t.Fatalf("ready must close while VM is alive: %q %v", wire, err)
	}
	select {
	case <-reported:
		t.Fatal("reported while VM alive")
	default:
	}
	signals <- syscall.SIGTERM
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("signal did not stop execution")
	}
}
