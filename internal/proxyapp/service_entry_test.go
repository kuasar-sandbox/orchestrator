package proxyapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestPendingContinuationStopWaitsForEntryFailure(t *testing.T) {
	t.Setenv(continuationEnvironment, "unconsumed")
	service := NewService()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Shutdown(ctx) }()
	for {
		service.mu.Lock()
		pending, changed := !service.earlyStopAt.IsZero(), service.changed
		service.mu.Unlock()
		if pending {
			break
		}
		select {
		case <-changed:
		case <-time.After(time.Second):
			t.Fatal("pending continuation stop was not recorded")
		}
	}
	if service.stopping() {
		t.Fatal("stopped before restoring inherited responsibilities")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	failure := errors.New("invalid inherited descriptor")
	service.FinishEntry(failure)
	if err := service.Shutdown(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("entry failure was lost: %v", err)
	}
	if err := service.requestShutdown(); !errors.Is(err, failure) {
		t.Fatalf("failed entry resumed a stop with no restored owner: %v", err)
	}
	if got := os.Getenv(continuationEnvironment); got != "unconsumed" {
		t.Fatal("constructor consumed the continuation")
	}
}

func TestExpiredCandidateStopStillReachesCoordinator(t *testing.T) {
	left, right, err := newServicePair()
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	defer right.Close()
	parent, err := serviceConnection(left, "boot", "operation")
	if err != nil {
		t.Fatal(err)
	}
	defer parent.conn.Close()
	child, err := serviceConnection(right, "boot", "operation")
	if err != nil {
		t.Fatal(err)
	}
	defer child.conn.Close()
	expired := time.Now().Add(-time.Second).Round(0)
	if err := child.send(serviceMessage{Kind: "shutdown", Deadline: expired}); err != nil {
		t.Fatalf("expired business budget suppressed stop delivery: %v", err)
	}
	_ = parent.conn.SetReadDeadline(time.Now().Add(time.Second))
	message, err := parent.read()
	if err != nil || !message.Deadline.Equal(expired) {
		t.Fatalf("deadline was lost in transport: %+v %v", message, err)
	}
}

func TestCandidateStopDuringPreparationPreventsRunner(t *testing.T) {
	for _, fromCandidate := range []bool{false, true} {
		t.Run(fmt.Sprint(fromCandidate), func(t *testing.T) {
			service := NewService()
			service.started, service.candidate = true, true
			service.stopRecord, _ = newStopRecord()
			defer service.stopRecord.file.Close()
			cfg := testProxyConfig(t)
			service.baseline, _ = FreezeConfig(cfg)
			entered, release := make(chan struct{}), make(chan struct{})
			service.prepare = func(context.Context, ServiceInput) (*Startup, error) {
				close(entered)
				<-release // An application callback which finishes concurrently with stop.
				return &Startup{Effective: service.baseline, Runtime: &Runtime{}}, nil
			}
			service.runner = func(context.Context, *EffectiveConfig, *Runtime) error {
				t.Error("candidate started business after full stop acceptance")
				return nil
			}
			done := make(chan error, 1)
			go func() { done <- service.startLocal(context.Background(), "boot", "proxy-b", &retainedResources{}) }()
			<-entered
			if fromCandidate {
				// No parent is needed to exercise local publication/cancellation;
				// failure to forward is still reported to this caller.
				if err := service.requestShutdown(); !errors.Is(err, ErrNotReady) {
					t.Fatal(err)
				}
			} else {
				if _, err := service.stopRecord.request(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			if err := <-done; !errors.Is(err, ErrStopped) {
				t.Fatal(err)
			}
			if service.localDone != nil {
				t.Fatal("runner was published")
			}
		})
	}
}

func TestCandidateControlDuringPreparation(t *testing.T) {
	for _, mode := range []string{"disconnect", "error_and_drain", "error_and_drain_no_ack", "failed_snapshot"} {
		t.Run(mode, func(t *testing.T) {
			left, right, err := newServicePair()
			if err != nil {
				t.Fatal(err)
			}
			defer left.Close()
			defer right.Close()
			parent, err := serviceConnection(left, "boot", "operation")
			if err != nil {
				t.Fatal(err)
			}
			defer parent.conn.Close()
			child, err := serviceConnection(right, "boot", "operation")
			if err != nil {
				t.Fatal(err)
			}
			defer child.conn.Close()
			// Release the original copies so closing parent really disconnects B.
			left.Close()
			right.Close()
			service := NewService()
			service.started, service.candidate, service.parent = true, true, child
			service.timeout = time.Second
			service.stopRecord, _ = newStopRecord()
			defer service.stopRecord.file.Close()
			service.baseline, _ = FreezeConfig(testProxyConfig(t))
			entered := make(chan struct{})
			failure := errors.New("application preparation failed")
			service.prepare = func(ctx context.Context, _ ServiceInput) (*Startup, error) {
				close(entered)
				<-ctx.Done()
				if mode == "error_and_drain" || mode == "error_and_drain_no_ack" {
					return nil, failure
				}
				return nil, ctx.Err()
			}
			service.runner = func(context.Context, *EffectiveConfig, *Runtime) error {
				t.Error("business started during preparation cleanup")
				return nil
			}
			if mode == "failed_snapshot" {
				// State at the prepare-error/drain intersection: a diagnostic
				// instance exists, but no business runner or completion channel.
				service.local = NewInstance("proxy-b", "boot", "operation", 0)
				service.local.finish(failure)
				service.stopDeadline = time.Now().Add(time.Second)
				service.stopRequested = true
			}
			done := make(chan error, 1)
			go func() { done <- service.runCandidate(context.Background()) }()
			if mode != "failed_snapshot" {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("candidate did not prepare")
				}
			}
			if mode == "disconnect" {
				parent.conn.Close()
			} else {
				deadline := time.Now().Add(time.Second)
				if mode == "error_and_drain_no_ack" {
					deadline = time.Now().Add(20 * time.Millisecond)
				}
				if err := parent.send(serviceMessage{Kind: "drain", Deadline: deadline}); err != nil {
					t.Fatal(err)
				}
				if mode == "error_and_drain" {
					parent.conn.SetReadDeadline(deadline)
					message, err := parent.read()
					if err != nil || message.Kind != "error" || message.Error != failure.Error() {
						t.Fatalf("terminal error not delivered: %+v, %v", message, err)
					}
					if err := parent.send(serviceMessage{Kind: "error_ack", Error: message.Error}); err != nil {
						t.Fatal(err)
					}
				}
			}
			select {
			case err := <-done:
				switch mode {
				case "disconnect":
					if err == nil {
						t.Fatal("control loss was hidden")
					}
				case "error_and_drain", "error_and_drain_no_ack":
					if !errors.Is(err, failure) {
						t.Fatalf("preparation error was hidden: %v", err)
					}
				case "failed_snapshot":
					if err != nil {
						t.Fatal(err)
					}
				}
			case <-time.After(time.Second):
				t.Fatal("unstarted candidate waited for a nonexistent business completion")
			}
		})
	}
}

func TestServiceRealShutdownBeforeContinuationRun(t *testing.T) {
	if testing.Short() {
		t.Skip("real process continuation and whole-service stop")
	}
	for _, mode := range []string{"candidate", "resume"} {
		for _, expired := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/expired=%v", mode, expired), func(t *testing.T) {
				dir, err := os.MkdirTemp("", "proxy-entry-")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(dir) })
				effective, _ := testMasterEffective(t)
				cfg := effective.Config()
				cfg.ConfigSocket, cfg.StatsSocket, cfg.ShmPath = filepath.Join(dir, "config.sock"), filepath.Join(dir, "stats.sock"), filepath.Join(dir, "routes.shm")
				cfg.Workers, cfg.ShutdownTimeout = 2, "3s"
				if expired {
					cfg.ShutdownTimeout = "200ms"
				}
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				cfg.DataListen = listener.Addr().String()
				listener.Close()
				serveFixtureRoutes(t, cfg.ConfigSocket, false)
				input, _ := json.Marshal(cfg)
				configPath := filepath.Join(dir, "config.json")
				if err := os.WriteFile(configPath, input, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "stop-before-run-"+mode), nil, 0600); err != nil {
					t.Fatal(err)
				}
				executable, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				logfile, err := os.Create(filepath.Join(dir, "service.log"))
				if err != nil {
					t.Fatal(err)
				}
				defer logfile.Close()
				command := exec.Command(executable, "fixture-service", configPath, executable)
				command.Stdout, command.Stderr = logfile, logfile
				if err := command.Start(); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- command.Wait() }()
				t.Cleanup(func() {
					_ = command.Process.Signal(syscall.SIGTERM)
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						_ = command.Process.Kill()
						<-done
					}
					if t.Failed() {
						body, _ := os.ReadFile(logfile.Name())
						t.Logf("service log:\n%s", body)
					}
				})
				management := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", cfg.StatsSocket)
				}}, Timeout: time.Second}
				waitServiceStatus(t, management, func(status Status) bool { return status.Ready })
				response, err := management.Post("http://proxy/fixture/restart", "", nil)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(response.Body)
				response.Body.Close()
				if response.StatusCode != http.StatusOK {
					t.Fatalf("restart: %d %s", response.StatusCode, body)
				}
				var entry struct {
					PID      int
					Accepted time.Time
				}
				until := time.Now().Add(8 * time.Second)
				for time.Now().Before(until) {
					body, err := os.ReadFile(filepath.Join(dir, "entry-stop-"+mode+".json"))
					if err == nil && json.Unmarshal(body, &entry) == nil {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if entry.PID == 0 || (entry.PID == command.Process.Pid) != (mode == "resume") {
					t.Fatalf("entry identity: %+v", entry)
				}
				pids := fixtureDescendants(t, command.Process.Pid)
				client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: time.Second}
				response, err = client.Get("http://" + cfg.DataListen + "/fixture/id")
				if err != nil {
					t.Fatal(err)
				}
				_, err = io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != 200 {
					t.Fatalf("business before stop restoration: %d %v", response.StatusCode, err)
				}
				if expired {
					if remaining := time.Until(entry.Accepted.Add(cfg.ShutdownTimeoutDur() + 50*time.Millisecond)); remaining > 0 {
						time.Sleep(remaining)
					}
				}
				if err := os.WriteFile(filepath.Join(dir, "continue-entry"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					done <- err
					if err != nil && !expired {
						t.Fatal(err)
					}
					if expired {
						var stopped struct {
							Forced bool `json:"forced"`
						}
						body, readErr := os.ReadFile(filepath.Join(dir, "service-result.json"))
						if err == nil || readErr != nil || json.Unmarshal(body, &stopped) != nil || !stopped.Forced {
							t.Fatalf("expired stop did not report actual forced close: exit=%v read=%v body=%s", err, readErr, body)
						}
					}
				case <-time.After(6 * time.Second):
					t.Fatal("early Shutdown did not stop the original PID and candidate")
				}
				var result struct{ Deadline time.Time }
				body, err = os.ReadFile(filepath.Join(dir, "coordinator-stop.json"))
				if err != nil || json.Unmarshal(body, &result) != nil {
					t.Fatalf("entry result: %s %v", body, err)
				}
				if !result.Deadline.Equal(entry.Accepted.Add(cfg.ShutdownTimeoutDur())) {
					t.Fatalf("deadline reset: %+v entry=%+v", result, entry)
				}
				if _, err := os.Stat(filepath.Join(dir, "entry-prepared-"+mode)); !os.IsNotExist(err) {
					t.Fatalf("stopped continuation prepared business: %v", err)
				}
				for _, pid := range pids {
					if err := unix.Kill(pid, 0); err != unix.ESRCH {
						t.Fatalf("owned process %d remains: %v", pid, err)
					}
				}
				paths, _ := filepath.Glob(filepath.Join(dir, ".proxy-*"))
				if len(paths) != 0 {
					t.Fatalf("candidate resources remain: %v", paths)
				}
				t.Logf("mode=%s expired=%v main_pid=%d owned_pids=%v first_deadline=%s: stopped, no business initialization", mode, expired, command.Process.Pid, pids, result.Deadline)
			})
		}
	}
}

func fixtureDescendants(t *testing.T, pid int) []int {
	t.Helper()
	paths, err := filepath.Glob(fmt.Sprintf("/proc/%d/task/*/children", pid))
	if err != nil {
		t.Fatal(err)
	}
	var result []int
	seen := map[int]bool{}
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range strings.Fields(string(body)) {
			child, err := strconv.Atoi(value)
			if err != nil {
				t.Fatal(err)
			}
			if seen[child] {
				continue
			}
			seen[child] = true
			result = append(result, child)
			result = append(result, fixtureDescendants(t, child)...)
		}
	}
	return result
}
