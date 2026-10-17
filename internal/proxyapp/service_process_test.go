package proxyapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"golang.org/x/sys/unix"
)

var fixtureBuild = "initial"

var fixtureTargetOnce sync.Once
var fixtureTargetDirectory string
var fixtureTargetError error

// Every scenario uses the same source and target build marker. Compile that
// immutable image once per test process, then copy it into each scenario's
// private deployment so chmod/replacement faults cannot affect another test.
func fixtureTargetBinary(t *testing.T) string {
	t.Helper()
	fixtureTargetOnce.Do(func() {
		fixtureTargetDirectory, fixtureTargetError = os.MkdirTemp("", "proxy-target-build-")
		if fixtureTargetError != nil {
			return
		}
		buildArgs := []string{"test", "-c", "-o", filepath.Join(fixtureTargetDirectory, "target-proxy"), "-ldflags", "-X github.com/kuasar-sandbox/orchestrator/internal/proxyapp.fixtureBuild=target"}
		if fixtureRace {
			buildArgs = append(buildArgs, "-race")
		}
		buildArgs = append(buildArgs, ".")
		started := time.Now()
		output, err := exec.Command("go", buildArgs...).CombinedOutput()
		if err != nil {
			fixtureTargetError = fmt.Errorf("target build: %w\n%s", err, output)
		}
		t.Logf("compiled compatible target once in %s (race=%v)", time.Since(started), fixtureRace)
	})
	if fixtureTargetError != nil {
		t.Fatal(fixtureTargetError)
	}
	return filepath.Join(fixtureTargetDirectory, "target-proxy")
}

type serviceFixtureExtension struct{ s *Service }

// Cancel only after the target really has an open process-local descriptor.
// This context is part of the test executable, not a production fault hook.
type fixtureCancelOnTargetOpen struct {
	context.Context
	target   os.FileInfo
	cancel   context.CancelFunc
	observed bool
}

func fixtureTargetOpen(target os.FileInfo) bool {
	entries, _ := os.ReadDir("/proc/self/fd")
	for _, entry := range entries {
		if info, err := os.Stat("/proc/self/fd/" + entry.Name()); err == nil && os.SameFile(info, target) {
			return true
		}
	}
	return false
}

func (c *fixtureCancelOnTargetOpen) Err() error {
	if !c.observed && fixtureTargetOpen(c.target) {
		c.observed = true
		c.cancel()
	}
	return c.Context.Err()
}

func (*serviceFixtureExtension) Start(context.Context, proxyextension.MasterHost) error { return nil }
func (*serviceFixtureExtension) Shutdown(context.Context) error                         { return nil }

// Fail actual candidate worker processes before they can advertise readiness.
// Only the test executable consults this file; A's workers use ordinary paths.
func fixtureCandidateWorkerFailure(bootstrap *WorkerBootstrap) error {
	cfg := bootstrap.EffectiveConfig().Config()
	if !strings.HasPrefix(filepath.Base(cfg.StatsSocket), ".proxy-") {
		return nil
	}
	dir := filepath.Dir(cfg.StatsSocket)
	fault, err := os.ReadFile(filepath.Join(dir, "candidate-fault"))
	if err != nil || string(fault) != "worker" {
		return nil
	}
	body, err := json.Marshal(struct {
		PID      int
		Identity controlIdentity
	}{os.Getpid(), bootstrap.identity})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("candidate-worker-fault-%d.json", os.Getpid())), body, 0600); err != nil {
		return err
	}
	return fmt.Errorf("fixture candidate worker initialization failed")
}

func (e *serviceFixtureExtension) WrapManagement(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fixture/cancel-opening-restart":
			e.s.mu.Lock()
			path := e.s.input.Executable
			e.s.mu.Unlock()
			info, err := os.Stat(path)
			if err != nil || fixtureTargetOpen(info) {
				http.Error(w, "target unavailable or already open", 500)
				return
			}
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			opening := &fixtureCancelOnTargetOpen{Context: ctx, target: info, cancel: cancel}
			id, err := e.s.InplaceRestart(opening)
			if !opening.observed || id != "" || !errors.Is(err, context.Canceled) || fixtureTargetOpen(info) {
				http.Error(w, fmt.Sprintf("target cancellation: observed=%v id=%s err=%v", opening.observed, id, err), 500)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/fixture/concurrent-restart":
			type result struct {
				id  RestartID
				err error
			}
			results := make(chan result, 8)
			start := make(chan struct{})
			for range 8 {
				go func() {
					ctx, cancel := context.WithCancel(r.Context())
					<-start
					id, err := e.s.InplaceRestart(ctx)
					cancel() // An accepted operation outlives this caller.
					results <- result{id, err}
				}()
			}
			close(start)
			var accepted RestartID
			var failure error
			for range 8 {
				result := <-results
				if result.err == nil && result.id != "" && accepted == "" {
					accepted = result.id
				} else if result.id != "" || !errors.Is(result.err, ErrRestartInProgress) {
					failure = errors.Join(failure, fmt.Errorf("unexpected concurrent result: %s %v", result.id, result.err))
				}
			}
			if failure != nil || accepted == "" {
				http.Error(w, fmt.Sprintf("concurrent acceptance: id=%s err=%v", accepted, failure), 500)
				return
			}
			fmt.Fprint(w, accepted)
		case "/fixture/cancelled-restart":
			ctx, cancel := context.WithCancel(r.Context())
			cancel()
			id, err := e.s.InplaceRestart(ctx)
			if id != "" || !errors.Is(err, context.Canceled) {
				http.Error(w, fmt.Sprintf("cancelled request: id=%s err=%v", id, err), 500)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/fixture/restart":
			id, err := e.s.InplaceRestart(r.Context())
			if err != nil {
				http.Error(w, err.Error(), 409)
				return
			}
			fmt.Fprint(w, id)
		case "/fixture/stop":
			go e.s.Shutdown(context.Background())
			w.WriteHeader(202)
		case "/fixture/fail-local":
			w.WriteHeader(202)
			w.(http.Flusher).Flush()
			e.s.mu.Lock()
			instance := e.s.local
			e.s.mu.Unlock()
			// Test-only recoverable instance termination. The coordinator and
			// its control/reaping responsibilities remain alive.
			instance.requestStop(time.Now().Add(e.s.timeout))
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func runFixtureService() error {
	service := NewService()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var continuation *Continuation
	var input ServiceInput
	if ContinuationPresent() {
		var err error
		continuation, err = ReceiveContinuation()
		if err != nil {
			return err
		}
		defer continuation.Close()
		input = continuation.Input()
	} else {
		if len(os.Args) != 4 {
			return fmt.Errorf("fixture input missing")
		}
		raw, err := os.ReadFile(os.Args[2])
		if err != nil {
			return err
		}
		input = ServiceInput{Config: raw, Executable: os.Args[3], Native: true, Signals: true}
	}
	if continuation != nil {
		if err := fixtureStopBeforeRun(service, input, continuation.envelope.Mode); err != nil {
			service.FinishEntry(err)
			return err
		}
	}
	prepare := func(ctx context.Context, input ServiceInput) (*Startup, error) {
		var cfg publicconfig.Proxy
		if err := json.Unmarshal(input.Config, &cfg); err != nil {
			return nil, err
		}
		if continuation != nil {
			mode := continuation.envelope.Mode
			if _, err := os.Stat(filepath.Join(filepath.Dir(cfg.StatsSocket), "stop-before-run-"+mode)); err == nil {
				_ = os.WriteFile(filepath.Join(filepath.Dir(cfg.StatsSocket), "entry-prepared-"+mode), nil, 0600)
			}
		}
		// This fault hook exists only in the test executable, after restored
		// supervision and before application preparation can fail.
		if continuation != nil && continuation.envelope.Mode == "candidate" {
			fault, err := os.ReadFile(filepath.Join(filepath.Dir(cfg.StatsSocket), "candidate-fault"))
			if err == nil {
				if err := os.WriteFile(filepath.Join(filepath.Dir(cfg.StatsSocket), "candidate-fault-pid"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
					return nil, err
				}
				switch string(fault) {
				case "initialization":
					return nil, fmt.Errorf("fixture candidate initialization failed")
				case "configuration":
					cfg.Workers++ // A compatible ELF with an incompatible Configure result.
				case "control":
					service.mu.Lock()
					parent := service.parent
					service.mu.Unlock()
					parent.conn.Close()
					return nil, fmt.Errorf("fixture candidate control closed")
				}
			}
		}
		if continuation != nil && continuation.envelope.Mode == "resume" {
			if _, err := os.Stat(filepath.Join(filepath.Dir(cfg.StatsSocket), "fail-resume")); err == nil {
				return nil, fmt.Errorf("fixture A-prime initialization failure")
			}
		}
		effective, err := FreezeConfig(&cfg)
		if err != nil {
			return nil, err
		}
		return &Startup{Effective: effective, Runtime: &Runtime{Logger: testMasterLogger(), MasterExtension: &serviceFixtureExtension{s: service}}}, nil
	}
	err := RunEntry(ctx, service, input, continuation, prepare, RunMaster)
	if continuation != nil {
		var cfg publicconfig.Proxy
		if json.Unmarshal(input.Config, &cfg) == nil {
			service.mu.Lock()
			body, _ := json.Marshal(struct {
				Deadline time.Time
				Started  bool
			}{service.stopDeadline, service.local != nil})
			service.mu.Unlock()
			_ = os.WriteFile(filepath.Join(filepath.Dir(cfg.StatsSocket), "entry-result-"+continuation.envelope.Mode+".json"), body, 0600)
		}
	}
	var config publicconfig.Proxy
	if json.Unmarshal(input.Config, &config) == nil {
		if continuation == nil || continuation.envelope.Mode == "resume" {
			service.mu.Lock()
			state, _ := json.Marshal(struct{ Deadline time.Time }{service.stopDeadline})
			service.mu.Unlock()
			_ = os.WriteFile(filepath.Join(filepath.Dir(config.StatsSocket), "coordinator-stop.json"), state, 0600)
		}
		result, _ := json.Marshal(map[string]bool{"forced": errors.Is(service.Shutdown(context.Background()), ErrForcedClose)})
		_ = os.WriteFile(filepath.Join(filepath.Dir(config.StatsSocket), "service-result.json"), result, 0600)
	}
	return err
}

// This injection belongs only to the test executable. It models a programmatic
// caller racing startup before any continuation descriptor has been restored by
// Service.Run, and allows the parent test to observe the actual process first.
func fixtureStopBeforeRun(service *Service, input ServiceInput, mode string) error {
	var cfg publicconfig.Proxy
	if err := json.Unmarshal(input.Config, &cfg); err != nil {
		return err
	}
	dir := filepath.Dir(cfg.StatsSocket)
	if _, err := os.Stat(filepath.Join(dir, "stop-before-run-"+mode)); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Shutdown(ctx) }()
	var accepted time.Time
	for {
		var changed <-chan struct{}
		service.mu.Lock()
		accepted, changed = service.earlyStopAt, service.changed
		service.mu.Unlock()
		if !accepted.IsZero() {
			break
		}
		select {
		case <-changed:
		case err := <-done:
			return fmt.Errorf("pre-Run stop returned before restoration: %v", err)
		case <-time.After(time.Second):
			return fmt.Errorf("pre-Run stop was not recorded")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		return fmt.Errorf("pre-Run caller cancellation: %v", err)
	}
	body, _ := json.Marshal(struct {
		PID      int
		Accepted time.Time
	}{os.Getpid(), accepted})
	if err := os.WriteFile(filepath.Join(dir, "entry-stop-"+mode+".json"), body, 0600); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dir, "continue-entry")); err == nil {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("fixture entry was not released")
}

func TestServiceRealImageReplacement(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{concurrent: true})
}

func TestServiceRealSignalReplacement(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{signals: true})
}
func TestServiceRealAutomaticRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{recovery: true})
}
func TestServiceRealShutdownDuringRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{recovery: true, stopRecovery: true})
}

func TestServiceRealExecFailureRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{execFailure: true, metrics: "ipv4"})
}

func TestServiceRealMetricsResources(t *testing.T) {
	for _, mode := range []string{"ipv4", "ipv6", "dynamic", "occupied", "boundary"} {
		t.Run(mode, func(t *testing.T) {
			testServiceRealImageReplacement(t, serviceScenario{metrics: mode})
		})
	}
}

func TestServiceRealLegacyConductor(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{legacy: true})
}

func TestServiceRealPIDAdmissionAndCandidateReconnect(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{pidAdmission: true})
}

func TestServiceRealCandidateCleanupRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{cleanupFailure: true})
}

func TestServiceRealCandidatePreparationRollback(t *testing.T) {
	for _, fault := range []string{"initialization", "configuration", "control", "worker"} {
		t.Run(fault, func(t *testing.T) {
			testServiceRealImageReplacement(t, serviceScenario{preparationFault: fault})
		})
	}
}

func TestServiceRealCandidateFailureDuringDrain(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{candidateFailure: true})
}

func TestServiceRealShutdownDuringOriginalDrain(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{stopDrain: true})
}

func TestServiceRealForcedShutdownDuringOriginalDrain(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{stopDrain: true, forceStopDrain: true})
}

func TestServiceRealSuccessorFailureDuringCandidateDrain(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{successorFailure: true})
}

type serviceScenario struct {
	cleanupFailure                                                                                              bool
	pidAdmission                                                                                                bool
	preparationFault                                                                                            string
	concurrent                                                                                                  bool
	signals, recovery, stopRecovery, execFailure, candidateFailure, stopDrain, forceStopDrain, successorFailure bool
	legacy                                                                                                      bool
	metrics                                                                                                     string
}

func testServiceRealImageReplacement(t *testing.T, scenario serviceScenario) {
	signals, recovery, stopRecovery := scenario.signals, scenario.recovery, scenario.stopRecovery
	if testing.Short() {
		t.Skip("real compatible target build and process lifecycle")
	}
	effective, _ := testMasterEffective(t)
	cfg := effective.Config()
	// Keep real Unix endpoints below the kernel limit independently of Go's
	// full test-name-based temporary directory. Product length checks stay on.
	shortRoot, err := os.MkdirTemp("", "proxy-lifecycle-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortRoot) })
	cfg.ConfigSocket = filepath.Join(shortRoot, "config.sock")
	cfg.StatsSocket = filepath.Join(shortRoot, "stats.sock")
	cfg.ShmPath = filepath.Join(shortRoot, "routes.shm")
	cfg.Workers = 2
	cfg.ShutdownTimeout = "3s"
	if scenario.successorFailure {
		cfg.ShutdownTimeout = "8s"
	}
	cfg.InplaceRestart = signals
	var occupiedMetrics net.Listener
	if scenario.metrics != "" {
		cfg.MetricsListen, occupiedMetrics = fixtureMetricsAddress(t, scenario.metrics)
		if occupiedMetrics != nil {
			defer occupiedMetrics.Close()
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DataListen = listener.Addr().String()
	listener.Close()
	pidfile := ""
	if scenario.pidAdmission {
		pidfile = filepath.Join(shortRoot, "admitted.pid")
	}
	disconnectCandidate := serveFixtureRoutes(t, cfg.ConfigSocket, false, routeFixtureOptions{legacy: scenario.legacy, pidfile: pidfile})
	dir := filepath.Dir(cfg.StatsSocket)
	initial, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	deployment := filepath.Join(dir, "deployed-proxy")
	copyExecutable(t, initial, deployment)
	configPath := filepath.Join(dir, "input.json")
	raw, _ := json.Marshal(cfg)
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target-proxy")
	copyExecutable(t, fixtureTargetBinary(t), target)
	logfile, err := os.Create(filepath.Join(dir, "service.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logfile.Close()
	command := exec.Command(deployment, "fixture-service", configPath, deployment)
	command.Stdout = logfile
	command.Stderr = logfile
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			_ = command.Process.Kill()
			<-done
		}
		data, _ := os.ReadFile(logfile.Name())
		if t.Failed() {
			t.Logf("service log:\n%s", data)
		}
	})
	pid := command.Process.Pid
	if pidfile != "" {
		if err := os.WriteFile(pidfile, []byte(fmt.Sprint(pid)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
	management := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.StatsSocket)
	}}, Timeout: time.Second}
	status := waitServiceStatus(t, management, func(status Status) bool { return status.Ready })
	if status.PID != pid || status.CoordinatorPID != pid || !status.ServiceReady {
		t.Fatalf("initial identity: %+v", status)
	}
	initialBoot, initialMetrics := status.BootID, status.MetricsAddress
	if scenario.metrics != "" {
		if scenario.metrics != "dynamic" && initialMetrics != cfg.MetricsListen {
			t.Fatalf("A changed configured metrics address: %s want=%s", initialMetrics, cfg.MetricsListen)
		}
		checkFixtureMetrics(t, initialMetrics)
	}
	cancelPaths := []string{"/fixture/cancelled-restart"}
	if scenario.concurrent {
		cancelPaths = append(cancelPaths, "/fixture/cancel-opening-restart")
	}
	for _, path := range cancelPaths {
		response, err := management.Post("http://proxy"+path, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusNoContent {
			t.Fatalf("pre-acceptance cancellation: %s %d %s %v", path, response.StatusCode, body, err)
		}
	}
	status = waitServiceStatus(t, management, func(status Status) bool { return status.Ready })
	if status.InplaceRestart != nil || len(status.Instances) != 1 || status.BootID != initialBoot {
		t.Fatalf("cancelled request changed the running service: %+v", status)
	}
	if err := os.Rename(target, deployment); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var businessErrors []error
	var expectedLosses []error
	var faultWindow atomic.Bool
	var builds = map[string]int{}
	loadCtx, stopLoad := context.WithCancel(context.Background())
	loadDone := make(chan struct{})
	go func() {
		defer close(loadDone)
		for loadCtx.Err() == nil {
			response, err := client.Get("http://" + cfg.DataListen + "/fixture/id")
			if err == nil {
				body, readErr := io.ReadAll(response.Body)
				response.Body.Close()
				if readErr != nil {
					err = readErr
				} else if response.StatusCode != 200 {
					err = fmt.Errorf("business %d %q", response.StatusCode, body)
				} else {
					mu.Lock()
					builds[response.Header.Get("X-Fixture-Build")]++
					mu.Unlock()
				}
			}
			if err != nil {
				mu.Lock()
				if faultWindow.Load() && (errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) {
					expectedLosses = append(expectedLosses, err)
				} else {
					t.Logf("unexpected connection result: during_injected_failure=%v type=%T error=%v", faultWindow.Load(), err, err)
					businessErrors = append(businessErrors, err)
				}
				mu.Unlock()
			}
		}
	}()
	defer func() { stopLoad(); <-loadDone }()
	var previous RestartID
	stopFromCandidate := false
	var stoppedCandidate int
	var cancelledAttempt time.Time
	rejectRestart := scenario.legacy || scenario.metrics == "boundary"
	iterations := 2
	if scenario.preparationFault != "" {
		iterations++ // One rejected candidate, then two complete operations.
	}
	for iteration := 0; iteration < iterations; iteration++ {
		var releaseHeld func()
		if (scenario.execFailure || scenario.candidateFailure || scenario.stopDrain || scenario.successorFailure || scenario.cleanupFailure) && iteration == 0 || scenario.concurrent || scenario.pidAdmission || scenario.metrics != "" && !rejectRestart && !(scenario.metrics == "occupied" && iteration == 0) {
			release := make(chan struct{})
			var once sync.Once
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-release:
					fmt.Fprint(w, "complete\n")
				case <-r.Context().Done():
				}
			}))
			defer upstream.Close()
			defer once.Do(func() { close(release) })
			request, _ := http.NewRequest("GET", "http://"+cfg.DataListen+"/fixture/hold", nil)
			request.Header.Set("X-Fixture-Target", upstream.URL)
			// The original request predates B preparation. Its client deadline
			// must include that separate 30s budget, not just the drain budget.
			// The server's shutdown_timeout and full-body assertion stay fixed.
			heldClient := &http.Client{Transport: client.Transport, Timeout: candidatePrepareTimeout + cfg.ShutdownTimeoutDur() + time.Second}
			heldAt := time.Now()
			response, err := heldClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			releaseHeld = func() {
				t.Logf("release finite response: iteration=%d held_for=%s", iteration, time.Since(heldAt))
				if !scenario.forceStopDrain {
					once.Do(func() { close(release) })
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if scenario.forceStopDrain {
					if err == nil || string(body) != "begin\n" {
						t.Fatalf("forced original request did not expose truncation: %q %v", body, err)
					}
					return
				}
				if err != nil || string(body) != "begin\ncomplete\n" {
					t.Fatalf("held request truncated: %q %v", body, err)
				}
			}
		}
		if recovery && iteration == 0 {
			if err := os.WriteFile(filepath.Join(dir, "fail-resume"), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if scenario.preparationFault != "" && iteration == 0 {
			if err := os.WriteFile(filepath.Join(dir, "candidate-fault"), []byte(scenario.preparationFault), 0600); err != nil {
				t.Fatal(err)
			}
		}
		operationRequestedAt := time.Now()
		var id RestartID
		if signals {
			for repeat := 0; repeat < 4; repeat++ {
				if err := command.Process.Signal(syscall.SIGUSR2); err != nil {
					t.Fatal(err)
				}
			}
			status = waitServiceStatus(t, management, func(status Status) bool { return status.InplaceRestart != nil && status.InplaceRestart.ID != previous })
			id = status.InplaceRestart.ID
		} else {
			path := "/fixture/restart"
			if scenario.concurrent {
				path = "/fixture/concurrent-restart"
			}
			// Acceptance includes reading and hashing the target executable. It
			// has no one-second status-probe SLA; keep this separate from the
			// unchanged business, preparation and drain deadlines below.
			actionClient := *management
			actionClient.Timeout = 10 * time.Second
			acceptedAt := time.Now()
			response, err := actionClient.Post("http://proxy"+path, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("restart acceptance: iteration=%d elapsed=%s status=%d", iteration, time.Since(acceptedAt), response.StatusCode)
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if rejectRestart {
				if response.StatusCode != http.StatusConflict {
					t.Fatalf("incompatible operation accepted: %d %s", response.StatusCode, body)
				}
				status = waitServiceStatus(t, management, func(status Status) bool { return status.Ready })
				if status.InplaceRestart != nil || len(status.Instances) != 1 || status.BootID != initialBoot || status.Data != "serving" {
					t.Fatalf("rejected operation changed A: %+v", status)
				}
				t.Logf("rejected before acceptance while A serves: %s", body)
				continue
			}
			if response.StatusCode != 200 {
				t.Fatalf("accept: %d %s", response.StatusCode, body)
			}
			id = RestartID(body)
		}
		if id == "" || id == previous {
			t.Fatalf("operation ID %q", id)
		}
		previous = id
		if scenario.preparationFault != "" && iteration == 0 {
			waitBudget := 20 * time.Second
			if scenario.preparationFault == "worker" {
				// Persistent worker failures consume the real preparation budget.
				waitBudget = candidatePrepareTimeout + cfg.ShutdownTimeoutDur() + 5*time.Second
			}
			rolledBack := waitServiceStatusFor(t, management, waitBudget, func(status Status) bool {
				if scenario.preparationFault == "worker" && (!status.Ready || status.BootID != initialBoot || status.Data != "serving") {
					t.Fatalf("unready candidate workers interrupted A: %+v", status)
				}
				return status.InplaceRestart != nil && status.InplaceRestart.ID == id && status.InplaceRestart.Result != ""
			})
			if rolledBack.InplaceRestart.Result != "failed" || rolledBack.InplaceRestart.LastError == "" || !rolledBack.Ready || !rolledBack.ServiceReady || rolledBack.BootID != initialBoot || len(rolledBack.Instances) != 1 || rolledBack.Data != "serving" {
				t.Fatalf("preparation failure did not preserve A: %+v", rolledBack)
			}
			expectedError := map[string]string{
				"initialization": "fixture candidate initialization failed",
				"configuration":  "Configure changed a required takeover setting",
				"control":        "EOF",
				"worker":         "context deadline exceeded",
			}[scenario.preparationFault]
			errorMatches := strings.Contains(rolledBack.InplaceRestart.LastError, expectedError)
			if scenario.preparationFault == "control" {
				errorMatches = strings.Contains(rolledBack.InplaceRestart.LastError, "read unixpacket") &&
					(errorMatches || strings.Contains(rolledBack.InplaceRestart.LastError, "connection reset by peer"))
			}
			if !errorMatches {
				t.Fatalf("candidate fault %s took a different failure path: %s", scenario.preparationFault, rolledBack.InplaceRestart.LastError)
			}
			body, err := os.ReadFile(filepath.Join(dir, "candidate-fault-pid"))
			if err != nil {
				t.Fatal(err)
			}
			failedPID, err := strconv.Atoi(string(body))
			if err != nil || failedPID <= 0 || unix.Kill(failedPID, 0) != unix.ESRCH {
				t.Fatalf("failed candidate was not reaped: %s %v", body, err)
			}
			if scenario.preparationFault == "worker" {
				elapsed := time.Since(operationRequestedAt)
				if elapsed < 30*time.Second {
					t.Fatalf("candidate preparation ended before its required 30s budget: %s", elapsed)
				}
				paths, err := filepath.Glob(filepath.Join(dir, "candidate-worker-fault-*.json"))
				if err != nil || len(paths) < cfg.Workers {
					t.Fatalf("candidate worker fault did not run: %v %v", paths, err)
				}
				epochs := make(map[string]map[uint64]bool)
				boots := make(map[string]bool)
				for _, path := range paths {
					body, err := os.ReadFile(path)
					var failed struct {
						PID      int
						Identity controlIdentity
					}
					if err != nil || json.Unmarshal(body, &failed) != nil || failed.PID <= 0 || unix.Kill(failed.PID, 0) != unix.ESRCH {
						t.Fatalf("failed candidate worker was not reaped: %s %v", body, err)
					}
					if failed.Identity.RestartID != id || failed.Identity.BootID == "" || failed.Identity.BootID == initialBoot {
						t.Fatalf("worker fault belongs to another instance or operation: %+v", failed)
					}
					boots[failed.Identity.BootID] = true
					if epochs[failed.Identity.WorkerID] == nil {
						epochs[failed.Identity.WorkerID] = make(map[uint64]bool)
					}
					epochs[failed.Identity.WorkerID][failed.Identity.WorkerEpoch] = true
				}
				if len(boots) != 1 || len(epochs) != cfg.Workers {
					t.Fatalf("unexpected candidate generations or worker set: boots=%v epochs=%v", boots, epochs)
				}
				for worker, generations := range epochs {
					if len(generations) < 2 || generations[0] {
						t.Fatalf("persistent worker failure was not retried across valid epochs: %s epochs=%v", worker, generations)
					}
				}
				t.Logf("candidate preparation exhausted its real budget in %s; reaped %d worker processes from one BootID, epochs=%v", elapsed, len(paths), epochs)
			}
			for _, suffix := range []string{".sock", ".shm"} {
				paths, err := filepath.Glob(filepath.Join(dir, ".proxy-*"+suffix))
				if err != nil || len(paths) != 0 {
					t.Fatalf("failed preparation retained candidate resources: %v %v", paths, err)
				}
			}
			t.Logf("candidate rollback: fault=%s main_pid=%d failed_pid=%d boot=%s operation=%s error=%s", scenario.preparationFault, pid, failedPID, initialBoot, id, rolledBack.InplaceRestart.LastError)
			if err := os.Remove(filepath.Join(dir, "candidate-fault")); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if scenario.concurrent {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			checkFixtureRestartRejected(t, fixtureManagement(draining.Instances[1].StatsSocket), "nested")
			checkFixtureRestartRejected(t, management, ErrRestartInProgress.Error())
			releaseHeld()
		}
		if scenario.metrics == "occupied" && iteration == 0 {
			status = waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Result != ""
			})
			if status.InplaceRestart.Result != "failed" || !strings.Contains(status.InplaceRestart.LastError, "address already in use") || !status.Ready || status.BootID != initialBoot || len(status.Instances) != 1 {
				t.Fatalf("occupied metrics did not roll back before A drain: %+v", status)
			}
			occupiedMetrics.Close()
			continue
		}
		var candidateMetrics string
		if scenario.metrics != "" {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			candidate := draining.Instances[1]
			candidateMetrics = candidate.MetricsAddress
			if !candidate.Ready || candidateMetrics == initialMetrics {
				t.Fatalf("candidate did not acquire distinct metrics: %+v", draining)
			}
			if scenario.metrics != "dynamic" {
				want, _ := candidateMetricsAddress(cfg.MetricsListen)
				if candidateMetrics != want {
					t.Fatalf("candidate metrics=%s want=%s", candidateMetrics, want)
				}
			}
			checkFixtureMetrics(t, candidateMetrics)
			checkFixturePortHeld(t, initialMetrics)
			candidateClient := fixtureManagement(candidate.StatsSocket)
			checkFixtureRestartRejected(t, candidateClient, "nested")
			checkFixtureRestartRejected(t, management, ErrRestartInProgress.Error())
			if !scenario.execFailure || iteration != 0 {
				releaseHeld()
			}
		}
		if scenario.pidAdmission {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			candidate := draining.Instances[1]
			// The authority checks actual SO_PEERCRED, and only the original
			// main PID appears in its file. B must reconnect through that PID.
			reconnected := disconnectCandidate(candidate.InstanceID)
			select {
			case <-reconnected:
			case <-time.After(2 * time.Second):
				t.Fatal("candidate failed to reconnect under original PID admission")
			}
			waitServiceStatus(t, fixtureManagement(candidate.StatsSocket), func(status Status) bool { return status.Ready })
			releaseHeld()
		}
		if scenario.cleanupFailure && iteration == 0 {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			candidate := draining.Instances[1]
			// Replace only this test-owned directory entry, preserving the real
			// mapping for restoration. The product must refuse to unlink a file
			// with a different identity and keep retrying the same B's cleanup.
			path := filepath.Join(filepath.Dir(cfg.ShmPath), ".proxy-"+candidate.BootID+".shm")
			backup := path + ".held"
			if err := os.Rename(path, backup); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("not the candidate mapping"), 0600); err != nil {
				t.Fatal(err)
			}
			releaseHeld()
			var prior time.Time
			for _, minimumAttempts := range []int{2, 3, 4} {
				recovering := waitServiceStatus(t, management, func(status Status) bool {
					return status.Ready && status.InplaceRestart != nil && status.InplaceRestart.ID == id && status.InplaceRestart.Attempts >= minimumAttempts && status.InplaceRestart.NextAttempt != nil && status.InplaceRestart.NextAttempt.After(prior)
				})
				if recovering.InplaceRestart.Result != "" || len(recovering.Instances) != 2 || recovering.Instances[1].PID != candidate.PID || !strings.Contains(recovering.InplaceRestart.LastError, "ownership changed") {
					t.Fatalf("cleanup responsibility lost: %+v", recovering)
				}
				if err := unix.Kill(candidate.PID, 0); err != unix.ESRCH {
					t.Fatalf("candidate process was not joined before path retry: %v", err)
				}
				body, err := os.ReadFile(path)
				if err != nil || string(body) != "not the candidate mapping" {
					t.Fatalf("foreign path removed: %q %v", body, err)
				}
				prior = *recovering.InplaceRestart.NextAttempt
				t.Logf("cleanup recovery: operation=%s main_pid=%d candidate_pid=%d attempts=%d next=%s error=%s", id, pid, candidate.PID, recovering.InplaceRestart.Attempts, prior, recovering.InplaceRestart.LastError)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(backup, path); err != nil {
				t.Fatal(err)
			}
			// No second trigger: the accepted operation must finish itself.
		}
		if scenario.successorFailure && iteration == 0 {
			originalDraining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && status.Workers.Ready == 0
			})
			// A no longer accepts, so this finite request belongs to B.
			releaseCandidate := holdServiceRequest(t, client, cfg.DataListen, originalDraining.Instances[1].PID)
			releaseHeld()
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_b" && status.Ready
			})
			response, err := management.Post("http://proxy/fixture/fail-local", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			recovered := waitServiceStatus(t, management, func(status Status) bool {
				return status.Ready && status.BootID != draining.BootID
			})
			if recovered.InplaceRestart.ID != id || len(recovered.Instances) != 2 || recovered.Instances[1].PID != draining.Instances[1].PID || recovered.Instances[1].Data != "draining" {
				t.Fatalf("successor recovery did not preserve B drain: %+v", recovered)
			}
			t.Logf("A-prime recovered during B drain: main_pid=%d old_boot=%s new_boot=%s candidate_pid=%d attempts=%d", pid, draining.BootID, recovered.BootID, recovered.Instances[1].PID, recovered.InplaceRestart.Attempts)
			releaseCandidate()
		}
		if scenario.stopDrain && iteration == 0 {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			candidate := draining.Instances[1]
			stopLoad()
			<-loadDone
			response, err := management.Post("http://proxy/fixture/stop", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			// B has no held work and can exit while A still drains. Its management
			// socket need not remain alive; the coordinator retains its exit status.
			stopping := waitServiceStatus(t, management, func(status Status) bool {
				return len(status.Instances) == 2 && status.Instances[1].Phase == "stopped"
			})
			if stopping.ServiceReady || stopping.Ready {
				t.Fatalf("stopping service advertised readiness: %+v", stopping)
			}
			if err := unix.Kill(candidate.PID, 0); err != unix.ESRCH {
				t.Fatalf("idle candidate did not end independently: %v", err)
			}
			releaseHeld()
			stopFromCandidate, stoppedCandidate = true, candidate.PID
			break
		}
		if scenario.candidateFailure && iteration == 0 {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			failedPID := draining.Instances[1].PID
			faultWindow.Store(true)
			if err := unix.Kill(failedPID, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			replaced := waitServiceStatus(t, management, func(status Status) bool {
				return len(status.Instances) == 2 && status.Instances[1].PID != failedPID && status.Instances[1].Ready
			})
			faultWindow.Store(false)
			if replaced.InplaceRestart.ID != id || replaced.Data != "draining" {
				t.Fatalf("B recovery did not preserve A drain: %+v", replaced)
			}
			if err := unix.Kill(failedPID, 0); err != unix.ESRCH {
				t.Fatalf("failed candidate not reaped: %v", err)
			}
			releaseHeld()
		}
		if scenario.execFailure && iteration == 0 {
			waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a"
			})
			// Change only this task-owned executable's mode after B is serving.
			// execve now returns real EACCES; the running B remains unaffected.
			if err := os.Chmod(deployment, 0600); err != nil {
				t.Fatal(err)
			}
			releaseHeld()
		}
		if (recovery || scenario.execFailure) && iteration == 0 {
			var candidatePath string
			until := time.Now().Add(5 * time.Second)
			for time.Now().Before(until) {
				paths, _ := filepath.Glob(filepath.Join(dir, ".proxy-*.sock"))
				if len(paths) == 1 {
					candidatePath = paths[0]
					break
				}
				time.Sleep(time.Millisecond)
			}
			if candidatePath == "" {
				t.Fatal("no candidate management socket")
			}
			candidateClient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", candidatePath)
			}}, Timeout: time.Second}
			recovering := waitServiceStatus(t, candidateClient, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Attempts >= 4 && status.InplaceRestart.NextAttempt != nil
			})
			if !recovering.Ready || !recovering.ServiceReady || recovering.CoordinatorPID != pid || recovering.InplaceRestart.ID != id || recovering.InplaceRestart.LastError == "" {
				t.Fatalf("missing supervised recovery: %+v", recovering)
			}
			t.Logf("recovery: main_pid=%d candidate_pid=%d operation=%s target=%s attempts=%d last_error=%q next_attempt=%v", pid, recovering.PID, id, recovering.InplaceRestart.Target, recovering.InplaceRestart.Attempts, recovering.InplaceRestart.LastError, recovering.InplaceRestart.NextAttempt)
			if scenario.metrics != "" {
				if recovering.MetricsAddress != candidateMetrics {
					t.Fatalf("retry replaced candidate metrics: %+v", recovering)
				}
				checkFixtureMetrics(t, candidateMetrics)
				checkFixturePortHeld(t, initialMetrics)
			}
			if stopRecovery {
				stopLoad()
				<-loadDone
				response, err := candidateClient.Post("http://proxy/fixture/stop", "", nil)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				stopFromCandidate = true
				stoppedCandidate = recovering.PID
				cancelledAttempt = *recovering.InplaceRestart.NextAttempt
				break
			}
			if scenario.execFailure {
				if err := os.Chmod(deployment, 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Remove(filepath.Join(dir, "fail-resume")); err != nil {
					t.Fatal(err)
				}
			}
		}
		status = waitServiceStatus(t, management, func(status Status) bool {
			return status.InplaceRestart != nil && status.InplaceRestart.ID == id && status.InplaceRestart.Result != ""
		})
		wantResult := "completed"
		if scenario.candidateFailure && iteration == 0 {
			wantResult = "completed_with_forced_close"
		}
		if status.InplaceRestart.Result != wantResult || !status.Ready || len(status.Instances) != 1 || status.PID != pid {
			t.Fatalf("completion: %+v", status)
		}
		t.Logf("complete: iteration=%d main_pid=%d operation=%s target=%s attempts=%d result=%s forced=%d", iteration, pid, id, status.InplaceRestart.Target, status.InplaceRestart.Attempts, status.InplaceRestart.Result, status.InplaceRestart.ForcedCloseCount)
		if scenario.metrics != "" {
			if status.MetricsAddress != initialMetrics {
				t.Fatalf("A-prime changed metrics address: %s want=%s", status.MetricsAddress, initialMetrics)
			}
			checkFixtureMetrics(t, initialMetrics)
			released, err := net.Listen("tcp", candidateMetrics)
			if err != nil {
				t.Fatalf("candidate metrics resource leaked: %v", err)
			}
			released.Close()
		}
		actual, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.Stat(deployment)
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(actual, want) {
			t.Fatal("main PID did not execute the selected target")
		}
	}
	stopLoad()
	<-loadDone
	mu.Lock()
	defer mu.Unlock()
	if len(businessErrors) != 0 {
		t.Fatalf("business failures without retries: %v", businessErrors)
	}
	if !rejectRestart && builds["target"] == 0 {
		t.Fatalf("no target business response: %v", builds)
	}
	if rejectRestart && builds["initial"] == 0 {
		t.Fatalf("no ordinary business under compatibility rejection: %v", builds)
	}
	t.Logf("business results: builds=%v unexpected_failures=%d expected_injected_connection_losses=%v", builds, len(businessErrors), expectedLosses)
	if !stopFromCandidate {
		response, err := management.Post("http://proxy/fixture/stop", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	}
	select {
	case err := <-done:
		done <- err
		if scenario.forceStopDrain {
			body, readErr := os.ReadFile(filepath.Join(dir, "service-result.json"))
			var result struct {
				Forced bool `json:"forced"`
			}
			if err == nil || readErr != nil || json.Unmarshal(body, &result) != nil || !result.Forced {
				t.Fatalf("public Shutdown lost the original forced result: exit=%v read=%v body=%s", err, readErr, body)
			}
			t.Logf("complete service Shutdown preserved forced close of A; B and workers reaped")
		} else if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("whole service did not stop")
	}
	if stoppedCandidate != 0 {
		if err := unix.Kill(stoppedCandidate, 0); err != unix.ESRCH {
			t.Fatalf("candidate was not reaped: %v", err)
		}
		// Cross the already-advertised retry deadline before checking cleanup.
		if remaining := time.Until(cancelledAttempt.Add(100 * time.Millisecond)); remaining > 0 {
			time.Sleep(remaining)
		}
		if err := unix.Kill(stoppedCandidate, 0); err != unix.ESRCH {
			t.Fatalf("candidate survived stop: %v", err)
		}
	}
	for _, path := range []string{cfg.StatsSocket, cfg.ShmPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("resource remains: %s: %v", path, err)
		}
	}
	paths, _ := filepath.Glob(filepath.Join(dir, ".proxy-*"))
	if len(paths) != 0 {
		t.Fatalf("candidate resources remain: %v", paths)
	}
}

func fixtureManagement(socket string) *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}, Timeout: time.Second}
}

func checkFixtureRestartRejected(t *testing.T, client *http.Client, reason string) {
	t.Helper()
	response, err := client.Post("http://proxy/fixture/restart", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusConflict || !strings.Contains(string(body), reason) {
		t.Fatalf("restart rejection: status=%d body=%q err=%v", response.StatusCode, body, err)
	}
}

// Selecting a free adjacent pair belongs to test setup. Production must bind
// exactly the configured p and p+1, and the assertions below check those binds.
func fixtureMetricsAddress(t *testing.T, mode string) (string, net.Listener) {
	t.Helper()
	if mode == "dynamic" {
		return "127.0.0.1:0", nil
	}
	if mode == "boundary" {
		return "127.0.0.1:65535", nil
	}
	host := "127.0.0.1"
	if mode == "ipv6" {
		host = "::1"
	}
	for attempt := 0; attempt < 50; attempt++ {
		first, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			t.Fatal(err)
		}
		address := first.Addr().String()
		_, port, _ := net.SplitHostPort(address)
		number, _ := strconv.Atoi(port)
		second, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(number+1)))
		first.Close()
		if err != nil {
			continue
		}
		if mode == "occupied" {
			return address, second
		}
		second.Close()
		return address, nil
	}
	t.Fatal("cannot allocate an adjacent metrics pair for the fixture")
	return "", nil
}

func checkFixturePortHeld(t *testing.T, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if listener != nil {
		listener.Close()
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("metrics listener was not continuously held: %s: %v", address, err)
	}
}

func checkFixtureMetrics(t *testing.T, address string) {
	t.Helper()
	checkFixturePortHeld(t, address)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: time.Second}
	response, err := client.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	// The fixture sends only an initial Bookmark, so its master counter
	// registry can legitimately be empty. Check the actual exposition route.
	if err != nil || response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/plain; version=0.0.4" {
		t.Fatalf("metrics unavailable: status=%d bytes=%d err=%v", response.StatusCode, len(body), err)
	}
}

func holdServiceRequest(t *testing.T, client *http.Client, address string, parent int) func() {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			fmt.Fprint(w, "complete\n")
		case <-r.Context().Done():
		}
	}))
	request, _ := http.NewRequest("GET", "http://"+address+"/fixture/hold", nil)
	request.Header.Set("X-Fixture-Target", upstream.URL)
	response, err := client.Do(request)
	if err != nil {
		upstream.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		response.Body.Close()
		upstream.Close()
	})
	worker, err := strconv.Atoi(response.Header.Get("X-Fixture-PID"))
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", worker))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	if len(fields) < 2 || fields[1] != strconv.Itoa(parent) {
		t.Fatalf("held request belongs to worker %d with stat %q, expected B parent %d", worker, stat, parent)
	}
	return func() {
		once.Do(func() { close(release) })
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(body) != "begin\ncomplete\n" {
			t.Fatalf("finite candidate response truncated: %q %v", body, err)
		}
	}
}

func copyExecutable(t *testing.T, source, dest string) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(output, input)
	closeErr := output.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("copy binary: %v %v", err, closeErr)
	}
}

func waitServiceStatus(t *testing.T, client *http.Client, condition func(Status) bool) Status {
	t.Helper()
	return waitServiceStatusFor(t, client, 20*time.Second, condition)
}

func waitServiceStatusFor(t *testing.T, client *http.Client, budget time.Duration, condition func(Status) bool) Status {
	t.Helper()
	deadline := time.Now().Add(budget)
	var last Status
	var lastErr error
	for time.Now().Before(deadline) {
		response, err := client.Get("http://proxy/status")
		if err == nil {
			err = json.NewDecoder(response.Body).Decode(&last)
			response.Body.Close()
			if err == nil && condition(last) {
				return last
			}
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("status did not satisfy condition: %+v operation=%+v err=%v", last, last.InplaceRestart, lastErr)
	return last
}
