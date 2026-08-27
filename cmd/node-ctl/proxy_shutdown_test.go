package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
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

	publicproxy "github.com/kuasar-sandbox/orchestrator/app/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	proxyMasterHelperEnv = "KUASAR_TEST_PROXY_MASTER_HELPER"
	proxyWorkerHelperEnv = "KUASAR_TEST_PROXY_WORKER_HELPER"
	proxyConfigHelperEnv = "KUASAR_TEST_PROXY_CONFIG"
	proxyPIDsHelperEnv   = "KUASAR_TEST_PROXY_PIDS"
	proxyHooksHelperEnv  = "KUASAR_TEST_PROXY_HOOKS"
	proxyRuntimeFailEnv  = "KUASAR_TEST_PROXY_RUNTIME_FAIL"
)

// The proxy master starts workers by re-executing os.Executable. These test-only
// helpers let the real master/worker code run from the test binary, including the
// inherited listener descriptors and CommandContext shutdown path.
func init() {
	if os.Getenv(proxyWorkerHelperEnv) == "1" {
		runProxyWorkerHelper()
	}
	if os.Getenv(proxyMasterHelperEnv) == "1" {
		runProxyMasterHelper()
	}
}

func runProxyMasterHelper() {
	_ = os.Unsetenv(proxyMasterHelperEnv)
	_ = os.Setenv(proxyWorkerHelperEnv, "1")
	cfgPath := os.Getenv(proxyConfigHelperEnv)
	if err := runProxy([]string{"--config", cfgPath}, slog.New(slog.NewTextHandler(os.Stderr, nil))); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func runProxyWorkerHelper() {
	file, err := os.OpenFile(os.Getenv(proxyPIDsHelperEnv), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	_, writeErr := fmt.Fprintln(file, os.Getpid())
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		fmt.Fprintln(os.Stderr, "record proxy worker pid:", writeErr, closeErr)
		os.Exit(2)
	}
	hooks := publicproxy.Hooks{
		Configure: func(context.Context, *publicproxy.Config) error {
			return fmt.Errorf("worker called Configure")
		},
		BindRuntime: func(_ context.Context, process publicproxy.Process, _ *publicproxy.Runtime) error {
			if process.Role != publicproxy.RoleWorker || process.WorkerID == "" || process.WorkerEpoch == 0 {
				return fmt.Errorf("invalid worker process: %+v", process)
			}
			hookFile, err := os.OpenFile(os.Getenv(proxyHooksHelperEnv), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			_, writeErr := fmt.Fprintf(hookFile, "%s %s %d\n", process.Role, process.WorkerID, process.WorkerEpoch)
			closeErr := hookFile.Close()
			if writeErr != nil {
				return writeErr
			}
			if closeErr != nil {
				return closeErr
			}
			if os.Getenv(proxyRuntimeFailEnv) == "1" {
				return fmt.Errorf("worker runtime unavailable")
			}
			return nil
		},
	}
	if err := publicproxy.New(hooks).Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

type shutdownRouteSource struct {
	policy routesync.Policy
}

func (s *shutdownRouteSource) Range(context.Context, func(routesync.RouteEntry) error) error {
	return nil
}

func (s *shutdownRouteSource) Subscribe() (<-chan routesync.Event, func()) {
	return make(chan routesync.Event), func() {}
}

func (*shutdownRouteSource) OnWake(context.Context, string) {}
func (s *shutdownRouteSource) Policy() routesync.Policy     { return s.policy }

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func readWorkerPIDs(path string) []int {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var pids []int
	for _, line := range strings.Fields(string(b)) {
		if pid, err := strconv.Atoi(line); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

func proxyHTTPReady(addr string) bool {
	client := http.Client{Timeout: 100 * time.Millisecond}
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return true
}

func waitProxyTestReady(masterExited <-chan struct{}, masterErr *error, pidPath, dataAddr, mmdsAddr string, workers int) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-masterExited:
			return fmt.Errorf("proxy master exited before readiness: %v", *masterErr)
		default:
		}
		pids := readWorkerPIDs(pidPath)
		if len(pids) == workers && proxyHTTPReady(dataAddr) && proxyHTTPReady(mmdsAddr) {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("proxy readiness timed out: workers=%v data=%s mmds=%s", readWorkerPIDs(pidPath), dataAddr, mmdsAddr)
}

func waitProxyReplacement(masterExited <-chan struct{}, masterErr *error, pidPath, dataAddr, mmdsAddr string, priorPIDs []int) error {
	prior := make(map[int]struct{}, len(priorPIDs))
	for _, pid := range priorPIDs {
		prior[pid] = struct{}{}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-masterExited:
			return fmt.Errorf("proxy master exited before worker replacement: %v", *masterErr)
		default:
		}
		for _, pid := range readWorkerPIDs(pidPath) {
			if _, existed := prior[pid]; !existed && processExists(pid) && proxyHTTPReady(dataAddr) && proxyHTTPReady(mmdsAddr) {
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("proxy replacement timed out: workers=%v", readWorkerPIDs(pidPath))
}

func waitProxyHookRecords(masterExited <-chan struct{}, masterErr *error, hookPath string, expected int) ([]string, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-masterExited:
			return nil, fmt.Errorf("proxy master exited before worker runtime binding: %v", *masterErr)
		default:
		}
		contents, err := os.ReadFile(hookPath)
		if err == nil {
			records := strings.Fields(string(contents))
			if len(records) >= expected*3 {
				return records, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	contents, _ := os.ReadFile(hookPath)
	return nil, fmt.Errorf("worker runtime binding timed out: %q", contents)
}

func assertAddressReusable(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return ln.Close()
}

func TestProxyMasterRepeatedShutdownReapsWorkersAndListeners(t *testing.T) {
	dir := t.TempDir()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	configSocket := filepath.Join(dir, "node-ctl.socket")
	dataAddr := reserveLoopbackAddress(t)
	mmdsAddr := reserveLoopbackAddress(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	serverCtx, stopServer := context.WithCancel(context.Background())
	t.Cleanup(stopServer)
	serverReady := make(chan struct{})
	source := &shutdownRouteSource{policy: routesync.Policy{
		Domain:   "proxy.test",
		AuthMode: "off",
		MMDS:     &routesync.MMDSProxyPolicy{Enabled: true, Listen: mmdsAddr},
	}}
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- configsock.New(configSocket, configsock.Deps{
			RouteSource: source,
			Plugins:     configsock.NewRegistry(),
		}, log).ServeReady(serverCtx, serverReady)
	}()
	select {
	case <-serverReady:
	case err := <-serverDone:
		t.Fatalf("config socket exited before readiness: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("config socket readiness timed out")
	}

	cfgPath := filepath.Join(dir, "proxy.yaml")
	cfg := fmt.Sprintf(`config_socket: %s
paths:
  run_root: %s
data_listen: %s
proxy_socket: %s
shm_path: %s
route_capacity: 16
workers: 2
auth: off
park_timeout: 100ms
`, configSocket, filepath.Join(dir, "run"), dataAddr, filepath.Join(dir, "proxy.sock"), filepath.Join(dir, "proxy-routes.shm"))
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	for attempt := 1; attempt <= 10; attempt++ {
		pidPath := filepath.Join(dir, fmt.Sprintf("workers-%02d.pids", attempt))
		hookPath := filepath.Join(dir, fmt.Sprintf("hooks-%02d.log", attempt))
		logPath := filepath.Join(dir, fmt.Sprintf("proxy-%02d.log", attempt))
		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(testBinary)
		cmd.Env = append(os.Environ(),
			proxyMasterHelperEnv+"=1",
			proxyWorkerHelperEnv+"=",
			proxyConfigHelperEnv+"="+cfgPath,
			proxyPIDsHelperEnv+"="+pidPath,
			proxyHooksHelperEnv+"="+hookPath,
			"GOMAXPROCS=1",
		)
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err := cmd.Start(); err != nil {
			_ = logFile.Close()
			t.Fatal(err)
		}
		masterExited := make(chan struct{})
		var masterErr error
		go func() {
			masterErr = cmd.Wait()
			close(masterExited)
		}()

		if err := waitProxyTestReady(masterExited, &masterErr, pidPath, dataAddr, mmdsAddr, 2); err != nil {
			select {
			case <-masterExited:
			default:
				_ = cmd.Process.Kill()
				<-masterExited
			}
			_ = logFile.Close()
			b, _ := os.ReadFile(logPath)
			t.Fatalf("attempt %d: %v\n%s", attempt, err, b)
		}
		workerPIDs := readWorkerPIDs(pidPath)
		hookRecords, err := waitProxyHookRecords(masterExited, &masterErr, hookPath, 2)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if len(hookRecords) != 3*2 || hookRecords[0] != "worker" || hookRecords[3] != "worker" {
			t.Fatalf("attempt %d: worker BindRuntime records=%q", attempt, hookRecords)
		}
		if attempt == 1 {
			removedConfig := cfgPath + ".removed"
			if err := os.Rename(cfgPath, removedConfig); err != nil {
				t.Fatal(err)
			}
			if err := syscall.Kill(workerPIDs[0], syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			if err := waitProxyReplacement(masterExited, &masterErr, pidPath, dataAddr, mmdsAddr, workerPIDs); err != nil {
				_ = os.Rename(removedConfig, cfgPath)
				_ = cmd.Process.Kill()
				<-masterExited
				_ = logFile.Close()
				contents, _ := os.ReadFile(logPath)
				t.Fatalf("replacement without proxy.yaml: %v\n%s", err, contents)
			}
			if err := os.Rename(removedConfig, cfgPath); err != nil {
				t.Fatal(err)
			}
			workerPIDs = readWorkerPIDs(pidPath)
			hookRecords, err = waitProxyHookRecords(masterExited, &masterErr, hookPath, 3)
			if err != nil {
				t.Fatal(err)
			}
			if len(hookRecords) != 3*3 {
				t.Fatalf("replacement BindRuntime records=%q", hookRecords)
			}
		}
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("attempt %d: terminate proxy master: %v", attempt, err)
		}
		select {
		case <-masterExited:
			if masterErr != nil {
				_ = logFile.Close()
				b, _ := os.ReadFile(logPath)
				t.Fatalf("attempt %d: proxy master shutdown: %v\n%s", attempt, masterErr, b)
			}
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-masterExited
			t.Fatalf("attempt %d: proxy master shutdown timed out", attempt)
		}
		if err := logFile.Close(); err != nil {
			t.Fatal(err)
		}
		for _, pid := range workerPIDs {
			if processExists(pid) {
				_ = syscall.Kill(pid, syscall.SIGKILL)
				t.Fatalf("attempt %d: proxy worker %d survived master shutdown", attempt, pid)
			}
		}
		if err := assertAddressReusable(dataAddr); err != nil {
			t.Fatalf("attempt %d: data listener was not released: %v", attempt, err)
		}
		if err := assertAddressReusable(mmdsAddr); err != nil {
			t.Fatalf("attempt %d: MMDS listener was not released: %v", attempt, err)
		}
	}
}

func TestProxyWorkerRuntimeFailureDoesNotBecomeReady(t *testing.T) {
	dir := t.TempDir()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	configSocket := filepath.Join(dir, "node-ctl.socket")
	dataAddr := reserveLoopbackAddress(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	serverCtx, stopServer := context.WithCancel(context.Background())
	t.Cleanup(stopServer)
	serverReady := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- configsock.New(configSocket, configsock.Deps{
			RouteSource: &shutdownRouteSource{policy: routesync.Policy{Domain: "proxy.test", AuthMode: "off"}},
			Plugins:     configsock.NewRegistry(),
		}, log).ServeReady(serverCtx, serverReady)
	}()
	select {
	case <-serverReady:
	case err := <-serverDone:
		t.Fatalf("config socket exited before readiness: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("config socket readiness timed out")
	}

	cfgPath := filepath.Join(dir, "proxy.yaml")
	cfg := fmt.Sprintf(`config_socket: %s
paths:
  run_root: %s
data_listen: %s
proxy_socket: %s
shm_path: %s
route_capacity: 16
workers: 1
auth: off
park_timeout: 100ms
`, configSocket, filepath.Join(dir, "run"), dataAddr, filepath.Join(dir, "proxy.sock"), filepath.Join(dir, "proxy-routes.shm"))
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	pidPath := filepath.Join(dir, "workers.pids")
	hookPath := filepath.Join(dir, "hooks.log")
	logPath := filepath.Join(dir, "proxy.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(testBinary)
	cmd.Env = append(os.Environ(),
		proxyMasterHelperEnv+"=1",
		proxyWorkerHelperEnv+"=",
		proxyConfigHelperEnv+"="+cfgPath,
		proxyPIDsHelperEnv+"="+pidPath,
		proxyHooksHelperEnv+"="+hookPath,
		proxyRuntimeFailEnv+"=1",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	masterExited := make(chan struct{})
	var masterErr error
	go func() {
		masterErr = cmd.Wait()
		close(masterExited)
	}()
	t.Cleanup(func() {
		select {
		case <-masterExited:
		default:
			_ = cmd.Process.Kill()
			<-masterExited
		}
		_ = logFile.Close()
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-masterExited:
			contents, _ := os.ReadFile(logPath)
			t.Fatalf("proxy master exited while failed worker was supervised: %v\n%s", masterErr, contents)
		default:
		}
		if records, err := os.ReadFile(hookPath); err == nil && strings.Contains(string(records), "worker proxy-0 1") {
			break
		}
		if time.Now().After(deadline) {
			contents, _ := os.ReadFile(logPath)
			t.Fatalf("worker BindRuntime failure was not observed\n%s", contents)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if proxyHTTPReady(dataAddr) {
		t.Fatal("worker entered serving readiness after BindRuntime failure")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-masterExited:
		if masterErr != nil {
			contents, _ := os.ReadFile(logPath)
			t.Fatalf("proxy master shutdown: %v\n%s", masterErr, contents)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("proxy master shutdown timed out")
	}
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := assertAddressReusable(dataAddr); err != nil {
		t.Fatalf("data listener was not released: %v", err)
	}
}
