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
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	proxyMasterHelperEnv = "KUASAR_TEST_PROXY_MASTER_HELPER"
	proxyWorkerHelperEnv = "KUASAR_TEST_PROXY_WORKER_HELPER"
	proxyConfigHelperEnv = "KUASAR_TEST_PROXY_CONFIG"
	proxyPIDsHelperEnv   = "KUASAR_TEST_PROXY_PIDS"
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
	cfg, err := loadProxyConfig(cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runProxyMaster(ctx, cfgPath, cfg, slog.New(slog.NewTextHandler(os.Stderr, nil))); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func runProxyWorkerHelper() {
	cfg, err := loadProxyConfig(os.Getenv(proxyConfigHelperEnv))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	f, err := os.OpenFile(os.Getenv(proxyPIDsHelperEnv), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	_, writeErr := fmt.Fprintln(f, os.Getpid())
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		fmt.Fprintln(os.Stderr, "record proxy worker pid:", writeErr, closeErr)
		os.Exit(2)
	}
	if err := runProxyWorker(context.Background(), cfg, slog.New(slog.NewTextHandler(os.Stderr, nil))); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func loadProxyConfig(path string) (*config.ProxyFileConfig, error) {
	if path == "" {
		return nil, fmt.Errorf("missing %s", proxyConfigHelperEnv)
	}
	return config.LoadProxy(path)
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
