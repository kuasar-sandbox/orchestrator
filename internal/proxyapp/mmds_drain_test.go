package proxyapp

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestRealWorkersDrainAcceptedMMDSIndependently(t *testing.T) {
	effective, _ := testMasterEffective(t)
	cfg := effective.Config()
	dir, err := os.MkdirTemp("", "proxy-mmds-drain-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	cfg.ConfigSocket, cfg.StatsSocket, cfg.ShmPath = filepath.Join(dir, "config.sock"), filepath.Join(dir, "stats.sock"), filepath.Join(dir, "routes.shm")
	cfg.DataListen, cfg.Workers, cfg.ShutdownTimeout = "fd=data", 2, "3s"
	serviceSocket := filepath.Join(dir, "service.sock")
	listener, err := net.Listen("unix", serviceSocket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/svc" || r.Header.Get("E2b-Sandbox-Id") != "sandbox-1" || r.Header.Get("E2b-Sandbox-Service") != "local" {
			t.Error("MMDS service identity was not preserved")
		}
		close(entered)
		select {
		case <-release:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"mmds":"complete"}`)
		case <-r.Context().Done():
			t.Error("MMDS service dependency canceled before response release")
		}
	})}
	go server.Serve(listener)
	defer server.Close()
	serveFixtureRoutes(t, cfg.ConfigSocket, true, routeFixtureOptions{
		metadataService: serviceSocket,
		route:           &routesync.RouteEntry{SandboxID: "sandbox-1", StableID: "sandbox-1", Profile: "e2b", State: routesync.StateRunning, RunID: "run-1", FloatingIP: "127.0.0.1", MmdsSecret: strings.Repeat("ab", 32), MMDSRoutes: `{"routes":[{"path":"/svc","type":"service","service":"local"}]}`},
	})
	files := map[string]*os.File{}
	addresses := map[string]string{}
	for _, name := range []string{"data", "mmds"} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		file, err := listenerFile(listener)
		if err != nil {
			t.Fatal(err)
		}
		files[name], addresses[name] = file, listener.Addr().String()
		listener.Close()
		defer file.Close()
	}
	effective, err = FreezeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	instance := NewInstance("proxy", "", "", cfg.Workers)
	var extensionContext context.Context
	extension := &endingMasterExtension{
		start:    func(ctx context.Context, _ proxyextension.MasterHost) error { extensionContext = ctx; return nil },
		shutdown: func(context.Context) error { return nil },
	}
	done := make(chan error, 1)
	go func() {
		done <- RunMaster(context.Background(), effective, &Runtime{Logger: testMasterLogger(), ListenerFiles: files, MasterExtension: extension, Instance: instance})
	}()
	wait, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	joined := false
	defer func() {
		finish()
		instance.requestStop(time.Now())
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("failed test could not join the master and workers")
			}
		}
	}()
	if err := instance.WaitReady(wait); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 3 * time.Second}
	tokenRequest, _ := http.NewRequest("PUT", "http://"+addresses["mmds"]+"/latest/api/token", nil)
	tokenRequest.Header.Set("X-metadata-token-ttl-seconds", "60")
	response, err := client.Do(tokenRequest)
	if err != nil {
		t.Fatal(err)
	}
	token, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("token: %d %q %v", response.StatusCode, token, err)
	}
	request, _ := http.NewRequest("GET", "http://"+addresses["mmds"]+"/svc", nil)
	request.Header.Set("X-metadata-token", string(token))
	type result struct {
		response *http.Response
		err      error
	}
	resultCh := make(chan result, 1)
	go func() { response, err := client.Do(request); resultCh <- result{response, err} }()
	select {
	case <-entered:
	case <-wait.Done():
		t.Fatal("MMDS request did not reach service")
	}
	instance.requestStop(time.Now().Add(3 * time.Second))
	for {
		instance.mu.Lock()
		status, changed := instance.status, instance.changed
		instance.mu.Unlock()
		if status.Data == "stopped" {
			if status.MMDS != "draining" {
				t.Fatalf("MMDS request was not retained: %+v", status)
			}
			break
		}
		select {
		case <-changed:
		case <-wait.Done():
			t.Fatal("data waited for MMDS business")
		}
	}
	if extensionContext.Err() != nil {
		t.Fatal("master dependencies ended during MMDS request")
	}
	finish()
	got := <-resultCh
	if got.err != nil {
		t.Fatal(got.err)
	}
	body, err := io.ReadAll(got.response.Body)
	got.response.Body.Close()
	if err != nil || got.response.StatusCode != http.StatusAccepted || got.response.Header.Get("Content-Type") != "application/json" || string(body) != `{"mmds":"complete"}` {
		t.Fatalf("MMDS response truncated: status=%d body=%q err=%v", got.response.StatusCode, body, err)
	}
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-wait.Done():
		t.Fatal("master did not join workers")
	}
}
