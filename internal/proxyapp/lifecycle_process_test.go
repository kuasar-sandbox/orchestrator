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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sys/unix"
)

// These subprocesses use the actual sealed worker bootstrap, shared mappings,
// listeners, supervision and private controls. Only the conductor and Guest
// business are fixtures; real Guest acceptance is a separate E2E layer.
func TestMain(m *testing.M) {
	if ContinuationPresent() || (len(os.Args) > 1 && os.Args[1] == "fixture-service") {
		if err := runFixtureService(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if !WorkerBootstrapPresent() {
		code := m.Run()
		if fixtureTargetDirectory != "" {
			_ = os.RemoveAll(fixtureTargetDirectory)
		}
		os.Exit(code)
	}
	err := func() error {
		bootstrap, err := ReceiveWorkerBootstrap()
		if err != nil {
			return err
		}
		defer bootstrap.Close()
		if err := bootstrap.VerifyExecutable(); err != nil {
			return err
		}
		if err := fixtureCandidateWorkerFailure(bootstrap); err != nil {
			return err
		}
		worker, err := PrepareWorker(bootstrap)
		if err != nil {
			return err
		}
		defer worker.Close()
		return worker.Run(context.Background(), &Runtime{Logger: testMasterLogger(), WorkerExtension: &lifecycleFixtureExtension{shutdownFault: bootstrap.EffectiveConfig().Config().ShmPath + ".shutdown-fault"}})
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

type lifecycleFixtureExtension struct{ shutdownFault string }

func (*lifecycleFixtureExtension) Start(context.Context, proxyextension.WorkerHost) error { return nil }
func (e *lifecycleFixtureExtension) Shutdown(context.Context) error {
	mode, _ := os.ReadFile(e.shutdownFault)
	switch string(mode) {
	case "exit":
		// Only the test worker exits here, after its MMDS-drained ACK and
		// before the extension/stopped ACK. This is a real nonzero wait result.
		os.Exit(23)
	case "error":
		return errors.New("fixture worker extension completion failed")
	}
	return nil
}
func (*lifecycleFixtureExtension) WrapIngress(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fixture/id" {
			w.Header().Set("X-Fixture-Build", fixtureBuild)
			fmt.Fprintf(w, "%s %d", fixtureBuild, os.Getpid())
			return
		}
		if r.URL.Path != "/fixture/hold" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("X-Fixture-PID", fmt.Sprint(os.Getpid()))
		fmt.Fprint(w, "begin\n")
		w.(http.Flusher).Flush()
		request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, r.Header.Get("X-Fixture-Target"), nil)
		if err != nil {
			return
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return
		}
		defer response.Body.Close()
		_, _ = io.Copy(w, response.Body)
	})
}

func TestMasterRealWorkersReadyAndIndependentDrain(t *testing.T) {
	for _, scenario := range []struct{ metadata, force, endingError bool }{{false, false, false}, {true, false, false}, {true, true, false}, {true, false, true}} {
		metadata, force := scenario.metadata, scenario.force
		t.Run(fmt.Sprintf("mmds-%v-forced-%v-extension-error-%v", metadata, force, scenario.endingError), func(t *testing.T) {
			effective, _ := testMasterEffective(t)
			cfg := effective.Config()
			cfg.Workers = 2
			cfg.DataListen = "fd=data"
			cfg.ShutdownTimeout = "3s"
			data, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}
			defer data.Close()
			dataFile, err := data.File()
			if err != nil {
				t.Fatal(err)
			}
			defer dataFile.Close()
			files := map[string]*os.File{"data": dataFile}
			var metadataListener *net.TCPListener
			if metadata {
				metadataListener, err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
				if err != nil {
					t.Fatal(err)
				}
				defer metadataListener.Close()
				file, err := metadataListener.File()
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				files["mmds"] = file
			}
			serveFixtureRoutes(t, cfg.ConfigSocket, metadata)
			effective, err = FreezeConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			instance := NewInstance("proxy", "", "", cfg.Workers)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			backgroundCanceled := make(chan struct{})
			backgroundDone := make(chan struct{})
			releaseBackground := make(chan struct{})
			var releaseOnce sync.Once
			releaseTask := func() { releaseOnce.Do(func() { close(releaseBackground) }) }
			defer releaseTask()
			shutdownEntered := make(chan struct{})
			shutdownErr := errors.New("fixture master extension completion failed")
			var started context.Context
			var shutdownDeadline time.Time
			var masterExtension proxyextension.MasterExtension
			if !force {
				masterExtension = &endingMasterExtension{
					start: func(ctx context.Context, _ proxyextension.MasterHost) error {
						started = ctx
						go func() {
							<-ctx.Done()
							close(backgroundCanceled)
							<-releaseBackground
							close(backgroundDone)
						}()
						return nil
					},
					shutdown: func(ctx context.Context) error {
						defer close(shutdownEntered)
						if started.Err() != context.Canceled {
							return errors.New("master extension context still live at Shutdown")
						}
						instance.mu.Lock()
						deadline := instance.deadline
						allEnded := len(instance.workers) == cfg.Workers
						for _, worker := range instance.workers {
							allEnded = allEnded && !worker.alive && worker.dataDone && worker.mmdsDone
						}
						instance.mu.Unlock()
						if got, ok := ctx.Deadline(); !ok || !got.Equal(shutdownDeadline) || !deadline.Equal(shutdownDeadline) || !allEnded {
							return errors.New("master extension Shutdown preceded worker completion or reset deadline")
						}
						if _, err := os.Stat(cfg.ShmPath); err != nil {
							return fmt.Errorf("shared state released before master extension: %w", err)
						}
						// Announce callback entry separately from return; the test
						// releases background completion only after observing it.
						select {
						case shutdownEntered <- struct{}{}:
						case <-ctx.Done():
							return ctx.Err()
						}
						select {
						case <-backgroundDone:
						case <-ctx.Done():
							return ctx.Err()
						}
						if scenario.endingError {
							return shutdownErr
						}
						return nil
					},
				}
			}
			done := make(chan error, 1)
			go func() {
				done <- RunMaster(ctx, effective, &Runtime{Logger: testMasterLogger(), ListenerFiles: files, Instance: instance, MasterExtension: masterExtension})
			}()
			wait, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			if err := instance.WaitReady(wait); err != nil {
				t.Fatalf("ready: %v status=%+v", err, instance.Status())
			}
			if s := instance.Snapshot(); s.Workers.Ready != 2 || !s.RoutesSynced || (!metadata && s.MMDS != "disabled") {
				t.Fatalf("status=%+v", s)
			}
			for _, probe := range []struct {
				method, path string
				code         int
			}{{"GET", "/readyz", 200}, {"GET", "/status", 200}, {"POST", "/readyz", 405}, {"DELETE", "/status", 405}} {
				request, _ := http.NewRequest(probe.method, "http://proxy"+probe.path, nil)
				response := doUnixRequest(t, cfg.StatsSocket, request)
				if response.StatusCode != probe.code || response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Content-Type") != "application/json" {
					t.Fatalf("probe %s %s: %d %v", probe.method, probe.path, response.StatusCode, response.Header)
				}
				var payload any
				if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
			}
			release := make(chan struct{})
			released := false
			upstreamStarted := make(chan struct{}, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamStarted <- struct{}{}
				select {
				case <-release:
					fmt.Fprint(w, "complete\n")
				case <-r.Context().Done():
				}
			}))
			defer func() {
				if !released {
					close(release)
				}
				upstream.Close()
			}()
			client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 8 * time.Second}
			request, _ := http.NewRequest("GET", "http://"+data.Addr().String()+"/fixture/hold", nil)
			request.Header.Set("X-Fixture-Target", upstream.URL)
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal(response.Status)
			}
			select {
			case <-upstreamStarted:
			case <-wait.Done():
				t.Fatal("business did not reach upstream")
			}
			instance.mu.Lock()
			var pids []int
			for _, worker := range instance.workers {
				pids = append(pids, worker.pid)
			}
			instance.mu.Unlock()
			budget := 3 * time.Second
			if force {
				budget = 200 * time.Millisecond
			}
			if metadata {
				request, _ := http.NewRequest("PUT", "http://"+metadataListener.Addr().String()+"/latest/api/token", nil)
				response, err := client.Do(request)
				if err != nil {
					t.Fatalf("MMDS unavailable before drain: %v", err)
				}
				response.Body.Close()
				if response.StatusCode != 400 {
					t.Fatalf("MMDS protocol response=%d", response.StatusCode)
				}
			}
			shutdownDeadline = time.Now().Add(budget)
			instance.requestStop(shutdownDeadline)
			probe, _ := http.NewRequest("GET", "http://proxy/readyz", nil)
			unready := doUnixRequest(t, cfg.StatsSocket, probe)
			unready.Body.Close()
			if unready.StatusCode != 503 {
				t.Fatalf("draining ready=%d", unready.StatusCode)
			}

			select {
			case err := <-done:
				t.Fatalf("master ended with business inflight: %v", err)
			default:
			}
			if !force && started.Err() != nil {
				t.Fatal("master extension context canceled while data was inflight")
			}
			select {
			case <-backgroundCanceled:
				t.Fatal("master extension canceled while data was inflight")
			case <-shutdownEntered:
				t.Fatal("master extension Shutdown started while data was inflight")
			default:
			}
			if metadata && !force {
				for {
					instance.mu.Lock()
					status, changed := instance.status, instance.changed
					instance.mu.Unlock()
					if status.MMDS == "stopped" {
						if status.Data != "draining" {
							t.Fatalf("data ended before release: %+v", status)
						}
						break
					}
					select {
					case <-changed:
					case <-wait.Done():
						t.Fatal("MMDS drain waited for another endpoint's work")
					}
				}
			}
			if !force {
				close(release)
				released = true
			}
			body, err := io.ReadAll(response.Body)
			if !force && (err != nil || string(body) != "begin\ncomplete\n") {
				t.Fatalf("truncated business %q: %v", body, err)
			}
			if force && (err == nil || string(body) != "begin\n") {
				t.Fatalf("forced close must be visible, body=%q err=%v", body, err)
			}
			if !force {
				select {
				case <-shutdownEntered:
				case <-wait.Done():
					t.Fatal("master extension Shutdown did not run")
				}
				select {
				case err := <-done:
					t.Fatalf("master returned before extension background completion: %v", err)
				default:
				}
				releaseTask()
			}
			select {
			case err := <-done:
				if (!force && !scenario.endingError && err != nil) || (errors.Is(err, ErrForcedClose) != force) || (scenario.endingError && !errors.Is(err, shutdownErr)) {
					instance.mu.Lock()
					for index, worker := range instance.workers {
						t.Logf("worker %d pid=%d epoch=%d alive=%v prepared=%v ready=%v dataDone=%v mmdsDone=%v", index, worker.pid, worker.epoch, worker.alive, worker.prepared, worker.ready, worker.dataDone, worker.mmdsDone)
					}
					instance.mu.Unlock()
					t.Fatal(err)
				}
			case <-wait.Done():
				t.Fatal("master failed to reap workers")
			}
			for _, pid := range pids {
				if pid <= 0 {
					t.Fatal("worker PID missing")
				}
				if err := unix.Kill(pid, 0); !errors.Is(err, unix.ESRCH) {
					t.Fatalf("worker %d was not reaped: %v", pid, err)
				}
			}
			if status := instance.Status(); status.Ready || status.Phase != "stopped" {
				t.Fatalf("final status=%+v", status)
			}
			for _, path := range []string{cfg.ShmPath, cfg.StatsSocket} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("resource survived: %s %v", path, err)
				}
			}
			if err := data.SetDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			connection, err := net.Dial("tcp", data.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			accepted, err := data.Accept()
			if err != nil {
				t.Fatalf("shared listener was shut down: %v", err)
			}
			accepted.Close()
		})
	}
}

type routeFixtureOptions struct {
	legacy          bool
	pidfile         string
	metadataService string
	route           *routesync.RouteEntry
}

func serveFixtureRoutes(t *testing.T, socket string, metadata bool, options ...routeFixtureOptions) func(string) <-chan struct{} {
	t.Helper()
	capabilities := []string{routesync.InplaceRestartV1}
	if len(options) != 0 && options[0].legacy {
		capabilities = nil

	}
	if err := os.MkdirAll(filepath.Dir(socket), 0700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	type connectionKey struct{}
	var sessionsMu sync.Mutex
	sessions := map[string]net.Conn{}
	reconnects := map[string]chan struct{}{}
	server := &http.Server{Handler: h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn := r.Context().Value(connectionKey{}).(*net.UnixConn)
		if len(options) != 0 && options[0].pidfile != "" {
			allowed, err := os.ReadFile(options[0].pidfile)
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(allowed)))
			var peer *unix.Ucred
			raw, rawErr := conn.SyscallConn()
			if rawErr == nil {
				rawErr = raw.Control(func(fd uintptr) { peer, _ = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
			}
			if err != nil || parseErr != nil || rawErr != nil || peer == nil || int(peer.Pid) != pid {
				http.Error(w, "PID not admitted", http.StatusForbidden)
				return
			}
		}
		_, err := routesync.ReadRegister(r.Body)
		if err != nil {
			return
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/internal/plugin/"), "/register")
		sessionsMu.Lock()
		sessions[id] = conn
		if next := reconnects[id]; next != nil {
			close(next)
			delete(reconnects, id)
		}
		sessionsMu.Unlock()
		policy := routesync.Policy{AuthMode: "enforce", ParkTimeoutMS: 30000, MMDS: &routesync.MMDSProxyPolicy{Enabled: metadata, Listen: "fd=mmds"}}
		if len(options) != 0 && options[0].metadataService != "" {
			policy.MMDS.Services = map[string]string{"local": "unix://" + options[0].metadataService}
		}
		if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version, Policy: policy, ProxyCapabilities: capabilities}}); err != nil {
			return
		}
		if len(options) != 0 && options[0].route != nil {
			if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeUpsert, Route: options[0].route}); err != nil {
				return
			}
		}
		if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeBookmark}); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		_, _ = io.Copy(io.Discard, r.Body)
	}), &http2.Server{}), ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
		return context.WithValue(ctx, connectionKey{}, conn)
	}}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close(); listener.Close() })
	return func(id string) <-chan struct{} {
		sessionsMu.Lock()
		conn := sessions[id]
		next := make(chan struct{})
		reconnects[id] = next
		sessionsMu.Unlock()
		if conn == nil {
			t.Fatalf("no registered connection for %s", id)
		}
		conn.Close()
		return next
	}
}

func TestMasterReportsWorkerCompletionFailure(t *testing.T) {
	for _, mode := range []string{"error", "exit"} {
		t.Run(mode, func(t *testing.T) {
			effective, _ := testMasterEffective(t)
			cfg := effective.Config()
			dir, err := os.MkdirTemp("", "proxy-completion-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			cfg.ConfigSocket = filepath.Join(dir, "config.sock")
			cfg.StatsSocket = filepath.Join(dir, "stats.sock")
			cfg.ShmPath = filepath.Join(dir, "routes.shm")
			cfg.DataListen = "fd=data"
			cfg.Workers = 2
			if err := os.WriteFile(cfg.ShmPath+".shutdown-fault", []byte(mode), 0600); err != nil {
				t.Fatal(err)
			}
			serveFixtureRoutes(t, cfg.ConfigSocket, false)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			file, err := listenerFile(listener)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			effective, err = FreezeConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			instance := NewInstance("proxy", "", "", cfg.Workers)
			done := make(chan error, 1)
			go func() {
				done <- RunMaster(context.Background(), effective, &Runtime{Logger: testMasterLogger(), Instance: instance, ListenerFiles: map[string]*os.File{"data": file}})
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := instance.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			instance.mu.Lock()
			pids := []int{instance.workers[0].pid, instance.workers[1].pid}
			instance.mu.Unlock()
			instance.requestStop(time.Now().Add(time.Second))
			select {
			case err := <-done:
				if err == nil || errors.Is(err, ErrForcedClose) != (mode != "error") {
					t.Fatalf("incorrect completion failure: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("worker completion did not finish")
			}
			for _, pid := range pids {
				if err := unix.Kill(pid, 0); err != unix.ESRCH {
					t.Fatalf("worker %d not reaped: %v", pid, err)
				}
			}
		})
	}
}
