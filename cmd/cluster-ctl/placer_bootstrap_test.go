package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
	"gopkg.in/yaml.v3"
)

func startupMembership() clustercfg.MembershipConfig {
	return clustercfg.MembershipConfig{Active: 1, Versions: []clustercfg.MembershipVersion{{Version: 1, Members: []clustercfg.MembershipMember{{ID: "r"}}}}, Owners: clustercfg.MembershipOwnerConfig{RouteLink: 1, NodeLink: 1, NodeList: 1, PlacerLink: 1}}
}
func startupLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func waitStartup(ctx context.Context, reg *clusterclient.Registry) error {
	_, err := waitPlacerMembership(ctx, reg, startupLog(), 30*time.Millisecond, time.Millisecond, 5*time.Millisecond)
	return err
}

func TestBootstrapErrorClassification(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		retry bool
	}{
		{"refused", syscall.ECONNREFUSED, true}, {"reset", syscall.ECONNRESET, true}, {"unreachable", syscall.ENETUNREACH, true}, {"timeout", context.DeadlineExceeded, true}, {"EOF", io.EOF, true}, {"short", io.ErrUnexpectedEOF, true},
		{"DNS temporary", &net.DNSError{IsTemporary: true}, true}, {"DNS permanent", &net.DNSError{IsNotFound: true}, false},
		{"unix absent", &net.OpError{Op: "dial", Net: "unix", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ENOENT}}, true},
		{"local absent", &os.PathError{Op: "open", Path: "cert", Err: syscall.ENOENT}, false},
		{"cert", x509.UnknownAuthorityError{}, false}, {"hostname", x509.HostnameError{}, false}, {"verify", &tls.CertificateVerificationError{Err: context.DeadlineExceeded}, false},
		{"address", &net.AddrError{Err: "bad", Addr: "bad"}, false}, {"json", &json.SyntaxError{}, false}, {"complete truncated JSON", &clusterclient.MembershipJSONError{Err: io.ErrUnexpectedEOF}, false}, {"type", &json.UnmarshalTypeError{}, false}, {"cancel", context.Canceled, false}, {"unknown", errors.New("unknown"), false},
		{"mixed", errors.Join(syscall.ECONNREFUSED, errors.New("permanent")), false},
	}
	for _, status := range []int{408, 429, 500, 502, 503, 504, 401, 403, 404, 400, 501} {
		tests = append(tests, struct {
			name  string
			err   error
			retry bool
		}{fmt.Sprint(status), &clusterclient.MembershipStatusError{StatusCode: status}, status == 408 || status == 429 || status == 500 || status == 502 || status == 503 || status == 504})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, err := range []error{tt.err, fmt.Errorf("target: %w", tt.err), &url.Error{Op: "Get", URL: "http://registry", Err: tt.err}, errors.Join(tt.err, tt.err)} {
				if got := retryPlacerBootstrap(err); got != tt.retry {
					t.Fatalf("%T => %v want %v", err, got, tt.retry)
				}
			}
		})
	}
}

func TestBootstrapHTTPRecovery(t *testing.T) {
	for _, mode := range []string{"503", "stall", "interrupted"} {
		t.Run(mode, func(t *testing.T) {
			var hits atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hits.Add(1) == 1 {
					switch mode {
					case "503":
						w.WriteHeader(503)
					case "stall":
						<-r.Context().Done()
					case "interrupted":
						w.Header().Set("Content-Length", "1000")
						fmt.Fprint(w, `{"active":`)
					}
					return
				}
				json.NewEncoder(w).Encode(startupMembership())
			}))
			defer srv.Close()
			reg, _ := clusterclient.NewRegistry(srv.URL, nil)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := waitStartup(ctx, reg); err != nil {
				t.Fatal(err)
			}
			if hits.Load() < 2 {
				t.Fatal("did not retry")
			}
		})
	}
}

func TestBootstrapPermanentResponses(t *testing.T) {
	for _, body := range []string{"unauthorized", "forbidden", "missing", "invalid", `{"active":`, `{"active":"wrong"}`, `{} garbage`, `{} {`, `{} {}`, `{}`, `{"active":1,"versions":[]}`} {
		t.Run(body, func(t *testing.T) {
			var hits atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				switch body {
				case "unauthorized":
					w.WriteHeader(401)
				case "forbidden":
					w.WriteHeader(403)
				case "missing":
					w.WriteHeader(404)
				}
				fmt.Fprint(w, body)
			}))
			defer srv.Close()
			reg, _ := clusterclient.NewRegistry(srv.URL, nil)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := waitStartup(ctx, reg)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || hits.Load() != 1 {
				t.Fatalf("err=%v hits=%d", err, hits.Load())
			}
		})
	}
	t.Run("certificate", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS reached handler") }))
		defer srv.Close()
		reg, _ := clusterclient.NewRegistry(srv.URL, nil)
		if err := waitStartup(t.Context(), reg); err == nil || retryPlacerBootstrap(err) {
			t.Fatalf("err=%v", err)
		}
	})
}

// The log hook synchronizes first failure without timing-dependent sleeps.
type startupHandler struct{ events chan string }

func (h startupHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h startupHandler) Handle(_ context.Context, r slog.Record) error {
	select {
	case h.events <- r.Message:
	default:
	}
	return nil
}
func (h startupHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h startupHandler) WithGroup(string) slog.Handler      { return h }

func TestBootstrapDelayedListener(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			address := filepath.Join(t.TempDir(), "registry.sock")
			bootstrap := address
			if network == "tcp" {
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				address = ln.Addr().String()
				ln.Close()
				bootstrap = "http://" + address
			}
			reg, err := clusterclient.NewRegistry(bootstrap, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			events := make(chan string, 8)
			done := make(chan error, 1)
			go func() {
				_, err := waitPlacerMembership(ctx, reg, slog.New(startupHandler{events}), 50*time.Millisecond, time.Millisecond, 5*time.Millisecond)
				done <- err
			}()
			select {
			case <-events:
			case <-ctx.Done():
				t.Fatal("no initial failure")
			}
			ln, err := net.Listen(network, address)
			if err != nil {
				t.Fatal(err)
			}
			srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(startupMembership()) })}
			go srv.Serve(ln)
			defer srv.Close()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			select {
			case message := <-events:
				if message != "placer: registry bootstrap recovered" {
					t.Fatal(message)
				}
			default:
				t.Fatal("no recovery log")
			}
		})
	}
}

func TestBootstrapCancelRequestAndBackoff(t *testing.T) {
	for _, phase := range []string{"request", "backoff"} {
		t.Run(phase, func(t *testing.T) {
			entered := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if phase == "request" {
					close(entered)
					<-r.Context().Done()
				} else {
					w.WriteHeader(503)
				}
			}))
			defer srv.Close()
			reg, _ := clusterclient.NewRegistry(srv.URL, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			events := make(chan string, 2)
			done := make(chan error, 1)
			go func() {
				_, err := waitPlacerMembership(ctx, reg, slog.New(startupHandler{events}), time.Minute, time.Minute, time.Minute)
				done <- err
			}()
			if phase == "request" {
				<-entered
			} else {
				<-events
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation blocked")
			}
		})
	}
}

func writeStartupConfig(t *testing.T, bootstrap string, mutate func(*clustercfg.PlacerConfig)) string {
	t.Helper()
	cfg := clustercfg.DefaultPlacer()
	cfg.Registry.Bootstrap = bootstrap
	cfg.Placer.Listen = "127.0.0.1:0"
	cfg.ImportGroups = []clustercfg.GroupSourceConfig{{SourceID: "test", SourceType: "file", Path: t.TempDir()}}
	if mutate != nil {
		mutate(&cfg)
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "placer.yaml")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPlacerLocalErrorsBeforeNetwork(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(503) }))
	defer srv.Close()
	for _, mode := range []string{"groups", "directory", "not-directory", "registry-tls", "placer-tls", "listen", "port", "scheme", "url"} {
		t.Run(mode, func(t *testing.T) {
			path := writeStartupConfig(t, srv.URL, func(c *clustercfg.PlacerConfig) {
				switch mode {
				case "scheme":
					c.Registry.Bootstrap = "ftp://registry"
				case "url":
					c.Registry.Bootstrap = "http://"
				case "listen":
					c.Placer.Listen = "bad address"
				case "port":
					c.Placer.Listen = "127.0.0.1:99999"
				case "groups":
					c.ImportGroups = nil
				case "directory":
					c.ImportGroups[0].Path = filepath.Join(t.TempDir(), "missing")
				case "not-directory":
					p := filepath.Join(t.TempDir(), "file")
					os.WriteFile(p, nil, 0600)
					c.ImportGroups[0].Path = p
				case "registry-tls":
					c.Registry.TLS = clustercfg.TLS{Cert: "missing", Key: "missing"}
				case "placer-tls":
					c.Placer.TLS = clustercfg.TLS{Cert: "missing", Key: "missing"}
				}
			})
			if err := runPlacer([]string{"--config", path}, startupLog()); err == nil {
				t.Fatal("expected local error")
			}
			if hits.Load() != 0 {
				t.Fatal("network attempted before local validation")
			}
		})
	}
}

func TestPlacerCommandSignals(t *testing.T) {
	if path := os.Getenv("PLACER_BOOTSTRAP_TEST_CONFIG"); path != "" {
		os.Args = []string{"cluster-ctl", "placer", "--config", path}
		main()
		return
	}
	for _, sig := range []os.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
			defer srv.Close()
			path := writeStartupConfig(t, srv.URL, nil)
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestPlacerCommandSignals$")
			cmd.Env = append(os.Environ(), "PLACER_BOOTSTRAP_TEST_CONFIG="+path)
			pipe, err := cmd.StderrPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			scanner := bufio.NewScanner(pipe)
			found := false
			for scanner.Scan() {
				if strings.Contains(scanner.Text(), "waiting for registry membership") {
					found = true
					break
				}
			}
			if !found {
				t.Fatal("command exited before waiting")
			}
			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			var rest strings.Builder
			for scanner.Scan() {
				rest.WriteString(scanner.Text())
			}
			if err := cmd.Wait(); err != nil {
				t.Fatalf("signal became startup failure: %v %s", err, rest.String())
			}
			if strings.Contains(rest.String(), "level=ERROR") {
				t.Fatal(rest.String())
			}
		})
	}
}

func TestPlacerCommandRecoversSameProcess(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	path := writeStartupConfig(t, "http://"+addr, nil)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestPlacerCommandSignals$")
	cmd.Env = append(os.Environ(), "PLACER_BOOTSTRAP_TEST_CONFIG="+path)
	pipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	scanner := bufio.NewScanner(pipe)
	waiting := false
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "waiting for registry membership") {
			waiting = true
			break
		}
	}
	if !waiting {
		t.Fatal("command did not wait after connection refusal")
	}
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == clusterclient.MembershipPath {
			json.NewEncoder(w).Encode(startupMembership())
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})}
	go srv.Serve(ln)
	defer srv.Close()
	initialized := false
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), `msg="cluster-ctl placer"`) {
			initialized = true
			break
		}
	}
	if !initialized {
		t.Fatal("same process did not continue initialization")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	for scanner.Scan() {
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
}
