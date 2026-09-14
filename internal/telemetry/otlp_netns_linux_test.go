package telemetry

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/golang/snappy"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The namespace belongs to a child of this test. Do not touch named namespaces
// or infer support from EUID: probe the actual unshare/ip/setns operations.
func otlpTestNamespace(t *testing.T) *netns.NetNS {
	t.Helper()
	cmd := exec.Command("unshare", "--net", "--", "sh", "-c", "set -eu; ip link set lo up; ip addr add 192.0.2.2/32 dev lo; echo ready; read finish")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostics bytes.Buffer
	cmd.Stderr = &diagnostics
	if err := cmd.Start(); err != nil {
		if os.Getenv("REQUIRE_TELEMETRY_NETNS") == "1" {
			t.Fatal(err)
		}
		t.Skipf("unshare unavailable: %v", err)
	}
	t.Cleanup(func() { _ = input.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() })
	if scanner := bufio.NewScanner(output); !scanner.Scan() || scanner.Text() != "ready" {
		_ = cmd.Wait()
		if os.Getenv("REQUIRE_TELEMETRY_NETNS") == "1" {
			t.Fatalf("namespace setup: %s", diagnostics.String())
		}
		t.Skipf("namespace setup unavailable: %s", diagnostics.String())
	}
	ns, err := appnet.OpenProxyNetNS(fmt.Sprintf("/proc/%d/ns/net", cmd.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ns.Close() })
	if err := ns.Do(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	return ns
}

func TestOTLPProxyNetNSCollectorIsolation(t *testing.T) {
	ns := otlpTestNamespace(t)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	original, err := os.Stat("/proc/thread-self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	child, err := os.Stat(ns.String())
	if err != nil || os.SameFile(original, child) {
		t.Fatal("namespace is not isolated", err)
	}
	assertRestored := func() {
		t.Helper()
		current, err := os.Stat("/proc/thread-self/ns/net")
		if err != nil || !os.SameFile(original, current) {
			t.Fatal("calling thread changed netns", err)
		}
	}
	// Reserve both addresses in the host namespace throughout the test. A
	// listener incorrectly created on the host cannot start on either port.
	hostHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "host") }))
	defer hostHTTP.Close()
	hostGRPC, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer hostGRPC.Close()
	observed := make(chan pmetric.Metrics, 4)
	// This real exporter destination exists only on the host's private loopback.
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			defer gz.Close()
			body = gz
		}
		raw, err := io.ReadAll(body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		request := pmetricotlp.NewExportRequest()
		if err := request.UnmarshalProto(raw); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		observed <- request.Metrics()
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer sink.Close()
	cfg, err := config.DecodeTelemetry(strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ProxyNetNS = ns.String()
	cfg.Query.Backend = "none"
	cfg.Collector = map[string]any{
		"receivers": map[string]any{"sandboxotlp": map[string]any{"http_listen": hostHTTP.Listener.Addr().String(), "grpc_listen": hostGRPC.Addr().String()}},
		"exporters": map[string]any{"otlp_http/host": map[string]any{"endpoint": sink.URL}},
		"service": map[string]any{"telemetry": map[string]any{"metrics": map[string]any{"level": "none"}}, "pipelines": map[string]any{"metrics": map[string]any{
			"receivers": []any{"sandboxotlp"}, "exporters": []any{"otlp_http/host"},
		}}},
	}
	view := NewView(4)
	defer view.InvalidateSync()
	route := testRoute("sid")
	route.FloatingIP = "192.0.2.2"
	route.EnvdUDS = "" // No envd fixture or extra observations in this test.
	upsert(t, view, route)
	view.Bookmark()
	collector, err := NewCollector(context.Background(), *cfg, view, nil, customotel.Components{}, testLogger(), make(chan error, 2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := collector.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := collector.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRestored()
	dial := func(ctx context.Context, address string) (conn net.Conn, err error) {
		err = ns.Do(func() error {
			d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("192.0.2.2")}}
			conn, err = d.DialContext(ctx, "tcp4", address)
			return err
		})
		return conn, err
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, address string) (net.Conn, error) { return dial(ctx, address) }}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	raw, err := spoofedOTLP(t).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Post(hostHTTP.URL+"/v1/metrics", "application/x-protobuf", bytes.NewReader(raw))
	if err != nil {
		t.Fatal("HTTP listener unreachable in its namespace", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal(response.Status)
	}
	assertOTLPIdentity(t, observed)
	connection, err := grpc.NewClient(hostGRPC.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dial))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := pmetricotlp.NewGRPCClient(connection).Export(ctx, spoofedOTLP(t)); err != nil {
		t.Fatal("gRPC listener unreachable in its namespace", err)
	}
	assertOTLPIdentity(t, observed)
	assertRestored()
	// The query backend is independent of the listeners too. Exercise its real
	// HTTP client against a host-only remote-read endpoint after both exports.
	queried := make(chan struct{}, 1)
	querySink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/read" {
			t.Errorf("unexpected query path: %s", r.URL.Path)
		}
		queried <- struct{}{}
		result := &prompb.ReadResponse{Results: []*prompb.QueryResult{{}}}
		raw, err := result.Marshal()
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")
		_, _ = w.Write(snappy.Encode(nil, raw))
	}))
	defer querySink.Close()
	queryConfig := cfg.Query
	queryConfig.Prometheus.Endpoint = querySink.URL
	reader, err := NewPrometheus(queryConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Shutdown(context.Background())
	if _, _, _, err := reader.Bounds(ctx, envdSelection("sid")); err != nil {
		t.Fatal("remote query left the host namespace", err)
	}
	select {
	case <-queried:
	default:
		t.Fatal("query did not reach the host endpoint")
	}
	assertRestored()
	response, err = hostHTTP.Client().Get(hostHTTP.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "host" {
		t.Fatal("host listener was affected", string(body), err)
	}
}

func TestOTLPProxyNetNSStartFailureCleanup(t *testing.T) {
	ns := otlpTestNamespace(t)
	for _, spec := range []string{"", ns.String()} {
		t.Run(map[bool]string{true: "isolated", false: "current"}[spec != ""], func(t *testing.T) {
			var target *netns.NetNS
			if spec != "" {
				target = ns
			}
			blocker, err := appnet.ListenTCPInNetNS(target, "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Close()
			cfg := defaultOTLPConfig()
			cfg.HTTPListen, cfg.GRPCListen = "127.0.0.1:0", blocker.Addr().String()
			r := &otlpReceiver{cfg: cfg, proxyNetNS: spec, view: NewView(1)}
			if err := r.Start(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "OTLP gRPC listen") {
				t.Fatal("occupied gRPC port accepted", err)
			}
			if r.httpListener == nil {
				t.Fatal("HTTP was not created before the failure")
			}
			probe, err := appnet.ListenTCPInNetNS(target, r.httpListener.Addr().String())
			if err != nil {
				t.Fatal("partially started HTTP listener leaked", err)
			}
			_ = probe.Close()
			if err := r.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOTLPProxyNetNSInvalidFailsClosed(t *testing.T) {
	for _, spec := range []string{"kuasar-test-no-such-netns", filepath.Join(t.TempDir(), "missing")} {
		r := &otlpReceiver{cfg: defaultOTLPConfig(), proxyNetNS: spec}
		if err := r.Start(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "proxy_netns") {
			t.Fatal("invalid netns accepted", err)
		}
		if r.httpListener != nil || r.grpcListener != nil {
			t.Fatal("invalid netns fell back to host")
		}
	}
	file := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	r := &otlpReceiver{cfg: defaultOTLPConfig(), proxyNetNS: file}
	if err := r.Start(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "setns") {
		t.Fatal("non-namespace file accepted", err)
	}
}

func TestOTLPProxyNetNSNoSetnsPermission(t *testing.T) {
	const marker = "KUASAR_OTLP_NO_SETNS_TEST"
	if os.Getenv(marker) == "1" {
		r := &otlpReceiver{cfg: defaultOTLPConfig(), proxyNetNS: "/proc/self/ns/net"}
		if err := r.Start(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "setns") {
			t.Fatal("setns failure silently accepted", err)
		}
		if r.httpListener != nil || r.grpcListener != nil {
			t.Fatal("listener survived permission failure")
		}
		return
	}
	if os.Geteuid() != 0 {
		if os.Getenv("REQUIRE_TELEMETRY_NETNS") == "1" {
			t.Fatal("root needed to launch an explicitly unprivileged subprocess")
		}
		t.Skip("root needed to launch an explicitly unprivileged subprocess")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// go test's work directory is private. Copy only this test binary into a
	// root-owned traversable directory so the child can execute after setuid.
	directory, err := os.MkdirTemp("", "otlp-no-setns-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	if err := os.Chmod(directory, 0755); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	path := filepath.Join(directory, "test")
	copy, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0555)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(copy, source)
	closeErr := copy.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatal(copyErr, closeErr)
	}
	cmd := exec.Command(path, "-test.run=^TestOTLPProxyNetNSNoSetnsPermission$")
	cmd.Env = append(os.Environ(), marker+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("unprivileged check: %v: %s", err, output)
	}
}
