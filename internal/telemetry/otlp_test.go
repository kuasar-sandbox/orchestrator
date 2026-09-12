package telemetry

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func startOTLP(t *testing.T, view *View) (*otlpReceiver, <-chan pmetric.Metrics) {
	t.Helper()
	cfg, err := config.DecodeTelemetry(bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	// Independent ephemeral ports are safe for this internal receiver harness.
	cfg.Telemetry.OTLP.HTTPListen, cfg.Telemetry.OTLP.GRPCListen = "127.0.0.1:0", "127.0.0.1:0"
	observed := make(chan pmetric.Metrics, 32)
	guard := &identityProcessor{view: view, final: true, next: metricsConsumer(t, func(_ context.Context, metrics pmetric.Metrics) error {
		copy := pmetric.NewMetrics()
		metrics.CopyTo(copy)
		observed <- copy
		return nil
	})}
	r := &otlpReceiver{cfg: *cfg, view: view, next: guard, requests: make(chan struct{}, cfg.Telemetry.OTLP.MaxRequests), fatal: func(err error) { t.Error(err) }}
	if err := r.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := r.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return r, observed
}

func spoofedOTLP(t *testing.T) pmetricotlp.ExportRequest {
	t.Helper()
	metrics, err := decodeEnvd(bytes.NewReader(envdJSON()))
	if err != nil {
		t.Fatal(err)
	}
	attrs := metrics.ResourceMetrics().At(0).Resource().Attributes()
	attrs.PutStr(SandboxIDAttribute, "victim")
	attrs.PutStr(StableIDAttribute, "victim-stable")
	attrs.PutStr("sandbox.run_id", "fake")
	return pmetricotlp.NewExportRequestFromMetrics(metrics)
}

func assertOTLPIdentity(t *testing.T, observed <-chan pmetric.Metrics) {
	t.Helper()
	select {
	case metrics := <-observed:
		attrs := metrics.ResourceMetrics().At(0).Resource().Attributes()
		for key, want := range map[string]string{SandboxIDAttribute: "sid", StableIDAttribute: "stable-sid", sourceAttribute: "otlp"} {
			got, ok := attrs.Get(key)
			if !ok || got.Str() != want {
				t.Fatalf("trusted %s = %v", key, got)
			}
		}
		if _, ok := attrs.Get("sandbox.run_id"); ok {
			t.Fatal("run attribute escaped")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OTLP not delivered")
	}
}

func TestOTLPHTTPAndGRPCTransportIdentity(t *testing.T) {
	view := NewView(10, time.Second)
	upsert(t, view, testRoute("sid"))
	view.Bookmark()
	r, observed := startOTLP(t, view)
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{}}
	t.Cleanup(client.CloseIdleConnections)
	for _, contentType := range []string{"application/x-protobuf", "application/json"} {
		var raw []byte
		var err error
		if contentType == "application/json" {
			raw, err = spoofedOTLP(t).MarshalJSON()
		} else {
			raw, err = spoofedOTLP(t).MarshalProto()
		}
		if err != nil {
			t.Fatal(err)
		}
		var body bytes.Buffer
		gz := gzip.NewWriter(&body)
		if _, err := gz.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequest("POST", "http://"+r.httpListener.Addr().String()+"/v1/metrics", &body)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", contentType)
		request.Header.Set("Content-Encoding", "gzip")
		request.Header.Set("X-Forwarded-For", "192.0.2.99")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatal(response.Status, string(raw))
		}
		assertOTLPIdentity(t, observed)
	}
	connection, err := grpc.NewClient(r.grpcListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := pmetricotlp.NewGRPCClient(connection).Export(ctx, spoofedOTLP(t)); err != nil {
		t.Fatal(err)
	}
	assertOTLPIdentity(t, observed)
	view.InvalidateSync()
	if _, err := pmetricotlp.NewGRPCClient(connection).Export(ctx, spoofedOTLP(t)); err == nil {
		t.Fatal("disconnected stream accepted")
	}
}

func TestOTLPUnknownPeerFailsClosed(t *testing.T) {
	view := NewView(10, time.Second)
	route := testRoute("sid")
	route.FloatingIP = "127.0.0.2"
	upsert(t, view, route)
	view.Bookmark()
	r, observed := startOTLP(t, view)
	raw, err := spoofedOTLP(t).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest("POST", "http://"+r.httpListener.Addr().String()+"/v1/metrics", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("X-Forwarded-For", "127.0.0.2")
	client := &http.Client{Transport: &http.Transport{}, Timeout: time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err == nil {
		response.Body.Close()
		t.Fatal("untrusted HTTP peer passed TCP acceptance", response.StatusCode)
	}
	connection, err := grpc.NewClient(r.grpcListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := pmetricotlp.NewGRPCClient(connection).Export(ctx, spoofedOTLP(t)); status.Code(err) != codes.Unavailable {
		t.Fatal("untrusted gRPC peer", err)
	}
	select {
	case <-observed:
		t.Fatal("unknown peer reached pipeline")
	default:
	}
	// The same HTTP client recovers after full sync/route arrival by dialing a
	// new connection; no anonymous persistent connection can become an identity.
	route.FloatingIP = "127.0.0.1"
	upsert(t, view, route)
	request, err = http.NewRequest("POST", "http://"+r.httpListener.Addr().String()+"/v1/metrics", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	response, err = client.Do(request)
	if err != nil {
		t.Fatal("known peer did not recover", err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("known peer", response.StatusCode)
	}
	assertOTLPIdentity(t, observed)
}

func TestOTLPPersistentConnectionRevokedOnRemap(t *testing.T) {
	view := NewView(10, time.Second)
	upsert(t, view, testRoute("sid"))
	view.Bookmark()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	identified := &identityListener{Listener: listener, view: view}
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	accepted, err := identified.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	old := accepted.RemoteAddr().(identityAddr).entry
	upsert(t, view, testRoute("successor"))
	if err := view.withCurrent(old, func() error { return nil }); err == nil {
		t.Fatal("old peer became successor")
	}
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Read(make([]byte, 1)); err != io.EOF {
		t.Fatal("old connection not closed", err)
	}
}
