package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func envdBody() map[string]any {
	return map[string]any{"ts": time.Now().Unix(), "cpu_count": 4, "cpu_used_pct": 12.34, "mem_total": 1024, "mem_used": 512, "mem_cache": 128, "disk_total": 2048, "disk_used": 1024}
}
func envdJSON() []byte         { raw, _ := json.Marshal(envdBody()); return raw }
func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func metricsConsumer(t testing.TB, consume func(context.Context, pmetric.Metrics) error) consumer.Metrics {
	t.Helper()
	c, err := consumer.NewMetrics(consume)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func unixHTTP(t testing.TB, handler http.Handler) string {
	t.Helper()
	// Keep sun_path within 107 bytes regardless of testing's generated name.
	directory, err := os.MkdirTemp("", "telemetry-test-")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "envd.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() { _ = server.Close(); _ = os.Remove(path); _ = os.Remove(directory) })
	return path
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestEnvdMappingAndMalformedFields(t *testing.T) {
	metrics, err := decodeEnvd(bytes.NewReader(envdJSON()))
	if err != nil {
		t.Fatal(err)
	}
	scope := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	if scope.Metrics().Len() != 7 {
		t.Fatal(scope.Metrics().Len())
	}
	values := []float64{4, 12.34, 1024, 512, 128, 2048, 1024}
	for i, want := range values {
		m := scope.Metrics().At(i)
		if m.Name() != resourceMetrics[i].name || m.Unit() != resourceMetrics[i].unit || m.Gauge().DataPoints().At(0).DoubleValue() != want {
			t.Fatalf("metric %d: %v", i, m)
		}
		if m.Gauge().DataPoints().At(0).Timestamp().AsTime().Unix() <= 0 {
			t.Fatal("lost timestamp")
		}
	}
	for field := range envdBody() {
		t.Run("missing-"+field, func(t *testing.T) {
			body := envdBody()
			delete(body, field)
			raw, _ := json.Marshal(body)
			if _, err := decodeEnvd(bytes.NewReader(raw)); err == nil {
				t.Fatal("missing value accepted")
			}
		})
		for _, bad := range []any{nil, "invalid", -1} {
			t.Run(fmt.Sprint(field, "-", bad), func(t *testing.T) {
				body := envdBody()
				body[field] = bad
				raw, _ := json.Marshal(body)
				if _, err := decodeEnvd(bytes.NewReader(raw)); err == nil {
					t.Fatal("bad value accepted")
				}
			})
		}
	}
	for _, raw := range []string{"{}", "null", "[]", "{}{}", strings.Repeat(" ", 65537), `{"cpu_used_pct":NaN}`} {
		if _, err := decodeEnvd(strings.NewReader(raw)); err == nil {
			t.Fatal("bad body accepted")
		}
	}
}

func TestEnvdTimeoutAndStaleResponse(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprint("stale=", stale), func(t *testing.T) {
			view := NewView(1)
			route := testRoute("sid")
			entry := upsert(t, view, route)
			view.Bookmark()
			var writes atomic.Int32
			next := metricsConsumer(t, func(context.Context, pmetric.Metrics) error { writes.Add(1); return nil })
			r := &envdReceiver{view: view, cfg: envdReceiverConfig{Timeout: 20 * time.Millisecond}, next: next}
			started, release := make(chan struct{}), make(chan struct{})
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path != "/metrics" || req.Header.Get("X-Access-Token") != route.EnvdAccessToken {
					t.Error("wrong path/token")
				}
				close(started)
				if !stale {
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				<-release // Deliberately ignore transport cancellation.
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(envdJSON())), Header: make(http.Header)}, nil
			})}
			done := make(chan error, 1)
			go func() { done <- r.scrape(context.Background(), client, entry) }()
			<-started
			if stale {
				route.EnvdUDS = "/successor.sock"
				upsert(t, view, route)
				close(release)
			}
			select {
			case err := <-done:
				if err == nil || (!stale && !errors.Is(err, context.DeadlineExceeded)) {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("scrape cancellation hung")
			}
			if writes.Load() != 0 {
				t.Fatal("stale/timeout response delivered")
			}
		})
	}
}

func TestEnvdUDSConnectionIsolationAndBoundedWorkers(t *testing.T) {
	view := NewView(10)
	var bad, active, maximum atomic.Int32
	for i := range 2 {
		id, token := fmt.Sprint(i), fmt.Sprint("token-", i)
		path := unixHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Access-Token") != token {
				bad.Add(1)
			}
			n := active.Add(1)
			defer active.Add(-1)
			for previous := maximum.Load(); n > previous && !maximum.CompareAndSwap(previous, n); previous = maximum.Load() {
			}
			body := envdBody()
			body["cpu_count"] = i + 1
			_ = json.NewEncoder(w).Encode(body)
		}))
		route := testRoute(id)
		route.FloatingIP = ""
		route.EnvdUDS = path
		route.EnvdAccessToken = token
		upsert(t, view, route)
	}
	view.Bookmark()
	observed := make(chan string, 32)
	next := metricsConsumer(t, func(ctx context.Context, metrics pmetric.Metrics) error {
		value, _ := metrics.ResourceMetrics().At(0).Resource().Attributes().Get(SandboxIDAttribute)
		id := value.Str()
		count := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).DoubleValue()
		if (id == "0" && count != 1) || (id == "1" && count != 2) {
			bad.Add(1)
		}
		observed <- id
		return nil
	})
	r := &envdReceiver{view: view, cfg: envdReceiverConfig{CollectionInterval: 20 * time.Millisecond, Timeout: time.Second, Concurrency: 1}, next: next, log: testLogger()}
	if err := r.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := r.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	counts := map[string]int{}
	for counts["0"] < 3 || counts["1"] < 3 {
		select {
		case id := <-observed:
			counts[id]++
		case <-time.After(3 * time.Second):
			t.Fatal("scrapes missing", counts)
		}
	}
	if bad.Load() != 0 {
		t.Fatal("cross-sandbox UDS/token reuse")
	}
	if maximum.Load() > 1 {
		t.Fatal("worker limit exceeded", maximum.Load())
	}
}
