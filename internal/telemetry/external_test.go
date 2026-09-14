package telemetry

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/prometheus/prometheus/prompb"
)

func TestPrometheusGenericSelectionAndResponseScope(t *testing.T) {
	stamp := time.Now().Add(-time.Minute).Truncate(time.Second).UnixMilli()
	var wrong atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tenant/api/v1/read" || r.Header.Get("Authorization") != "Bearer private" {
			t.Error("read endpoint/credentials")
		}
		raw, _ := io.ReadAll(r.Body)
		raw, err := snappy.Decode(nil, raw)
		requireNoQueryError(t, err)
		var request prompb.ReadRequest
		if err := request.Unmarshal(raw); err != nil || len(request.Queries) != 1 {
			t.Error("invalid read", err)
			w.WriteHeader(400)
			return
		}
		got := map[string]string{}
		for _, matcher := range request.Queries[0].Matchers {
			got[matcher.Name] = matcher.Value
		}
		if got["sandbox_id"] != "sid" || got["sandbox_telemetry_source"] != "custom-source" || got["rack"] != "west" || got["otel.kind"] != "" || strings.Contains(got["__name__"], "sandbox.cpu") {
			t.Error("generic selection changed", got)
		}
		id := "sid"
		if wrong.Load() {
			id = "different"
		}
		response := prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: []*prompb.TimeSeries{{Labels: []prompb.Label{{Name: "__name__", Value: "custom_temperature"}, {Name: "sandbox_id", Value: id}, {Name: "sandbox_telemetry_source", Value: "custom-source"}, {Name: "rack", Value: "west"}}, Samples: []prompb.Sample{{Timestamp: stamp, Value: -8}, {Timestamp: stamp + 1000, Value: -4}}}}}}}
		raw, err = response.Marshal()
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")
		_, _ = w.Write(snappy.Encode(nil, raw))
	}))
	defer server.Close()
	cfg := prometheusQueryConfig(server.URL + "/tenant")
	cfg.Prometheus.Headers = map[string]string{"Authorization": "Bearer private"}
	backend, err := NewPrometheus(cfg)
	requireNoQueryError(t, err)
	defer backend.Shutdown(context.Background())
	query := extension.Query{Selection: extension.Selection{SandboxID: "sid", Metrics: []string{"custom_temperature"}, Attributes: map[string]string{sourceAttribute: "custom-source", "rack": "west"}}, Start: time.UnixMilli(stamp), End: time.UnixMilli(stamp + 1000), Aggregation: extension.Raw}
	series, err := backend.Query(context.Background(), query)
	if err != nil || len(series) != 1 || len(series[0].Points) != 2 || series[0].Points[0].Value != -8 {
		t.Fatal("arbitrary raw series", series, err)
	}
	query.Aggregation, query.Step = extension.Max, 5*time.Second
	series, err = backend.Query(context.Background(), query)
	if err != nil || len(series) != 1 || series[0].Attributes[SandboxIDAttribute] != "sid" {
		t.Fatal("generic MAX", series, err)
	}
	wrong.Store(true)
	if _, err := backend.Query(context.Background(), query); err == nil {
		t.Fatal("wrong SandboxID response accepted")
	}
}

func TestExternalReadCancellationRedirectsAndLimits(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer server.Close()
	backend, err := NewPrometheus(prometheusQueryConfig(server.URL))
	requireNoQueryError(t, err)
	defer backend.Shutdown(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, _, err := backend.Bounds(ctx, envdSelection("sid")); done <- err }()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader ignored cancellation")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("remote connection not cancelled")
	}
	var leaked atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer other.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL, 307) }))
	defer redirect.Close()
	client := newRemoteClient(map[string]string{"Authorization": "private"})
	defer client.Shutdown(context.Background())
	if _, err := client.request(context.Background(), redirect.URL, nil, nil); err == nil || leaked.Load() != 0 {
		t.Fatal("credential-bearing redirect followed", err)
	}
	oversized := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")
		_, _ = w.Write(binary.AppendUvarint(nil, maxRemoteBytes+1))
	}))
	defer oversized.Close()
	backend.endpoint = oversized.URL
	if _, _, _, err := backend.Bounds(context.Background(), envdSelection("sid")); err == nil {
		t.Fatal("snappy expansion limit not enforced")
	}
}

func TestPrometheusLongRetentionEmptySingleRead(t *testing.T) {
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		response := prompb.ReadResponse{Results: []*prompb.QueryResult{{}}}
		raw, _ := response.Marshal()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")
		_, _ = w.Write(snappy.Encode(nil, raw))
	}))
	defer server.Close()
	cfg := prometheusQueryConfig(server.URL)
	cfg.Lookback = "8760h"
	backend, err := NewPrometheus(cfg)
	requireNoQueryError(t, err)
	defer backend.Shutdown(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, found, err := backend.Bounds(ctx, envdSelection("new-sandbox")); err != nil || found {
		t.Fatal("empty history", found, err)
	}
	if reads.Load() != 1 {
		t.Fatal("one-year empty history did not use one read", reads.Load())
	}
	response := httptest.NewRecorder()
	queryHandlerForTest(backend).ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/new-sandbox/metrics", nil))
	if response.Code != 200 || response.Body.String() != "[]\n" || reads.Load() != 2 {
		t.Fatal(response.Code, response.Body.String(), reads.Load())
	}
}

func TestClickHouseReadOnlySQLAndJSONContract(t *testing.T) {
	var requests atomic.Int32
	stamp := time.Now().Add(-time.Minute).Truncate(5 * time.Second).Add(123 * time.Millisecond)
	sid := "sid' OR 1=1 --"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		params := r.URL.Query()
		query := params.Get("query")
		if r.Header.Get("X-ClickHouse-Key") != "private" || params.Get("wait_end_of_query") != "1" || params.Get("readonly") != "1" || params.Get("cancel_http_readonly_queries_on_client_close") != "1" {
			t.Error("auth/read-only/cancellation policy")
		}
		if !strings.HasPrefix(query, "SELECT") || strings.Contains(query, sid) || !strings.Contains(query, "ResourceAttributes['sandbox.id'] = {sid:String}") || params.Get("param_sid") != sid {
			t.Error("non-parameterized or non-read query", query)
		}
		if !strings.Contains(query, "bitAnd(Flags,1)=0") {
			t.Error("no-recorded-value was not filtered")
		}
		if strings.HasPrefix(query, "SELECT count()") {
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 1, "first": stamp.UnixMilli(), "last": stamp.UnixMilli()})
			return
		}
		if !strings.Contains(query, "max(value)") || params.Get("param_step") != "5000" {
			t.Error("not epoch millisecond MAX", query)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"bucket": stamp.UnixMilli() / 5000 * 5000, "metric": "custom.temperature", "attributes": map[string]string{SandboxIDAttribute: sid, "otel.kind": "Gauge", "point.rack": "west"}, "value": -1})
	}))
	defer server.Close()
	cfg := clickhouseQueryConfig(server.URL, "test_read_only")
	cfg.ClickHouse.Headers = map[string]string{"X-ClickHouse-Key": "private"}
	backend, err := NewClickHouse(cfg)
	requireNoQueryError(t, err)
	defer backend.Shutdown(context.Background())
	if requests.Load() != 0 {
		t.Fatal("query constructor created a schema")
	}
	selection := extension.Selection{SandboxID: sid, Metrics: []string{"custom.temperature"}, Attributes: map[string]string{"point.rack": "west"}}
	start, end, found, err := backend.Bounds(context.Background(), selection)
	if err != nil || !found || !start.Equal(stamp) || !end.Equal(stamp) {
		t.Fatal(start, end, found, err)
	}
	series, err := backend.Query(context.Background(), extension.Query{Selection: selection, Start: start, End: end, Step: 5 * time.Second, Aggregation: extension.Max})
	if err != nil || len(series) != 1 || series[0].Points[0].Value != -1 {
		t.Fatal(series, err)
	}
	if requests.Load() != 2 {
		t.Fatal("unexpected schema/write request")
	}
}

func TestClickHouseRejectsErrorAfterHTTP200(t *testing.T) {
	for _, raw := range []string{"Code: 60. DB::Exception: unknown table", `{"stamp":` + strconv.FormatInt(time.Now().UnixMilli(), 10) + `,"metric":"sandbox.cpu.count","value":2}` + "\nCode: 241. DB::Exception: memory limit"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.Copy(w, bytes.NewBufferString(raw)) }))
		backend, err := NewClickHouse(clickhouseQueryConfig(server.URL, "test"))
		requireNoQueryError(t, err)
		_, err = backend.Query(context.Background(), extension.Query{Selection: envdSelection("sid"), Start: time.Now().Add(-time.Minute), End: time.Now(), Step: 5 * time.Second, Aggregation: extension.Max})
		if err == nil {
			t.Fatal("HTTP 200 error body accepted")
		}
		_ = backend.Shutdown(context.Background())
		server.Close()
	}
}
