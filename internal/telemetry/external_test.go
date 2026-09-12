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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/prometheus/prometheus/prompb"
)

func TestPrometheusPrimaryWriteReadMAXAndIdentity(t *testing.T) {
	var mu sync.Mutex
	var series []prompb.TimeSeries
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private" || r.Header.Get("Content-Encoding") != "snappy" {
			t.Error("missing protocol/credential headers")
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		raw, err = snappy.Decode(nil, raw)
		if err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/tenant/api/v1/write":
			var request prompb.WriteRequest
			if err := request.Unmarshal(raw); err != nil {
				t.Error(err)
				return
			}
			for _, item := range request.Timeseries {
				if !sort.SliceIsSorted(item.Labels, func(i, j int) bool { return item.Labels[i].Name < item.Labels[j].Name }) {
					t.Error("labels not canonical")
				}
				for _, label := range item.Labels {
					if forbiddenAttribute(label.Name) {
						t.Error("run label escaped")
					}
				}
			}
			series = append(series, request.Timeseries...)
			w.WriteHeader(204)
		case "/tenant/api/v1/read":
			reads.Add(1)
			var request prompb.ReadRequest
			if err := request.Unmarshal(raw); err != nil {
				t.Error(err)
				return
			}
			if len(request.Queries) != 1 {
				t.Error("unexpected queries")
				return
			}
			query := request.Queries[0]
			id := ""
			for _, matcher := range query.Matchers {
				if matcher.Name == SandboxIDAttribute {
					id = matcher.Value
				}
				if matcher.Name == StableIDAttribute {
					t.Error("StableID queried")
				}
			}
			result := &prompb.QueryResult{}
			for i := range series {
				var same bool
				for _, label := range series[i].Labels {
					if label.Name == SandboxIDAttribute && label.Value == id {
						same = true
					}
				}
				if same {
					item := &prompb.TimeSeries{Labels: series[i].Labels}
					for _, sample := range series[i].Samples {
						if sample.Timestamp >= query.StartTimestampMs && sample.Timestamp <= query.EndTimestampMs {
							item.Samples = append(item.Samples, sample)
						}
					}
					if len(item.Samples) != 0 {
						result.Timeseries = append(result.Timeseries, item)
					}
				}
			}
			response := prompb.ReadResponse{Results: []*prompb.QueryResult{result}}
			raw, err := response.Marshal()
			if err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.Header().Set("Content-Encoding", "snappy")
			_, _ = w.Write(snappy.Encode(nil, raw))
		default:
			t.Error("wrong endpoint", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	cfg := localConfig(t)
	cfg.Type = "prometheus"
	cfg.Retention = "1h"
	cfg.Prometheus = config.TelemetryRemote{Endpoint: server.URL + "/tenant", Headers: map[string]string{"Authorization": "Bearer private"}}
	backend, err := NewPrometheus(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Shutdown(context.Background())
	stamp := time.Now().Add(-time.Minute).Truncate(5 * time.Second).Add(123456789 * time.Nanosecond)
	first, second := resourceSamples(t, "sid", stamp), resourceSamples(t, "sid", stamp.Add(time.Second))
	first[1].Value = 90
	second[2].Value = 9999
	second[4].Value = 256
	if err := backend.Write(context.Background(), append(first, second...)); err != nil {
		t.Fatal(err)
	}
	start, end, found, err := backend.Bounds(context.Background(), "sid")
	if err != nil || !found || start.UnixMilli() != stamp.UnixMilli() || end.UnixMilli() != stamp.Add(time.Second).UnixMilli() {
		t.Fatal("bounds", start, end, found, err)
	}
	query := extension.Query{SandboxID: "sid", Start: start, End: end, Step: 5 * time.Second}
	points, err := backend.Query(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	result, err := aggregate(points, query)
	if err != nil || len(result) != 1 || result[0].CPUUsedPct != 90 || result[0].MemTotal != 9999 || result[0].MemCache != 256 {
		t.Fatal("MAX", result, err)
	}
	if _, _, found, err := backend.Bounds(context.Background(), "stable-sid"); err != nil || found {
		t.Fatal("StableID fallback", found, err)
	}
	if reads.Load() == 0 {
		t.Fatal("no reader requests")
	}
}

func TestExternalReadCancellationRedirectsAndLimits(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer server.Close()
	cfg := localConfig(t)
	cfg.Type = "prometheus"
	cfg.Prometheus.Endpoint = server.URL
	backend, err := NewPrometheus(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Shutdown(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, _, err := backend.Bounds(ctx, "sid"); done <- err }()
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
		buffer := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buffer, maxRemoteBytes+1)
		_, _ = w.Write(buffer[:n])
	}))
	defer oversized.Close()
	backend.endpoint = oversized.URL
	if _, _, _, err := backend.Bounds(context.Background(), "sid"); err == nil {
		t.Fatal("snappy expansion limit not enforced")
	}
}

func TestClickHousePrimarySQLAndJSONContract(t *testing.T) {
	var creates, alters, writes atomic.Int32
	stamp := time.Now().Add(-time.Minute).Truncate(time.Second)
	sandbox := "sid' OR 1=1 --"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-Key") != "private" || r.URL.Query().Get("wait_end_of_query") != "1" {
			t.Error("missing auth/buffering policy")
		}
		query := r.URL.Query().Get("query")
		switch {
		case strings.HasPrefix(query, "CREATE TABLE"):
			creates.Add(1)
			if !strings.Contains(query, "DateTime64(3, 'UTC')") || !strings.Contains(query, "ENGINE = MergeTree") {
				t.Error("table schema", query)
			}
		case strings.HasPrefix(query, "ALTER TABLE"):
			alters.Add(1)
			if !strings.Contains(query, "MODIFY TTL") {
				t.Error(query)
			}
		case strings.HasPrefix(query, "INSERT INTO"):
			writes.Add(1)
			decoder := json.NewDecoder(r.Body)
			count := 0
			for {
				var row map[string]any
				if err := decoder.Decode(&row); err != nil {
					if err != io.EOF {
						t.Error(err)
					}
					break
				}
				count++
				if row["sandbox_id"] != "sid" || row["stable_id"] != "stable-sid" || row["source"] != "envd" {
					t.Error("untrusted write identity", row)
				}
			}
			if count != 7 {
				t.Error("write batch", count)
			}
		case strings.HasPrefix(query, "SELECT"):
			if r.URL.Query().Get("param_sid") != sandbox || strings.Contains(query, sandbox) || !strings.Contains(query, "sandbox_id = {sid:String}") {
				t.Error("non-parameterized sandbox lookup")
			}
			if r.URL.Query().Get("readonly") != "1" || r.URL.Query().Get("cancel_http_readonly_queries_on_client_close") != "1" {
				t.Error("query cancellation policy")
			}
			if strings.Contains(query, "count()") {
				_ = json.NewEncoder(w).Encode(map[string]any{"count": 7, "first": stamp.UnixMilli(), "last": stamp.UnixMilli()})
			} else {
				if !strings.Contains(query, "max(value)") || strings.Contains(query, "avg(") || r.URL.Query().Get("param_step") != "5" {
					t.Error("not field MAX", query)
				}
				for field, metric := range resourceMetrics {
					_ = json.NewEncoder(w).Encode(map[string]any{"stamp": stamp.UnixMilli() / 5000 * 5000, "metric": metric.name, "value": field + 1})
				}
			}
		default:
			t.Error("unexpected SQL", query)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	cfg := localConfig(t)
	cfg.Type = "clickhouse"
	cfg.ClickHouse = config.TelemetryClickHouse{Endpoint: server.URL, Database: "default", Table: "sandbox_metrics", Headers: map[string]string{"X-ClickHouse-Key": "private"}}
	backend, err := OpenClickHouse(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Shutdown(context.Background())
	if err := backend.Write(context.Background(), resourceSamples(t, "sid", stamp)); err != nil {
		t.Fatal(err)
	}
	start, end, found, err := backend.Bounds(context.Background(), sandbox)
	if err != nil || !found || start.UnixMilli() != stamp.UnixMilli() || end.UnixMilli() != stamp.UnixMilli() {
		t.Fatal(start, end, found, err)
	}
	points, err := backend.Query(context.Background(), extension.Query{SandboxID: sandbox, Start: start, End: end, Step: 5 * time.Second})
	if err != nil || len(points) != 7 {
		t.Fatal(points, err)
	}
	if creates.Load() != 1 || alters.Load() != 1 || writes.Load() != 1 {
		t.Fatal("table/write lifecycle")
	}
}

func TestClickHouseRejectsErrorAfterHTTP200(t *testing.T) {
	for _, raw := range []string{"Code: 60. DB::Exception: unknown table", "{\"stamp\":" + strconv.FormatInt(time.Now().UnixMilli(), 10) + ",\"metric\":\"sandbox.cpu.count\",\"value\":2}\nCode: 241. DB::Exception: memory limit"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.Copy(w, bytes.NewBufferString(raw)) }))
		backend := &ClickHouse{remoteClient: newRemoteClient(nil), endpoint: server.URL, table: "`default`.`sandbox_metrics`", retention: time.Hour}
		_, err := backend.Query(context.Background(), extension.Query{SandboxID: "sid", Start: time.Now().Add(-time.Minute), End: time.Now(), Step: 5 * time.Second})
		if err == nil {
			t.Fatal("HTTP 200 error body accepted")
		}
		_ = backend.Shutdown(context.Background())
		server.Close()
	}
}
