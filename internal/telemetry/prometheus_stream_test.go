package telemetry

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestPrometheusHistogramSamplesFallback(t *testing.T) {
	stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	integer := prompb.FromIntHistogram(stamp.UnixMilli(), &histogram.Histogram{
		Schema: 0, Count: 3, Sum: 3, ZeroThreshold: 0.1, ZeroCount: 1,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{2},
	})
	floating := prompb.FromFloatHistogram(stamp.UnixMilli(), &histogram.FloatHistogram{
		Schema: 0, Count: 3.5, Sum: 3.5, ZeroThreshold: 0.1, ZeroCount: 1.5,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []float64{2},
	})
	for _, tc := range []struct {
		name   string
		sample prompb.Histogram
		want   float64
		wrong  bool
		bad    bool
	}{
		{name: "integer", sample: integer, want: 3},
		{name: "float", sample: floating, want: 3.5},
		{name: "wrong sandbox", sample: integer, wrong: true},
		{name: "invalid histogram", sample: floating, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "sid"
			if tc.wrong {
				id = "other"
			}
			if tc.bad {
				tc.sample.PositiveCounts = nil // Present span without its bucket.
			}
			before := tc.sample
			before.Timestamp--
			response := prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: []*prompb.TimeSeries{{
				Labels:     []prompb.Label{{Name: "__name__", Value: "task_distribution"}, {Name: "sandbox_id", Value: id}},
				Histograms: []prompb.Histogram{before, tc.sample},
			}}}}}
			raw, err := response.Marshal()
			requireNoQueryError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err == nil {
					body, err = snappy.Decode(nil, body)
				}
				var request prompb.ReadRequest
				if err != nil || request.Unmarshal(body) != nil || len(request.Queries) != 1 {
					t.Error("invalid remote read request")
					w.WriteHeader(400)
					return
				}
				for _, matcher := range request.Queries[0].Matchers {
					if matcher.Name == "otel.part" || matcher.Name == "otel.bound" {
						t.Error("derived histogram attribute sent as physical label")
					}
				}
				w.Header().Set("Content-Type", "application/x-protobuf")
				w.Header().Set("Content-Encoding", "snappy")
				_, _ = w.Write(snappy.Encode(nil, raw))
			}))
			defer server.Close()
			backend, err := NewPrometheus(prometheusQueryConfig(server.URL))
			requireNoQueryError(t, err)
			defer backend.Shutdown(context.Background())
			query := extension.Query{Selection: extension.Selection{SandboxID: "sid", Metrics: []string{"task_distribution"}, Attributes: map[string]string{"otel.part": "count"}}, Start: stamp, End: stamp, Aggregation: extension.Raw}
			series, err := backend.Query(t.Context(), query)
			if tc.bad || tc.wrong {
				if err == nil {
					t.Fatal("invalid or out-of-scope histogram accepted", series)
				}
				return
			}
			if err != nil || len(series) != 1 || len(series[0].Points) != 1 || series[0].Points[0].Value != tc.want || !series[0].Points[0].Timestamp.Equal(stamp) {
				t.Fatal("fallback histogram count, derived selection or inclusive edge", series, err)
			}
		})
	}
}

func prometheusFrame(t testing.TB, frame *prompb.ChunkedReadResponse) []byte {
	t.Helper()
	raw, err := frame.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	result := binary.AppendUvarint(nil, uint64(len(raw)))
	result = binary.BigEndian.AppendUint32(result, crc32.Checksum(raw, crc32.MakeTable(crc32.Castagnoli)))
	return append(result, raw...)
}

func prometheusChunk(t testing.TB, id string, stamp int64) *prompb.ChunkedReadResponse {
	t.Helper()
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		t.Fatal(err)
	}
	appender.Append(stamp, 2)
	appender.Append(stamp+1000, 4)
	return &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{{
		Labels: []prompb.Label{{Name: "__name__", Value: "sandbox.cpu.count"}, {Name: SandboxIDAttribute, Value: id}, {Name: sourceAttribute, Value: "envd"}, {Name: "otel.kind", Value: "Gauge"}},
		Chunks: []prompb.Chunk{{Type: prompb.Chunk_XOR, MinTimeMs: stamp, MaxTimeMs: stamp + 1000, Data: chunk.Bytes()}},
	}}}
}

func TestPrometheusStreamSparseHistoryAndInclusiveEdges(t *testing.T) {
	stamp := time.Now().Add(-time.Minute).Truncate(5 * time.Second).Add(123 * time.Millisecond).UnixMilli()
	first := stamp - (360 * 24 * time.Hour).Milliseconds()
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		raw, _ := io.ReadAll(r.Body)
		raw, err := snappy.Decode(nil, raw)
		if err != nil {
			t.Error(err)
			return
		}
		var request prompb.ReadRequest
		if err := request.Unmarshal(raw); err != nil || len(request.Queries) != 1 || !reflect.DeepEqual(request.AcceptedResponseTypes, []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS, prompb.ReadRequest_SAMPLES}) {
			t.Error("stream negotiation", request, err)
			return
		}
		w.Header().Set("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")
		// Return all chunks even for narrow requests, as the real remote-read
		// protocol includes complete edge chunks. The client must filter samples.
		for _, observed := range []int64{first, stamp} {
			frame := prometheusChunk(t, "sid", observed)
			_, _ = w.Write(prometheusFrame(t, frame))
		}
	}))
	defer server.Close()
	backend, err := NewPrometheus(config.TelemetryQuery{Lookback: "8760h", Prometheus: config.TelemetryPrometheus{Endpoint: server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Shutdown(context.Background())
	start, end, found, err := backend.Bounds(context.Background(), envdSelection("sid"))
	if err != nil || !found || start.UnixMilli() != first || end.UnixMilli() != stamp+1000 || reads.Load() != 1 {
		t.Fatal("sparse bounds", start, end, found, err, reads.Load())
	}
	points, err := backend.Query(context.Background(), extension.Query{Selection: envdSelection("sid"), Aggregation: extension.Max, Start: time.UnixMilli(stamp + 1000), End: time.UnixMilli(stamp + 1000), Step: 5 * time.Second})
	if err != nil || len(points) != 1 || points[0].Metric != resourceMetrics[e2bCPUCount].name || len(points[0].Points) != 1 || points[0].Points[0].Value != 4 || reads.Load() != 2 {
		t.Fatal("inclusive edge query", points, err, reads.Load())
	}
}

func TestPrometheusStreamRejectsInvalidFrames(t *testing.T) {
	valid := prometheusFrame(t, prometheusChunk(t, "sid", 1000))
	badChecksum := bytes.Clone(valid)
	badChecksum[len(badChecksum)-1] ^= 1
	frames := map[string][]byte{
		"checksum": badChecksum, "truncated length": {128}, "truncated checksum": valid[:3],
		"truncated message": valid[:len(valid)-1], "oversized": binary.AppendUvarint(nil, maxRemoteBytes+1), "zero length": {0},
	}
	for name, mutate := range map[string]func(*prompb.ChunkedReadResponse){
		"query index":    func(f *prompb.ChunkedReadResponse) { f.QueryIndex = 1 },
		"wrong sandbox":  func(f *prompb.ChunkedReadResponse) { f.ChunkedSeries[0].Labels[1].Value = "other" },
		"wrong source":   func(f *prompb.ChunkedReadResponse) { f.ChunkedSeries[0].Labels[2].Value = "otlp" },
		"unknown metric": func(f *prompb.ChunkedReadResponse) { f.ChunkedSeries[0].Labels[0].Value = "application.metric" },
		"duplicate label": func(f *prompb.ChunkedReadResponse) {
			f.ChunkedSeries[0].Labels = append(f.ChunkedSeries[0].Labels, f.ChunkedSeries[0].Labels[0])
		},
		"wrong encoding": func(f *prompb.ChunkedReadResponse) { f.ChunkedSeries[0].Chunks[0].Type = prompb.Chunk_HISTOGRAM },
		"short chunk":    func(f *prompb.ChunkedReadResponse) { f.ChunkedSeries[0].Chunks[0].Data = []byte{1} },
		"corrupt chunk":  func(f *prompb.ChunkedReadResponse) { f.ChunkedSeries[0].Chunks[0].Data = []byte{0, 1} },
	} {
		frame := prometheusChunk(t, "sid", 1000)
		mutate(frame)
		frames[name] = prometheusFrame(t, frame)
	}
	for name, raw := range frames {
		t.Run(name, func(t *testing.T) {
			if err := readPrometheusChunks(context.Background(), bytes.NewReader(raw), 0, 10000, func(name string, attrs map[string]string) (func(int64, float64) error, error) {
				if !selectedSeries(envdSelection("sid"), name, attrs) {
					return nil, ErrInvalidMetrics
				}
				return func(int64, float64) error { return nil }, nil
			}); err == nil {
				t.Fatal("invalid stream accepted")
			}
		})
	}
	if err := readPrometheusChunks(context.Background(), bytes.NewReader(nil), 0, 10000, nil); err != nil {
		t.Fatal("empty stream", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	err := readPrometheusChunks(ctx, bytes.NewReader(valid), 0, 10000, func(string, map[string]string) (func(int64, float64) error, error) {
		return func(int64, float64) error { cancel(); return nil }, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("sample iteration ignored cancellation", err)
	}
}

func TestPrometheusStreamBodyCancellation(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")
		_, _ = w.Write([]byte{128}) // Incomplete frame length after successful headers.
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer server.Close()
	backend, err := NewPrometheus(config.TelemetryQuery{Lookback: "8760h", Prometheus: config.TelemetryPrometheus{Endpoint: server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Shutdown(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, _, err := backend.Bounds(ctx, envdSelection("sid")); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stream did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream read ignored cancellation")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stream connection not cancelled")
	}
}
