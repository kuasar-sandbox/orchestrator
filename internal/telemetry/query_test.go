package telemetry

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

type fakeReader struct {
	points     []extension.Point
	found      bool
	start, end time.Time
	boundsIDs  []string
	queries    []extension.Query
	err        error
}

type blockingReader struct {
	extension.Reader
	entered chan struct{}
}

func (r blockingReader) Bounds(ctx context.Context, _ string) (time.Time, time.Time, bool, error) {
	r.entered <- struct{}{}
	<-ctx.Done()
	return time.Time{}, time.Time{}, false, ctx.Err()
}
func TestQueryCapacityAndBackendCancellation(t *testing.T) {
	reader := blockingReader{entered: make(chan struct{}, 8)}
	handler := QueryHandler(reader)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var requests sync.WaitGroup
	for range 8 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/sandboxes/sid/metrics", nil).WithContext(ctx))
		}()
	}
	for range 8 {
		select {
		case <-reader.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("query did not reach reader")
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/sid/metrics", nil))
	if response.Code != 503 || response.Header().Get("Retry-After") != "1" {
		t.Fatal("query concurrency not bounded")
	}
	cancel()
	done := make(chan struct{})
	go func() { requests.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("query cancellation did not reach reader")
	}
}

func (f *fakeReader) Bounds(_ context.Context, id string) (time.Time, time.Time, bool, error) {
	f.boundsIDs = append(f.boundsIDs, id)
	return f.start, f.end, f.found, f.err
}
func (f *fakeReader) Query(_ context.Context, q extension.Query) ([]extension.Point, error) {
	f.queries = append(f.queries, q)
	return f.points, f.err
}

func TestCalculateStepBoundaries(t *testing.T) {
	for _, tc := range []struct{ duration, step time.Duration }{
		{0, 5 * time.Second}, {time.Hour - time.Nanosecond, 5 * time.Second}, {time.Hour, 30 * time.Second},
		{6*time.Hour - time.Nanosecond, 30 * time.Second}, {6 * time.Hour, time.Minute},
		{12*time.Hour - time.Nanosecond, time.Minute}, {12 * time.Hour, 2 * time.Minute},
		{24*time.Hour - time.Nanosecond, 2 * time.Minute}, {24 * time.Hour, 5 * time.Minute},
		{7*24*time.Hour - time.Nanosecond, 5 * time.Minute}, {7 * 24 * time.Hour, 15 * time.Minute}, {365 * 24 * time.Hour, 15 * time.Minute},
	} {
		if got := CalculateStep(time.Unix(0, 0), time.Unix(0, 0).Add(tc.duration)); got != tc.step {
			t.Errorf("%s: %s != %s", tc.duration, got, tc.step)
		}
	}
}

func TestQueryBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		status      int
		start, end  int64
		bounds      bool
	}{
		{"none", "", 200, 100, 1000, true}, {"start", "?start=200", 200, 200, 1000, true}, {"end", "?end=900", 200, 100, 900, true},
		{"both", "?start=20&end=2000", 200, 20, 2000, false}, {"equal", "?start=0&end=0", 200, 0, 0, false},
		// E2B resolves omitted boundaries before ValidateRange. A supplied bound
		// outside existing history can therefore produce a reversed range.
		{"start after history", "?start=1001", 400, 0, 0, true}, {"end before history", "?end=99", 400, 0, 0, true},
		{"start at last", "?start=1000", 200, 1000, 1000, true}, {"end at first", "?end=100", 200, 100, 100, true},
		{"explicit before history", "?start=0&end=99", 200, 0, 99, false}, {"explicit after history", "?start=1001&end=2000", 200, 1001, 2000, false},
		{"reversed", "?start=2&end=1", 400, 0, 0, false}, {"negative", "?start=-1", 400, 0, 0, false},
		{"duplicate", "?start=1&start=2", 400, 0, 0, false}, {"empty", "?end=", 400, 0, 0, false},
		{"nan", "?start=NaN", 400, 0, 0, false}, {"fractional", "?start=1.1", 400, 0, 0, false},
		{"overflow", "?end=9999999999999999999", 400, 0, 0, false}, {"beyond2299", "?end=10413792000", 400, 0, 0, false},
		{"querysyntax", "?start=1;end=2", 400, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeReader{found: true, start: time.Unix(100, 0), end: time.Unix(1000, 0)}
			response := httptest.NewRecorder()
			QueryHandler(reader).ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/exact-sandbox/metrics"+tc.query, nil))
			if response.Code != tc.status {
				t.Fatalf("status %d: %s", response.Code, response.Body.String())
			}
			if (len(reader.boundsIDs) != 0) != tc.bounds {
				t.Fatalf("bounds: %v", reader.boundsIDs)
			}
			if tc.status != 200 {
				if len(reader.queries) != 0 {
					t.Fatal("invalid range queried reader")
				}
				return
			}
			if response.Body.String() != "[]\n" {
				t.Fatal(response.Body.String())
			}
			if len(reader.queries) != 1 || reader.queries[0].SandboxID != "exact-sandbox" || reader.queries[0].Start.Unix() != tc.start || reader.queries[0].End.Unix() != tc.end {
				t.Fatalf("queries: %+v", reader.queries)
			}
		})
	}
}

func TestIndependentMAXAndExactE2BSchema(t *testing.T) {
	first := [extension.FieldCount]float64{2, 90.5, 1000, 400, 300, 8000, 2000}
	second := [extension.FieldCount]float64{4, 5.5, 900, 800, 100, 7000, 4000}
	reader := &fakeReader{found: true, start: time.Unix(100, 0), end: time.Unix(104, 0)}
	for field := range extension.FieldCount {
		reader.points = append(reader.points, extension.Point{Timestamp: time.Unix(101, 0), Field: field, Value: first[field]}, extension.Point{Timestamp: time.Unix(104, 0), Field: field, Value: second[field]})
	}
	response := httptest.NewRecorder()
	QueryHandler(reader).ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/sid/metrics", nil))
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	var got []map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"timestamp": "1970-01-01T00:01:40Z", "timestampUnix": float64(100), "cpuCount": float64(4), "cpuUsedPct": 90.5,
		"memTotal": float64(1000), "memUsed": float64(800), "memCache": float64(300), "diskTotal": float64(8000), "diskUsed": float64(4000)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("schema/MAX: got %+v want %+v", got, want)
	}
}

func TestQueryEmptyAndSandboxIDOnly(t *testing.T) {
	for _, query := range []string{"", "?start=10", "?end=20", "?start=10&end=20"} {
		reader := &fakeReader{}
		response := httptest.NewRecorder()
		QueryHandler(reader).ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/not-stable-id/metrics"+query, nil))
		if response.Code != 200 || response.Body.String() != "[]\n" {
			t.Fatal(response.Code, response.Body.String())
		}
		if len(reader.boundsIDs) > 1 {
			t.Fatal("fallback lookup")
		}
		for _, id := range reader.boundsIDs {
			if id != "not-stable-id" {
				t.Fatal("rewrote sandbox identity")
			}
		}
	}
	response := httptest.NewRecorder()
	QueryHandler(nil).ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/sid/metrics", nil))
	if response.Code != 503 {
		t.Fatal(response.Code)
	}
	result, err := aggregate([]extension.Point{{Timestamp: time.Unix(10, 0), Field: extension.MemCache, Value: 42}}, extension.Query{Start: time.Unix(0, 0), End: time.Unix(20, 0), Step: 5 * time.Second})
	if err != nil || len(result) != 0 {
		t.Fatalf("incomplete bucket must not fill zeros: %v %v", result, err)
	}
}
