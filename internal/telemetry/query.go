package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	extension "github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

// SandboxMetric matches E2B's public schema, including the deprecated timestamp.
// All fields represent actual guest observations. Incomplete buckets are omitted
// rather than inventing zero values for missing fields.
type SandboxMetric struct {
	Timestamp     time.Time `json:"timestamp"`
	TimestampUnix int64     `json:"timestampUnix"`
	CPUCount      int32     `json:"cpuCount"`
	CPUUsedPct    float64   `json:"cpuUsedPct"`
	MemTotal      int64     `json:"memTotal"`
	MemUsed       int64     `json:"memUsed"`
	MemCache      int64     `json:"memCache"`
	DiskTotal     int64     `json:"diskTotal"`
	DiskUsed      int64     `json:"diskUsed"`
}

var maxQueryTime = time.Date(2299, 12, 31, 23, 59, 59, 999999999, time.UTC)

func ValidateRange(start, end time.Time) error {
	if start.Before(time.Unix(0, 0)) || end.Before(time.Unix(0, 0)) || start.After(maxQueryTime) || end.After(maxQueryTime) {
		return errors.New("metrics timestamps must be between 1970 and 2299")
	}
	if start.After(end) {
		return errors.New("start time cannot be after end time")
	}
	return nil
}

func CalculateStep(start, end time.Time) time.Duration {
	switch duration := end.Sub(start); {
	case duration < time.Hour:
		return 5 * time.Second
	case duration < 6*time.Hour:
		return 30 * time.Second
	case duration < 12*time.Hour:
		return time.Minute
	case duration < 24*time.Hour:
		return 2 * time.Minute
	case duration < 7*24*time.Hour:
		return 5 * time.Minute
	default:
		return 15 * time.Minute
	}
}

// QueryHandler serves only the private, registered query UDS. The conductor
// owns public authentication and sandbox existence/ownership checks.
func QueryHandler(reader extension.Reader) http.Handler {
	mux := http.NewServeMux()
	queries := make(chan struct{}, 8)
	mux.HandleFunc("GET /sandboxes/{id}/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if reader == nil {
			queryError(w, 503, "metrics storage unavailable")
			return
		}
		select {
		case queries <- struct{}{}:
			defer func() { <-queries }()
		default:
			w.Header().Set("Retry-After", "1")
			queryError(w, 503, "metrics query capacity exhausted")
			return
		}
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			queryError(w, 400, "invalid query parameters")
			return
		}
		start, err := queryBoundary(values, "start")
		if err != nil {
			queryError(w, 400, err.Error())
			return
		}
		end, err := queryBoundary(values, "end")
		if err != nil {
			queryError(w, 400, err.Error())
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		id := r.PathValue("id")
		if start == nil || end == nil {
			first, last, found, err := reader.Bounds(ctx, id)
			if err != nil {
				queryError(w, 503, "metrics storage unavailable")
				return
			}
			if !found {
				_, _ = w.Write([]byte("[]\n"))
				return
			}
			if start == nil {
				start = &first
			}
			if end == nil {
				end = &last
			}
		}
		if err := ValidateRange(*start, *end); err != nil {
			queryError(w, 400, err.Error())
			return
		}
		query := extension.Query{SandboxID: id, Start: *start, End: *end, Step: CalculateStep(*start, *end)}
		points, err := reader.Query(ctx, query)
		if err != nil {
			queryError(w, 503, "metrics storage unavailable")
			return
		}
		result, err := aggregate(points, query)
		if err != nil {
			queryError(w, 503, "metrics storage returned invalid observations")
			return
		}
		if err := ctx.Err(); err != nil {
			queryError(w, 503, "metrics query cancelled")
			return
		}
		_ = json.NewEncoder(w).Encode(result)
	})
	return mux
}

func queryBoundary(values url.Values, name string) (*time.Time, error) {
	raw, present := values[name]
	if !present {
		return nil, nil
	}
	if len(raw) != 1 || raw[0] == "" {
		return nil, errors.New("invalid " + name + " timestamp")
	}
	seconds, err := strconv.ParseInt(raw[0], 10, 64)
	if err != nil || seconds < 0 || seconds > maxQueryTime.Unix() {
		return nil, errors.New("invalid " + name + " timestamp")
	}
	value := time.Unix(seconds, 0).UTC()
	return &value, nil
}

func queryError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": message})
}

func aggregate(points []extension.Point, query extension.Query) ([]SandboxMetric, error) {
	type bucket struct {
		values [extension.FieldCount]float64
		seen   uint8
	}
	buckets := make(map[int64]*bucket)
	step := query.Step.Milliseconds()
	if step <= 0 {
		return nil, errors.New("invalid query step")
	}
	for _, point := range points {
		if point.Field >= extension.FieldCount || math.IsNaN(point.Value) || math.IsInf(point.Value, 0) || point.Value < 0 {
			return nil, errors.New("invalid metric point")
		}
		// Readers may already have epoch-aligned MAX buckets. A first bucket can
		// begin before Start; backends must filter raw samples before aggregation.
		stamp := point.Timestamp.UnixMilli() / step * step
		if stamp < query.Start.UnixMilli()/step*step || stamp > query.End.UnixMilli() {
			continue
		}
		b := buckets[stamp]
		if b == nil {
			if len(buckets) >= 100000 {
				return nil, errors.New("metrics result exceeds bucket limit")
			}
			b = &bucket{}
			buckets[stamp] = b
		}
		mask := uint8(1) << point.Field
		if b.seen&mask == 0 || point.Value > b.values[point.Field] {
			b.values[point.Field] = point.Value
		}
		b.seen |= mask
	}
	stamps := make([]int64, 0, len(buckets))
	for stamp := range buckets {
		stamps = append(stamps, stamp)
	}
	sort.Slice(stamps, func(i, j int) bool { return stamps[i] < stamps[j] })
	out := make([]SandboxMetric, 0, len(stamps))
	for _, stamp := range stamps {
		b := buckets[stamp]
		if b.seen != 1<<extension.FieldCount-1 {
			continue
		}
		for i, value := range b.values {
			if extension.Field(i) != extension.CPUUsedPct && (value >= float64(math.MaxInt64) || math.Trunc(value) != value) {
				return nil, errors.New("integer observation out of range")
			}
		}
		if b.values[extension.CPUCount] > math.MaxInt32 {
			return nil, errors.New("cpu count out of range")
		}
		timestamp := time.UnixMilli(stamp).UTC()
		out = append(out, SandboxMetric{Timestamp: timestamp, TimestampUnix: timestamp.Unix(),
			CPUCount: int32(b.values[extension.CPUCount]), CPUUsedPct: b.values[extension.CPUUsedPct],
			MemTotal: int64(b.values[extension.MemTotal]), MemUsed: int64(b.values[extension.MemUsed]), MemCache: int64(b.values[extension.MemCache]),
			DiskTotal: int64(b.values[extension.DiskTotal]), DiskUsed: int64(b.values[extension.DiskUsed])})
	}
	return out, nil
}
