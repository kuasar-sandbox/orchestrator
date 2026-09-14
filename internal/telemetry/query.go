package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	extension "github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
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

// QueryHandler serves only the lease-registered UDS. Conductor authorizes the
// exact sandbox path. Every handler gets a Reader permanently bound to that ID.
func QueryHandler(reader extension.Reader, factory extension.MetricsHandler) http.Handler {
	mux := http.NewServeMux()
	queries := make(chan struct{}, 8)
	mux.HandleFunc("GET /sandboxes/{id}/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if reader == nil || factory == nil {
			queryError(w, 503, "metrics query unavailable")
			return
		}
		id := r.PathValue("id")
		if err := validateSelection(extension.Selection{SandboxID: id}); err != nil {
			queryError(w, 400, "invalid sandbox scope")
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
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		handler := factory(extension.QueryScope{SandboxID: id, Reader: scopedReader{reader: reader, id: id}})
		if handler == nil {
			queryError(w, 503, "metrics handler unavailable")
			return
		}
		handler.ServeHTTP(w, r.WithContext(ctx))
	})
	return mux
}

type scopedReader struct {
	reader extension.Reader
	id     string
}

func (r scopedReader) selection(selection extension.Selection) (extension.Selection, error) {
	if selection.SandboxID != "" && selection.SandboxID != r.id {
		return selection, errors.New("query cannot replace the authorized SandboxID")
	}
	selection.SandboxID = r.id
	selection.Metrics = append([]string(nil), selection.Metrics...)
	selection.Attributes = maps.Clone(selection.Attributes)
	return selection, validateSelection(selection)
}
func (r scopedReader) Bounds(ctx context.Context, selection extension.Selection) (time.Time, time.Time, bool, error) {
	selection, err := r.selection(selection)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	return r.reader.Bounds(ctx, selection)
}
func (r scopedReader) Query(ctx context.Context, query extension.Query) ([]extension.Series, error) {
	selection, err := r.selection(query.Selection)
	if err != nil {
		return nil, err
	}
	query.Selection = selection
	if _, err := newSeriesBuckets(query); err != nil {
		return nil, err
	}
	series, err := r.reader.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(series) > 100000 {
		return nil, errors.New("metrics result exceeds series limit")
	}
	points := 0
	for _, series := range series {
		if !selectedSeries(selection, series.Metric, series.Attributes) {
			return nil, errors.New("query backend returned a different sandbox or selection")
		}
		points += len(series.Points)
		if points > maxQueryPoints {
			return nil, errors.New("metrics result exceeds point limit")
		}
	}
	return series, ctx.Err()
}

var e2bFields = [...]string{"cpuCount", "cpuUsedPct", "memTotal", "memUsed", "memCache", "diskTotal", "diskUsed"}

// E2BHandler is an independent compatibility adapter. Its seven mappings and
// default envd source do not constrain generic readers or custom HTTP handlers.
func E2BHandler(cfg config.TelemetryE2B) extension.MetricsHandler {
	metricNames := make([]string, len(e2bFields))
	for i, field := range e2bFields {
		metricNames[i] = cfg.Metrics[field]
	}
	return func(scope extension.QueryScope) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
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
			ctx := r.Context()
			selection := extension.Selection{SandboxID: scope.SandboxID, Metrics: metricNames, Attributes: map[string]string{sourceAttribute: cfg.Source}}
			if start == nil || end == nil {
				first, last, found, err := scope.Reader.Bounds(ctx, selection)
				if err != nil {
					queryError(w, 503, "metrics backend unavailable")
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
			query := extension.Query{Selection: selection, Start: *start, End: *end, Step: CalculateStep(*start, *end), Aggregation: extension.Max}
			series, err := scope.Reader.Query(ctx, query)
			if err != nil {
				queryError(w, 503, "metrics backend unavailable")
				return
			}
			points, err := e2bPoints(series, metricNames)
			if err != nil {
				queryError(w, 503, "metrics backend returned invalid observations")
				return
			}
			result, err := aggregate(points, query)
			if err != nil {
				queryError(w, 503, "metrics backend returned invalid observations")
				return
			}
			if ctx.Err() != nil {
				queryError(w, 503, "metrics query cancelled")
				return
			}
			_ = json.NewEncoder(w).Encode(result)
		})
	}
}

func e2bPoints(series []extension.Series, metricNames []string) ([]e2bPoint, error) {
	fields := make(map[string]e2bField, len(metricNames))
	for i, name := range metricNames {
		fields[name] = e2bField(i)
	}
	var points []e2bPoint
	for _, series := range series {
		field, ok := fields[series.Metric]
		if !ok {
			return nil, errors.New("unexpected E2B metric")
		}
		for _, point := range series.Points {
			if len(points) >= maxQueryPoints {
				return nil, errors.New("metrics result exceeds point limit")
			}
			points = append(points, e2bPoint{Timestamp: point.Timestamp, Field: field, Value: point.Value})
		}
	}
	return points, nil
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": message})
}

func aggregate(points []e2bPoint, query extension.Query) ([]SandboxMetric, error) {
	type bucket struct {
		values [e2bFieldCount]float64
		seen   uint8
	}
	buckets := make(map[int64]*bucket)
	step := query.Step.Milliseconds()
	if step <= 0 {
		return nil, errors.New("invalid query step")
	}
	for _, point := range points {
		if point.Field >= e2bFieldCount || math.IsNaN(point.Value) || math.IsInf(point.Value, 0) || point.Value < 0 {
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
		if b.seen != 1<<e2bFieldCount-1 {
			continue
		}
		for i, value := range b.values {
			if e2bField(i) != e2bCPUUsedPct && (value >= float64(math.MaxInt64) || math.Trunc(value) != value) {
				return nil, errors.New("integer observation out of range")
			}
		}
		if b.values[e2bCPUCount] > math.MaxInt32 {
			return nil, errors.New("cpu count out of range")
		}
		timestamp := time.UnixMilli(stamp).UTC()
		out = append(out, SandboxMetric{Timestamp: timestamp, TimestampUnix: timestamp.Unix(),
			CPUCount: int32(b.values[e2bCPUCount]), CPUUsedPct: b.values[e2bCPUUsedPct],
			MemTotal: int64(b.values[e2bMemTotal]), MemUsed: int64(b.values[e2bMemUsed]), MemCache: int64(b.values[e2bMemCache]),
			DiskTotal: int64(b.values[e2bDiskTotal]), DiskUsed: int64(b.values[e2bDiskUsed])})
	}
	return out, nil
}
