package telemetry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"time"

	"github.com/golang/snappy"
	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// Prometheus is a readable primary using the standard Snappy/protobuf remote
// read/write HTTP protocols. Raw sample reads preserve exact sparse history;
// PromQL lookback, interpolation and range-vector boundary artifacts do not
// enter E2B compatibility. Backends must accept Prometheus 3 UTF-8 label names.
type Prometheus struct {
	*remoteClient
	endpoint  string
	retention time.Duration
}

func NewPrometheus(cfg config.TelemetryStorage) (*Prometheus, error) {
	retention, err := time.ParseDuration(cfg.Retention)
	if err != nil {
		return nil, err
	}
	return &Prometheus{remoteClient: newRemoteClient(cfg.Prometheus.Headers), endpoint: cfg.Prometheus.Endpoint, retention: retention}, nil
}

func (p *Prometheus) Write(ctx context.Context, samples []extension.Sample) error {
	if len(samples) > maxWriteSamples {
		return ErrInvalidMetrics
	}
	// Group by the complete canonical label set, without an unbounded cross-
	// request cache. Prometheus requires sorted labels and per-series samples.
	request := prompb.WriteRequest{}
	series := make(map[string]int)
	now := time.Now()
	for _, sample := range samples {
		if err := ctx.Err(); err != nil {
			return err
		}
		if sample.Timestamp.Before(now.Add(-p.retention)) {
			continue
		}
		if sample.Timestamp.After(now.Add(time.Minute)) || math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
			return ErrInvalidMetrics
		}
		builder := labels.NewScratchBuilder(len(sample.Labels) + 1)
		builder.Add(labels.MetricName, sample.Metric)
		for key, value := range sample.Labels {
			builder.Add(key, value)
		}
		builder.Sort()
		canonical := builder.Labels()
		key := canonical.String()
		index, exists := series[key]
		if !exists {
			index = len(request.Timeseries)
			series[key] = index
			entry := prompb.TimeSeries{}
			canonical.Range(func(label labels.Label) {
				entry.Labels = append(entry.Labels, prompb.Label{Name: label.Name, Value: label.Value})
			})
			request.Timeseries = append(request.Timeseries, entry)
		}
		request.Timeseries[index].Samples = append(request.Timeseries[index].Samples, prompb.Sample{Timestamp: sample.Timestamp.UnixMilli(), Value: sample.Value})
	}
	if len(request.Timeseries) == 0 {
		return nil
	}
	for i := range request.Timeseries {
		points := request.Timeseries[i].Samples
		sort.SliceStable(points, func(a, b int) bool { return points[a].Timestamp < points[b].Timestamp })
	}
	if request.Size() > maxRemoteBytes {
		return ErrInvalidMetrics
	}
	raw, err := request.Marshal()
	if err != nil {
		return err
	}
	endpoint, err := url.JoinPath(p.endpoint, "api/v1/write")
	if err != nil {
		return err
	}
	_, err = p.request(ctx, endpoint, snappy.Encode(nil, raw), map[string]string{"Content-Type": "application/x-protobuf", "Content-Encoding": "snappy", "X-Prometheus-Remote-Write-Version": "0.1.0"})
	return err
}

const remoteReadWindow = 6 * time.Hour

func (p *Prometheus) read(ctx context.Context, id string, start, end int64, visit func(extension.Field, int64, float64) error) error {
	request := prompb.ReadRequest{AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES}}
	query := &prompb.Query{StartTimestampMs: start, EndTimestampMs: end}
	for _, matcher := range resourceMatchers(id) {
		kind := prompb.LabelMatcher_EQ
		if matcher.Type == labels.MatchRegexp {
			kind = prompb.LabelMatcher_RE
		}
		query.Matchers = append(query.Matchers, &prompb.LabelMatcher{Type: kind, Name: matcher.Name, Value: matcher.Value})
	}
	request.Queries = []*prompb.Query{query}
	raw, err := request.Marshal()
	if err != nil {
		return err
	}
	endpoint, err := url.JoinPath(p.endpoint, "api/v1/read")
	if err != nil {
		return err
	}
	raw, err = p.request(ctx, endpoint, snappy.Encode(nil, raw), map[string]string{"Content-Type": "application/x-protobuf", "Content-Encoding": "snappy", "X-Prometheus-Remote-Read-Version": "0.1.0"})
	if err != nil {
		return err
	}
	length, err := snappy.DecodedLen(raw)
	if err != nil || length > maxRemoteBytes {
		return errors.New("invalid or oversized Prometheus remote read response")
	}
	raw, err = snappy.Decode(nil, raw)
	if err != nil {
		return err
	}
	var response prompb.ReadResponse
	if err := response.Unmarshal(raw); err != nil {
		return err
	}
	if len(response.Results) != 1 || response.Results[0] == nil {
		return errors.New("unexpected Prometheus query result count")
	}
	for _, series := range response.Results[0].Timeseries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if series == nil {
			return ErrInvalidMetrics
		}
		attrs := make(map[string]string, len(series.Labels))
		for _, label := range series.Labels {
			if _, exists := attrs[label.Name]; exists {
				return ErrInvalidMetrics
			}
			attrs[label.Name] = label.Value
		}
		field, ok := metricField(attrs[labels.MetricName])
		if !ok || attrs[SandboxIDAttribute] != id || attrs[sourceAttribute] != "envd" || attrs["otel.kind"] != "Gauge" {
			return fmt.Errorf("Prometheus returned a series outside the requested sandbox resource scope")
		}
		for _, sample := range series.Samples {
			if sample.Timestamp < start || sample.Timestamp > end {
				continue
			}
			if err := visit(field, sample.Timestamp, sample.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Prometheus) Bounds(ctx context.Context, id string) (start, end time.Time, found bool, err error) {
	if id == "" {
		return
	}
	last := time.Now().Add(time.Minute).UnixMilli()
	first := time.Now().Add(-p.retention).UnixMilli()
	// Two bounded directional scans usually need one request apiece. Sparse
	// histories still obey the caller deadline and use constant response memory.
	for begin := first; begin <= last; {
		finish := min(last, begin+remoteReadWindow.Milliseconds()-1)
		err = p.read(ctx, id, begin, finish, func(_ extension.Field, stamp int64, _ float64) error {
			value := time.UnixMilli(stamp).UTC()
			if !found || value.Before(start) {
				start = value
			}
			found = true
			return nil
		})
		if err != nil || found {
			break
		}
		begin = finish + 1
	}
	if err != nil || !found {
		return
	}
	var haveEnd bool
	for finish := last; finish >= start.UnixMilli(); {
		begin := max(start.UnixMilli(), finish-remoteReadWindow.Milliseconds()+1)
		err = p.read(ctx, id, begin, finish, func(_ extension.Field, stamp int64, _ float64) error {
			value := time.UnixMilli(stamp).UTC()
			if !haveEnd || value.After(end) {
				end = value
			}
			haveEnd = true
			return nil
		})
		if err != nil || haveEnd {
			break
		}
		finish = begin - 1
	}
	if err == nil && !haveEnd {
		found = false
	} // Retention may advance between requests.
	return
}

func (p *Prometheus) Query(ctx context.Context, query extension.Query) ([]extension.Point, error) {
	if err := ValidateRange(query.Start, query.End); err != nil {
		return nil, err
	}
	buckets, err := newPointBuckets(query.Step)
	if err != nil {
		return nil, err
	}
	start, end := max(query.Start.UnixMilli(), time.Now().Add(-p.retention).UnixMilli()), min(query.End.UnixMilli(), time.Now().Add(time.Minute).UnixMilli())
	for begin := start; begin <= end; {
		finish := min(end, begin+remoteReadWindow.Milliseconds()-1)
		if err := p.read(ctx, query.SandboxID, begin, finish, buckets.add); err != nil {
			return nil, err
		}
		begin = finish + 1
	}
	return buckets.points(), nil
}

var _ extension.Storage = (*Prometheus)(nil)
