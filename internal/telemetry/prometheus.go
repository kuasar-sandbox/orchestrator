package telemetry

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"mime"
	"net/url"
	"sort"
	"time"

	"github.com/golang/snappy"
	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
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

func (p *Prometheus) read(ctx context.Context, id string, start, end int64, visit func(extension.Field, int64, float64) error) error {
	request := prompb.ReadRequest{AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS, prompb.ReadRequest_SAMPLES}}
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
	response, err := p.open(ctx, endpoint, snappy.Encode(nil, raw), map[string]string{"Content-Type": "application/x-protobuf", "Content-Encoding": "snappy", "X-Prometheus-Remote-Read-Version": "0.1.0"})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	media, params, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		return fmt.Errorf("invalid Prometheus response content type: %w", err)
	}
	if media == "application/x-streamed-protobuf" && params["proto"] == "prometheus.ChunkedReadResponse" && response.Header.Get("Content-Encoding") == "" {
		return readPrometheusChunks(ctx, response.Body, id, start, end, visit)
	}
	if media != "application/x-protobuf" || response.Header.Get("Content-Encoding") != "snappy" {
		return errors.New("unsupported Prometheus response format")
	}
	raw, err = readRemoteBody(response.Body)
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
	var samples prompb.ReadResponse
	if err := samples.Unmarshal(raw); err != nil {
		return err
	}
	if len(samples.Results) != 1 || samples.Results[0] == nil {
		return errors.New("unexpected Prometheus query result count")
	}
	for _, series := range samples.Results[0].Timeseries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if series == nil {
			return ErrInvalidMetrics
		}
		field, err := remoteResourceField(series.Labels, id)
		if err != nil {
			return err
		}
		for _, sample := range series.Samples {
			if err := ctx.Err(); err != nil {
				return err
			}
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

// The standard remote-read stream is uvarint length, big-endian CRC32C, then
// ChunkedReadResponse protobuf. Decode one bounded frame at a time; importing
// storage/remote just for its framing helper would also link the scrape manager.
// https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto
func readPrometheusChunks(ctx context.Context, body io.Reader, id string, start, end int64, visit func(extension.Field, int64, float64) error) error {
	reader := bufio.NewReader(body)
	checksumTable := crc32.MakeTable(crc32.Castagnoli)
	var buffer []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		size, err := binary.ReadUvarint(reader)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if size == 0 || size > maxRemoteBytes {
			return errors.New("invalid or oversized Prometheus stream frame")
		}
		var checksum uint32
		if err := binary.Read(reader, binary.BigEndian, &checksum); err != nil {
			return err
		}
		if uint64(cap(buffer)) < size {
			buffer = make([]byte, size)
		}
		buffer = buffer[:size]
		if _, err := io.ReadFull(reader, buffer); err != nil {
			return err
		}
		if crc32.Checksum(buffer, checksumTable) != checksum {
			return errors.New("invalid Prometheus stream checksum")
		}
		var frame prompb.ChunkedReadResponse
		if err := frame.Unmarshal(buffer); err != nil {
			return err
		}
		if frame.QueryIndex != 0 {
			return errors.New("unexpected Prometheus stream query index")
		}
		for _, series := range frame.ChunkedSeries {
			if series == nil {
				return ErrInvalidMetrics
			}
			field, err := remoteResourceField(series.Labels, id)
			if err != nil {
				return err
			}
			for _, encoded := range series.Chunks {
				if encoded.Type != prompb.Chunk_XOR || len(encoded.Data) < 2 {
					return errors.New("invalid Prometheus resource chunk encoding")
				}
				chunk, err := chunkenc.FromData(chunkenc.EncXOR, encoded.Data)
				if err != nil {
					return err
				}
				iterator := chunk.Iterator(nil)
				for kind := iterator.Next(); kind != chunkenc.ValNone; kind = iterator.Next() {
					if err := ctx.Err(); err != nil {
						return err
					}
					if kind != chunkenc.ValFloat {
						return ErrInvalidMetrics
					}
					stamp, value := iterator.At()
					// Edge chunks can include observations outside requested bounds.
					if stamp >= start && stamp <= end {
						if err := visit(field, stamp, value); err != nil {
							return err
						}
					}
				}
				if err := iterator.Err(); err != nil {
					return err
				}
			}
		}
	}
}

func remoteResourceField(seriesLabels []prompb.Label, id string) (extension.Field, error) {
	attrs := make(map[string]string, len(seriesLabels))
	for _, label := range seriesLabels {
		if _, exists := attrs[label.Name]; exists {
			return 0, ErrInvalidMetrics
		}
		attrs[label.Name] = label.Value
	}
	field, ok := metricField(attrs[labels.MetricName])
	if !ok || attrs[SandboxIDAttribute] != id || attrs[sourceAttribute] != "envd" || attrs["otel.kind"] != "Gauge" {
		return 0, errors.New("Prometheus returned a series outside the requested sandbox resource scope")
	}
	return field, nil
}

func (p *Prometheus) Bounds(ctx context.Context, id string) (start, end time.Time, found bool, err error) {
	if id == "" {
		return
	}
	now := time.Now()
	// One streaming request, including for empty or sparse long-retention
	// histories. Neither request count nor response memory grows with retention.
	err = p.read(ctx, id, now.Add(-p.retention).UnixMilli(), now.Add(time.Minute).UnixMilli(), func(_ extension.Field, stamp int64, value float64) error {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil // Prometheus stale markers are not observations.
		}
		observed := time.UnixMilli(stamp).UTC()
		if !found || observed.Before(start) {
			start = observed
		}
		if !found || observed.After(end) {
			end = observed
		}
		found = true
		return nil
	})
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
	if start <= end {
		if err := p.read(ctx, query.SandboxID, start, end, buckets.add); err != nil {
			return nil, err
		}
	}
	return buckets.points(), nil
}

var _ extension.Storage = (*Prometheus)(nil)
