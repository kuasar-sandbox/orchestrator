package telemetry

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"math"
	"mime"
	"net/url"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Prometheus reads the standard Snappy/protobuf remote-read HTTP protocol. Raw sample reads preserve exact sparse history;
// PromQL lookback, interpolation and range-vector boundary artifacts do not
// enter E2B compatibility. Label mapping is independent of the chosen Collector exporter.
type Prometheus struct {
	*remoteClient
	endpoint   string
	retention  time.Duration
	labelNames map[string]string
}

func NewPrometheus(cfg config.TelemetryQuery) (*Prometheus, error) {
	retention, err := time.ParseDuration(cfg.Lookback)
	if err != nil {
		return nil, err
	}
	return &Prometheus{remoteClient: newRemoteClient(cfg.Prometheus.Headers), endpoint: cfg.Prometheus.Endpoint, retention: retention, labelNames: maps.Clone(cfg.Prometheus.Labels)}, nil
}

func (p *Prometheus) read(ctx context.Context, selection extension.Selection, start, end int64, visitor seriesVisitor) error {
	if err := validateSelection(selection); err != nil {
		return err
	}
	// Native histograms become scalar parts at the read boundary. These two
	// derived attributes are filtered after decoding, not sent as physical labels.
	physicalSelection := selection
	physicalSelection.Attributes = maps.Clone(selection.Attributes)
	delete(physicalSelection.Attributes, "otel.part")
	delete(physicalSelection.Attributes, "otel.bound")
	visit := func(name string, raw map[string]string) (func(int64, float64) error, error) {
		attributes := maps.Clone(raw)
		for logical, physical := range p.labelNames {
			if physical == logical {
				continue
			}
			if value, ok := raw[physical]; ok {
				if _, collision := raw[logical]; collision {
					return nil, errors.New("ambiguous Prometheus label mapping")
				}
				delete(attributes, physical)
				attributes[logical] = value
			}
		}
		matched, err := matchPrometheusSeries(physicalSelection, name, attributes)
		if err != nil {
			return nil, err
		}
		if !matched || !selectedSeries(selection, name, attributes) {
			return func(int64, float64) error { return nil }, nil
		}
		return visitor(name, attributes)
	}
	request := prompb.ReadRequest{AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS, prompb.ReadRequest_SAMPLES}}
	query := &prompb.Query{StartTimestampMs: start, EndTimestampMs: end}
	for _, matcher := range selectionMatchers(physicalSelection) {
		kind := prompb.LabelMatcher_EQ
		if matcher.Type == labels.MatchRegexp {
			kind = prompb.LabelMatcher_RE
		}
		name := matcher.Name
		if mapped := p.labelNames[name]; mapped != "" {
			name = mapped
		}
		query.Matchers = append(query.Matchers, &prompb.LabelMatcher{Type: kind, Name: name, Value: matcher.Value})
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
		return readPrometheusChunks(ctx, response.Body, start, end, visit)
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
		appendPoint, err := visitPrometheusSeries(series.Labels, visit)
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
			if err := appendPoint(sample.Timestamp, sample.Value); err != nil {
				return err
			}
		}
		for _, sample := range series.Histograms {
			if err := ctx.Err(); err != nil {
				return err
			}
			if sample.Timestamp >= start && sample.Timestamp <= end {
				if err := visitPrometheusHistogram(series.Labels, sample.Timestamp, sample.ToFloatHistogram(), visit); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// The standard remote-read stream is uvarint length, big-endian CRC32C, then
// ChunkedReadResponse protobuf. Decode one bounded frame at a time; importing
// storage/remote just for its framing helper would also link the scrape manager.
// https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto
func readPrometheusChunks(ctx context.Context, body io.Reader, start, end int64, visit seriesVisitor) error {
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
			appendPoint, err := visitPrometheusSeries(series.Labels, visit)
			if err != nil {
				return err
			}
			for _, encoded := range series.Chunks {
				var encoding chunkenc.Encoding
				switch encoded.Type {
				case prompb.Chunk_XOR:
					encoding = chunkenc.EncXOR
				case prompb.Chunk_HISTOGRAM:
					encoding = chunkenc.EncHistogram
				case prompb.Chunk_FLOAT_HISTOGRAM:
					encoding = chunkenc.EncFloatHistogram
				default:
					return errors.New("invalid Prometheus chunk encoding")
				}
				if len(encoded.Data) < 2 {
					return errors.New("invalid Prometheus chunk data")
				}
				chunk, err := chunkenc.FromData(encoding, encoded.Data)
				if err != nil {
					return err
				}
				iterator := chunk.Iterator(nil)
				for kind := iterator.Next(); kind != chunkenc.ValNone; kind = iterator.Next() {
					if err := ctx.Err(); err != nil {
						return err
					}
					if kind == chunkenc.ValHistogram || kind == chunkenc.ValFloatHistogram {
						stamp, histogram := iterator.AtFloatHistogram(nil)
						if stamp >= start && stamp <= end {
							if err := visitPrometheusHistogram(series.Labels, stamp, histogram, visit); err != nil {
								return err
							}
						}
						continue
					}
					if kind != chunkenc.ValFloat {
						return ErrInvalidMetrics
					}
					stamp, value := iterator.At()
					// Edge chunks can include observations outside requested bounds.
					if stamp >= start && stamp <= end {
						if err := appendPoint(stamp, value); err != nil {
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

// Preserve native bucket intervals rather than inventing inclusive cumulative
// bounds for negative/zero buckets. Count/sum and absolute bucket counts are
// scalar observations at the original time; this does not compute rates.
func visitPrometheusHistogram(seriesLabels []prompb.Label, stamp int64, h *histogram.FloatHistogram, visit seriesVisitor) error {
	if h == nil {
		return ErrInvalidMetrics
	}
	if value.IsStaleNaN(h.Sum) {
		return nil
	}
	if err := h.Validate(); err != nil {
		return fmt.Errorf("invalid Prometheus histogram: %w", err)
	}
	name, attributes, err := prometheusSeriesLabels(seriesLabels)
	if err != nil {
		return err
	}
	if _, exists := attributes["otel.part"]; exists {
		return errors.New("Prometheus histogram label conflicts with scalar part")
	}
	if _, exists := attributes["otel.bound"]; exists {
		return errors.New("Prometheus histogram label conflicts with scalar bound")
	}
	appendPart := func(part, bound string, number float64) error {
		attrs := maps.Clone(attributes)
		attrs["otel.part"] = part
		if bound != "" {
			attrs["otel.bound"] = bound
		}
		appendPoint, err := visit(name, attrs)
		if err != nil {
			return err
		}
		return appendPoint(stamp, number)
	}
	if err := appendPart("count", "", h.Count); err != nil {
		return err
	}
	if err := appendPart("sum", "", h.Sum); err != nil {
		return err
	}
	iterator := h.AllBucketIterator()
	for count := 0; iterator.Next(); count++ {
		if count >= maxQueryPoints {
			return errors.New("Prometheus histogram exceeds bucket limit")
		}
		bucket := iterator.At()
		left, right := "(", ")"
		if bucket.LowerInclusive {
			left = "["
		}
		if bucket.UpperInclusive {
			right = "]"
		}
		bound := left + strconv.FormatFloat(bucket.Lower, 'g', -1, 64) + "," + strconv.FormatFloat(bucket.Upper, 'g', -1, 64) + right
		if err := appendPart("native_bucket", bound, bucket.Count); err != nil {
			return err
		}
	}
	return nil
}

func visitPrometheusSeries(seriesLabels []prompb.Label, visit seriesVisitor) (func(int64, float64) error, error) {
	name, attrs, err := prometheusSeriesLabels(seriesLabels)
	if err != nil {
		return nil, err
	}
	return visit(name, attrs)
}

func prometheusSeriesLabels(seriesLabels []prompb.Label) (string, map[string]string, error) {
	attrs := make(map[string]string, len(seriesLabels))
	for _, label := range seriesLabels {
		if _, exists := attrs[label.Name]; exists {
			return "", nil, ErrInvalidMetrics
		}
		attrs[label.Name] = label.Value
	}
	name := attrs[labels.MetricName]
	delete(attrs, labels.MetricName)
	if name == "" || len(name) > 128 || len(attrs) > 110 {
		return "", nil, ErrInvalidMetrics
	}
	return name, attrs, nil
}

func (p *Prometheus) Bounds(ctx context.Context, selection extension.Selection) (start, end time.Time, found bool, err error) {
	now := time.Now()
	// One streaming raw-sample request also preserves sparse long histories.
	err = p.read(ctx, selection, now.Add(-p.retention).UnixMilli(), now.Add(time.Minute).UnixMilli(), func(_ string, _ map[string]string) (func(int64, float64) error, error) {
		return func(stamp int64, value float64) error {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil
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
		}, nil
	})
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return
}

func (p *Prometheus) Query(ctx context.Context, query extension.Query) ([]extension.Series, error) {
	buckets, err := newSeriesBuckets(query)
	if err != nil {
		return nil, err
	}
	start, end := max(query.Start.UnixMilli(), time.Now().Add(-p.retention).UnixMilli()), min(query.End.UnixMilli(), time.Now().Add(time.Minute).UnixMilli())
	if start <= end {
		if err := p.read(ctx, query.Selection, start, end, buckets.visit); err != nil {
			return nil, err
		}
	}
	result := buckets.result()
	return result, ctx.Err()
}

var _ extension.QueryBackend = (*Prometheus)(nil)
