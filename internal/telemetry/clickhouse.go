package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
)

// ClickHouse uses a dedicated, ordinary MergeTree table. Each canonical
// Collector sample is one row; E2B fields are independently MAX-aggregated by
// metric and time bucket. Duplicate/out-of-order inserts preserve that result.
type ClickHouse struct {
	*remoteClient
	endpoint, table string
	retention       time.Duration
}

func OpenClickHouse(ctx context.Context, cfg config.TelemetryStorage) (*ClickHouse, error) {
	retention, err := time.ParseDuration(cfg.Retention)
	if err != nil {
		return nil, err
	}
	// Database/table identifiers were strictly validated by final config validation.
	c := &ClickHouse{remoteClient: newRemoteClient(cfg.ClickHouse.Headers), endpoint: cfg.ClickHouse.Endpoint,
		table: "`" + cfg.ClickHouse.Database + "`.`" + cfg.ClickHouse.Table + "`", retention: retention}
	ttl := "toDateTime(timestamp) + toIntervalSecond(" + strconv.FormatInt(int64(retention/time.Second), 10) + ")"
	statement := "CREATE TABLE IF NOT EXISTS " + c.table + ` (
sandbox_id String, stable_id String, source LowCardinality(String), metric LowCardinality(String),
labels Map(String, String), timestamp DateTime64(3, 'UTC'), value Float64
) ENGINE = MergeTree PARTITION BY toDate(timestamp)
ORDER BY (sandbox_id, source, metric, timestamp) TTL ` + ttl
	for _, sql := range []string{statement, "ALTER TABLE " + c.table + " MODIFY TTL " + ttl} {
		raw, err := c.execute(ctx, sql, nil, nil, false)
		if err == nil && len(bytes.TrimSpace(raw)) != 0 {
			err = errors.New("unexpected ClickHouse DDL response")
		}
		if err != nil {
			_ = c.Shutdown(context.Background())
			return nil, fmt.Errorf("initialize telemetry ClickHouse table: %w", err)
		}
	}
	return c, nil
}

func (c *ClickHouse) execute(ctx context.Context, sql string, params url.Values, data []byte, readonly bool) ([]byte, error) {
	endpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, err
	}
	if params == nil {
		params = make(url.Values)
	}
	params.Set("query", sql)
	params.Set("wait_end_of_query", "1")
	params.Set("buffer_size", strconv.Itoa(maxRemoteBytes))
	params.Set("max_execution_time", "9")
	params.Set("max_threads", "2")
	params.Set("max_memory_usage", "268435456")
	params.Set("max_result_rows", "700000")
	params.Set("max_result_bytes", strconv.Itoa(maxRemoteBytes))
	params.Set("result_overflow_mode", "throw")
	params.Set("output_format_json_quote_64bit_integers", "0")
	if readonly {
		params.Set("readonly", "1")
		params.Set("cancel_http_readonly_queries_on_client_close", "1")
	}
	endpoint.RawQuery = params.Encode()
	return c.request(ctx, endpoint.String(), data, map[string]string{"Content-Type": "application/octet-stream"})
}

func (c *ClickHouse) Write(ctx context.Context, samples []extension.Sample) error {
	if len(samples) > maxWriteSamples {
		return ErrInvalidMetrics
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	now := time.Now()
	for _, sample := range samples {
		if err := ctx.Err(); err != nil {
			return err
		}
		if sample.Timestamp.Before(now.Add(-c.retention)) {
			continue
		}
		if sample.Timestamp.After(now.Add(time.Minute)) || math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
			return ErrInvalidMetrics
		}
		row := struct {
			SandboxID string            `json:"sandbox_id"`
			StableID  string            `json:"stable_id"`
			Source    string            `json:"source"`
			Metric    string            `json:"metric"`
			Labels    map[string]string `json:"labels"`
			Timestamp string            `json:"timestamp"`
			Value     float64           `json:"value"`
		}{sample.Labels[SandboxIDAttribute], sample.Labels[StableIDAttribute], sample.Labels[sourceAttribute], sample.Metric,
			sample.Labels, sample.Timestamp.UTC().Truncate(time.Millisecond).Format("2006-01-02 15:04:05.000"), sample.Value}
		if err := encoder.Encode(row); err != nil {
			return err
		}
		if body.Len() > maxRemoteBytes {
			return ErrInvalidMetrics
		}
	}
	if body.Len() == 0 {
		return nil
	}
	// Let ClickHouse coalesce concurrent scrapes instead of creating one tiny
	// MergeTree part per sandbox. Wait for the flush: errors/backpressure must
	// reach the Collector, not disappear behind a fire-and-forget acknowledgement.
	params := url.Values{"async_insert": {"1"}, "wait_for_async_insert": {"1"},
		"async_insert_use_adaptive_busy_timeout": {"0"}, "async_insert_busy_timeout_ms": {"20"},
		"async_insert_max_data_size": {"1048576"}, "async_insert_max_query_number": {"128"}}
	raw, err := c.execute(ctx, "INSERT INTO "+c.table+" (sandbox_id, stable_id, source, metric, labels, timestamp, value) FORMAT JSONEachRow", params, body.Bytes(), false)
	if err == nil && len(bytes.TrimSpace(raw)) != 0 {
		return errors.New("unexpected ClickHouse insert response")
	}
	return err
}

func (c *ClickHouse) predicate(id string, start, end int64) (string, url.Values) {
	params := url.Values{"param_sid": {id}, "param_start": {strconv.FormatInt(start, 10)}, "param_end": {strconv.FormatInt(end, 10)}}
	var names []string
	for _, metric := range resourceMetrics {
		names = append(names, "'"+metric.name+"'")
	}
	return "sandbox_id = {sid:String} AND source = 'envd' AND labels['otel.kind'] = 'Gauge' AND metric IN (" + strings.Join(names, ",") + ") AND timestamp >= fromUnixTimestamp64Milli({start:Int64}) AND timestamp <= fromUnixTimestamp64Milli({end:Int64})", params
}

func (c *ClickHouse) Bounds(ctx context.Context, id string) (start, end time.Time, found bool, err error) {
	predicate, params := c.predicate(id, time.Now().Add(-c.retention).UnixMilli(), time.Now().Add(time.Minute).UnixMilli())
	raw, err := c.execute(ctx, "SELECT count() AS count, toUnixTimestamp64Milli(min(timestamp)) AS first, toUnixTimestamp64Milli(max(timestamp)) AS last FROM "+c.table+" WHERE "+predicate+" FORMAT JSONEachRow", params, nil, true)
	if err != nil {
		return start, end, false, err
	}
	var row struct {
		Count uint64 `json:"count"`
		First int64  `json:"first"`
		Last  int64  `json:"last"`
	}
	if err := json.Unmarshal(raw, &row); err != nil {
		return start, end, false, err
	}
	if row.Count == 0 {
		return start, end, false, nil
	}
	start, end = time.UnixMilli(row.First).UTC(), time.UnixMilli(row.Last).UTC()
	if err := ValidateRange(start, end); err != nil {
		return start, end, false, err
	}
	return start, end, true, nil
}

func (c *ClickHouse) Query(ctx context.Context, query extension.Query) ([]extension.Point, error) {
	if err := ValidateRange(query.Start, query.End); err != nil {
		return nil, err
	}
	if query.Step < time.Second || query.Step%time.Second != 0 {
		return nil, errors.New("ClickHouse query step must use positive whole seconds")
	}
	predicate, params := c.predicate(query.SandboxID, max(query.Start.UnixMilli(), time.Now().Add(-c.retention).UnixMilli()), query.End.UnixMilli())
	params.Set("param_step", strconv.FormatInt(int64(query.Step/time.Second), 10))
	// Use integer epoch buckets to preserve milliseconds and avoid timezone or
	// server settings changing the DateTime64 return type of time functions.
	sql := "SELECT intDiv(toUnixTimestamp64Milli(timestamp), {step:Int64} * 1000) * {step:Int64} * 1000 AS stamp, metric, max(value) AS value FROM " + c.table + " WHERE " + predicate + " GROUP BY stamp, metric ORDER BY stamp, metric FORMAT JSONEachRow"
	raw, err := c.execute(ctx, sql, params, nil, true)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	points := make([]extension.Point, 0)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var row struct {
			Stamp  int64   `json:"stamp"`
			Metric string  `json:"metric"`
			Value  float64 `json:"value"`
		}
		if err := decoder.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		field, ok := metricField(row.Metric)
		if !ok || len(points) >= 700000 {
			return nil, ErrInvalidMetrics
		}
		points = append(points, extension.Point{Timestamp: time.UnixMilli(row.Stamp).UTC(), Field: field, Value: row.Value})
	}
	return points, nil
}

var _ extension.Storage = (*ClickHouse)(nil)
