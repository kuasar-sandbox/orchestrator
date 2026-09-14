package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
)

// ClickHouse reads the linked standard Collector exporter's five metrics tables.
// It never creates or alters schemas and owns no write/exporter connection.
type ClickHouse struct {
	*remoteClient
	endpoint string
	database string
	tables   config.TelemetryClickHouseTables
	lookback time.Duration
}

func NewClickHouse(cfg config.TelemetryQuery) (*ClickHouse, error) {
	lookback, err := time.ParseDuration(cfg.Lookback)
	if err != nil {
		return nil, err
	}
	return &ClickHouse{remoteClient: newRemoteClient(cfg.ClickHouse.Headers), endpoint: cfg.ClickHouse.Endpoint,
		database: cfg.ClickHouse.Database, tables: cfg.ClickHouse.Tables, lookback: lookback}, nil
}

func (c *ClickHouse) execute(ctx context.Context, sql string, params url.Values) ([]byte, error) {
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
	params.Set("max_result_rows", strconv.Itoa(maxQueryPoints))
	params.Set("max_result_bytes", strconv.Itoa(maxRemoteBytes))
	params.Set("result_overflow_mode", "throw")
	params.Set("output_format_json_quote_64bit_integers", "0")
	params.Set("readonly", "1")
	params.Set("cancel_http_readonly_queries_on_client_close", "1")
	endpoint.RawQuery = params.Encode()
	return c.request(ctx, endpoint.String(), nil, map[string]string{"Content-Type": "application/octet-stream"})
}

// Scalar labels match local storage for numbers, summaries and explicit buckets.
// The standard schema omits histogram HasSum and exponential ZeroThreshold;
// those absent facts are not reconstructed. Histogram sums are omitted, and
// exponential buckets expose their stored index/scale instead of invented bounds.
func (c *ClickHouse) source(selection extension.Selection, start, end int64) (string, string, url.Values, error) {
	if err := validateSelection(selection); err != nil {
		return "", "", nil, err
	}
	params := url.Values{"param_sid": {selection.SandboxID}, "param_start": {strconv.FormatInt(start, 10)}, "param_end": {strconv.FormatInt(end, 10)}}
	predicate := "ResourceAttributes['sandbox.id'] = {sid:String} AND bitAnd(Flags,1)=0 AND toUnixTimestamp64Milli(TimeUnix) >= {start:Int64} AND toUnixTimestamp64Milli(TimeUnix) <= {end:Int64}"
	if len(selection.Metrics) > 0 {
		names := make([]string, len(selection.Metrics))
		for i, name := range selection.Metrics {
			key := "metric" + strconv.Itoa(i)
			names[i] = "{" + key + ":String}"
			params.Set("param_"+key, name)
		}
		predicate += " AND MetricName IN (" + strings.Join(names, ",") + ")"
	}
	common := `mapConcat(
 mapFilter((k,v)-> k IN ('sandbox.id','sandbox.stable_id','sandbox.telemetry.source'),ResourceAttributes),
 mapApply((k,v)->(concat('resource.',k),v),mapFilter((k,v)-> k NOT IN ('sandbox.id','sandbox.stable_id','sandbox.telemetry.source'),ResourceAttributes)),
 map('otel.scope.name',ScopeName,'otel.scope.version',ScopeVersion,'otel.unit',MetricUnit),
 mapApply((k,v)->(concat('scope.',k),v),ScopeAttributes),
 mapApply((k,v)->(concat('point.',k),v),Attributes)`
	temporality := "'otel.temporality',multiIf(AggregationTemporality=2,'Cumulative',AggregationTemporality=1,'Delta','Unspecified')"
	type source struct{ kind, table, labels, parts string }
	// Each tuple is (part, bound/index, value); numbers have no part labels.
	definitions := []source{
		{"Gauge", c.tables.Gauge, "", "[tuple('','',Value)]"},
		{"Sum", c.tables.Sum, "," + temporality + ",'otel.monotonic',if(IsMonotonic,'true','false')", "[tuple('','',Value)]"},
		{"Summary", c.tables.Summary, "", "arrayConcat([tuple('count','',toFloat64(Count)),tuple('sum','',Sum)],arrayMap((q,v)->tuple('quantile',toString(q),v),`ValueAtQuantiles.Quantile`,`ValueAtQuantiles.Value`))"},
		{"Histogram", c.tables.Histogram, "," + temporality, "arrayConcat([tuple('count','',toFloat64(Count))],arrayMap((bound,count)->tuple('bucket',bound,toFloat64(count)),arrayConcat(arrayMap(x->toString(x),ExplicitBounds),['+Inf']),arrayCumSum(BucketCounts)))"},
		{"ExponentialHistogram", c.tables.ExponentialHistogram, "," + temporality + ",'otel.scale',toString(Scale)", "arrayConcat([tuple('count','',toFloat64(Count)),tuple('zero_count','',toFloat64(ZeroCount))],arrayMap((i,v)->tuple('positive_bucket',toString(PositiveOffset+toInt64(i)-1),toFloat64(v)),arrayEnumerate(PositiveBucketCounts),PositiveBucketCounts),arrayMap((i,v)->tuple('negative_bucket',toString(NegativeOffset+toInt64(i)-1),toFloat64(v)),arrayEnumerate(NegativeBucketCounts),NegativeBucketCounts))"},
	}
	var sources []string
	for _, definition := range definitions {
		attrs := common + ",map('otel.kind','" + definition.kind + "'" + definition.labels + "),if(part.1='',map(),map('otel.part',part.1)),if(part.2='',map(),map('otel.bound',part.2)))"
		sources = append(sources, "SELECT MetricName AS metric,mapSort("+attrs+") AS attributes,toUnixTimestamp64Milli(TimeUnix) AS stamp,part.3 AS value FROM `"+c.database+"`.`"+definition.table+"` ARRAY JOIN "+definition.parts+" AS part WHERE "+predicate)
	}
	keys := make([]string, 0, len(selection.Attributes))
	for key := range selection.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	predicates := []string{"isFinite(value)"}
	for i, key := range keys {
		suffix := strconv.Itoa(i)
		params.Set("param_key"+suffix, key)
		params.Set("param_value"+suffix, selection.Attributes[key])
		predicates = append(predicates, "attributes[{key"+suffix+":String}] = {value"+suffix+":String}")
	}
	return strings.Join(sources, " UNION ALL "), strings.Join(predicates, " AND "), params, nil
}

func (c *ClickHouse) Bounds(ctx context.Context, selection extension.Selection) (start, end time.Time, found bool, err error) {
	now := time.Now()
	source, predicate, params, err := c.source(selection, now.Add(-c.lookback).UnixMilli(), now.Add(time.Minute).UnixMilli())
	if err != nil {
		return start, end, false, err
	}
	raw, err := c.execute(ctx, "SELECT count() AS count,min(stamp) AS first,max(stamp) AS last FROM ("+source+") WHERE "+predicate+" FORMAT JSONEachRow", params)
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
	if ctx.Err() != nil {
		return start, end, false, ctx.Err()
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

func (c *ClickHouse) Query(ctx context.Context, query extension.Query) ([]extension.Series, error) {
	buckets, err := newSeriesBuckets(query)
	if err != nil {
		return nil, err
	}
	start, end := max(query.Start.UnixMilli(), time.Now().Add(-c.lookback).UnixMilli()), min(query.End.UnixMilli(), time.Now().Add(time.Minute).UnixMilli())
	if start > end {
		return []extension.Series{}, ctx.Err()
	}
	source, predicate, params, err := c.source(query.Selection, start, end)
	if err != nil {
		return nil, err
	}
	sql := "SELECT metric,attributes,stamp,value FROM (" + source + ") WHERE " + predicate + " ORDER BY metric,attributes,stamp"
	if query.Aggregation == extension.Max {
		params.Set("param_step", strconv.FormatInt(query.Step.Milliseconds(), 10))
		// Filter actual observations before independent per-series epoch MAX. The
		// first returned bucket may precede Start; no lookback sample enters it.
		sql = "SELECT metric,attributes,bucket,aggregate_value AS value FROM (SELECT metric,attributes,intDiv(stamp,{step:Int64})*{step:Int64} AS bucket,max(value) AS aggregate_value FROM (" + source + ") WHERE " + predicate + " GROUP BY metric,attributes,bucket) ORDER BY metric,attributes,bucket"
	}
	raw, err := c.execute(ctx, sql+" FORMAT JSONEachRow", params)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	for count := 0; ; count++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var row struct {
			Metric     string            `json:"metric"`
			Attributes map[string]string `json:"attributes"`
			Stamp      *int64            `json:"stamp"`
			Bucket     *int64            `json:"bucket"`
			Value      *float64          `json:"value"`
		}
		if err := decoder.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if count >= maxQueryPoints || row.Value == nil {
			return nil, ErrInvalidMetrics
		}
		var appendPoint func(int64, float64) error
		if query.Aggregation == extension.Max {
			if row.Bucket == nil || row.Stamp != nil {
				return nil, ErrInvalidMetrics
			}
			appendPoint, err = buckets.visitBucket(row.Metric, row.Attributes)
			if err == nil {
				err = appendPoint(*row.Bucket, *row.Value)
			}
		} else {
			if row.Stamp == nil || row.Bucket != nil {
				return nil, ErrInvalidMetrics
			}
			appendPoint, err = buckets.visit(row.Metric, row.Attributes)
			if err == nil {
				err = appendPoint(*row.Stamp, *row.Value)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("invalid ClickHouse observation: %w", err)
		}
	}
	result := buckets.result()
	return result, ctx.Err()
}

var _ extension.QueryBackend = (*ClickHouse)(nil)
