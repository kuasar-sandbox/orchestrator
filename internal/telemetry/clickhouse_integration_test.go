package telemetry

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
)

// Optional live-engine test. It creates and drops only its uniquely named table
// in an explicitly supplied, disposable ClickHouse database (default: default).
func TestClickHouseIntegration(t *testing.T) {
	endpoint := os.Getenv("TELEMETRY_CLICKHOUSE_TEST_URL")
	if endpoint == "" {
		t.Skip("set TELEMETRY_CLICKHOUSE_TEST_URL for live ClickHouse SQL/TTL verification")
	}
	cfg := config.TelemetryStorage{Type: "clickhouse", Retention: "1h", ClickHouse: config.TelemetryClickHouse{
		Endpoint: endpoint, Database: "default", Table: fmt.Sprintf("telemetry_test_%d_%d", os.Getpid(), time.Now().UnixNano()),
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backend, err := OpenClickHouse(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := backend.execute(cleanupCtx, "DROP TABLE "+backend.table, nil, nil, false); err != nil {
			t.Error(err)
		}
		_ = backend.Shutdown(cleanupCtx)
	}()
	now := time.Now().Add(-time.Minute).Truncate(5 * time.Second)
	first, second := resourceSamples(t, "sid", now.Add(123*time.Millisecond)), resourceSamples(t, "sid", now.Add(2123*time.Millisecond))
	first[1].Value, second[1].Value = 70, 20
	first[4].Value, second[4].Value = 100, 300
	// Opposite order plus a duplicate; both field maxima must survive.
	for _, samples := range [][]extension.Sample{second, first, first} {
		if err := backend.Write(ctx, samples); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	backend, err = OpenClickHouse(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start, end, found, err := backend.Bounds(ctx, "sid")
	if err != nil || !found || !start.Equal(first[0].Timestamp) || !end.Equal(second[0].Timestamp) {
		t.Fatal("restart/bounds/millisecond precision", start, end, found, err)
	}
	if _, _, found, err := backend.Bounds(ctx, "stable-sid"); err != nil || found {
		t.Fatal("StableID fallback", err)
	}
	points, err := backend.Query(ctx, extension.Query{SandboxID: "sid", Start: start, End: end, Step: 5 * time.Second})
	if err != nil || len(points) != 7 {
		t.Fatal("SQL aggregation", points, err)
	}
	for _, point := range points {
		if !point.Timestamp.Equal(now) || point.Field == extension.CPUUsedPct && point.Value != 70 || point.Field == extension.MemCache && point.Value != 300 {
			t.Fatal("independent field MAX", point)
		}
	}
}
