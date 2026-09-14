package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	telemetryextension "github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandboxer/pkg/usage"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type statsTestSource struct{ routes []routesync.RouteEntry }

func (s statsTestSource) Range(ctx context.Context, emit func(routesync.RouteEntry) error) error {
	for _, route := range s.routes {
		if err := emit(route); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (statsTestSource) Subscribe() (<-chan routesync.Event, func()) {
	return make(chan routesync.Event), func() {}
}
func (statsTestSource) OnWake(context.Context, string) { panic("telemetry issued wake") }
func (statsTestSource) Policy() routesync.Policy       { return routesync.Policy{} }

type statsTestReader func(context.Context, conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error)

func (f statsTestReader) ReadStats(ctx context.Context, q conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error) {
	return f(ctx, q)
}

func TestSandboxStatsReceiverUsesConductorLeaseAndBoundedBatches(t *testing.T) {
	// This is the actual config_socket HTTP/Plugin Plane and SO_PEERCRED
	// authorization, with a controllable domain reader for cancellation/errors.
	dir, err := os.MkdirTemp("", "stats-collector-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "conductor.sock")
	view := NewView(130)
	defer view.InvalidateSync()
	source := statsTestSource{}
	for i := 0; i < 65; i++ {
		source.routes = append(source.routes, routesync.RouteEntry{SandboxID: fmt.Sprintf("sid-%02d", i), StableID: "stable", State: routesync.StatePaused})
	}
	var mu sync.Mutex
	var batches []conductorextension.StatsRequest
	var sourceErr error
	backend := statsTestReader(func(ctx context.Context, q conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error) {
		mu.Lock()
		defer mu.Unlock()
		if sourceErr != nil {
			return nil, sourceErr
		}
		batches = append(batches, q)
		if q.Usage.View != "saved" || len(q.Sections) != 1 || q.Sections[0] != "usage" || len(q.SandboxIDs) > 64 {
			t.Error("unbounded or wrong native section", q)
		}
		rows := make([]conductorextension.SandboxStats, len(q.SandboxIDs))
		for i, id := range q.SandboxIDs {
			record := savedStatsRecord(id)
			raw, err := json.Marshal(usage.View{Saved: &record, UnknownTail: true})
			if err != nil {
				return nil, err
			}
			rows[i] = conductorextension.SandboxStats{SandboxID: id, StableID: "stable", Usage: raw}
		}
		return rows, ctx.Err()
	})
	registry := configsock.NewRegistry()
	server := configsock.New(socket, configsock.Deps{Plugins: registry, RouteSource: source, Stats: backend, API: http.NotFoundHandler()}, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done, ready := make(chan error, 1), make(chan struct{})
	go func() { done <- server.ServeReady(ctx, ready) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("conductor socket unavailable")
	}
	var deliveries []pmetric.Metrics
	r := &sandboxStatsReceiver{view: view, socket: socket, cfg: sandboxStatsConfig{Timeout: time.Second, Concurrency: 2}, slots: make(chan struct{}, 2), log: testLogger()}
	r.next = metricsConsumer(t, func(_ context.Context, m pmetric.Metrics) error {
		mu.Lock()
		defer mu.Unlock()
		copy := pmetric.NewMetrics()
		m.CopyTo(copy)
		deliveries = append(deliveries, copy)
		return nil
	})
	if err := r.read(t.Context(), "usage", []string{"sid-00"}); err == nil {
		t.Fatal("UDS connection acquired stats without a plugin lease")
	}
	subscriber := routesync.NewSubscriber(func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "unix", socket) }, routesync.TelemetryPluginID,
		routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute}, Telemetry: &routesync.Telemetry{}}, view, nil, testLogger())
	subCtx, stopSub := context.WithCancel(ctx)
	subDone := make(chan struct{})
	go func() { subscriber.Run(subCtx); close(subDone) }()
	t.Cleanup(func() { stopSub(); <-subDone })
	deadline := time.Now().Add(3 * time.Second)
	for {
		entries, changed := view.snapshot()
		if len(entries) == 65 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("route subscription unavailable")
		}
		select {
		case <-changed:
		case <-time.After(20 * time.Millisecond):
		}
	}
	r.collect(t.Context(), "usage")
	if len(batches) != 2 || len(deliveries) != 2 {
		t.Fatal("65 paused objects did not use two bounded conductor reads", len(batches), len(deliveries))
	}
	seen := map[string]bool{}
	for _, m := range deliveries {
		for i := 0; i < m.ResourceMetrics().Len(); i++ {
			resource := m.ResourceMetrics().At(i)
			id, _ := resource.Resource().Attributes().Get(SandboxIDAttribute)
			seen[id.Str()] = true
			for j := 0; j < resource.ScopeMetrics().At(0).Metrics().Len(); j++ {
				metric := resource.ScopeMetrics().At(0).Metrics().At(j)
				if metric.Name() == "sandbox.usage.cpu.seconds" && metric.Gauge().DataPoints().At(0).Timestamp().AsTime().UnixNano() != savedStatsRecord(id.Str()).SavedUTC {
					t.Fatal("saved endpoint restamped")
				}
			}
		}
	}
	if len(seen) != 65 {
		t.Fatal("lost paused identities", len(seen))
	}
	mu.Lock()
	sourceErr = errors.New("configured source unavailable")
	mu.Unlock()
	if err := r.read(t.Context(), "usage", []string{"sid-00"}); err == nil || len(deliveries) != 2 {
		t.Fatal("failed source became a sample", err)
	}
	stopSub()
	<-subDone
	mu.Lock()
	sourceErr = nil
	mu.Unlock()
	// The subscriber returning does not synchronize with server-side socket
	// teardown. Wait for the actual HTTP authorization denial with a healthy
	// source, so a source failure cannot masquerade as lease revocation.
	// A request admitted just before teardown can instead finish with 503 when
	// its captured lease is canceled; that is not proof of subsequent denial.
	deadline = time.Now().Add(3 * time.Second)
	request := conductorextension.StatsRequest{SandboxIDs: []string{"sid-00"}, Sections: []string{"usage"}, Usage: conductorextension.UsageQuery{View: "saved"}}
	for {
		_, err := configsock.ReadNativeStats(t.Context(), socket, request)
		if err != nil && strings.Contains(err.Error(), "403") {
			break
		}
		if (err != nil && !strings.Contains(err.Error(), "HTTP 503")) || time.Now().After(deadline) {
			t.Fatal("lease did not revoke native read authorization", err)
		}
		time.Sleep(time.Millisecond)
	}
	if err := r.read(t.Context(), "usage", []string{"sid-00"}); err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatal("revoked lease did not deny a subsequent native read", err)
	}
}

func savedStatsRecord(id string) usage.Record {
	return usage.Record{Sequence: 9, SavedUTC: 1700000000000000000, Snapshot: usage.Snapshot{SandboxID: id, RunEpoch: "native-metadata-must-not-be-a-label", StartedUTC: 1699999990000000000,
		Counters: []usage.Counter{{Name: "ch.cpu", Source: "private-process-identity", SourceKnown: true, KnownTotal: usage.Uint128{Hi: 1, Lo: 9007199254740993}, Complete: false, Status: usage.Missing}},
		Gauges:   []usage.Gauge{{Name: "guest.memory", PositionKnown: true, IntegralTotal: usage.Uint128{Hi: 2, Lo: 99}, SpanTotal: 10, CoveredTotal: 0, Status: usage.Missing}},
	}}
}

func TestUsageProjectionKeepsSavedTotalsAfterNativeGaugeBreak(t *testing.T) {
	for _, initial := range []uint64{0, 10} {
		t.Run(fmt.Sprint(initial), func(t *testing.T) {
			gauge := usage.Gauge{Name: "guest.memory"}
			for i, value := range []uint64{initial, 20} {
				if err := gauge.Observe("ram", uint64(i+1), int64(i+1)*int64(time.Second), value, usage.OK, 0, time.Second); err != nil {
					t.Fatal(err)
				}
			}
			gauge.Break(usage.Paused)
			if gauge.PositionKnown || gauge.ValueKnown {
				t.Fatal("native pause did not break current position")
			}
			record := savedStatsRecord("sid")
			record.Snapshot.Gauges = []usage.Gauge{gauge, {Name: "never-observed", Status: usage.Missing}}
			record.Snapshot.Counters[0].SourceKnown = false // A new source can retain saved known totals.
			record.Snapshot.Counters = append(record.Snapshot.Counters, usage.Counter{Name: "never-observed", Status: usage.Missing})
			raw, err := json.Marshal(usage.View{Saved: &record})
			if err != nil {
				t.Fatal(err)
			}
			metrics := pmetric.NewMetrics()
			if err := appendNativeStats(metrics, conductorextension.SandboxStats{SandboxID: "sid", Usage: raw}, "usage", time.Now()); err != nil {
				t.Fatal(err)
			}
			samples, err := canonicalSamples(metrics)
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]float64{}
			for _, sample := range samples {
				if sample.Labels["point.usage.name"] == "never-observed" {
					t.Fatal("unobserved source became a measured zero")
				}
				if sample.Labels["point.usage.name"] == "" {
					continue
				}
				if sample.Timestamp.UnixNano() != record.SavedUTC {
					t.Fatal("cumulative endpoint was restamped")
				}
				seen[sample.Metric] = sample.Value
			}
			if len(seen) != 4 || seen["sandbox.usage.memory.integral"] != float64(initial) || seen["sandbox.usage.memory.span"] != 1 || seen["sandbox.usage.memory.covered"] != 1 || seen["sandbox.usage.cpu.seconds"] == 0 {
				t.Fatal("lost known saved quantities after source break", seen)
			}
		})
	}
}

func TestNativeStatsProjectionObservationTimesZerosAndPrecision(t *testing.T) {
	now := time.Unix(1800000000, 0)
	stamp := int64(1700000000)
	zero, capacity, headroom, reserved := uint64(0), uint64(8192), uint64(1024), uint64(2048)
	cpuCapacity, cpuAllocatable, cpu := float64(4), 0.75, json.Number("0")
	row := conductorextension.SandboxStats{SandboxID: "sid", StableID: "stable", Resource: &conductorextension.ResourceStats{CPUCapacity: &cpuCapacity, CPUAllocatable: &cpuAllocatable, MemoryCapacity: &capacity, MemoryHeadroom: &headroom, MemoryReserved: &reserved, MemoryUsed: &zero, CPUSeconds: &cpu, TimestampUnix: &stamp}}
	m := pmetric.NewMetrics()
	if err := appendNativeStats(m, row, "resource", now); err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for i := 0; i < m.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len(); i++ {
		metric := m.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(i)
		p := metric.Gauge().DataPoints().At(0)
		values[metric.Name()] = p.DoubleValue()
		if metric.Name() == "sandbox.resource.cpu.seconds" || metric.Name() == "sandbox.resource.memory.used" {
			if p.Timestamp().AsTime().Unix() != stamp {
				t.Fatal("host sample restamped")
			}
		} else if !p.Timestamp().AsTime().Equal(now) {
			t.Fatal("current specification timestamp")
		}
	}
	if len(values) != 7 || values["sandbox.resource.memory.capacity"] != 8192 || values["sandbox.resource.memory.headroom"] != 1024 || values["sandbox.resource.memory.reserved"] != 2048 || values["sandbox.resource.memory.used"] != 0 {
		t.Fatal(values)
	}
	row.Resource.MemoryUsed, row.Resource.CPUSeconds, row.Resource.TimestampUnix = nil, nil, nil
	m = pmetric.NewMetrics()
	if err := appendNativeStats(m, row, "resource", now); err != nil || m.DataPointCount() != 5 {
		t.Fatal("missing host values became zero", err, m.DataPointCount())
	}
	record := savedStatsRecord("sid")
	raw, err := json.Marshal(usage.View{Saved: &record})
	if err != nil {
		t.Fatal(err)
	}
	row.Usage = raw
	var previous []telemetryextension.Sample
	for range 2 {
		m = pmetric.NewMetrics()
		if err := appendNativeStats(m, row, "usage", now); err != nil {
			t.Fatal(err)
		}
		body, err := canonicalSamples(m)
		if err != nil {
			t.Fatal(err)
		}
		if previous != nil && !reflect.DeepEqual(body, previous) {
			t.Fatal("repeated cumulative read was integrated")
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), record.Snapshot.RunEpoch) || strings.Contains(string(encoded), "private-process-identity") {
			t.Fatal("native source/run identity leaked into series")
		}
		previous = body
	}
	// The native record remains exact; scalar projection deliberately rounds.
	var decoded usage.View
	if err := json.Unmarshal(row.Usage, &decoded); err != nil || decoded.Saved.Snapshot.Counters[0].KnownTotal != record.Snapshot.Counters[0].KnownTotal {
		t.Fatal("native 128-bit input was changed", err)
	}
}
