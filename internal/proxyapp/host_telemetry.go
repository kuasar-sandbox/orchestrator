package proxyapp

import (
	"context"
	"log/slog"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/hostmetrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func runHostProxyMetrics(ctx context.Context, stats *proxystats.MasterStats, table *proxyshm.Table, logger *slog.Logger) {
	if stats == nil || table == nil {
		return
	}
	sender := hostmetrics.New("node-ctl", "proxy", nil)
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if !sender.Available() {
				continue
			}
			window, cancel := context.WithTimeout(ctx, 5*time.Second)
			collectHostProxyMetrics(window, stats, table, logger, sender)
			cancel()
		}
	}
}

func collectHostProxyMetrics(ctx context.Context, stats *proxystats.MasterStats, table *proxyshm.Table, logger *slog.Logger, sender *hostmetrics.Publisher) {
	if !table.Synced() {
		return
	}
	snapshots, ok := stats.MetricsSnapshot()
	if !ok {
		return
	}
	stamp := time.Now()
	batch := make([]hostmetrics.Group, 0, 64)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		if err := sender.EmitBatches(ctx, batch); err != nil {
			if logger != nil {
				logger.Debug("proxy metrics OTLP unavailable", "err", err)
			}
			return false
		}
		batch = batch[:0]
		return true
	}
	healthy := true
	for _, snapshot := range snapshots {
		route, found, revision := table.LookupRevision(snapshot.SandboxID)
		if !found || route.RunID != snapshot.RunID || route.State != snapshot.State {
			continue
		}
		actual, err := stats.SandboxTrafficStats(proxystats.WithPhysicalTelemetry(ctx), snapshot.SandboxID, snapshot.RunID, types.Profile(route.Profile), types.State(route.State))
		if err != nil || actual == nil {
			continue
		}
		attrs := map[string]string{"sandbox.id": snapshot.SandboxID, "sandbox.stable_id": route.StableID}
		if snapshot.RunID != "" {
			attrs["sandbox.run_id"] = snapshot.RunID
		}
		values := []hostmetrics.Sample{
			{Name: "sandbox.traffic.state", Unit: "1", Timestamp: stamp, Value: 1, Attributes: map[string]string{"state": actual.State}},
			{Name: "sandbox.traffic.inflight.parking", Unit: "{connection}", Timestamp: stamp, Value: float64(actual.Inflight.Parking)},
		}
		values = append(values, hostmetrics.Sample{Name: "sandbox.traffic.inflight.connected", Unit: "{connection}", Timestamp: stamp, Value: float64(actual.Inflight.Connected)})
		if snapshot.HoldKnown {
			values = append(values, hostmetrics.Sample{Name: "sandbox.traffic.hold.connected", Unit: "{item}", Timestamp: stamp, Value: float64(snapshot.Hold)})
		}
		max := route.EffectiveMaxInflight
		values = append(values, hostmetrics.Sample{Name: "sandbox.traffic.max_inflight", Unit: "{connection}", Timestamp: stamp, Value: float64(max.Total)})
		for service, limit := range map[string]uint32{"forward": max.Forward, "e2b:envd": max.E2BEnvd, "e2b:code-interpreter": max.E2BCodeInterpreter, "exec": max.Exec} {
			values = append(values, hostmetrics.Sample{Name: "sandbox.traffic.service.max_inflight", Unit: "{connection}", Timestamp: stamp, Value: float64(limit), Attributes: map[string]string{"service": service}})
		}
		if actual.IdleSince != nil {
			values = append(values, hostmetrics.Sample{Name: "sandbox.traffic.idle_since", Unit: "s", Timestamp: stamp, Value: float64(actual.IdleSince.UnixNano()) / 1e9})
		}
		for service, counters := range actual.Services {
			labels := map[string]string{"service": service}
			values = append(values,
				hostmetrics.Sample{Name: "sandbox.traffic.service.connected", Unit: "{connection}", Timestamp: stamp, Value: float64(counters.Connected), Attributes: labels},
				hostmetrics.Sample{Name: "sandbox.traffic.service.parking", Unit: "{connection}", Timestamp: stamp, Value: float64(counters.Parking), Attributes: labels})
			if counters.IdleSince != nil {
				values = append(values, hostmetrics.Sample{Name: "sandbox.traffic.service.idle_since", Unit: "s", Timestamp: stamp, Value: float64(counters.IdleSince.UnixNano()) / 1e9, Attributes: labels})
			}
		}
		// Do not emit a stale route's counters after an attachment is replaced.
		_, found, currentRevision := table.LookupRevision(snapshot.SandboxID)
		if !found || currentRevision != revision || !table.Synced() {
			continue
		}
		batch = append(batch, hostmetrics.Group{Resource: attrs, Samples: values})
		if len(batch) == 64 && !flush() {
			healthy = false
			break
		}
	}
	if healthy {
		flush()
	}
}
