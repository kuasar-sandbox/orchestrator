package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandboxer/pkg/usage"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
)

type sandboxStatsConfig struct {
	Resource    time.Duration `mapstructure:"resource_interval"`
	Traffic     time.Duration `mapstructure:"traffic_interval"`
	Usage       time.Duration `mapstructure:"usage_interval"`
	Timeout     time.Duration `mapstructure:"timeout"`
	Concurrency int           `mapstructure:"concurrency"`
}

func (c *sandboxStatsConfig) Validate() error {
	if c.Timeout <= 0 || c.Timeout > conductorextension.StatsTimeout || c.Concurrency < 1 || c.Concurrency > conductorextension.MaxStatsConcurrency {
		return errors.New("sandboxstats requires timeout in (0, 5s] and concurrency in [1, 8]")
	}
	enabled := false
	for _, interval := range []time.Duration{c.Resource, c.Traffic, c.Usage} {
		if interval == 0 {
			continue
		}
		if interval < time.Second || interval > time.Hour {
			return errors.New("sandboxstats intervals must be zero (disabled) or in [1s, 1h]")
		}
		enabled = true
	}
	if !enabled {
		return errors.New("sandboxstats requires at least one section")
	}
	return nil
}

type sandboxStatsReceiver struct {
	view   *View
	socket string
	cfg    sandboxStatsConfig
	next   consumer.Metrics
	log    *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}
	slots  chan struct{}
}

func sandboxStatsFactory(view *View, socket string, logger *slog.Logger) receiver.Factory {
	return receiver.NewFactory(component.MustNewType("sandboxstats"), func() component.Config {
		return &sandboxStatsConfig{Resource: 5 * time.Second, Traffic: 10 * time.Second, Usage: time.Minute, Timeout: 5 * time.Second, Concurrency: 4}
	}, receiver.WithMetrics(func(_ context.Context, _ receiver.Settings, raw component.Config, next consumer.Metrics) (receiver.Metrics, error) {
		return &sandboxStatsReceiver{view: view, socket: socket, cfg: *raw.(*sandboxStatsConfig), next: next, log: logger}, nil
	}, component.StabilityLevelStable))
}

func (r *sandboxStatsReceiver) Start(ctx context.Context, _ component.Host) error {
	ctx, r.cancel = context.WithCancel(ctx)
	r.slots = make(chan struct{}, r.cfg.Concurrency)
	r.done = make(chan struct{})
	var workers sync.WaitGroup
	for section, interval := range map[string]time.Duration{"resource": r.cfg.Resource, "traffic": r.cfg.Traffic, "usage": r.cfg.Usage} {
		if interval == 0 {
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			timer := time.NewTimer(0)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
				r.collect(ctx, section)
				timer.Reset(interval)
			}
		}()
	}
	go func() { workers.Wait(); close(r.done) }()
	return nil
}

func (r *sandboxStatsReceiver) Shutdown(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *sandboxStatsReceiver) collect(ctx context.Context, section string) {
	entries, _ := r.view.snapshot()
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		// Resource is a live-runtime read. Traffic and saved usage can apply to
		// paused sandboxes and must not inherit guest active-only admission.
		if section == "resource" && entry.route.State != routesync.StateRunning && entry.route.State != routesync.StateStarting {
			continue
		}
		ids = append(ids, entry.route.SandboxID)
	}
	sort.Strings(ids)
	var workers sync.WaitGroup
	defer workers.Wait()
	for start := 0; start < len(ids); start += conductorextension.MaxStatsSandboxes {
		batch := ids[start:min(start+conductorextension.MaxStatsSandboxes, len(ids))]
		select {
		case r.slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-r.slots }()
			if err := r.read(ctx, section, batch); err != nil && ctx.Err() == nil && r.log != nil {
				r.log.Warn("conductor stats collection failed", "section", section, "sandboxes", len(batch), "err", err)
			}
		}()
	}
}

func (r *sandboxStatsReceiver) read(ctx context.Context, section string, ids []string) error {
	query := conductorextension.StatsRequest{SandboxIDs: ids, Sections: []string{section}}
	if section == "usage" {
		query.Usage.View = "saved"
	}
	readCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	rows, err := configsock.ReadNativeStats(readCtx, r.socket, query)
	if err != nil {
		return err
	}
	observed := time.Now()
	metrics := pmetric.NewMetrics()
	for _, row := range rows {
		if err := appendNativeStats(metrics, row, section, observed); err != nil {
			return err
		}
	}
	if metrics.DataPointCount() == 0 {
		return nil
	}
	// The conductor authorized the exact objects and validated the binding.
	// An accepted response is independent of subsequent route invalidation.
	deliveryCtx, stop := context.WithTimeout(ctx, r.cfg.Timeout)
	defer stop()
	return r.next.ConsumeMetrics(deliveryCtx, metrics)
}

func appendNativeStats(metrics pmetric.Metrics, row conductorextension.SandboxStats, section string, observed time.Time) error {
	if row.SandboxID == "" {
		return ErrIdentity
	}
	resource := metrics.ResourceMetrics().AppendEmpty()
	attrs := resource.Resource().Attributes()
	attrs.PutStr(SandboxIDAttribute, row.SandboxID)
	attrs.PutStr(StableIDAttribute, row.StableID)
	attrs.PutStr(sourceAttribute, "sandboxstats")
	scope := resource.ScopeMetrics().AppendEmpty()
	scope.Scope().SetName("sandbox.stats." + section)
	add := func(name, unit string, value float64, stamp time.Time, attributes map[string]string) {
		metric := scope.Metrics().AppendEmpty()
		metric.SetName("sandbox." + section + "." + name)
		metric.SetUnit(unit)
		point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
		point.SetTimestamp(pcommon.NewTimestampFromTime(stamp))
		point.SetDoubleValue(value)
		for key, value := range attributes {
			point.Attributes().PutStr(key, value)
		}
	}
	addUint := func(name, unit string, value *uint64, stamp time.Time) {
		if value != nil {
			add(name, unit, float64(*value), stamp, nil)
		}
	}
	switch section {
	case "resource":
		r := row.Resource
		if r == nil {
			return errors.New("missing native resource section")
		}
		if r.CPUCapacity != nil {
			add("cpu.capacity", "{core}", *r.CPUCapacity, observed, nil)
		}
		if r.CPUAllocatable != nil {
			add("cpu.allocatable", "{core}", *r.CPUAllocatable, observed, nil)
		}
		addUint("memory.capacity", "By", r.MemoryCapacity, observed)
		addUint("memory.headroom", "By", r.MemoryHeadroom, observed)
		addUint("memory.reserved", "By", r.MemoryReserved, observed)
		if r.TimestampUnix != nil {
			stamp := time.Unix(*r.TimestampUnix, 0)
			addUint("memory.used", "By", r.MemoryUsed, stamp)
			if r.CPUSeconds != nil {
				value, err := r.CPUSeconds.Float64()
				if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
					return ErrInvalidMetrics
				}
				add("cpu.seconds", "s", value, stamp, nil)
			}
		} else if r.MemoryUsed != nil || r.CPUSeconds != nil {
			return errors.New("native host observation lacks timestamp")
		}
	case "traffic":
		r := row.Traffic
		if r == nil {
			return errors.New("missing native traffic section")
		}
		add("state", "1", 1, observed, map[string]string{"state": r.State})
		add("max_inflight", "{connection}", float64(r.MaxInflight.Total), observed, nil)
		if r.IdleSince != nil {
			add("idle_since", "s", float64(r.IdleSince.UnixNano())/1e9, observed, nil)
		}
		add("inflight.parking", "{connection}", float64(r.Inflight.Parking), observed, nil)
		add("inflight.connected", "{connection}", float64(r.Inflight.Connected), observed, nil)
		for service, counters := range r.Services {
			labels := map[string]string{"service": service}
			add("service.parking", "{connection}", float64(counters.Parking), observed, labels)
			add("service.connected", "{connection}", float64(counters.Connected), observed, labels)
			if counters.IdleSince != nil {
				add("service.idle_since", "s", float64(counters.IdleSince.UnixNano())/1e9, observed, labels)
			}
		}
		for service, limit := range map[string]uint32{"forward": r.MaxInflight.Forward, "e2b:envd": r.MaxInflight.E2BEnvd, "e2b:code-interpreter": r.MaxInflight.E2BCodeInterpreter, "exec": r.MaxInflight.Exec} {
			add("service.max_inflight", "{connection}", float64(limit), observed, map[string]string{"service": service})
		}
		for plane, c := range map[string]conductorextension.TrafficCounters{"platform": r.Platform, "transit": r.Transit} {
			addUint(plane+".rx.packets", "{packet}", c.RXPackets, observed)
			addUint(plane+".rx.bytes", "By", c.RXBytes, observed)
			addUint(plane+".tx.packets", "{packet}", c.TXPackets, observed)
			addUint(plane+".tx.bytes", "By", c.TXBytes, observed)
		}
	case "usage":
		var view usage.View
		if err := json.Unmarshal(row.Usage, &view); err != nil {
			return err
		}
		// These are observations of read state, independent of saved quantities.
		add("saved.available", "1", boolFloat(view.Saved != nil), observed, nil)
		add("enabled", "1", boolFloat(view.Enabled), observed, nil)
		add("saving", "1", boolFloat(view.Saving), observed, nil)
		add("unknown_tail", "1", boolFloat(view.UnknownTail), observed, nil)
		add("save_error", "1", boolFloat(view.SaveError != ""), observed, nil)
		add("read_error", "1", boolFloat(view.ReadError != ""), observed, nil)
		if view.Saved == nil {
			return nil
		}
		record := view.Saved
		if record.Snapshot.SandboxID != row.SandboxID || record.SavedUTC <= 0 {
			return ErrIdentity
		}
		stamp := time.Unix(0, record.SavedUTC)
		// The record is a cumulative saved endpoint, potentially behind live or
		// preceding an unknown tail. Never integrate it again or claim monotonic
		// reliable Counter semantics. Repeat reads retain its original timestamp.
		for _, c := range record.Snapshot.Counters {
			if !c.SourceKnown && c.KnownTotal == (usage.Uint128{}) {
				continue
			}
			add("cpu.seconds", "s", uint128Float(c.KnownTotal)/1e9, stamp, map[string]string{"usage.name": c.Name, "usage.status": c.Status, "usage.complete": fmt.Sprint(c.Complete), "usage.source_known": fmt.Sprint(c.SourceKnown)})
		}
		for _, g := range record.Snapshot.Gauges {
			labels := map[string]string{"usage.name": g.Name, "usage.status": g.Status, "usage.position_known": fmt.Sprint(g.PositionKnown), "usage.value_known": fmt.Sprint(g.ValueKnown)}
			// Break/newRun invalidate the current source boundary, not the
			// already saved lifetime contribution. Conversely, an initial failed
			// attempt does not establish a measured zero memory integral.
			if g.ValueKnown || g.CoveredTotal > 0 || g.IntegralTotal != (usage.Uint128{}) {
				add("memory.integral", "By.s", uint128Float(g.IntegralTotal)/1e9, stamp, labels)
			}
			if g.ValueKnown || g.SpanTotal > 0 {
				add("memory.span", "s", float64(g.SpanTotal)/1e9, stamp, labels)
				add("memory.covered", "s", float64(g.CoveredTotal)/1e9, stamp, labels)
			}
		}
	default:
		return errors.New("unknown native stats section")
	}
	return nil
}

func uint128Float(value usage.Uint128) float64 {
	return math.Ldexp(float64(value.Hi), 64) + float64(value.Lo)
}
func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
