package orch

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/hostmetrics"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func networkCounterSamples(plane string, value api.TrafficCounters, observed time.Time) []hostmetrics.Sample {
	out := make([]hostmetrics.Sample, 0, 4)
	fields := []struct {
		name, unit string
		ptr        *uint64
	}{
		{"rx.packets", "{packet}", value.RXPackets},
		{"rx.bytes", "By", value.RXBytes},
		{"tx.packets", "{packet}", value.TXPackets},
		{"tx.bytes", "By", value.TXBytes},
	}
	for _, field := range fields {
		if field.ptr == nil {
			continue
		} // Unknown is not an observed zero.
		v := *field.ptr
		sample := hostmetrics.Sample{Name: "sandbox.traffic." + plane + "." + field.name, Unit: field.unit, Timestamp: observed}
		if v <= math.MaxInt64 {
			number := int64(v)
			sample.IntValue = &number
		} else {
			sample.Value = float64(v)
		}
		out = append(out, sample)
	}
	return out
}

// Event producers copy only metric identities and native observations. A full
// queue drops the observation; neither routing nor teardown waits for export.
type conductorHostObserver struct {
	publisher *hostmetrics.Publisher
	events    chan hostmetrics.Group
	states    map[string]observedSandboxState // under holdMu, in route publication order
}

type observedSandboxState struct {
	state types.State
	runID string
}

func (o *Orchestrator) StartHostObservations(parent context.Context, logger *slog.Logger) func() {
	ctx, cancel := context.WithCancel(parent)
	h := &conductorHostObserver{
		publisher: hostmetrics.New("node-ctl", "conductor", map[string]string{"node.id": o.cfg.Cluster.NodeID}),
		events:    make(chan hostmetrics.Group, 256), states: make(map[string]observedSandboxState),
	}
	o.hostObserver = h
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case first := <-h.events:
				groups := []hostmetrics.Group{first}
				for pending := min(len(h.events), 63); pending > 0; pending-- {
					groups = append(groups, <-h.events)
				}
				if h.publisher.Available() {
					if err := h.publisher.EmitBatches(ctx, groups); err != nil && logger != nil {
						logger.Debug("conductor host observation unavailable", "err", err)
					}
				}
			case <-ticker.C:
				if o.vs != nil && o.cfg.Sandbox.Network.Switch != "" && h.publisher.Available() {
					window, stop := context.WithTimeout(ctx, 5*time.Second)
					o.collectHostNetworkMetrics(window, h.publisher, logger)
					stop()
				}
			}
		}
	}()
	return func() { cancel(); <-done }
}

func (h *conductorHostObserver) offer(group hostmetrics.Group) bool {
	select {
	case h.events <- group:
		return true
	default:
		return false
	}
}

func sandboxMetricResource(sb *types.Sandbox) map[string]string {
	attrs := map[string]string{"sandbox.id": sb.ID, "sandbox.stable_id": sb.StableID()}
	if sb.RunID != "" {
		attrs["sandbox.run_id"] = sb.RunID
	}
	return attrs
}

func sandboxStateObservation(sb *types.Sandbox, state string, observed time.Time) hostmetrics.Group {
	return hostmetrics.Group{Resource: sandboxMetricResource(sb), Samples: []hostmetrics.Sample{{
		Name: "sandbox.lifecycle.state", Unit: "1", Timestamp: observed, Value: 1,
		Attributes: map[string]string{"state": state},
	}}}
}

func (o *Orchestrator) observeHostStateLocked(sb *types.Sandbox) {
	h := o.hostObserver
	if h == nil || !types.ValidLocalSandboxID(sb.ID) {
		return
	}
	state := observedSandboxState{sb.State, sb.RunID}
	if prev, exists := h.states[sb.ID]; exists && prev == state {
		return
	}
	if h.offer(sandboxStateObservation(sb, string(sb.State), time.Now())) {
		h.states[sb.ID] = state
	}
}

// The caller owns the allocation fence until physical detach and any durable
// ownership clear. The trusted snapshot is discarded on stale identity or
// unavailable native counters. Only the background worker contacts Telemetry.
func (o *Orchestrator) detachObservedSandboxPortLocked(ctx context.Context, port string, sb *types.Sandbox, runID string) error {
	if h := o.hostObserver; h != nil && sb != nil && sb.VswitchPort == port && h.publisher.Available() {
		readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		rows, err := o.readNetworkTrafficWithFence(readCtx, []*types.Sandbox{sb}, true)
		cancel()
		if err == nil {
			if group, ok := o.networkObservation(sb, rows[0], time.Now()); ok {
				if runID != "" {
					group.Resource["sandbox.run_id"] = runID
				}
				h.offer(group)
			}
		}
	}
	return o.vs.Detach(ctx, port)
}

func networkMetricEligible(sb *types.Sandbox) bool {
	if sb == nil || sb.VswitchPort == "" {
		return false
	}
	switch sb.State {
	case types.StateStarting, types.StateRunning, types.StatePausing, types.StatePaused:
		return true
	default:
		return false
	}
}

// The public default List projection intentionally omits Starting sandboxes,
// but network attachment may already be active during Starting. Include both
// views without adding a second observer or changing public List semantics.
func (o *Orchestrator) collectHostNetworkMetrics(ctx context.Context, sender *hostmetrics.Publisher, logger *slog.Logger) {
	for _, state := range []string{string(types.StateStarting), ""} {
		cursor := ""
		for {
			if ctx.Err() != nil {
				return
			}
			rows, next, err := o.st.List(ctx, state, "", 100, cursor)
			if err != nil {
				if logger != nil {
					logger.Debug("network telemetry inventory unavailable", "state", state, "err", err)
				}
				return
			}
			candidates := make([]*types.Sandbox, 0, len(rows))
			for _, sb := range rows {
				if networkMetricEligible(sb) {
					candidates = append(candidates, sb)
				}
			}
			for i := 0; i < len(candidates); i += 32 {
				end := min(i+32, len(candidates))
				groups := o.readHostNetworkBatch(ctx, candidates[i:end])
				if len(groups) == 0 {
					continue
				}
				if err := sender.EmitBatches(ctx, groups); err != nil {
					if logger != nil {
						logger.Debug("network OTLP unavailable", "err", err)
					}
					return
				}
			}
			if next == "" {
				break
			}
			cursor = next
		}
	}
}

// Network detach can invalidate only a subset of a batch. Retry split halves,
// not one failing 32-port batch or unbounded per-port polling.
func (o *Orchestrator) readHostNetworkBatch(ctx context.Context, objects []*types.Sandbox) []hostmetrics.Group {
	if len(objects) == 0 || ctx.Err() != nil {
		return nil
	}
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	network, err := o.readNetworkTraffic(readCtx, objects)
	cancel()
	if err != nil {
		if len(objects) == 1 || (!errors.Is(err, errNetworkBinding) && !errors.Is(err, errStatsBindingChanged)) {
			return nil
		}
		half := len(objects) / 2
		out := o.readHostNetworkBatch(ctx, objects[:half])
		return append(out, o.readHostNetworkBatch(ctx, objects[half:])...)
	}
	observed := time.Now()
	out := make([]hostmetrics.Group, 0, len(objects))
	for i, sb := range objects {
		if group, ok := o.networkObservation(sb, network[i], observed); ok {
			out = append(out, group)
		}
	}
	return out
}

func (o *Orchestrator) networkObservation(sb *types.Sandbox, network trafficNetwork, observed time.Time) (hostmetrics.Group, bool) {
	// A port and its addresses can be reused. Include its observed attachment
	// generation and require the connector's MAC to match the durable owner.
	want, err := net.ParseMAC(sb.PortMAC)
	got, gotErr := net.ParseMAC(network.portMAC)
	if err != nil || gotErr != nil || want.String() != got.String() {
		return hostmetrics.Group{}, false
	}
	samples := networkCounterSamples("platform", network.platform, observed)
	samples = append(samples, networkCounterSamples("transit", network.transit, observed)...)
	if len(samples) == 0 {
		return hostmetrics.Group{}, false
	}
	attrs := sandboxMetricResource(sb)
	attrs["switch"] = o.cfg.Sandbox.Network.Switch
	attrs["port"] = sb.VswitchPort
	attrs["attachment.generation"] = strconv.FormatUint(uint64(network.generation), 10)
	return hostmetrics.Group{Resource: attrs, Samples: samples}, true
}
