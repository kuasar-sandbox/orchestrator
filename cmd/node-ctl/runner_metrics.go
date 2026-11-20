package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/hostmetrics"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	nativeusage "github.com/kuasar-sandbox/sandboxer/pkg/usage"
	"github.com/kuasar-sandbox/sandboxer/pkg/usagereader"
)

type runnerMetricsIdentity struct {
	sandboxID, stableID, runID, runDir, baseDir string
}

type runnerMetrics struct {
	id                 runnerMetricsIdentity
	resource           *hostmetrics.Publisher
	usage              *hostmetrics.Publisher
	lastSaved          uint64
	baselineUnreadable bool
	currentEpoch       string
	log                *slog.Logger
}

func newRunnerMetrics(id runnerMetricsIdentity, log *slog.Logger) *runnerMetrics {
	if id.sandboxID == "" || id.runID == "" || id.runDir == "" {
		return nil
	}
	common := map[string]string{"sandbox.id": id.sandboxID, "sandbox.stable_id": id.stableID}
	runtime := map[string]string{"sandbox.id": id.sandboxID, "sandbox.stable_id": id.stableID, "sandbox.run_id": id.runID}
	return &runnerMetrics{id: id, resource: hostmetrics.New("node-ctl", "runner", runtime), usage: hostmetrics.New("node-ctl", "runner", common), log: log}
}

func (r *runnerMetrics) resourceSample(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 900*time.Millisecond)
	defer cancel()
	obs, err := ctl.ReadResourceStats(ctx, filepath.Join(r.id.runDir, "ctl.sock"), r.id.sandboxID)
	if err != nil {
		return
	}
	now := time.Now()
	samples := []hostmetrics.Sample{
		{Name: "sandbox.resource.cpu.capacity", Unit: "{core}", Value: float64(obs.CPUCapacity), Timestamp: now},
		{Name: "sandbox.resource.cpu.allocatable", Unit: "{core}", Value: obs.CPUAllocatable, Timestamp: now},
		{Name: "sandbox.resource.memory.capacity", Unit: "By", Value: float64(obs.MemoryCapacity), Timestamp: now},
		{Name: "sandbox.resource.memory.headroom", Unit: "By", Value: float64(obs.MemoryHeadroom), Timestamp: now},
	}
	if obs.TimestampUnix != nil {
		stamp := time.Unix(*obs.TimestampUnix, 0)
		if obs.MemoryUsed != nil {
			samples = append(samples, hostmetrics.Sample{Name: "sandbox.resource.memory.used", Unit: "By", Timestamp: stamp, Value: float64(*obs.MemoryUsed)})
		}
		if obs.CPUUsageUsec != nil {
			samples = append(samples, hostmetrics.Sample{Name: "sandbox.resource.cpu.seconds", Unit: "s", Timestamp: stamp, Value: float64(*obs.CPUUsageUsec) / 1e6})
		}
	}
	if err := r.resource.Emit(ctx, samples); err != nil && r.log != nil {
		r.log.Debug("runner resource telemetry unavailable", "sid", r.id.sandboxID, "err", err)
	}
}

// Capture the native file position before the child can open the same logical
// sandbox on resume. A child that exits without writing must not replay its predecessor.
func (r *runnerMetrics) captureUsageBaseline(ctx context.Context) {
	if r.id.baseDir == "" {
		return
	}
	view, err := r.readUsage(ctx, true)
	if err != nil {
		r.baselineUnreadable = !errors.Is(err, os.ErrNotExist)
		return
	}
	if view.Saved != nil {
		r.lastSaved = view.Saved.Sequence
	}
}

func (r *runnerMetrics) readUsage(ctx context.Context, offline bool) (nativeusage.View, error) {
	readCtx, cancel := context.WithTimeout(ctx, 1100*time.Millisecond)
	defer cancel()
	var view nativeusage.View
	opts := usagereader.Options{SandboxID: r.id.sandboxID,
		File: filepath.Join(r.id.baseDir, r.id.sandboxID+".usage"), Saved: offline,
		Limit: 10, Offline: offline}
	if !offline {
		opts.ControlSocket = filepath.Join(r.id.runDir, "ctl.sock")
	}
	body, err := usagereader.Read(readCtx, opts)
	if err != nil {
		return view, err
	}
	err = json.Unmarshal(body, &view)
	return view, err
}

func (r *runnerMetrics) usageSample(ctx context.Context, offline bool) {
	if r.id.baseDir == "" {
		return
	}
	view, err := r.readUsage(ctx, offline)
	if err != nil {
		return
	}
	r.emitUsageView(ctx, view, offline)
}

// The holder retirement path captures the view before acknowledging that a new
// run may own the shared paths. Publishing this value performs no further file
// or ctl.sock reads.
func (r *runnerMetrics) emitUsageView(ctx context.Context, view nativeusage.View, offline bool) {
	if view.Live != nil {
		r.currentEpoch = view.Live.RunEpoch
	}
	// A saved endpoint does not imply complete coverage. Preserve validity
	// status even if the checkpoint itself was already reported. Offline
	// readers have no authoritative Enabled/Saving state: do not fabricate it.
	now := time.Now()
	flag := func(name string, set bool) hostmetrics.Sample {
		v := 0.0
		if set {
			v = 1
		}
		return hostmetrics.Sample{Name: "sandbox.usage." + name, Unit: "1", Timestamp: now, Value: v}
	}
	status := []hostmetrics.Sample{
		flag("saved.available", view.Saved != nil), flag("unknown_tail", view.UnknownTail),
	}
	if !offline {
		status = append(status, flag("enabled", view.Enabled), flag("saving", view.Saving),
			flag("save_error", view.SaveError != ""), flag("read_error", view.ReadError != ""))
	}
	sequence := r.lastSaved
	record := view.Saved
	if shouldPublishUsageEndpoint(view, offline, r.lastSaved) &&
		record.Snapshot.SandboxID == r.id.sandboxID && record.SavedUTC > 0 &&
		(!r.baselineUnreadable || r.currentEpoch != "" && record.Snapshot.RunEpoch == r.currentEpoch) {
		status = appendUsageEndpoint(status, record)
		sequence = record.Sequence
	}
	// Status and saved measurements keep their own timestamps in the same
	// request. A stalled receiver costs at most one send on the exit path.
	err := r.usage.Emit(ctx, status)
	if err == nil || errors.Is(err, hostmetrics.ErrPartialAcceptance) {
		r.lastSaved = sequence
	}
	if err != nil && r.log != nil {
		r.log.Debug("runner usage telemetry unavailable", "sid", r.id.sandboxID, "sequence", sequence, "err", err)
	}
}

func appendUsageEndpoint(samples []hostmetrics.Sample, record *nativeusage.Record) []hostmetrics.Sample {
	timestamp := time.Unix(0, record.SavedUTC)
	for _, c := range record.Snapshot.Counters {
		if !c.SourceKnown && c.KnownTotal == (nativeusage.Uint128{}) {
			continue
		}
		attrs := map[string]string{"usage.name": c.Name, "usage.status": c.Status, "usage.complete": strconv.FormatBool(c.Complete), "usage.source_known": strconv.FormatBool(c.SourceKnown)}
		value := math.Ldexp(float64(c.KnownTotal.Hi), 64) + float64(c.KnownTotal.Lo)
		samples = append(samples, hostmetrics.Sample{Name: "sandbox.usage.cpu.seconds", Unit: "s", Timestamp: timestamp, Value: value / 1e9, Attributes: attrs})
	}
	for _, g := range record.Snapshot.Gauges {
		attrs := map[string]string{"usage.name": g.Name, "usage.status": g.Status, "usage.position_known": strconv.FormatBool(g.PositionKnown), "usage.value_known": strconv.FormatBool(g.ValueKnown)}
		if g.ValueKnown || g.CoveredTotal > 0 || g.IntegralTotal != (nativeusage.Uint128{}) {
			value := math.Ldexp(float64(g.IntegralTotal.Hi), 64) + float64(g.IntegralTotal.Lo)
			samples = append(samples, hostmetrics.Sample{Name: "sandbox.usage.memory.integral", Unit: "By.s", Timestamp: timestamp, Value: value / 1e9, Attributes: attrs})
		}
		if g.ValueKnown || g.SpanTotal > 0 {
			samples = append(samples,
				hostmetrics.Sample{Name: "sandbox.usage.memory.span", Unit: "s", Timestamp: timestamp, Value: float64(g.SpanTotal) / 1e9, Attributes: attrs},
				hostmetrics.Sample{Name: "sandbox.usage.memory.covered", Unit: "s", Timestamp: timestamp, Value: float64(g.CoveredTotal) / 1e9, Attributes: attrs})
		}
	}
	return samples
}

// One sampler per running unit. The existing sandbox-ctl remains the sole Usage accumulator.
func (r *runnerMetrics) sampleUntil(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var count int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count++
			if !r.resource.Available() {
				continue
			}
			r.resourceSample(ctx)
			if count%12 == 0 {
				r.usageSample(ctx, false)
			}
		}
	}
}

// shouldPublishUsageEndpoint distinguishes an earlier saved run's cumulative
// endpoint from this runner's fresh observations, without assuming that native
// RunEpoch equals the orchestrator's RunID. Offline final saved records remain
// authoritative after the child exits; each runner sends any sequence only once.
func shouldPublishUsageEndpoint(view nativeusage.View, offline bool, lastSeq uint64) bool {
	saved := view.Saved
	if saved == nil || saved.Sequence == 0 || saved.Sequence <= lastSeq {
		return false
	}
	if !offline && view.Live != nil && view.Live.RunEpoch != "" &&
		view.Live.RunEpoch != saved.Snapshot.RunEpoch {
		return false
	}
	return true
}
