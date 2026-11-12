package proxystats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type workerContribution struct {
	epoch     uint64
	sequence  uint64
	hello     bool
	ready     bool
	faulted   bool
	counters  map[string]uint64
	traffic   map[string]SandboxSnapshot
	lastFrame []byte
}

type aggregateTraffic struct {
	services map[string]aggregateService
}

type aggregateService struct {
	parking uint64
	egress  uint64
	idle    timePoint
}

// routeObservation comes from the ordered authority stream, never a stats query.
// Its hold contribution is independent of how many workers serve the sandbox.
type routeObservation struct {
	runID, state string
	since        timePoint // current run's running baseline; preserved through its pause gate
	hold         aggregateService
	holdKnown    bool
}

// MasterStats holds continuously aggregated worker contributions. Public reads
// touch this cache only; they never query worker processes.
type MasterStats struct {
	// InplaceRestart is set before sharing the master with readers. It reports
	// operation state only; no other instance's counters are collected.
	InplaceRestart func() bool
	mu             sync.Mutex

	metrics  *metrics.M
	expected map[string]struct{}
	workers  map[string]*workerContribution
	traffic  map[string]aggregateTraffic

	clock            idleclock.Proof
	clockWatch       clockWatcher
	clockUnavailable bool
	now              func() timePoint
	trustSince       timePoint
	routes           map[string]routeObservation
	physicalOnly     bool // confirmed v11 session without negotiated hold support
}

func NewMasterStats(mx *metrics.M, workerIDs []string) *MasterStats {
	if mx == nil {
		mx = metrics.New()
	}
	expected := make(map[string]struct{}, len(workerIDs))
	for _, workerID := range workerIDs {
		if workerID != "" {
			expected[workerID] = struct{}{}
		}
	}
	now := currentTimePoint()
	watch, watchErr := idleclock.NewWatch()
	return &MasterStats{
		clockWatch: watch, clockUnavailable: watchErr != nil,
		metrics: mx, expected: expected, workers: make(map[string]*workerContribution),
		traffic: make(map[string]aggregateTraffic), now: currentTimePoint,
		trustSince: now, routes: make(map[string]routeObservation),
	}
}

// BeginWorker marks a configured worker unavailable before process start. A
// replacement cannot make stats available until its new epoch sends ready.
func (m *MasterStats) BeginWorker(workerID string, epoch uint64) error {
	if workerID == "" || epoch == 0 {
		return errors.New("proxystats: worker id and epoch are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, expected := m.expected[workerID]; !expected {
		return fmt.Errorf("proxystats: unexpected worker %q", workerID)
	}
	if current := m.workers[workerID]; current != nil {
		return fmt.Errorf("proxystats: worker %q contribution still active", workerID)
	}
	m.workers[workerID] = &workerContribution{
		epoch: epoch, counters: make(map[string]uint64), traffic: make(map[string]SandboxSnapshot),
	}
	m.trustSince = laterPoint(m.trustSince, m.now())
	return nil
}

// Receive applies one absolute frame for the worker/epoch selected by the
// supervisor. Equal sequence numbers are harmless duplicate delivery; lower
// sequence numbers and same-epoch counter regressions are protocol failures.
func (m *MasterStats) Receive(workerID string, epoch uint64, frame Frame) error {
	if err := validateFrame(frame); err != nil {
		return err
	}
	encoded, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("proxystats: encode validated frame: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	wasAvailable := m.availableLocked()
	worker := m.workers[workerID]
	if worker == nil || worker.epoch != epoch || frame.Epoch != epoch {
		return fmt.Errorf("proxystats: worker %q epoch mismatch", workerID)
	}
	if frame.Type == TypeHello {
		if frame.WorkerID != workerID {
			return fmt.Errorf("proxystats: hello worker %q, expected %q", frame.WorkerID, workerID)
		}
		if worker.hello {
			return errors.New("proxystats: duplicate hello")
		}
		worker.hello = true
		return nil
	}
	if !worker.hello {
		return errors.New("proxystats: frame before hello")
	}
	if frame.Sequence < worker.sequence {
		return fmt.Errorf("proxystats: sequence regressed from %d to %d", worker.sequence, frame.Sequence)
	}
	if frame.Sequence == worker.sequence {
		if bytes.Equal(encoded, worker.lastFrame) {
			return nil
		}
		return fmt.Errorf("proxystats: sequence %d was reused with different content", frame.Sequence)
	}
	if worker.sequence == 0 && (frame.Type != TypeReady || frame.Sequence != 1) {
		return errors.New("proxystats: first post-hello frame must be ready seq=1")
	}
	if worker.sequence != 0 && frame.Sequence != worker.sequence+1 {
		return fmt.Errorf("proxystats: sequence gap from %d to %d", worker.sequence, frame.Sequence)
	}

	affected := make(map[string]struct{})
	switch frame.Type {
	case TypeReady:
		if worker.ready || worker.faulted {
			return errors.New("proxystats: unexpected ready frame")
		}
		worker.ready = true
	case TypeUpdate:
		if !worker.ready || worker.faulted {
			return errors.New("proxystats: update from unavailable worker")
		}
		// Validate the complete absolute counter snapshot before changing either
		// the worker contribution or the process-wide Prometheus counters. A
		// rejected frame must not leave a prefix of map-iteration-order deltas
		// applied, because the worker is terminated after a stream fault and
		// cumulative Prometheus counters cannot be rolled back on exit.
		if frame.Counters != nil {
			for name := range worker.counters {
				if _, present := frame.Counters[name]; !present {
					return fmt.Errorf("proxystats: counter snapshot omitted %q", name)
				}
			}
			for name, value := range frame.Counters {
				if value > math.MaxInt64 {
					return fmt.Errorf("proxystats: counter %q exceeds supported range", name)
				}
				if value < worker.counters[name] {
					return fmt.Errorf("proxystats: counter %q regressed", name)
				}
			}
		}
		for name, value := range frame.Counters {
			previous := worker.counters[name]
			if delta := value - previous; delta != 0 {
				m.metrics.Add(name, int64(delta))
			}
			worker.counters[name] = value
		}
		for _, snapshot := range frame.Traffic {
			worker.traffic[snapshot.SandboxID] = cloneSandboxSnapshot(snapshot)
			affected[snapshot.SandboxID] = struct{}{}
		}
	case TypeRemove:
		if !worker.ready || worker.faulted {
			return errors.New("proxystats: remove from unavailable worker")
		}
		for _, sandboxID := range frame.SandboxIDs {
			delete(worker.traffic, sandboxID)
			affected[sandboxID] = struct{}{}
		}
	case TypeGoodbye:
		worker.ready = false
		worker.faulted = true
	default:
		return fmt.Errorf("proxystats: unexpected frame type %q", frame.Type)
	}
	worker.sequence = frame.Sequence
	worker.lastFrame = encoded
	for sandboxID := range affected {
		if err := m.recomputeLocked(sandboxID); err != nil {
			return err
		}
	}
	if !wasAvailable && m.availableLocked() {
		m.trustSince = laterPoint(m.trustSince, m.now())
	}
	return nil
}

func cloneSandboxSnapshot(snapshot SandboxSnapshot) SandboxSnapshot {
	clone := SandboxSnapshot{SandboxID: snapshot.SandboxID, Services: make(map[string]ServiceSnapshot, len(snapshot.Services))}
	for service, state := range snapshot.Services {
		if state.IdleSince != nil {
			wall := state.IdleSince.UTC()
			state.IdleSince = &wall
		}
		clone.Services[service] = state
	}
	return clone
}

func (m *MasterStats) recomputeLocked(sandboxID string) error {
	aggregate := aggregateTraffic{services: make(map[string]aggregateService)}
	found := false
	for _, worker := range m.workers {
		snapshot, present := worker.traffic[sandboxID]
		if !present {
			continue
		}
		found = true
		for service, state := range snapshot.Services {
			current := aggregate.services[service]
			if math.MaxUint64-current.parking < state.Parking || math.MaxUint64-current.egress < state.Connected {
				return fmt.Errorf("proxystats: aggregate overflow for sandbox %q", sandboxID)
			}
			current.parking += state.Parking
			current.egress += state.Connected
			current.idle = laterPoint(current.idle, pointFromSnapshot(state))
			aggregate.services[service] = current
		}
	}
	if !found {
		delete(m.traffic, sandboxID)
		return nil
	}
	m.traffic[sandboxID] = aggregate
	return nil
}

// StreamFault enters the required fail-closed window while the supervisor is
// terminating a worker whose backend FDs may still be alive.
func (m *MasterStats) StreamFault(workerID string, epoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if worker := m.workers[workerID]; worker != nil && worker.epoch == epoch {
		worker.faulted = true
		worker.ready = false
	}
}

// WorkerExited is called only after cmd.Wait confirms process exit. Kernel FD
// closure then permits removal of all of that worker's contributions.
func (m *MasterStats) WorkerExited(workerID string, epoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	worker := m.workers[workerID]
	if worker == nil || worker.epoch != epoch {
		return
	}
	affected := make([]string, 0, len(worker.traffic))
	for sandboxID := range worker.traffic {
		affected = append(affected, sandboxID)
	}
	delete(m.workers, workerID)
	for _, sandboxID := range affected {
		_ = m.recomputeLocked(sandboxID)
	}
	m.trustSince = laterPoint(m.trustSince, m.now())
}

func (m *MasterStats) availableLocked() bool {
	if len(m.expected) == 0 {
		return false
	}
	for workerID := range m.expected {
		worker := m.workers[workerID]
		if worker == nil || !worker.hello || !worker.ready || worker.faulted {
			return false
		}
	}
	return true
}

func (m *MasterStats) Available() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.availableLocked()
}

// SandboxTrafficStats implements orch.SandboxTrafficProvider structurally.
func (m *MasterStats) SandboxTrafficStats(ctx context.Context, sandboxID, runID string, profile types.Profile, state types.State) (result *api.TrafficStats, returnErr error) {
	physicalView := IsPhysicalTelemetry(ctx)
	defer func() {
		if result != nil && m.InplaceRestart != nil {
			result.InplaceRestart = m.InplaceRestart()
			if physicalView && result.InplaceRestart {
				result.IdleSince = nil
			}
		}
	}()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.availableLocked() {
		return nil, api.ErrStatsUnavailable
	}
	if state != types.StateStarting && state != types.StateRunning && state != types.StatePaused && state != types.StatePausing {
		return nil, api.ErrStatsConflict
	}
	route := m.routes[sandboxID]
	if (!m.physicalOnly && !route.holdKnown) || route.runID != runID || route.state != string(state) {
		return nil, api.ErrStatsUnavailable
	}
	services, ok := profileServices(profile)
	if !ok {
		return nil, api.ErrStatsUnavailable
	}
	// Queries cannot manufacture a trusted zero or move the run baseline. A
	// pre-running / paused route has no running baseline for its service view.
	baseline := route.since
	if baseline.bootNS <= 0 {
		if state == types.StateRunning {
			return nil, api.ErrStatsUnavailable
		}
		baseline = m.now()
	}
	baseline = laterPoint(m.trustSince, baseline)
	aggregate := m.traffic[sandboxID]
	result = &api.TrafficStats{
		State: string(state), Services: make(map[string]api.ServiceTrafficStats, len(services)),
	}
	var topIdle timePoint
	for _, service := range services {
		current := aggregate.services[service]
		if service == "hold" {
			if m.physicalOnly || physicalView {
				continue
			}
			current = route.hold
		}
		if math.MaxUint64-result.Inflight.Connected < current.egress || math.MaxUint64-result.Inflight.Parking < current.parking {
			return nil, api.ErrStatsUnavailable
		}
		item := api.ServiceTrafficStats{Parking: current.parking, Connected: current.egress}
		result.Inflight.Parking += current.parking
		result.Inflight.Connected += current.egress
		if current.parking == 0 && current.egress == 0 {
			idle := laterPoint(baseline, current.idle)
			wall := idle.wall.UTC()
			item.IdleSince = &wall
			topIdle = laterPoint(topIdle, idle)
		}
		result.Services[service] = item
	}
	if state == types.StateRunning && result.Inflight.Parking == 0 && result.Inflight.Connected == 0 {
		wall := topIdle.wall.UTC()
		result.IdleSince = &wall
	}
	return result, nil
}

func profileServices(profile types.Profile) ([]string, bool) {
	switch profile {
	case types.ProfileE2B:
		return []string{
			string(proxy.ConnectServiceForward),
			string(proxy.ConnectServiceE2BEnvd),
			string(proxy.ConnectServiceE2BInterpreter),
			string(proxy.ConnectServiceExec),
			"hold",
		}, true
	case types.ProfileBare:
		return []string{string(proxy.ConnectServiceForward), string(proxy.ConnectServiceExec), "hold"}, true
	default:
		return nil, false
	}
}

// WorkerIDs returns stable diagnostic ordering for tests/supervision without
// exposing worker identity in the public stats response.
func (m *MasterStats) WorkerIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
