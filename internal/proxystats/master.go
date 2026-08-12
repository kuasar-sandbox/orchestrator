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
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
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

type runMarker struct {
	runID string
	since timePoint
}

// MasterStats holds continuously aggregated worker contributions. Public reads
// touch this cache only; they never query worker processes.
type MasterStats struct {
	mu sync.Mutex

	metrics  *metrics.M
	expected map[string]struct{}
	workers  map[string]*workerContribution
	traffic  map[string]aggregateTraffic

	now        func() timePoint
	trustSince timePoint
	runSince   map[string]runMarker
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
	return &MasterStats{
		metrics: mx, expected: expected, workers: make(map[string]*workerContribution),
		traffic: make(map[string]aggregateTraffic), now: currentTimePoint,
		trustSince: now, runSince: make(map[string]runMarker),
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
		if frame.Counters != nil {
			for name := range worker.counters {
				if _, present := frame.Counters[name]; !present {
					return fmt.Errorf("proxystats: counter snapshot omitted %q", name)
				}
			}
		}
		for name, value := range frame.Counters {
			if value > math.MaxInt64 {
				return fmt.Errorf("proxystats: counter %q exceeds supported range", name)
			}
			previous := worker.counters[name]
			if value < previous {
				return fmt.Errorf("proxystats: counter %q regressed", name)
			}
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
			if math.MaxUint64-current.parking < state.Parking || math.MaxUint64-current.egress < state.Egress {
				return fmt.Errorf("proxystats: aggregate overflow for sandbox %q", sandboxID)
			}
			current.parking += state.Parking
			current.egress += state.Egress
			current.idle = laterPoint(current.idle, pointFromSnapshot(state))
			aggregate.services[service] = current
		}
	}
	if !found {
		delete(m.traffic, sandboxID)
		delete(m.runSince, sandboxID)
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
func (m *MasterStats) SandboxTrafficStats(_ context.Context, sandboxID, runID string, profile types.Profile, state types.State) (*api.TrafficStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.availableLocked() {
		return nil, api.ErrStatsUnavailable
	}
	if state != types.StateStarting && state != types.StateRunning && state != types.StatePaused {
		return nil, api.ErrStatsConflict
	}
	services, ok := profileServices(profile)
	if !ok {
		return nil, api.ErrStatsUnavailable
	}
	now := m.now()
	marker := m.runSince[sandboxID]
	if state == types.StateRunning {
		if marker.runID != runID || marker.since.bootNS == 0 {
			marker = runMarker{runID: runID, since: now}
			m.runSince[sandboxID] = marker
		}
	} else if marker.runID != runID {
		// starting/paused observations cannot establish when this run became
		// running. Drop an older run marker and let the first running query set
		// the conservative lower bound.
		delete(m.runSince, sandboxID)
		marker = runMarker{}
	}
	baseline := laterPoint(m.trustSince, marker.since)
	aggregate := m.traffic[sandboxID]
	result := &api.TrafficStats{
		State: string(state), Services: make(map[string]api.ServiceTrafficStats, len(services)),
	}
	var topIdle timePoint
	for _, service := range services {
		current := aggregate.services[service]
		item := api.ServiceTrafficStats{Parking: current.parking, Egress: current.egress}
		result.Inflight.Parking += current.parking
		result.Inflight.Egress += current.egress
		if current.parking == 0 && current.egress == 0 {
			idle := laterPoint(baseline, current.idle)
			wall := idle.wall.UTC()
			item.IdleSince = &wall
			topIdle = laterPoint(topIdle, idle)
		}
		result.Services[service] = item
	}
	if state == types.StateRunning && result.Inflight.Parking == 0 && result.Inflight.Egress == 0 {
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
		}, true
	case types.ProfileBare:
		return []string{string(proxy.ConnectServiceForward), string(proxy.ConnectServiceExec)}, true
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

// RunGC bounds query-created run markers for sandboxes that never produced a
// worker traffic entry (and therefore cannot later emit a remove frame). Route
// lookup runs without the aggregate lock; a delete/recreate race can only drop a
// marker and make the next idle boundary more conservative.
func (m *MasterStats) RunGC(ctx context.Context, routeExists func(string) bool, interval time.Duration) {
	if routeExists == nil {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pruneRunMarkers(routeExists)
		}
	}
}

func (m *MasterStats) pruneRunMarkers(routeExists func(string) bool) {
	m.mu.Lock()
	ids := make([]string, 0, len(m.runSince))
	for sandboxID := range m.runSince {
		ids = append(ids, sandboxID)
	}
	m.mu.Unlock()
	for _, sandboxID := range ids {
		if routeExists(sandboxID) {
			continue
		}
		m.mu.Lock()
		delete(m.runSince, sandboxID)
		m.mu.Unlock()
	}
}
