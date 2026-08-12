package proxystats

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

const trafficShardCount = 32

var allServices = []string{
	string(proxy.ConnectServiceForward),
	string(proxy.ConnectServiceE2BEnvd),
	string(proxy.ConnectServiceE2BInterpreter),
	string(proxy.ConnectServiceExec),
}

func validService(service string) bool {
	for _, candidate := range allServices {
		if service == candidate {
			return true
		}
	}
	return false
}

type trafficShard struct {
	mu      sync.Mutex
	entries map[string]*trafficEntry
}

type trafficEntry struct {
	mu          sync.Mutex
	generation  uint64
	revision    uint64
	lastChanged timePoint
	services    map[string]serviceState
}

type trafficVersion struct {
	generation uint64
	revision   uint64
}

type serviceState struct {
	parking uint64
	egress  uint64
	idle    timePoint
}

type WorkerStats struct {
	shards [trafficShardCount]trafficShard
	now    func() timePoint
	// nextGeneration prevents an entry recreated after GC from reusing the
	// identity captured by an older in-flight batch acknowledgement. It advances
	// only on entry creation; hot-path state changes retain per-entry revisions.
	nextGeneration atomic.Uint64

	counterMu       sync.Mutex
	counters        map[string]uint64
	counterRevision uint64

	dirtyMu          sync.Mutex
	dirtyTraffic     map[string]trafficVersion
	dirtyCountersRev uint64
	removed          map[string]uint64
	removeRevision   uint64
	notify           chan struct{}
}

func NewWorkerStats() *WorkerStats {
	w := &WorkerStats{
		now:          currentTimePoint,
		counters:     make(map[string]uint64),
		dirtyTraffic: make(map[string]trafficVersion),
		removed:      make(map[string]uint64),
		notify:       make(chan struct{}, 1),
	}
	for i := range w.shards {
		w.shards[i].entries = make(map[string]*trafficEntry)
	}
	return w
}

func (w *WorkerStats) Inc(name string) {
	if w == nil || !validCounterName(name) {
		return
	}
	w.counterMu.Lock()
	w.counters[name]++
	w.counterRevision++
	revision := w.counterRevision
	w.counterMu.Unlock()
	w.dirtyMu.Lock()
	if revision > w.dirtyCountersRev {
		w.dirtyCountersRev = revision
	}
	w.dirtyMu.Unlock()
	w.signal()
}

func (w *WorkerStats) BeginParking(sandboxID string, service proxy.ConnectService) proxy.TrafficFlow {
	serviceName := string(service)
	if w == nil || !validSandboxID(sandboxID) || !validService(serviceName) {
		return inertFlow{}
	}
	entry := w.lockEntry(sandboxID, true)
	state := entry.services[serviceName]
	state.parking++
	state.idle = timePoint{}
	entry.services[serviceName] = state
	entry.revision++
	entry.lastChanged = w.now()
	version := trafficVersion{generation: entry.generation, revision: entry.revision}
	entry.mu.Unlock()
	w.markTraffic(sandboxID, version)
	return &trafficFlow{worker: w, sandboxID: sandboxID, service: serviceName, entry: entry, stage: flowParking}
}

func (w *WorkerStats) lockEntry(sandboxID string, create bool) *trafficEntry {
	shard := &w.shards[shardIndex(sandboxID)]
	shard.mu.Lock()
	entry := shard.entries[sandboxID]
	if entry == nil && create {
		entry = &trafficEntry{
			generation:  w.nextGeneration.Add(1),
			services:    make(map[string]serviceState),
			lastChanged: w.now(),
		}
		shard.entries[sandboxID] = entry
	}
	if entry != nil {
		entry.mu.Lock()
	}
	shard.mu.Unlock()
	return entry
}

func shardIndex(s string) int {
	var hash uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= 16777619
	}
	return int(hash % trafficShardCount)
}

func (w *WorkerStats) markTraffic(sandboxID string, version trafficVersion) {
	w.dirtyMu.Lock()
	delete(w.removed, sandboxID)
	current, found := w.dirtyTraffic[sandboxID]
	if !found || version.generation > current.generation ||
		(version.generation == current.generation && version.revision > current.revision) {
		w.dirtyTraffic[sandboxID] = version
	}
	w.dirtyMu.Unlock()
	w.signal()
}

func (w *WorkerStats) markRemoved(sandboxID string) {
	w.dirtyMu.Lock()
	w.removeRevision++
	delete(w.dirtyTraffic, sandboxID)
	w.removed[sandboxID] = w.removeRevision
	w.dirtyMu.Unlock()
	w.signal()
}

func (w *WorkerStats) signal() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

type flowStage uint8

const (
	flowParking flowStage = iota + 1
	flowEgress
	flowClosed
)

type trafficFlow struct {
	mu        sync.Mutex
	worker    *WorkerStats
	sandboxID string
	service   string
	entry     *trafficEntry
	stage     flowStage
}

func (f *trafficFlow) AttachBackend(conn net.Conn) net.Conn {
	if conn == nil {
		return nil
	}
	f.mu.Lock()
	if f.stage != flowParking {
		f.mu.Unlock()
		return conn
	}
	f.entry.mu.Lock()
	state := f.entry.services[f.service]
	if state.parking > 0 {
		state.parking--
	}
	state.egress++
	state.idle = timePoint{}
	f.entry.services[f.service] = state
	f.entry.revision++
	f.entry.lastChanged = f.worker.now()
	version := trafficVersion{generation: f.entry.generation, revision: f.entry.revision}
	f.entry.mu.Unlock()
	f.stage = flowEgress
	f.mu.Unlock()
	f.worker.markTraffic(f.sandboxID, version)
	return &trackedConn{Conn: conn, flow: f}
}

// Close ends only an unattached parking flow. Once a backend is attached, the
// tracked backend's final Close is the sole operation that ends egress.
func (f *trafficFlow) Close() {
	f.mu.Lock()
	if f.stage != flowParking {
		f.mu.Unlock()
		return
	}
	f.entry.mu.Lock()
	state := f.entry.services[f.service]
	if state.parking > 0 {
		state.parking--
	}
	if state.parking == 0 && state.egress == 0 {
		state.idle = f.worker.now()
	}
	f.entry.services[f.service] = state
	f.entry.revision++
	f.entry.lastChanged = f.worker.now()
	version := trafficVersion{generation: f.entry.generation, revision: f.entry.revision}
	f.entry.mu.Unlock()
	f.stage = flowClosed
	f.mu.Unlock()
	f.worker.markTraffic(f.sandboxID, version)
}

func (f *trafficFlow) finishEgress() {
	f.mu.Lock()
	if f.stage != flowEgress {
		f.mu.Unlock()
		return
	}
	f.entry.mu.Lock()
	state := f.entry.services[f.service]
	if state.egress > 0 {
		state.egress--
	}
	if state.parking == 0 && state.egress == 0 {
		state.idle = f.worker.now()
	}
	f.entry.services[f.service] = state
	f.entry.revision++
	f.entry.lastChanged = f.worker.now()
	version := trafficVersion{generation: f.entry.generation, revision: f.entry.revision}
	f.entry.mu.Unlock()
	f.stage = flowClosed
	f.mu.Unlock()
	f.worker.markTraffic(f.sandboxID, version)
}

type trackedConn struct {
	net.Conn
	flow *trafficFlow
	once sync.Once
	err  error
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.flow.finishEgress()
	})
	return c.err
}

func (c *trackedConn) CloseWrite() error {
	if closer, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return closer.CloseWrite()
	}
	return nil
}

func (c *trackedConn) CloseRead() error {
	if closer, ok := c.Conn.(interface{ CloseRead() error }); ok {
		return closer.CloseRead()
	}
	return nil
}

type inertFlow struct{}

func (inertFlow) AttachBackend(conn net.Conn) net.Conn { return conn }
func (inertFlow) Close()                               {}

type pendingBatch struct {
	frame           Frame
	counterRevision uint64
	trafficRevision map[string]trafficVersion
	removeRevision  map[string]uint64
}

func (w *WorkerStats) nextBatch(epoch, sequence uint64) (pendingBatch, bool) {
	w.dirtyMu.Lock()
	if len(w.removed) > 0 {
		ids := sortedKeys(w.removed, MaxFrameSandboxes)
		revisions := make(map[string]uint64, len(ids))
		for _, sid := range ids {
			revisions[sid] = w.removed[sid]
		}
		w.dirtyMu.Unlock()
		return pendingBatch{
			frame:          Frame{Type: TypeRemove, Version: Version, Epoch: epoch, Sequence: sequence, SandboxIDs: ids},
			removeRevision: revisions,
		}, true
	}
	dirtyCounters := w.dirtyCountersRev
	dirtySIDs := sortedKeys(w.dirtyTraffic, MaxFrameSandboxes)
	w.dirtyMu.Unlock()
	if dirtyCounters == 0 && len(dirtySIDs) == 0 {
		return pendingBatch{}, false
	}
	batch := pendingBatch{
		frame:           Frame{Type: TypeUpdate, Version: Version, Epoch: epoch, Sequence: sequence},
		trafficRevision: make(map[string]trafficVersion, len(dirtySIDs)),
	}
	if dirtyCounters != 0 {
		w.counterMu.Lock()
		batch.frame.Counters = cloneCounters(w.counters)
		batch.counterRevision = w.counterRevision
		w.counterMu.Unlock()
	}
	for _, sid := range dirtySIDs {
		entry := w.lockEntry(sid, false)
		if entry == nil {
			continue
		}
		batch.frame.Traffic = append(batch.frame.Traffic, snapshotEntry(sid, entry))
		batch.trafficRevision[sid] = trafficVersion{generation: entry.generation, revision: entry.revision}
		entry.mu.Unlock()
	}
	if len(batch.frame.Counters) == 0 && len(batch.frame.Traffic) == 0 {
		return pendingBatch{}, false
	}
	return batch, true
}

func snapshotEntry(sandboxID string, entry *trafficEntry) SandboxSnapshot {
	snapshot := SandboxSnapshot{SandboxID: sandboxID, Services: make(map[string]ServiceSnapshot, len(entry.services))}
	for service, state := range entry.services {
		item := ServiceSnapshot{Parking: state.parking, Egress: state.egress}
		if state.parking == 0 && state.egress == 0 {
			item.IdleSince, item.IdleSinceBootNS = snapshotIdle(state.idle)
		}
		snapshot.Services[service] = item
	}
	return snapshot
}

func (w *WorkerStats) ack(batch pendingBatch) {
	w.dirtyMu.Lock()
	if batch.counterRevision != 0 && w.dirtyCountersRev == batch.counterRevision {
		w.dirtyCountersRev = 0
	}
	for sid, revision := range batch.trafficRevision {
		if w.dirtyTraffic[sid] == revision {
			delete(w.dirtyTraffic, sid)
		}
	}
	for sid, revision := range batch.removeRevision {
		if w.removed[sid] == revision {
			delete(w.removed, sid)
		}
	}
	w.dirtyMu.Unlock()
}

func cloneCounters(source map[string]uint64) map[string]uint64 {
	result := make(map[string]uint64, len(source))
	for name, value := range source {
		result[name] = value
	}
	return result
}

func sortedKeys[V any](values map[string]V, limit int) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys
}

// StartSender publishes hello+ready synchronously, so callers can defer opening
// listeners until the reporting channel is proven writable. Subsequent updates
// are merged absolute snapshots sent by one background goroutine.
func (w *WorkerStats) StartSender(ctx context.Context, workerID string, epoch uint64, send func(Frame) error) (<-chan error, error) {
	if w == nil || send == nil || workerID == "" || epoch == 0 {
		return nil, fmt.Errorf("proxystats: invalid sender configuration")
	}
	if err := send(Frame{Type: TypeHello, Version: Version, WorkerID: workerID, Epoch: epoch}); err != nil {
		return nil, err
	}
	if err := send(Frame{Type: TypeReady, Version: Version, Epoch: epoch, Sequence: 1}); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- w.runSender(ctx, epoch, 1, send) }()
	return done, nil
}

func (w *WorkerStats) runSender(ctx context.Context, epoch, sequence uint64, send func(Frame) error) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		for {
			batch, ok := w.nextBatch(epoch, sequence+1)
			if !ok {
				break
			}
			if err := send(batch.frame); err != nil {
				return err
			}
			sequence++
			w.ack(batch)
		}
		select {
		case <-ctx.Done():
			_ = send(Frame{Type: TypeGoodbye, Version: Version, Epoch: epoch, Sequence: sequence + 1})
			return nil
		case <-w.notify:
		case <-ticker.C:
		}
	}
}

// RunGC removes long-idle local entries only after the route view confirms the
// SID no longer exists. A remove absolute update is emitted to the master.
func (w *WorkerStats) RunGC(ctx context.Context, routeExists func(string) bool, retention time.Duration) {
	if retention <= 0 {
		retention = 5 * time.Minute
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.gc(routeExists, retention)
		}
	}
}

func (w *WorkerStats) gc(routeExists func(string) bool, retention time.Duration) {
	cutoff := w.now().bootNS - retention.Nanoseconds()
	for shardIndex := range w.shards {
		shard := &w.shards[shardIndex]
		type candidate struct {
			sandboxID string
			entry     *trafficEntry
		}
		shard.mu.Lock()
		candidates := make([]candidate, 0, len(shard.entries))
		for sid, entry := range shard.entries {
			candidates = append(candidates, candidate{sandboxID: sid, entry: entry})
		}
		shard.mu.Unlock()

		for _, candidate := range candidates {
			if routeExists != nil && routeExists(candidate.sandboxID) {
				continue
			}
			// Reacquire in the normal shard→entry order and verify that no
			// delete/recreate happened while routeExists ran without locks.
			shard.mu.Lock()
			entry := shard.entries[candidate.sandboxID]
			if entry != candidate.entry {
				shard.mu.Unlock()
				continue
			}
			entry.mu.Lock()
			idle := entry.lastChanged.bootNS != 0 && entry.lastChanged.bootNS <= cutoff && entryIdle(entry)
			entry.mu.Unlock()
			if idle {
				delete(shard.entries, candidate.sandboxID)
				// Mark the removal before releasing shard.mu. A concurrent
				// BeginParking can then recreate the SID and its later markTraffic
				// deterministically supersedes this remove.
				w.markRemoved(candidate.sandboxID)
			}
			shard.mu.Unlock()
		}
	}
}

func entryIdle(entry *trafficEntry) bool {
	for _, service := range entry.services {
		if service.parking != 0 || service.egress != 0 {
			return false
		}
	}
	return true
}

var _ proxy.Counter = (*WorkerStats)(nil)
var _ proxy.TrafficTracker = (*WorkerStats)(nil)
