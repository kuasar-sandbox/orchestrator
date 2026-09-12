package telemetry

import (
	"container/heap"
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routeidentity"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

var ErrIdentity = errors.New("telemetry: current sandbox identity unavailable")

// View consumes the existing complete RouteEntry stream. It retains only the
// fields telemetry uses, never credentials for other planes or run identity.
// All authoritative reads and reverse-index changes share one lock.
type View struct {
	mu         sync.RWMutex
	byID       map[string]*target
	byIP       map[uint32]string
	synced     bool
	capacity   int
	interval   time.Duration
	generation uint64
	due        targetHeap
	changed    chan struct{}
}

type target struct {
	route      routesync.RouteEntry
	ctx        context.Context
	cancel     context.CancelFunc
	generation uint64
	next       time.Time
	index      int
	busy       bool
}

func NewView(capacity int, interval time.Duration) *View {
	return &View{byID: make(map[string]*target), byIP: make(map[uint32]string), capacity: capacity, interval: interval, changed: make(chan struct{}, 1)}
}

func (v *View) BeginSync() { v.InvalidateSync() }

func (v *View) InvalidateSync() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.synced = false
	for _, entry := range v.byID {
		entry.cancel()
	}
	clear(v.byID)
	clear(v.byIP)
	v.due = nil
	v.notify()
}

func (v *View) SetPolicy(routesync.Policy) {}

func (v *View) Bookmark() {
	v.mu.Lock()
	v.synced = true
	v.notify()
	v.mu.Unlock()
}

func telemetryRoute(r routesync.RouteEntry) routesync.RouteEntry {
	return routesync.RouteEntry{SandboxID: r.SandboxID, StableID: r.StableID, Profile: r.Profile,
		State: r.State, EnvdUDS: r.EnvdUDS, EnvdAccessToken: r.EnvdAccessToken, FloatingIP: r.FloatingIP}
}

func (v *View) ApplyUpsert(route routesync.RouteEntry) error {
	route = telemetryRoute(route)
	if route.SandboxID == "" || len(route.SandboxID) > 128 || len(route.StableID) > 128 || len(route.EnvdUDS) > 107 || len(route.EnvdAccessToken) > 4096 {
		return errors.New("telemetry: invalid route fields")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	old := v.byID[route.SandboxID]
	if old != nil && old.route == route && old.ctx.Err() == nil {
		return nil
	}
	if old == nil && len(v.byID) >= v.capacity {
		return errors.New("telemetry: route capacity exhausted")
	}
	v.removeLocked(route.SandboxID)
	v.generation++
	ctx, cancel := context.WithCancel(context.Background())
	entry := &target{route: route, ctx: ctx, cancel: cancel, generation: v.generation, index: -1}
	v.byID[route.SandboxID] = entry
	if ip, ok := routeidentity.IPv4(route.FloatingIP); ok && routeidentity.Active(route.State) {
		// Same last-upsert ownership rule as the MMDS reverse index. A delayed
		// delete of the previous owner cannot remove this successor mapping.
		if previous := v.byID[v.byIP[ip]]; previous != nil && previous != entry {
			previous.cancel()
			if previous.index >= 0 {
				heap.Remove(&v.due, previous.index)
			}
		}
		v.byIP[ip] = route.SandboxID
	}
	if scrapeEligible(route) {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(route.SandboxID))
		// Spread a full-sync across the interval rather than producing a burst.
		entry.next = time.Now().Add(time.Duration(hash.Sum64() % uint64(v.interval)))
		heap.Push(&v.due, entry)
	}
	v.notify()
	return nil
}

func scrapeEligible(route routesync.RouteEntry) bool {
	return route.Profile == "e2b" && route.State == routesync.StateRunning && route.EnvdUDS != "" && route.StableID != ""
}

func (v *View) ApplyDelete(id string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.removeLocked(id)
}

func (v *View) removeLocked(id string) {
	entry := v.byID[id]
	if entry == nil {
		return
	}
	entry.cancel()
	if entry.index >= 0 {
		heap.Remove(&v.due, entry.index)
	}
	if ip, ok := routeidentity.IPv4(entry.route.FloatingIP); ok && v.byIP[ip] == id {
		delete(v.byIP, ip)
	}
	delete(v.byID, id)
}

// ByFloatingIP follows MMDS: parse an IPv4 (including IPv4-mapped IPv6), look
// up its candidate SID, then validate the current primary route. Unsynced views
// and inactive routes fail closed.
func (v *View) ByFloatingIP(raw string) (string, bool) {
	entry, ok := v.peerTarget(raw)
	if !ok {
		return "", false
	}
	return entry.route.SandboxID, true
}

func (v *View) peerTarget(raw string) (*target, bool) {
	ip, ok := routeidentity.IPv4(raw)
	if !ok {
		return nil, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	entry := v.byID[v.byIP[ip]]
	if !v.synced || entry == nil || !routeidentity.Matches(entry.route, ip) || entry.route.StableID == "" {
		return nil, false
	}
	return entry, true
}

func (v *View) withCurrent(entry *target, operation func() error) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if !v.synced || entry == nil || entry.ctx.Err() != nil || !routeidentity.Active(entry.route.State) || v.byID[entry.route.SandboxID] != entry {
		return ErrIdentity
	}
	if ip, ok := routeidentity.IPv4(entry.route.FloatingIP); ok && v.byIP[ip] != entry.route.SandboxID {
		return ErrIdentity
	}
	// Linearize the final Collector delivery with route invalidation. A scrape
	// response returned after cancellation can never reach storage/exporters.
	return operation()
}

func (v *View) takeDue(now time.Time, limit int) []*target {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.synced {
		return nil
	}
	var jobs []*target
	for len(jobs) < limit && len(v.due) != 0 && !v.due[0].next.After(now) {
		entry := heap.Pop(&v.due).(*target)
		if !entry.busy {
			entry.busy = true
			jobs = append(jobs, entry)
		}
		entry.next = now.Add(v.interval)
		heap.Push(&v.due, entry)
	}
	return jobs
}

func (v *View) finished(entry *target) {
	v.mu.Lock()
	entry.busy = false
	v.mu.Unlock()
	v.notify()
}

func (v *View) notify() {
	select {
	case v.changed <- struct{}{}:
	default:
	}
}
func (v *View) nextDelay(now time.Time) time.Duration {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if !v.synced || len(v.due) == 0 {
		return time.Hour
	}
	return max(time.Millisecond, v.due[0].next.Sub(now))
}

type targetHeap []*target

func (h targetHeap) Len() int           { return len(h) }
func (h targetHeap) Less(i, j int) bool { return h[i].next.Before(h[j].next) }
func (h targetHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *targetHeap) Push(value any) {
	entry := value.(*target)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *targetHeap) Pop() any {
	old := *h
	entry := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	entry.index = -1
	return entry
}

var _ routesync.Sink = (*View)(nil)
var _ routesync.InvalidatableSink = (*View)(nil)
