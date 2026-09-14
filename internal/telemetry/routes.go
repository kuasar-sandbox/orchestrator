package telemetry

import (
	"context"
	"errors"
	"sync"

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
	generation uint64
	changed    chan struct{}
	schedules  map[*scrapeSchedule]struct{}
}

type target struct {
	route      routesync.RouteEntry
	ctx        context.Context
	cancel     context.CancelFunc
	generation uint64
}

func NewView(capacity int) *View {
	return &View{byID: make(map[string]*target), byIP: make(map[uint32]string), capacity: capacity, changed: make(chan struct{}), schedules: make(map[*scrapeSchedule]struct{})}
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
	for schedule := range v.schedules {
		schedule.clear()
	}
	v.notify()
}

func (v *View) SetPolicy(routesync.Policy) {}

func (v *View) Bookmark() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.synced {
		return
	}
	v.synced = true
	for schedule := range v.schedules {
		schedule.clear()
		for _, entry := range v.byID {
			schedule.update(entry.route.SandboxID, entry)
		}
	}
	v.notify()
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
	entry := &target{route: route, ctx: ctx, cancel: cancel, generation: v.generation}
	v.byID[route.SandboxID] = entry
	if ip, ok := routeidentity.IPv4(route.FloatingIP); ok && routeidentity.Active(route.State) {
		// Same last-upsert ownership rule as the MMDS reverse index. A delayed
		// delete of the previous owner cannot remove this successor mapping.
		if previous := v.byID[v.byIP[ip]]; previous != nil && previous != entry {
			previous.cancel()
			for schedule := range v.schedules {
				schedule.update(previous.route.SandboxID, nil)
			}
		}
		v.byIP[ip] = route.SandboxID
	}
	if v.synced {
		for schedule := range v.schedules {
			schedule.update(route.SandboxID, entry)
		}
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
	v.notify()
}

func (v *View) removeLocked(id string) {
	entry := v.byID[id]
	if entry == nil {
		return
	}
	entry.cancel()
	if ip, ok := routeidentity.IPv4(entry.route.FloatingIP); ok && v.byIP[ip] == id {
		delete(v.byIP, ip)
	}
	delete(v.byID, id)
	for schedule := range v.schedules {
		schedule.update(id, nil)
	}
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
	// Linearize source acceptance with route invalidation. The callback only
	// validates/enriches bounded pdata; it must not call a downstream consumer.
	return operation()
}

// notify broadcasts existing RouteEntry changes to every receiver instance.
// Callers hold mu; this carries no metric samples or alternate target protocol.
func (v *View) notify() {
	close(v.changed)
	v.changed = make(chan struct{})
}

// snapshot contains exact discovered objects, including paused objects whose
// saved native usage can be read. Source-specific eligibility belongs to the
// receiver; conductor remains authoritative for native stats.
func (v *View) snapshot() ([]*target, <-chan struct{}) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if !v.synced {
		return nil, v.changed
	}
	out := make([]*target, 0, len(v.byID))
	for _, entry := range v.byID {
		out = append(out, entry)
	}
	return out, v.changed
}

var _ routesync.Sink = (*View)(nil)
var _ routesync.InvalidatableSink = (*View)(nil)
