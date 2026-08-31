package proxyext

import (
	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// Host is the internal implementation of the public proxy master Host.
type Host struct {
	routes  *routeSource
	traffic *trafficSource
}

// ObservingSink applies every mutation to the core route view before updating
// the extension projection. Its fan-out is bounded and never executes a user
// callback on the route-sync goroutine.
type ObservingSink struct {
	core   routesync.Sink
	routes *routeSource
	table  *proxyshm.Table
}

// New constructs a master Host and an observing route-sync sink. It starts no
// goroutines; Watch executes in the Extension-owned calling goroutine.
func New(core routesync.Sink, table *proxyshm.Table, stats *proxystats.MasterStats) (*Host, *ObservingSink) {
	return newWithCapacity(core, table, stats, defaultQueueCapacity)
}

func newWithCapacity(core routesync.Sink, table *proxyshm.Table, stats *proxystats.MasterStats, capacity int) (*Host, *ObservingSink) {
	routes := newRouteSource(capacity)
	host := &Host{routes: routes, traffic: &trafficSource{routes: routes, stats: stats}}
	return host, &ObservingSink{core: core, routes: routes, table: table}
}

func (h *Host) Routes() proxyextension.RouteSource    { return h.routes }
func (h *Host) Traffic() proxyextension.TrafficSource { return h.traffic }

func (s *ObservingSink) BeginSync() {
	s.core.BeginSync()
	s.routes.beginSync()
}

func (s *ObservingSink) ApplyUpsert(route routesync.RouteEntry) error {
	if err := s.core.ApplyUpsert(route); err != nil {
		return err
	}
	_, _, revision := s.table.LookupRevision(route.SandboxID)
	s.routes.upsert(route, revision)
	return nil
}

func (s *ObservingSink) ApplyDelete(sandboxID string) {
	s.core.ApplyDelete(sandboxID)
	_, _, revision := s.table.LookupRevision(sandboxID)
	s.routes.delete(sandboxID, revision)
}

func (s *ObservingSink) Bookmark() {
	s.core.Bookmark()
	s.routes.bookmark()
}

func (s *ObservingSink) SetPolicy(policy routesync.Policy) { s.core.SetPolicy(policy) }

func (s *ObservingSink) InvalidateSync() {
	if invalidatable, ok := s.core.(routesync.InvalidatableSink); ok {
		invalidatable.InvalidateSync()
	}
	s.routes.invalidate()
}
