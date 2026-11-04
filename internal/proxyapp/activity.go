package proxyapp

import "github.com/kuasar-sandbox/orchestrator/internal/routesync"

func (s *readinessSink) ApplyDelete(sid string) {
	s.Sink.ApplyDelete(sid)
	if s.instance.stats != nil {
		s.instance.stats.ObserveRoute(routesync.RouteEntry{SandboxID: sid, State: routesync.StateDead})
	}
	s.instance.mu.Lock()
	delete(s.instance.activityRoutes, sid)
	s.instance.mu.Unlock()
}
func (s *readinessSink) ActivitySnapshot() []routesync.Activity {
	i := s.instance
	i.mu.Lock()
	ready := i.status.Ready && i.status.RoutesSynced
	var routes []routesync.RouteEntry
	for _, r := range i.activityRoutes {
		if r.IdleTimeout == nil {
			continue
		}
		routes = append(routes, r)
	}
	i.mu.Unlock()
	result := make([]routesync.Activity, 0, len(routes))
	for _, r := range routes {
		a := routesync.Activity{SandboxID: r.SandboxID, RunID: r.RunID, State: "unknown"}
		if ready && i.stats != nil {
			a = i.stats.Activity(r)
		}
		result = append(result, a)
	}
	return result
}
