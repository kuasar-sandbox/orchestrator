package proxyapp

import "github.com/kuasar-sandbox/orchestrator/internal/routesync"

func (s *readinessSink) ApplyDelete(sid string) {
	s.Sink.ApplyDelete(sid)
	s.instance.mu.Lock()
	delete(s.instance.activityRoutes, sid)
	s.instance.mu.Unlock()
}
func (s *readinessSink) ActivitySnapshot() []routesync.Activity {
	i := s.instance
	i.mu.Lock()
	ready := i.status.Ready && i.status.RoutesSynced
	routes := make([]routesync.RouteEntry, 0, len(i.activityRoutes))
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
