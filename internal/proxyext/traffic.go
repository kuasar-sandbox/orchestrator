package proxyext

import (
	"context"
	"errors"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type trafficSource struct {
	routes *routeSource
	stats  *proxystats.MasterStats
}

func (s *trafficSource) Get(ctx context.Context, sandboxID string) (proxyextension.TrafficView, error) {
	if err := ctx.Err(); err != nil {
		return proxyextension.TrafficView{}, err
	}
	if s == nil || s.routes == nil || s.stats == nil {
		return proxyextension.TrafficView{}, proxyextension.ErrTrafficUnavailable
	}
	route, maxInflight, found, err := s.routes.getTrafficRoute(ctx, sandboxID)
	if err != nil {
		return proxyextension.TrafficView{}, err
	}
	if !found {
		return proxyextension.TrafficView{}, proxyextension.ErrTrafficUnavailable
	}
	stats, err := s.stats.SandboxTrafficStats(ctx, sandboxID, route.RunID, types.Profile(route.Profile), types.State(route.State))
	if err != nil {
		switch {
		case errors.Is(err, api.ErrStatsConflict):
			return proxyextension.TrafficView{}, proxyextension.ErrTrafficConflict
		case errors.Is(err, api.ErrStatsUnavailable):
			return proxyextension.TrafficView{}, proxyextension.ErrTrafficUnavailable
		default:
			return proxyextension.TrafficView{}, proxyextension.ErrTrafficUnavailable
		}
	}
	if err := ctx.Err(); err != nil {
		return proxyextension.TrafficView{}, err
	}
	view := proxyextension.TrafficView{
		SandboxID: sandboxID, Profile: route.Profile,
		State:       proxyextension.RouteState(stats.State),
		MaxInflight: maxInflight,
		Inflight: proxyextension.TrafficInflight{
			Parking: stats.Inflight.Parking, Connected: stats.Inflight.Connected,
		},
		IdleSince: cloneTime(stats.IdleSince),
		Services:  make(map[string]proxyextension.ServiceTrafficView, len(stats.Services)),
	}
	for service, item := range stats.Services {
		view.Services[service] = proxyextension.ServiceTrafficView{
			Parking: item.Parking, Connected: item.Connected, IdleSince: cloneTime(item.IdleSince),
		}
	}
	return view, nil
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
