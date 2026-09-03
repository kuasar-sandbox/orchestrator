package proxyext

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type routeRecord struct {
	view        proxyextension.RouteView
	maxInflight config.MaxInflight
	seen        uint64
}

type changeKind uint8

const (
	changeUpsert changeKind = iota
	changeDelete
	changeSyncLost
	changeReset
)

type routeChange struct {
	kind      changeKind
	sandboxID string
	view      proxyextension.RouteView
}

type routeSource struct {
	mu sync.RWMutex

	routes         map[string]routeRecord
	state          proxyextension.RouteSyncState
	syncGeneration uint64
	stateVersion   uint64
	stateChanged   chan struct{}

	hub             *hub[routeChange]
	watchGeneration atomic.Uint64
}

func newRouteSource(capacity int) *routeSource {
	return &routeSource{
		routes:       make(map[string]routeRecord),
		state:        proxyextension.RouteSyncInitializing,
		stateChanged: make(chan struct{}),
		hub:          newHub[routeChange](capacity),
	}
}

func (s *routeSource) Get(ctx context.Context, sandboxID string) (proxyextension.RouteView, bool, error) {
	view, _, found, err := s.getTrafficRoute(ctx, sandboxID)
	return view, found, err
}

func (s *routeSource) getTrafficRoute(ctx context.Context, sandboxID string) (proxyextension.RouteView, config.MaxInflight, bool, error) {
	if err := ctx.Err(); err != nil {
		return proxyextension.RouteView{}, config.MaxInflight{}, false, err
	}
	s.mu.RLock()
	record, found := s.routes[sandboxID]
	s.mu.RUnlock()
	if !found {
		return proxyextension.RouteView{}, config.MaxInflight{}, false, nil
	}
	return cloneRouteView(record.view), record.maxInflight, true, nil
}

func (s *routeSource) SyncState() proxyextension.RouteSyncState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *routeSource) Watch(ctx context.Context, callback func(proxyextension.RouteEvent) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if callback == nil {
		return errors.New("proxy extension: route Watch callback is required")
	}
	for {
		if err := s.waitSynced(ctx); err != nil {
			return err
		}
		sub := s.hub.subscribe()
		generation := s.watchGeneration.Add(1)
		complete, err := s.snapshot(ctx, sub, generation, callback)
		if err != nil {
			s.hub.unsubscribe(sub)
			return err
		}
		if !complete {
			s.hub.unsubscribe(sub)
			continue
		}
		for {
			select {
			case <-sub.reset:
				s.hub.unsubscribe(sub)
				goto resync
			default:
			}
			select {
			case <-ctx.Done():
				s.hub.unsubscribe(sub)
				return ctx.Err()
			case <-sub.reset:
				s.hub.unsubscribe(sub)
				goto resync
			case change := <-sub.events:
				switch change.kind {
				case changeReset:
					s.hub.unsubscribe(sub)
					goto resync
				case changeSyncLost:
					if err := callback(proxyextension.RouteEvent{
						Generation: generation, Kind: proxyextension.RouteSyncLost,
					}); err != nil {
						s.hub.unsubscribe(sub)
						return err
					}
				default:
					if err := callback(routeEvent(generation, change)); err != nil {
						s.hub.unsubscribe(sub)
						return err
					}
				}
			}
		}
	resync:
	}
}

func (s *routeSource) waitSynced(ctx context.Context) error {
	for {
		s.mu.RLock()
		if s.state == proxyextension.RouteSyncSynced {
			s.mu.RUnlock()
			return nil
		}
		changed := s.stateChanged
		s.mu.RUnlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (s *routeSource) snapshot(ctx context.Context, sub *subscription[routeChange], generation uint64, callback func(proxyextension.RouteEvent) error) (bool, error) {
	s.mu.RLock()
	if s.state != proxyextension.RouteSyncSynced {
		s.mu.RUnlock()
		return false, nil
	}
	stateVersion := s.stateVersion
	views := make([]proxyextension.RouteView, 0, len(s.routes))
	for _, record := range s.routes {
		views = append(views, cloneRouteView(record.view))
	}
	s.mu.RUnlock()
	sort.Slice(views, func(i, j int) bool { return views[i].SandboxID < views[j].SandboxID })

	if !s.snapshotValid(ctx, sub, stateVersion) {
		return false, ctx.Err()
	}
	if err := callback(proxyextension.RouteEvent{Generation: generation, Kind: proxyextension.RouteSyncBegin}); err != nil {
		return false, err
	}
	for index := range views {
		if !s.snapshotValid(ctx, sub, stateVersion) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return false, nil
		}
		view := cloneRouteView(views[index])
		if err := callback(proxyextension.RouteEvent{
			Generation: generation, Kind: proxyextension.RouteUpsert,
			SandboxID: view.SandboxID, View: &view,
		}); err != nil {
			return false, err
		}
	}
	if !s.snapshotValid(ctx, sub, stateVersion) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := callback(proxyextension.RouteEvent{Generation: generation, Kind: proxyextension.RouteSyncEnd}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *routeSource) snapshotValid(ctx context.Context, sub *subscription[routeChange], stateVersion uint64) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-sub.reset:
		return false
	default:
	}
	s.mu.RLock()
	valid := s.state == proxyextension.RouteSyncSynced && s.stateVersion == stateVersion
	s.mu.RUnlock()
	return valid
}

func (s *routeSource) beginSync() {
	s.mu.Lock()
	s.syncGeneration++
	s.state = proxyextension.RouteSyncSyncing
	s.stateVersion++
	s.signalStateLocked()
	s.mu.Unlock()
	s.hub.publish(routeChange{kind: changeReset})
}

func (s *routeSource) upsert(route routesync.RouteEntry, revision uint64) {
	view := projectRoute(route, revision)
	s.mu.Lock()
	s.routes[route.SandboxID] = routeRecord{
		view: view, maxInflight: route.EffectiveMaxInflight, seen: s.syncGeneration,
	}
	s.mu.Unlock()
	s.hub.publish(routeChange{kind: changeUpsert, sandboxID: route.SandboxID, view: view})
}

func (s *routeSource) delete(sandboxID string, revision uint64) {
	s.mu.Lock()
	record, found := s.routes[sandboxID]
	delete(s.routes, sandboxID)
	s.mu.Unlock()
	if !found {
		record.view = proxyextension.RouteView{SandboxID: sandboxID}
	}
	record.view.Revision = revision
	s.hub.publish(routeChange{kind: changeDelete, sandboxID: sandboxID, view: record.view})
}

func (s *routeSource) bookmark() {
	s.mu.Lock()
	if s.state == proxyextension.RouteSyncSyncing {
		for sandboxID, record := range s.routes {
			if record.seen != s.syncGeneration {
				delete(s.routes, sandboxID)
			}
		}
	}
	s.state = proxyextension.RouteSyncSynced
	s.stateVersion++
	s.signalStateLocked()
	s.mu.Unlock()
}

func (s *routeSource) invalidate() {
	s.mu.Lock()
	s.state = proxyextension.RouteSyncStale
	s.stateVersion++
	s.signalStateLocked()
	s.mu.Unlock()
	s.hub.publish(routeChange{kind: changeSyncLost})
}

func (s *routeSource) signalStateLocked() {
	close(s.stateChanged)
	s.stateChanged = make(chan struct{})
}

func routeEvent(generation uint64, change routeChange) proxyextension.RouteEvent {
	view := cloneRouteView(change.view)
	kind := proxyextension.RouteUpsert
	if change.kind == changeDelete {
		kind = proxyextension.RouteDelete
	}
	return proxyextension.RouteEvent{
		Generation: generation, Kind: kind, SandboxID: change.sandboxID, View: &view,
	}
}

func projectRoute(route routesync.RouteEntry, revision uint64) proxyextension.RouteView {
	return proxyextension.RouteView{
		SandboxID: route.SandboxID, StableID: route.StableID,
		Profile: proxyextension.Profile(route.Profile), TemplateID: route.TemplateID,
		State: proxyextension.RouteState(route.State), RunID: route.RunID,
		EnvdUDS: route.EnvdUDS, CIUDS: route.CiUDS, FloatingIP: route.FloatingIP,
		ArtifactLocation:       proxyextension.ArtifactLocation(route.ArtifactLocation),
		APISecretFingerprint:   route.APISecretFingerprint,
		ManifestKeyFingerprint: route.ManifestKeyFingerprint, Revision: revision,
	}
}

func cloneRouteView(view proxyextension.RouteView) proxyextension.RouteView { return view }
