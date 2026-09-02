package proxyshm

import (
	"context"
	"errors"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

// MMDSView is the independent Proxy master's bounded confidential heap. Routes
// and values for one sandbox are replaced under one lock. BeginSync and
// Invalidate discard the entire view immediately, so no disconnected worker
// can keep receiving an old secret.
type MMDSView struct {
	mu        sync.RWMutex
	limit     int
	available bool
	byID      map[string]mmdsEntry
	services  mmdssvc.Registry
}

type mmdsEntry struct {
	routes    []sandboxcfg.MMDSRoute
	values    map[string][]byte
	available bool
}

type mmdsEntrySnapshot struct {
	entry mmdsEntry
	found bool
}

func NewMMDSView(limit int) *MMDSView {
	if limit <= 0 {
		limit = 1
	}
	return &MMDSView{limit: limit, byID: map[string]mmdsEntry{}, services: mmdssvc.Registry{}}
}

func (v *MMDSView) BeginSync() {
	v.mu.Lock()
	v.available = false
	v.byID = map[string]mmdsEntry{}
	v.services = mmdssvc.Registry{}
	v.mu.Unlock()
}

func (v *MMDSView) Bookmark() {
	v.mu.Lock()
	v.available = true
	v.mu.Unlock()
}

func (v *MMDSView) Upsert(route routesync.RouteEntry) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if route.MMDSRoutes == "" {
		delete(v.byID, route.SandboxID)
		return nil
	}
	if _, exists := v.byID[route.SandboxID]; !exists && len(v.byID) >= v.limit {
		return errors.New("proxyshm: MMDS heap capacity exhausted")
	}
	routes, err := sandboxcfg.DecodePersistedMMDSRoutes(route.MMDSRoutes)
	if err != nil {
		v.byID[route.SandboxID] = mmdsEntry{available: false}
		return err
	}
	entry := mmdsEntry{routes: routes, available: true}
	if sandboxcfg.MMDSHasSecretRoutes(routes) {
		if route.MMDSRouteSecretValues == nil {
			entry.available = false
		} else {
			entry.values = cloneBytesMap(map[string][]byte(*route.MMDSRouteSecretValues))
		}
	}
	v.byID[route.SandboxID] = entry
	return nil
}

func (v *MMDSView) Delete(sandboxID string) {
	v.mu.Lock()
	delete(v.byID, sandboxID)
	v.mu.Unlock()
}

func (v *MMDSView) snapshotEntry(sandboxID string) mmdsEntrySnapshot {
	v.mu.RLock()
	entry, found := v.byID[sandboxID]
	snapshot := mmdsEntrySnapshot{entry: cloneMMDSEntry(entry), found: found}
	v.mu.RUnlock()
	return snapshot
}

func (v *MMDSView) restoreEntry(sandboxID string, snapshot mmdsEntrySnapshot) {
	v.mu.Lock()
	if snapshot.found {
		v.byID[sandboxID] = cloneMMDSEntry(snapshot.entry)
	} else {
		delete(v.byID, sandboxID)
	}
	v.mu.Unlock()
}

func cloneMMDSEntry(entry mmdsEntry) mmdsEntry {
	entry.routes = append([]sandboxcfg.MMDSRoute(nil), entry.routes...)
	entry.values = cloneBytesMap(entry.values)
	return entry
}

// SetServices atomically replaces the complete conductor-projected registry.
// Invalid input yields an empty registry and an error, making every configured
// service route return 503 until a valid policy arrives.
func (v *MMDSView) SetServices(endpoints map[string]string) error {
	registry, err := mmdssvc.BuildRegistry(endpoints)
	if err != nil {
		registry = mmdssvc.Registry{}
	}
	v.mu.Lock()
	v.services = registry
	v.mu.Unlock()
	return err
}

func (v *MMDSView) Resolve(sandboxID, exactPath string) mmdsrpc.EndpointResponse {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if !v.available {
		return mmdsrpc.EndpointResponse{Unavailable: true}
	}
	entry, ok := v.byID[sandboxID]
	if !ok {
		return mmdsrpc.EndpointResponse{}
	}
	if !entry.available {
		return mmdsrpc.EndpointResponse{Unavailable: true}
	}
	route, found := sandboxcfg.LookupMMDSRoute(entry.routes, exactPath)
	if !found {
		return mmdsrpc.EndpointResponse{}
	}
	response := mmdsrpc.EndpointResponse{Found: true}
	switch route.Type {
	case "", sandboxcfg.MMDSRouteStatic:
		response.Type = sandboxcfg.MMDSRouteStatic
		response.ContentType = sandboxcfg.MMDSRuntimeContentType(route)
		response.Body = []byte(route.Data)
	case sandboxcfg.MMDSRouteSecret:
		response.Type = sandboxcfg.MMDSRouteSecret
		response.ContentType = sandboxcfg.MMDSRuntimeContentType(route)
		value, configured := entry.values[route.Secret]
		response.Present = configured
		response.Body = append([]byte(nil), value...)
	case sandboxcfg.MMDSRouteService:
		response.Type = sandboxcfg.MMDSRouteService
		response.Service = route.Service
		response.ServiceSocket = v.services[route.Service]
	default:
		return mmdsrpc.EndpointResponse{Unavailable: true}
	}
	return response
}

func cloneBytesMap(values map[string][]byte) map[string][]byte {
	if values == nil {
		return nil
	}
	out := make(map[string][]byte, len(values))
	for name, value := range values {
		out[name] = append([]byte(nil), value...)
	}
	return out
}

// WaitPolicy blocks only during Proxy startup, until the first
// conductor Hello has supplied the authoritative MMDS listener policy.
func (v *MasterView) WaitPolicy(ctx context.Context) (*routesync.MMDSProxyPolicy, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case <-v.policyReady:
	}
	v.policyMu.RLock()
	defer v.policyMu.RUnlock()
	if v.policyMMDS == nil {
		return nil, true
	}
	policy := *v.policyMMDS
	policy.Services = cloneStringMap(v.policyMMDS.Services)
	return &policy, true
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
