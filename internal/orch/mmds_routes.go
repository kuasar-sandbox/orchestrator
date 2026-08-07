package orch

import (
	"context"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type mmdsInitialRouteSecretValues struct {
	routesDigest string
	values       store.MMDSRouteSecretValues
}

func initialMMDSRouteSecretValues(doc sandboxcfg.MMDSDocument) *mmdsInitialRouteSecretValues {
	// A partial document containing only an explicit empty secrets object has
	// completed its merge job and creates no portable or confidential state.
	if !doc.RoutesPresent {
		return nil
	}
	return &mmdsInitialRouteSecretValues{
		routesDigest: sandboxcfg.MMDSRoutesDigest(doc.RoutesJSON),
		values:       store.MMDSRouteSecretValues(doc.SecretValues),
	}
}

func validateInitialMMDSRouteEntry(sb *types.Sandbox, initial *mmdsInitialRouteSecretValues) error {
	if initial == nil || sb == nil {
		return nil
	}
	return validateMMDSRouteEntry(sb, initial.values)
}

func validateMMDSRouteEntry(sb *types.Sandbox, secretValues store.MMDSRouteSecretValues) error {
	raw := sb.Metadata[sandboxcfg.NsMMDS]
	routes, err := sandboxcfg.DecodePersistedMMDSRoutes(raw)
	if err != nil {
		return err
	}
	entry := routeEntryBase(sb)
	entry.MMDSRoutes = raw
	if sandboxcfg.MMDSHasSecretRoutes(routes) {
		values := make(routesync.MMDSRouteSecretValues, len(secretValues))
		for name, value := range secretValues {
			values[name] = append([]byte(nil), value...)
		}
		entry.MMDSRouteSecretValues = &values
	}
	if err := routesync.ValidateMessage(&routesync.Msg{Type: routesync.TypeUpsert, Route: &entry}); err != nil {
		return fmt.Errorf("MMDS route entry exceeds the route-sync transport limit: %w", err)
	}
	return nil
}

func (o *Orchestrator) mmdsPolicy() sandboxcfg.MMDSPolicy {
	routes := o.cfg.MMDS.Routes
	return sandboxcfg.MMDSPolicy{
		Enabled:              routes.Enabled,
		MaxRoutesPerSandbox:  routes.MaxRoutesPerSandbox,
		MaxNamespaceBytes:    routes.MaxNamespaceBytes,
		MaxStaticBodyBytes:   routes.MaxStaticBodyBytes,
		MaxSecretValueBytes:  routes.MaxSecretValueBytes,
		ReservedPathPrefixes: append([]string(nil), routes.ReservedPathPrefixes...),
		Services:             o.cfg.MMDS.ServiceEndpoints(),
	}
}

func (o *Orchestrator) setMMDSBuildOwner(sandboxID, buildID string) {
	o.mu.Lock()
	if buildID == "" {
		delete(o.mmdsBuildOwners, sandboxID)
	} else {
		o.mmdsBuildOwners[sandboxID] = buildID
	}
	o.mu.Unlock()
}

func (o *Orchestrator) mmdsRouteSecretOwner(sandboxID string) (store.MMDSRouteSecretOwnerKind, string) {
	o.mu.Lock()
	buildID := o.mmdsBuildOwners[sandboxID]
	o.mu.Unlock()
	if buildID != "" {
		return store.MMDSRouteSecretOwnerBuild, buildID
	}
	return store.MMDSRouteSecretOwnerSandbox, sandboxID
}

// mmdsRouteProjection returns the routes-only declaration and current value
// view for one route entry. A nil value map with non-empty routes means the
// confidential store is unavailable and must be projected as fail-closed.
func (o *Orchestrator) mmdsRouteProjection(ctx context.Context, sb *types.Sandbox) (string, map[string][]byte, error) {
	if sb == nil {
		return "", nil, nil
	}
	raw, present := sb.Metadata[sandboxcfg.NsMMDS]
	if !present {
		return "", nil, nil
	}
	routes, err := sandboxcfg.DecodePersistedMMDSRoutes(raw)
	if err != nil {
		return "", nil, fmt.Errorf("decode persisted MMDS routes for %s: %w", sb.ID, err)
	}
	if !sandboxcfg.MMDSHasSecretRoutes(routes) {
		return raw, nil, nil
	}
	kind, ownerID := o.mmdsRouteSecretOwner(sb.ID)
	values, _, found, err := o.st.GetMMDSRouteSecretValues(ctx, kind, ownerID, sandboxcfg.MMDSRoutesDigest(raw))
	if err != nil {
		return raw, nil, err
	}
	if !found {
		return raw, map[string][]byte{}, nil
	}
	out := make(map[string][]byte, len(values))
	for name, value := range values {
		out[name] = append([]byte(nil), value...)
	}
	return raw, out, nil
}

// Incarnation binds MMDSv2 tokens to one live launch/resume. A paused or dead
// sandbox and a starting record without an assigned run id both fail closed.
func (o *Orchestrator) Incarnation(sandboxID string) (string, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	sb := o.reg[sandboxID]
	if sb == nil || sb.RunID == "" || (sb.State != types.StateStarting && sb.State != types.StateRunning) {
		return "", false
	}
	return sb.RunID, true
}

// MMDSAvailable implements mmds.Source for the in-process proxy. Unlike an
// external worker it has no route-sync cache; per-route store failures are
// surfaced by MMDSRoute and mapped to 503.
func (o *Orchestrator) MMDSAvailable() bool { return true }

// MMDSRoute resolves a single exact path from the internal proxy's conductor
// view. Secret values come from the encrypted store on each request; services
// come only from the conductor-owned, pre-parsed Unix-socket registry.
func (o *Orchestrator) MMDSRoute(ctx context.Context, sandboxID, exactPath string) (mmds.MMDSRoute, bool, error) {
	o.mu.Lock()
	sb := cloneSandbox(o.reg[sandboxID])
	o.mu.Unlock()
	if sb == nil || (sb.State != types.StateStarting && sb.State != types.StateRunning) {
		return mmds.MMDSRoute{}, false, nil
	}
	raw, present := sb.Metadata[sandboxcfg.NsMMDS]
	if !present {
		return mmds.MMDSRoute{}, false, nil
	}
	routes, err := sandboxcfg.DecodePersistedMMDSRoutes(raw)
	if err != nil {
		return mmds.MMDSRoute{}, false, fmt.Errorf("decode MMDS routes: %w", err)
	}
	route, found := sandboxcfg.LookupMMDSRoute(routes, exactPath)
	if !found {
		return mmds.MMDSRoute{}, false, nil
	}
	switch route.Type {
	case "", sandboxcfg.MMDSRouteStatic:
		return mmds.MMDSRoute{
			Type:        sandboxcfg.MMDSRouteStatic,
			ContentType: sandboxcfg.MMDSRuntimeContentType(route),
			Body:        []byte(route.Data),
		}, true, nil
	case sandboxcfg.MMDSRouteSecret:
		kind, ownerID := o.mmdsRouteSecretOwner(sandboxID)
		values, _, stored, err := o.st.GetMMDSRouteSecretValues(ctx, kind, ownerID, sandboxcfg.MMDSRoutesDigest(raw))
		if err != nil {
			return mmds.MMDSRoute{}, false, err
		}
		value, configured := values[route.Secret]
		return mmds.MMDSRoute{
			Type:        sandboxcfg.MMDSRouteSecret,
			ContentType: sandboxcfg.MMDSRuntimeContentType(route),
			Body:        append([]byte(nil), value...),
			Present:     stored && configured,
		}, true, nil
	case sandboxcfg.MMDSRouteService:
		socketPath := o.mmdsServices[route.Service]
		result := mmdssvc.Call(ctx, socketPath, route.Service, route.Path, sandboxID)
		return mmds.MMDSRoute{
			Type:        sandboxcfg.MMDSRouteService,
			StatusCode:  result.StatusCode,
			ContentType: result.ContentType,
			Body:        result.Body,
		}, true, nil
	default:
		return mmds.MMDSRoute{}, false, errors.New("invalid persisted MMDS route type")
	}
}
