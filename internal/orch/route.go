package orch

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

var errRouteBindingChanged = errors.New("ordinary route binding changed")

// LookupRoute reads the credential subject for an ordinary route without
// caching, resuming, publishing, or touching a backend.
func (o *Orchestrator) LookupRoute(ctx context.Context, sandboxID string, target proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	sb, err := o.lookupRouteSandbox(ctx, sandboxID)
	if err != nil {
		return proxy.RouteBinding{}, false, err
	}
	binding, present := routeBinding(sb, target)
	return binding, present, nil
}

// ActivateRoute is entered only after proxy.Proxy has applied the configured
// ordinary auth policy to expected. The common resume admission validates the
// binding under the lifecycle fence before any durable transition; the final
// store read validates it again and supplies the current backend.
func (o *Orchestrator) ActivateRoute(ctx context.Context, expected proxy.RouteBinding) (proxy.Route, bool, error) {
	if expected.SandboxID == "" {
		return proxy.Route{}, false, nil
	}
	current, _, err := o.ensureResumeAccepted(ctx, expected.SandboxID, nil, func(sb *types.Sandbox) error {
		// A Sandbox artifact (E) resume source is a cold start owned by explicit
		// Connect: the in-process router must not auto-boot it on data-plane
		// traffic (sandboxer#143 §1.3) — same contract as proxyshm.WorkerView.
		if sb.State == types.StatePaused && sb.ResumeKind == types.ResumeSandbox {
			return proxy.ErrColdSandbox
		}
		binding, present := routeBinding(sb, expected.Target)
		if !present || binding != expected {
			return errRouteBindingChanged
		}
		return nil
	})
	if errors.Is(err, errRouteBindingChanged) || errors.Is(err, api.ErrNotFound) {
		return proxy.Route{}, false, nil
	}
	if err != nil {
		return proxy.Route{}, false, err
	}
	if current == nil {
		return proxy.Route{}, false, nil
	}
	if current.State == types.StateStarting || current.State == types.StatePaused {
		if _, err := o.waitLaunchState(ctx, expected.SandboxID); err != nil {
			return proxy.Route{}, false, err
		}
	}

	ready, err := o.st.Get(ctx, expected.SandboxID)
	if err != nil {
		return proxy.Route{}, false, err
	}
	binding, present := routeBinding(ready, expected.Target)
	if !present || ready.State != types.StateRunning || binding != expected {
		return proxy.Route{}, false, nil
	}
	route := proxy.RouteForTarget(ready.Profile, ready.EnvdUDS, ready.CiUDS, ready.FloatingIP, expected.Target)
	if route.Kind != expected.Kind {
		return proxy.Route{}, false, nil
	}
	return route, true, nil
}

func (o *Orchestrator) lookupRouteSandbox(ctx context.Context, sandboxID string) (*types.Sandbox, error) {
	if sb := o.lookup(sandboxID); sb != nil {
		return sb, nil
	}
	return o.st.Get(ctx, sandboxID)
}

func routeBinding(sb *types.Sandbox, target proxy.ConnectTarget) (proxy.RouteBinding, bool) {
	if sb == nil || (sb.State != types.StateStarting && sb.State != types.StateRunning && sb.State != types.StatePaused) {
		return proxy.RouteBinding{}, false
	}
	binding := proxy.BindRoute(
		sb.ID, sb.AuthSandboxID(), sb.Profile,
		sb.EnvdAccessToken, sb.ForwardAccessToken, target,
	)
	if binding.SandboxID == "" || binding.AuthSandboxID == "" {
		return proxy.RouteBinding{}, false
	}
	return binding, true
}

var _ proxy.Router = (*Orchestrator)(nil)
