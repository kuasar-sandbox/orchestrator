package orch

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

var errExecIdentityChanged = errors.New("exec route identity changed")

// LookupExec performs the credential lookup required before an exec CONNECT can
// be authorized. It never caches, resumes, publishes, wakes, or otherwise
// changes sandbox lifecycle state.
func (o *Orchestrator) LookupExec(ctx context.Context, sandboxID string) (proxy.ExecIdentity, bool, error) {
	sb, err := o.lookupExecSandbox(ctx, sandboxID)
	if err != nil {
		return proxy.ExecIdentity{}, false, err
	}
	if !execRoutePresent(sb) {
		return proxy.ExecIdentity{}, false, nil
	}
	return execIdentity(sb), true, nil
}

// ActivateExec is called only after the proxy has verified a KAT against
// expected. It accepts or joins the common launch attempt and re-reads the
// node-local identity before returning a ctl.sock target subject.
func (o *Orchestrator) ActivateExec(ctx context.Context, sandboxID string, expected proxy.ExecIdentity) (proxy.ExecIdentity, bool, error) {
	sb, err := o.lookupExecSandbox(ctx, sandboxID)
	if err != nil {
		return proxy.ExecIdentity{}, false, err
	}
	if !execRoutePresent(sb) || execIdentity(sb) != expected {
		return proxy.ExecIdentity{}, false, nil
	}

	if sb.State == types.StatePaused {
		_, _, err := o.ensureResumeAccepted(ctx, sandboxID, nil, func(current *types.Sandbox) error {
			if !execRoutePresent(current) || execIdentity(current) != expected {
				return errExecIdentityChanged
			}
			return nil
		})
		if errors.Is(err, errExecIdentityChanged) {
			return proxy.ExecIdentity{}, false, nil
		}
		if err != nil {
			return proxy.ExecIdentity{}, false, err
		}
	}
	if sb.State == types.StatePaused || sb.State == types.StateStarting {
		if _, err := o.waitLaunchState(ctx, sandboxID); err != nil {
			return proxy.ExecIdentity{}, false, err
		}
	}

	ready, err := o.st.Get(ctx, sandboxID)
	if err != nil {
		return proxy.ExecIdentity{}, false, err
	}
	if ready == nil || ready.State != types.StateRunning || execIdentity(ready) != expected {
		return proxy.ExecIdentity{}, false, nil
	}
	return execIdentity(ready), true, nil
}

func (o *Orchestrator) lookupExecSandbox(ctx context.Context, sandboxID string) (*types.Sandbox, error) {
	if sb := o.lookup(sandboxID); sb != nil {
		return sb, nil
	}
	return o.st.Get(ctx, sandboxID)
}

func execRoutePresent(sb *types.Sandbox) bool {
	return sb != nil && (sb.State == types.StateStarting || sb.State == types.StateRunning || sb.State == types.StatePaused)
}

func execIdentity(sb *types.Sandbox) proxy.ExecIdentity {
	if sb == nil {
		return proxy.ExecIdentity{}
	}
	return proxy.ExecIdentity{
		NodeSandboxID: sb.ID,
		AuthSandboxID: sb.AuthSandboxID(),
		ServiceSecret: sb.ServiceSecret,
	}
}

var _ proxy.ExecRouter = (*Orchestrator)(nil)
