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
// expected. It resumes a paused sandbox through the existing single-flight and
// re-reads the node-local identity before returning a ctl.sock target subject.
func (o *Orchestrator) ActivateExec(ctx context.Context, sandboxID string, expected proxy.ExecIdentity) (proxy.ExecIdentity, bool, error) {
	sb, err := o.lookupExecSandbox(ctx, sandboxID)
	if err != nil {
		return proxy.ExecIdentity{}, false, err
	}
	if !execRoutePresent(sb) || execIdentity(sb) != expected {
		return proxy.ExecIdentity{}, false, nil
	}

	if sb.State == types.StatePaused || sb.State == types.StateStarting {
		err := o.resumeExecSandbox(ctx, sandboxID, expected)
		if errors.Is(err, errExecIdentityChanged) {
			return proxy.ExecIdentity{}, false, nil
		}
		if err != nil {
			return proxy.ExecIdentity{}, false, err
		}
	}

	ready, err := o.lookupExecSandbox(ctx, sandboxID)
	if err != nil {
		return proxy.ExecIdentity{}, false, err
	}
	if ready == nil || ready.State != types.StateRunning || execIdentity(ready) != expected {
		return proxy.ExecIdentity{}, false, nil
	}
	return execIdentity(ready), true, nil
}

// resumeExecSandbox preserves the normal resume fencing and lifecycle boundary
// while revalidating the already-authorized identity immediately before any
// resume side effect.
func (o *Orchestrator) resumeExecSandbox(ctx context.Context, sandboxID string, expected proxy.ExecIdentity) error {
	request := o.newResumeRequest(sandboxID)
	defer o.releaseResumeRequest(request)
	for {
		err := o.sf.Do(sandboxID, func() error {
			unlock := o.lifecycle.Lock(sandboxID)
			defer unlock()
			if !o.resumeRequestValid(request) {
				return errResumeFenced
			}
			current, err := o.lookupExecSandbox(ctx, sandboxID)
			if err != nil {
				return err
			}
			if !execRoutePresent(current) || execIdentity(current) != expected {
				return errExecIdentityChanged
			}
			preserveDeadline := o.hasDeadlineIntent(sandboxID)
			if err := o.resumeIfPaused(ctx, sandboxID, preserveDeadline); err != nil {
				return err
			}
			o.clearDeadlineIntent(sandboxID)
			return nil
		})
		if !errors.Is(err, errResumeFenced) {
			return err
		}
		if !o.resumeRequestValid(request) {
			return nil
		}
	}
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
