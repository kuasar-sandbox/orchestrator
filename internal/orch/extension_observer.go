package orch

import (
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ExtensionObserver receives authoritative object transitions after durable
// state, core caches, and existing core delivery have succeeded. Implementations
// must be bounded and non-blocking; they never invoke Extension callbacks.
type ExtensionObserver interface {
	SandboxUpsert(*types.Sandbox)
	SandboxDelete(*types.Sandbox)
	BuildUpsert(*types.Build)
	BuildRemove(*types.Build)
}

// SetExtensionObserver attaches the optional process-local observer. It is
// called once during startup before reconciliation or external listeners.
func (o *Orchestrator) SetExtensionObserver(observer ExtensionObserver) {
	o.extensionObserver = observer
}

func (o *Orchestrator) observeSandboxUpsert(sandbox *types.Sandbox) {
	if o.extensionObserver != nil {
		o.extensionObserver.SandboxUpsert(sandbox)
	}
}

func (o *Orchestrator) observeSandboxDelete(sandbox *types.Sandbox) {
	if o.extensionObserver != nil {
		o.extensionObserver.SandboxDelete(sandbox)
	}
}

func (o *Orchestrator) observeBuildUpsert(build *types.Build) {
	if o.extensionObserver != nil {
		o.extensionObserver.BuildUpsert(build)
	}
}

func (o *Orchestrator) observeBuildRemove(build *types.Build) {
	if o.extensionObserver != nil {
		o.extensionObserver.BuildRemove(build)
	}
}

// lockBuildEvent serializes a Build's durable mutation through every
// process-local publication derived from it. The Registry projection exists
// even when no Extension observer is installed, so this fence is unconditional:
// a later durable state must never be published before an earlier state.
func (o *Orchestrator) lockBuildEvent(buildID string) func() {
	return o.buildEventFences.Lock(buildID)
}

func unlockEventFence(unlock func()) {
	if unlock != nil {
		unlock()
	}
}

// lockExtensionSandboxEvent joins an otherwise lock-free insert to the same
// per-Sandbox lifecycle order used by later mutations. Existing lifecycle
// paths already hold that fence; this helper is needed only by import and is a
// no-op when observation is disabled.
func (o *Orchestrator) lockExtensionSandboxEvent(sandboxID string) func() {
	if o.extensionObserver == nil {
		return nil
	}
	return o.lifecycle.Lock(sandboxID)
}

func cloneBuildForObservation(build *types.Build) *types.Build {
	if build == nil {
		return nil
	}
	clone := *build
	if build.Names != nil {
		clone.Names = append([]string{}, build.Names...)
	}
	if build.Aliases != nil {
		clone.Aliases = append([]string{}, build.Aliases...)
	}
	clone.Metadata = cloneStringMap(build.Metadata)
	if build.Steps != nil {
		clone.Steps = make([]types.TemplateStep, len(build.Steps))
		for index, step := range build.Steps {
			clone.Steps[index] = step
			clone.Steps[index].Args = append([]string(nil), step.Args...)
		}
	}
	if build.Builder.Resources != nil {
		resources := *build.Builder.Resources
		clone.Builder.Resources = &resources
	}
	if build.Builder.Referer != nil {
		referer := *build.Builder.Referer
		if referer.Enabled != nil {
			enabled := *referer.Enabled
			referer.Enabled = &enabled
		}
		if referer.Writeback != nil {
			writeback := *referer.Writeback
			referer.Writeback = &writeback
		}
		clone.Builder.Referer = &referer
	}
	if build.Builder.Registry != nil {
		registry := *build.Builder.Registry
		if registry.TLS != nil {
			tls := *registry.TLS
			registry.TLS = &tls
		}
		clone.Builder.Registry = &registry
	}
	if build.ExecutionResult != nil {
		result := *build.ExecutionResult
		clone.ExecutionResult = &result
	}
	return &clone
}

func markBuildRemoved(build *types.Build, reason string) {
	if build == nil {
		return
	}
	build.Status = types.BuildError
	build.Reason = reason
	build.ExecutionClaimed = false
	build.ExecutionClaimedUnix = 0
	build.EnforcementStatus = ""
	build.Phase = ""
	build.PhaseSandboxID = ""
	build.RuntimeVswitchPort = ""
	build.RuntimeFloatingIP = ""
	build.RuntimePortMAC = ""
	build.RuntimeEnvdAccessToken = ""
	build.RuntimePrepareJSON = ""
	build.ExecutionResult = nil
	build.Metadata = cloneStringMapWithout(build.Metadata, sandboxcfg.NsMMDS)
}
