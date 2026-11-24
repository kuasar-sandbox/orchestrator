package orch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// precheckCluster validates trusted command identity and resolves the installed
// credential pair. Common configuration validation returns a separate result;
// even repeated calls must leave the original command and its credentials intact.
func (o *Orchestrator) precheckCluster(ctx context.Context, cmd *routesync.Command) (store.KeyPair, types.TemplateID, sandboxCreateConfig, error) {
	if cmd == nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: command is required")
	}
	if !types.ValidLocalSandboxID(cmd.SID) {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: invalid sandbox id")
	}
	profile, err := types.ParseProfile(cmd.Profile)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: %w", err)
	}
	if cmd.Cluster == nil || cmd.Cluster.Group == "" || cmd.Cluster.RouteKey == "" {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: group and route key are required")
	}
	detail, pair, err := o.clusterCreateGroup(ctx, cmd)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, err
	}
	templateRef, configuration := cmd.TemplateRef, cmd.Config
	if templateRef == "" {
		if detail.Context.TemplateID == nil {
			return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("%w: Group has no default template", api.ErrBadRequest)
		}
		templateRef = *detail.Context.TemplateID
		configuration, err = sandboxcfg.MergeCreateMetadata(detail.Context.SandboxConfig, cmd.Config)
		if err != nil {
			return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
		}
	}
	tmpl, err := types.ParseTemplateID(templateRef)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: template %q: %w", templateRef, err)
	}
	if tmpl.Profile != profile {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: profile %q does not match template profile %q", profile, tmpl.Profile)
	}
	normalized, err := o.normalizeSandboxCreateConfig(profile, configuration)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: %w", err)
	}
	return pair, tmpl, normalized, nil
}

// Consult accepted ownership before mutable Group state. A later Group deletion
// or revision cannot turn a lost acceptance into nonexecution proof.
func (o *Orchestrator) clusterCreateGroup(ctx context.Context, cmd *routesync.Command) (nodegroup.Detail, store.KeyPair, error) {
	existing, err := o.st.Get(ctx, cmd.SID)
	if err != nil {
		return nodegroup.Detail{}, store.KeyPair{}, err
	}
	if existing != nil {
		if !ownsSandbox(existing, cmd.GroupAPIKey) {
			return nodegroup.Detail{}, store.KeyPair{}, nodegroup.ErrForbidden
		}
		if err := validateClusterSandboxCredentialBinding(existing, cmd.APISecretFingerprint); err != nil {
			return nodegroup.Detail{}, store.KeyPair{}, err
		}
		if err := validateClusterSandboxContext(existing, cmd); err != nil {
			return nodegroup.Detail{}, store.KeyPair{}, fmt.Errorf("%w: %v", api.ErrAlreadyExists, err)
		}
		return nodegroup.Detail{}, store.KeyPair{}, api.ErrAlreadyExists
	}
	return o.admitClusterGroup(ctx, cmd, cmd.Cluster.Group)
}

func (o *Orchestrator) admitClusterCreate(ctx context.Context, cmd *routesync.Command) (*types.Sandbox, *launchAttempt, error) {
	pair, tmpl, normalized, err := o.precheckCluster(ctx, cmd)
	if err != nil {
		return nil, nil, err
	}
	return o.acceptClusterCreate(ctx, cmd, pair, tmpl, normalized)
}

// acceptClusterCreate retains cluster-only Hook constraints, then uses the same
// fresh Sandbox constructor and durable admission as the direct API.
func (o *Orchestrator) acceptClusterCreate(ctx context.Context, cmd *routesync.Command, pair store.KeyPair, tmpl types.TemplateID, normalized sandboxCreateConfig) (*types.Sandbox, *launchAttempt, error) {
	admissionStarted := time.Now()
	maxCheckpoints, err := types.ResolveMaxCheckpoints(cmd.MaxCheckpoints)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	timeoutSeconds := o.cfg.Sandbox.TimeoutSec
	var environment map[string]string
	autoPauseMemory := cloneBool(cmd.AutoPauseMemory)
	idleTimeout := types.CloneIdleTimeout(cmd.IdleTimeout)
	autoReset := cmd.AutoReset != nil && *cmd.AutoReset
	if o.extensionSandboxHook != nil {
		metadata := cloneStringMap(normalized.Metadata)
		// Preserve the existing canonical Cluster Hook projection without
		// destructively extracting credentials from cmd.Config.
		if normalized.Credentials != (sandboxcfg.Credentials{}) {
			raw, err := json.Marshal(normalized.Credentials)
			if err != nil {
				return nil, nil, fmt.Errorf("cluster create: marshal credential candidate: %w", err)
			}
			if metadata == nil {
				metadata = make(map[string]string)
			}
			metadata[sandboxcfg.NsCredentials] = string(raw)
		}
		clusterMetadata, clusterMetadataPresent := metadata[clusterstate.ObjectMetadataKey]
		candidate, err := o.prepareSandboxCreateHook(ctx, conductorextension.SandboxOriginCluster, cmd.SID, &conductorextension.SandboxCreateRequest{
			TemplateID: tmpl.String(), Profile: conductorextension.Profile(tmpl.Profile), TimeoutSeconds: timeoutSeconds,
			Metadata: metadata, IdleTimeout: types.CloneIdleTimeout(idleTimeout), AutoPauseMemory: cloneBool(cmd.AutoPauseMemory), AutoReset: autoReset,
		})
		if err != nil {
			return nil, nil, err
		}
		if candidate.TemplateID != tmpl.String() {
			return nil, nil, fmt.Errorf("%w: extension changed cluster-owned sandbox template identity", api.ErrBadRequest)
		}
		if candidate.TimeoutSeconds <= 0 {
			return nil, nil, fmt.Errorf("%w: cluster sandbox timeout must be positive", api.ErrBadRequest)
		}
		if candidate.MMDS != nil {
			return nil, nil, fmt.Errorf("%w: cluster create does not accept a top-level MMDS candidate", api.ErrBadRequest)
		}
		finalClusterMetadata, finalClusterMetadataPresent := candidate.Metadata[clusterstate.ObjectMetadataKey]
		if clusterMetadataPresent != finalClusterMetadataPresent || clusterMetadata != finalClusterMetadata {
			return nil, nil, fmt.Errorf("%w: extension changed cluster-owned sandbox context", api.ErrBadRequest)
		}
		normalized, err = o.normalizeSandboxCreateConfig(tmpl.Profile, candidate.Metadata)
		if err != nil {
			return nil, nil, fmt.Errorf("cluster create: %w", err)
		}
		timeoutSeconds, environment, autoPauseMemory = candidate.TimeoutSeconds, cloneStringMap(candidate.Env), cloneBool(candidate.AutoPauseMemory)
		idleTimeout = types.CloneIdleTimeout(candidate.IdleTimeout)
		if candidate.AutoReset != autoReset {
			return nil, nil, fmt.Errorf("%w: extension changed cluster-owned autoReset policy", api.ErrBadRequest)
		}
	}
	normalized.Workspaces = cmd.Workspaces
	normalized.Metadata = clusterSandboxMetadata(normalized.Metadata)
	// The adapter already copied an authenticated revision. Group changes after
	// that boundary do not revoke this invocation or restart its core admission.
	accepted, attempt, err := o.acceptSandboxCreate(ctx, sandboxCreateSpec{
		ID: cmd.SID, StableID: cmd.Cluster.StableID, MaxCheckpoints: maxCheckpoints,
		Cluster:  &types.ClusterSandboxContext{Group: cmd.Cluster.Group, RouteKey: cmd.Cluster.RouteKey},
		Template: tmpl, Pair: pair, Config: normalized, Env: environment,
		TimeoutSeconds: timeoutSeconds, IdleTimeout: idleTimeout, AutoPauseMemory: autoPauseMemory,
		AutoReset: autoReset,
	})
	if err != nil {
		return nil, nil, err
	}
	o.logLaunchPhase(attempt, accepted, "admission_duration", time.Since(admissionStarted))
	return accepted, attempt, nil
}
