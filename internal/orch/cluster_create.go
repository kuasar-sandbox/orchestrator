package orch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
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
	pair, err := o.resolveByFingerprint(ctx, cmd.APISecretFingerprint)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, err
	}
	tmpl, err := types.ParseTemplateID(cmd.TemplateRef)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: template %q: %w", cmd.TemplateRef, err)
	}
	if tmpl.Profile != profile {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: profile %q does not match template profile %q", profile, tmpl.Profile)
	}
	normalized, err := o.normalizeSandboxCreateConfig(profile, cmd.Config)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxCreateConfig{}, fmt.Errorf("cluster create: %w", err)
	}
	return pair, tmpl, normalized, nil
}

// acceptClusterCreate retains cluster-only Hook constraints, then uses the same
// fresh Sandbox constructor and durable admission as the direct API.
func (o *Orchestrator) acceptClusterCreate(ctx context.Context, cmd *routesync.Command, pair store.KeyPair, tmpl types.TemplateID, normalized sandboxCreateConfig) (*types.Sandbox, *launchAttempt, error) {
	admissionStarted := time.Now()
	timeoutSeconds := o.cfg.Sandbox.TimeoutSec
	var environment map[string]string
	autoPauseMemory := cloneBool(cmd.AutoPauseMemory)
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
			Metadata: metadata, AutoPauseMemory: cloneBool(cmd.AutoPauseMemory),
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
	}
	normalized.Metadata = clusterSandboxMetadata(normalized.Metadata)
	accepted, attempt, err := o.acceptSandboxCreate(ctx, sandboxCreateSpec{
		ID: cmd.SID, StableID: cmd.Cluster.StableID,
		Cluster:  &types.ClusterSandboxContext{Group: cmd.Cluster.Group, RouteKey: cmd.Cluster.RouteKey},
		Template: tmpl, Pair: pair, Config: normalized, Env: environment,
		TimeoutSeconds: timeoutSeconds, AutoPauseMemory: autoPauseMemory,
	})
	if err != nil {
		return nil, nil, err
	}
	o.logLaunchPhase(attempt, accepted, "admission_duration", time.Since(admissionStarted))
	return accepted, attempt, nil
}
