package orch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func (o *Orchestrator) Create(ctx context.Context, req api.CreateReq) (*types.Sandbox, error) {
	admissionStarted := time.Now()
	if req.TimeoutSec <= 0 {
		req.TimeoutSec = o.cfg.Sandbox.TimeoutSec
	}
	pair, err := o.resolveAllowed(ctx, req.APIKey)
	if err != nil {
		return nil, err
	}
	if pair.APISecret == "" {
		return nil, api.ErrNotAllowed
	}
	identity, metadata, err := sandboxcfg.ExtractIdentity(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	// Identity is core-owned after extraction. The Hook receives the remaining
	// request configuration, not another writable identity carrier.
	req.Metadata = metadata
	tmpl, normalized, mmdsDoc, err := o.normalizeSandboxCreateDefinition(ctx, req.APIKey, req.TemplateID, req.Metadata, req.MMDSHeader)
	if err != nil {
		return nil, err
	}
	sid := identity.ID
	if sid == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("orch: new id: %w", err)
		}
		sid = id.String()
	}

	if o.extensionSandboxHook != nil {
		candidate, err := o.prepareSandboxCreateHook(ctx, conductorextension.SandboxOriginDirect, sid, &conductorextension.SandboxCreateRequest{
			TemplateID: tmpl.String(), Profile: conductorextension.Profile(tmpl.Profile), TimeoutSeconds: req.TimeoutSec,
			Metadata: cloneStringMap(req.Metadata), Env: cloneStringMap(req.EnvVars), Secure: req.Secure,
			AutoPauseMemory: cloneBool(req.AutoPauseMemory), MMDS: cloneString(req.MMDSHeader),
		})
		if err != nil {
			return nil, err
		}
		if candidate.TimeoutSeconds <= 0 {
			candidate.TimeoutSeconds = o.cfg.Sandbox.TimeoutSec
		}
		if candidate.TimeoutSeconds <= 0 {
			return nil, fmt.Errorf("%w: sandbox timeout must be positive", api.ErrBadRequest)
		}
		finalTemplate, finalConfig, finalMMDS, err := o.normalizeSandboxCreateDefinition(ctx, req.APIKey, candidate.TemplateID, candidate.Metadata, candidate.MMDS)
		if err != nil {
			return nil, err
		}
		if finalTemplate.Profile != tmpl.Profile {
			return nil, fmt.Errorf("%w: extension changed core-owned sandbox profile", api.ErrBadRequest)
		}
		tmpl, normalized, mmdsDoc = finalTemplate, finalConfig, finalMMDS
		req.TimeoutSec, req.EnvVars, req.AutoPauseMemory = candidate.TimeoutSeconds, cloneStringMap(candidate.Env), cloneBool(candidate.AutoPauseMemory)
	}
	accepted, attempt, err := o.acceptSandboxCreate(ctx, sandboxCreateSpec{
		ID: sid, StableID: identity.StableID, Template: tmpl, Pair: pair,
		Config: normalized, Env: req.EnvVars, TimeoutSeconds: req.TimeoutSec,
		AutoPauseMemory: req.AutoPauseMemory, InitialMMDS: initialMMDSRouteSecretValues(mmdsDoc),
	})
	if err != nil {
		return nil, err
	}
	o.logLaunchPhase(attempt, accepted, "admission_duration", time.Since(admissionStarted))
	return cloneSandbox(accepted), nil
}

// normalizeSandboxCreateDefinition retains the direct API's template aliases
// and MMDS input boundary. Cluster commands keep their canonical template and
// trusted context, but share normalizeSandboxCreateConfig below.
func (o *Orchestrator) normalizeSandboxCreateDefinition(
	ctx context.Context,
	apiKey, templateRef string,
	metadata map[string]string,
	mmdsHeader *string,
) (types.TemplateID, sandboxCreateConfig, sandboxcfg.MMDSDocument, error) {
	tmpl, err := types.ParseTemplateID(templateRef)
	if err != nil {
		persist := o.resolveTemplateAlias(ctx, apiKey, templateRef)
		if persist == "" {
			return types.TemplateID{}, sandboxCreateConfig{}, sandboxcfg.MMDSDocument{}, err
		}
		if tmpl, err = types.ParseTemplateID(persist); err != nil {
			return types.TemplateID{}, sandboxCreateConfig{}, sandboxcfg.MMDSDocument{}, err
		}
	}
	// Template artifacts, not retention-bounded Build history, own portable
	// runtime defaults. Only this invocation's metadata enters Create here.
	mmds, metadata, err := sandboxcfg.ExtractMMDS(metadata, mmdsHeader, o.mmdsPolicy())
	if err != nil {
		return types.TemplateID{}, sandboxCreateConfig{}, sandboxcfg.MMDSDocument{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	normalized, err := o.normalizeSandboxCreateConfig(tmpl.Profile, metadata)
	if err != nil {
		return types.TemplateID{}, sandboxCreateConfig{}, sandboxcfg.MMDSDocument{}, err
	}
	return tmpl, normalized, mmds, nil
}

// sandboxCreateConfig is pure validation output. It keeps extracted credentials
// separate from metadata and never mutates a request/Command to pass results.
type sandboxCreateConfig struct {
	Metadata    map[string]string
	Credentials sandboxcfg.Credentials
}

func (o *Orchestrator) normalizeSandboxCreateConfig(profile types.Profile, metadata map[string]string) (sandboxCreateConfig, error) {
	if _, present := metadata[sandboxcfg.NsIdentity]; present {
		return sandboxCreateConfig{}, fmt.Errorf("%w: %s cannot override core-owned sandbox identity", api.ErrBadRequest, sandboxcfg.NsIdentity)
	}
	// MergeCreateMetadata validates/canonicalizes resource and traffic once and
	// preserves request-only namespaces without inheriting portable defaults.
	metadata, err := sandboxcfg.MergeCreateMetadata(nil, metadata)
	if err != nil {
		return sandboxCreateConfig{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	metadata, err = sandboxcfg.NormalizeRestoreMetadata(metadata)
	if err != nil {
		return sandboxCreateConfig{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	metadata, err = sandboxcfg.NormalizeCheckpointMetadata(metadata)
	if err != nil {
		return sandboxCreateConfig{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	credentials, metadata, err := sandboxcfg.ExtractCredentials(metadata)
	if err != nil {
		return sandboxCreateConfig{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if err := o.validateCreateCheckpointMode(metadata); err != nil {
		return sandboxCreateConfig{}, err
	}
	if err := validateSandboxCredentialOverrides(profile, credentials); err != nil {
		return sandboxCreateConfig{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	spec, err := sandboxcfg.ParseSpec(metadata)
	if err != nil {
		return sandboxCreateConfig{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if err := sandboxcfg.ValidateTrafficForProfile(profile, spec.Traffic); err != nil {
		return sandboxCreateConfig{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	return sandboxCreateConfig{Metadata: metadata, Credentials: credentials}, nil
}

// sandboxCreateSpec has already-selected identity and validated configuration.
// It is not a wire schema or another public configuration surface.
type sandboxCreateSpec struct {
	ID              string
	StableID        string
	Cluster         *types.ClusterSandboxContext
	Template        types.TemplateID
	Pair            store.KeyPair
	Config          sandboxCreateConfig
	Env             map[string]string
	TimeoutSeconds  int
	AutoPauseMemory *bool
	InitialMMDS     *mmdsInitialRouteSecretValues
}

// acceptSandboxCreate is the sole fresh Sandbox constructor for both direct
// and node-link Create. Source-specific authentication and Hook restrictions
// stay in the adapters; launch ownership and rollback stay in acceptFreshLaunch.
func (o *Orchestrator) acceptSandboxCreate(ctx context.Context, spec sandboxCreateSpec) (*types.Sandbox, *launchAttempt, error) {
	launchMode, err := types.LaunchModeForTemplate(spec.Template.Kind)
	if err != nil {
		return nil, nil, err
	}
	autoPauseMemory := true
	if spec.AutoPauseMemory != nil {
		autoPauseMemory = *spec.AutoPauseMemory
	}
	now := time.Now()
	sb := &types.Sandbox{
		ID: spec.ID, StableIDValue: spec.StableID,
		Profile: spec.Template.Profile, TemplateID: spec.Template.String(),
		State: types.StateStarting, LaunchMode: launchMode, AutoPauseMemory: autoPauseMemory,
		RunDir:    nodepath.SandboxRunDir(o.cfg.Paths.RunRoot, spec.ID),
		BaseDir:   nodepath.SandboxBaseDir(o.cfg.Paths.BaseRoot, spec.ID),
		APISecret: spec.Pair.APISecret, ManifestKey: spec.Pair.ManifestKey,
		Metadata: cloneStringMap(spec.Config.Metadata), Env: cloneStringMap(spec.Env),
		CreatedUnix: now.Unix(), DeadlineUnix: now.Add(time.Duration(spec.TimeoutSeconds) * time.Second).Unix(),
	}
	if spec.Cluster != nil {
		cluster := *spec.Cluster
		sb.Cluster = &cluster
	}
	// StableID must be final before any service credential is derived or minted.
	if err := materializeSandboxCredentials(sb, spec.Config.Credentials); err != nil {
		return nil, nil, fmt.Errorf("orch: create credentials: %w", err)
	}
	if spec.Template.Profile == types.ProfileE2B {
		sb.EnvdUDS = sb.RunDir + "/envd.sock"
		sb.CiUDS = sb.RunDir + "/ci.sock"
	}
	accepted, attempt, err := o.acceptFreshLaunch(ctx, sb, spec.Template, spec.InitialMMDS)
	if errors.Is(err, errLaunchClaimed) || errors.Is(err, store.ErrSandboxExists) {
		// Only fresh Create interprets a claimed launch as identity conflict.
		// Preserve the underlying error for internal diagnostics and tests.
		return nil, nil, errors.Join(api.ErrAlreadyExists, err)
	}
	return accepted, attempt, err
}
