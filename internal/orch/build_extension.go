package orch

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type normalizedBuildRegistration struct {
	templateID         string
	profile            types.Profile
	kind               types.Kind
	names              []string
	aliases            []string
	resources          types.BuildResources
	metadata           map[string]string
	builder            types.BuildOptions
	phaseResourcePatch string
	mmds               sandboxcfg.MMDSDocument
}

func buildRegisterRequest(spec api.RegisterSpec, templateID string) *conductorextension.BuildRegisterRequest {
	builder := spec.Builder
	// Registration Resources has one authority. The API boundary already
	// normalized builder.resources into spec.Resources; never expose the stale
	// duplicate to an Extension.
	builder.Resources = nil
	return &conductorextension.BuildRegisterRequest{
		TemplateID: templateID,
		Profile:    conductorextension.Profile(spec.Profile),
		Kind:       conductorextension.BuildKindImage,
		Names:      nonEmpty(spec.Name),
		Aliases:    append([]string(nil), spec.Tags...),
		Resources:  cloneBuildResources(spec.Resources),
		Metadata:   cloneStringMap(spec.Metadata),
		Builder:    publicBuildOptions(builder),
	}
}

func buildRegisterRequestFromBuild(build *types.Build) *conductorextension.BuildRegisterRequest {
	metadata := cloneStringMap(build.Metadata)
	if build.PhaseResourcePatch != "" {
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[sandboxcfg.NsResource] = build.PhaseResourcePatch
	}
	return &conductorextension.BuildRegisterRequest{
		TemplateID: build.TemplateID,
		Profile:    conductorextension.Profile(build.Profile),
		Kind:       conductorextension.BuildKind(build.Kind),
		Names:      append([]string(nil), build.Names...),
		Aliases:    append([]string(nil), build.Aliases...),
		Resources:  cloneBuildResources(build.Resources),
		Metadata:   metadata,
		Builder:    publicBuildOptions(build.Builder),
	}
}

func buildRegisterRequestFromCandidate(candidate *normalizedBuildRegistration) *conductorextension.BuildRegisterRequest {
	build := &types.Build{
		TemplateID: candidate.templateID,
		Profile:    candidate.profile, Kind: candidate.kind,
		Names: candidate.names, Aliases: candidate.aliases,
		Resources: candidate.resources, PhaseResourcePatch: candidate.phaseResourcePatch,
		Metadata: candidate.metadata, Builder: candidate.builder,
	}
	return buildRegisterRequestFromBuild(build)
}

// normalizeBuildRegistration is pure with respect to durable state and host
// resources. It is run once for preliminary request validation and again after
// a Hook so every modified field passes the core's canonical policy.
func (o *Orchestrator) normalizeBuildRegistration(request *conductorextension.BuildRegisterRequest, mmdsHeader *string) (*normalizedBuildRegistration, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: build registration candidate is required", api.ErrBadRequest)
	}
	profile := types.Profile(request.Profile)
	if !profile.Valid() {
		return nil, fmt.Errorf("%w: unknown build profile %q", api.ErrBadRequest, request.Profile)
	}
	kind := types.Kind(request.Kind)
	switch kind {
	case types.KindImg, types.KindSnp:
	default:
		return nil, fmt.Errorf("%w: unknown build kind %q", api.ErrBadRequest, request.Kind)
	}
	resources := internalBuildResources(request.Resources)
	if err := resources.ValidateRequired(); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if _, err := builderResourceProperties(resources); err != nil {
		o.recordRegistrationRejection("systemd_encoding")
		return nil, fmt.Errorf("%w: build resources cannot be enforced by systemd: %v", api.ErrBadRequest, err)
	}
	executionLimit, err := configresolve.BuilderExecutionLimit(o.cfg.Builder)
	if err != nil {
		return nil, err
	}
	if !executionLimit.AllowsOne(resources) {
		o.recordRegistrationRejection("execution_fit")
		return nil, fmt.Errorf("%w: build resources cannot fit builder.admission.execution", api.ErrBadRequest)
	}
	metadata := cloneStringMap(request.Metadata)
	if _, present := metadata[clusterstate.ObjectMetadataKey]; present {
		return nil, fmt.Errorf("%w: %s is node-managed cluster context", api.ErrBadRequest, clusterstate.ObjectMetadataKey)
	}
	builder := internalBuildOptions(clonePublicBuildOptions(request.Builder))
	if builder.Resources != nil {
		return nil, fmt.Errorf("%w: build builder.resources must be normalized into resources", api.ErrBadRequest)
	}
	mmdsDoc, metadata, err := sandboxcfg.ExtractMMDS(metadata, cloneString(mmdsHeader), o.mmdsPolicy())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	metadata, err = sandboxcfg.NormalizeResourceMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if _, err := sandboxcfg.ParseSpec(metadata); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	phaseResourcePatch := metadata[sandboxcfg.NsResource]
	if phaseResourcePatch != "" {
		metadata = cloneStringMapWithout(metadata, sandboxcfg.NsResource)
	}
	if err := o.validateBuildPhaseResources(phaseResourcePatch); err != nil {
		return nil, err
	}
	if err := o.validateBuildOptions(builder, false); err != nil {
		return nil, err
	}
	return &normalizedBuildRegistration{
		templateID: request.TemplateID,
		profile:    profile, kind: kind,
		names: append([]string(nil), request.Names...), aliases: append([]string(nil), request.Aliases...),
		resources: resources, metadata: metadata, builder: builder,
		phaseResourcePatch: phaseResourcePatch, mmds: mmdsDoc,
	}, nil
}

// mmdsSecretHeader retains request-scoped initial MMDS values entirely inside
// the core while a Build Hook edits the routes-only metadata projection. The
// final ExtractMMDS pass rebinds those values to the Hook's final routes and
// rejects values whose referenced route was removed.
func mmdsSecretHeader(document sandboxcfg.MMDSDocument) (*string, error) {
	if !document.SecretsPresent {
		return nil, nil
	}
	secrets := make(map[string]string, len(document.SecretValues))
	for name, value := range document.SecretValues {
		secrets[name] = string(value)
	}
	raw, err := json.Marshal(struct {
		Secrets map[string]string `json:"secrets"`
	}{Secrets: secrets})
	if err != nil {
		return nil, fmt.Errorf("encode retained MMDS secret values: %w", err)
	}
	header := string(raw)
	return &header, nil
}

func registeredBuildFromCandidate(buildID string, pair store.KeyPair, candidate *normalizedBuildRegistration, fromImage string) *types.Build {
	return &types.Build{
		BuildID: buildID, TemplateID: candidate.templateID,
		APISecret: pair.APISecret, ManifestKey: pair.ManifestKey,
		Profile: candidate.profile, Kind: candidate.kind,
		Status: types.BuildRegistered, FromImage: fromImage,
		Names: candidate.names, Aliases: candidate.aliases,
		Resources: candidate.resources, PhaseResourcePatch: candidate.phaseResourcePatch,
		Metadata: candidate.metadata, Builder: candidate.builder,
	}
}

func (o *Orchestrator) validateInitialBuildMMDS(build *types.Build, document sandboxcfg.MMDSDocument) (*mmdsInitialRouteSecretValues, error) {
	initial := initialMMDSRouteSecretValues(document)
	if initial == nil {
		return nil, nil
	}
	transportRow := &types.Sandbox{
		ID: "build-" + build.BuildID, Profile: build.Profile, TemplateID: build.TemplateID,
		State: types.StateRunning, RunID: "build-registration-check",
		APISecret: build.APISecret, ManifestKey: build.ManifestKey, Metadata: build.Metadata,
	}
	if err := validateInitialMMDSRouteEntry(transportRow, initial); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	return initial, nil
}

// clusterBuildRegistrationRequestDigest authenticates the original canonical
// command, including confidential registration fields, without persisting
// those fields in plaintext. CmdID is deliberately excluded so an ACK retry can
// carry a new transport command ID while retaining the same business identity.
func clusterBuildRegistrationRequestDigest(manifestKey string, command *routesync.Command) (string, error) {
	type identity struct {
		BuildID              string
		TemplateID           string
		Profile              string
		APISecretFingerprint string
		ImageRepo            string
		RegistryAuth         string
		Resources            *routesync.BuildResources
		Config               map[string]string
		MMDSSecrets          map[string]string
	}
	payload, err := json.Marshal(identity{
		BuildID: command.BuildID, TemplateID: command.TemplateRef, Profile: command.Profile,
		APISecretFingerprint: command.APISecretFingerprint,
		ImageRepo:            command.ImageRepo, RegistryAuth: command.RegistryAuth,
		Resources: command.BuildResources, Config: cloneStringMap(command.Config),
		MMDSSecrets: cloneStringMap(command.BuildMMDSSecrets),
	})
	if err != nil {
		return "", fmt.Errorf("build_register: encode request identity: %w", err)
	}
	digest := hmac.New(sha256.New, []byte(manifestKey))
	_, _ = digest.Write(payload)
	return hex.EncodeToString(digest.Sum(nil)), nil
}
