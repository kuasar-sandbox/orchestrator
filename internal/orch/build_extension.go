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
	templateID  string
	profile     types.Profile
	names       []string
	aliases     []string
	resources   types.BuildResources
	metadata    map[string]string
	env         map[string]string
	secure      bool
	builder     types.BuildOptions
	mmds        sandboxcfg.MMDSDocument
	credentials sandboxcfg.Credentials
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
		Names:      nonEmpty(spec.Name),
		Aliases:    append([]string(nil), spec.Tags...),
		Resources:  cloneBuildResources(spec.Resources),
		Metadata:   cloneStringMap(spec.Metadata),
		Env:        cloneStringMap(spec.EnvVars),
		Secure:     spec.Secure,
		Builder:    publicBuildOptions(builder),
	}
}

func buildRegisterRequestFromBuild(build *types.Build) *conductorextension.BuildRegisterRequest {
	return &conductorextension.BuildRegisterRequest{
		TemplateID: build.TemplateID,
		Profile:    conductorextension.Profile(build.Profile),
		Names:      append([]string(nil), build.Names...),
		Aliases:    append([]string(nil), build.Aliases...),
		Resources:  cloneBuildResources(build.Resources),
		Metadata:   cloneStringMap(build.Metadata),
		Env:        cloneStringMap(build.Env),
		Secure:     build.Secure,
		Builder:    publicBuildOptions(build.Builder),
	}
}

func buildRegisterRequestFromCandidate(candidate *normalizedBuildRegistration) *conductorextension.BuildRegisterRequest {
	build := &types.Build{
		TemplateID: candidate.templateID,
		Profile:    candidate.profile,
		Names:      candidate.names, Aliases: candidate.aliases,
		Resources: candidate.resources,
		Metadata:  candidate.metadata, Env: candidate.env, Secure: candidate.secure,
		Builder: candidate.builder,
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
	if builder.Target != nil {
		if err := builder.Target.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
		}
	}
	mmdsDoc, metadata, err := sandboxcfg.ExtractMMDS(metadata, cloneString(mmdsHeader), o.mmdsPolicy())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	credentials, metadata, err := sandboxcfg.ExtractCredentials(metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if err := validateSandboxCredentialOverrides(profile, credentials); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if _, present := metadata[sandboxcfg.NsRestore]; present {
		return nil, fmt.Errorf("%w: %s is not valid for template builds", api.ErrBadRequest, sandboxcfg.NsRestore)
	}
	metadata, err = sandboxcfg.NormalizeResourceMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	metadata, err = sandboxcfg.NormalizeTrafficMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	metadata, err = sandboxcfg.NormalizeCheckpointMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	spec, err := sandboxcfg.ParseSpec(metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if err := sandboxcfg.ValidateTrafficForProfile(profile, spec.Traffic); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if err := sandboxcfg.ValidateLaunchForProfile(profile, spec.Launch); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if err := o.validateBuildOptions(builder, false); err != nil {
		return nil, err
	}
	if err := validateExplicitBuildTargetConfig(builder.Target, metadata, request.Env, request.Secure, mmdsDoc, credentials); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	return &normalizedBuildRegistration{
		templateID: request.TemplateID,
		profile:    profile,
		names:      append([]string(nil), request.Names...), aliases: append([]string(nil), request.Aliases...),
		resources: resources, metadata: metadata,
		env: cloneStringMap(request.Env), secure: request.Secure, builder: builder,
		mmds: mmdsDoc, credentials: credentials,
	}, nil
}

func validateExplicitBuildTargetConfig(target *types.BuildTarget, metadata, env map[string]string, secure bool, mmds sandboxcfg.MMDSDocument, credentials sandboxcfg.Credentials) error {
	if target == nil {
		return nil
	}
	instanceOnly := secure || credentials != (sandboxcfg.Credentials{}) ||
		mmds.RoutesPresent || mmds.SecretsPresent || metadata[sandboxcfg.NsTraffic] != "" ||
		metadata[sandboxcfg.NsCheckpoint] != ""
	switch {
	case target.Kind == types.BuildTargetImage:
		for _, namespace := range []string{
			sandboxcfg.NsResource, sandboxcfg.NsTraffic, sandboxcfg.NsNetwork,
			sandboxcfg.NsLaunch, sandboxcfg.NsInit, sandboxcfg.NsMounts,
			sandboxcfg.NsFiles, sandboxcfg.NsMetadata, sandboxcfg.NsCheckpoint,
			sandboxcfg.NsMMDS,
		} {
			if _, present := metadata[namespace]; present {
				return fmt.Errorf("explicit image target cannot carry Sandbox configuration %s", namespace)
			}
		}
		if len(env) != 0 || instanceOnly {
			return fmt.Errorf("explicit image target cannot carry Sandbox instance configuration")
		}
	case target.Kind == types.BuildTargetSandbox && !target.Memory && instanceOnly:
		return fmt.Errorf("sandbox target with memory=false cannot carry traffic, credentials, MMDS, secure, or checkpoint configuration")
	}
	return nil
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

// retainBuildRegistrationCredentials keeps secret-bearing registration input
// outside the Extension projection while rebinding it to the post-Hook
// candidate for the final canonical validation pass.
func retainBuildRegistrationCredentials(request *conductorextension.BuildRegisterRequest, credentials sandboxcfg.Credentials) error {
	if request == nil || credentials == (sandboxcfg.Credentials{}) {
		return nil
	}
	raw, err := json.Marshal(credentials)
	if err != nil {
		return fmt.Errorf("encode retained build credentials: %w", err)
	}
	if request.Metadata == nil {
		request.Metadata = map[string]string{}
	}
	request.Metadata[sandboxcfg.NsCredentials] = string(raw)
	return nil
}

func registeredBuildFromCandidate(buildID string, pair store.KeyPair, candidate *normalizedBuildRegistration, fromImage string) *types.Build {
	return &types.Build{
		BuildID: buildID, TemplateID: candidate.templateID,
		APISecret: pair.APISecret, ManifestKey: pair.ManifestKey,
		Profile: candidate.profile,
		Status:  types.BuildRegistered, FromImage: fromImage,
		Names: candidate.names, Aliases: candidate.aliases,
		Resources: candidate.resources,
		Metadata:  candidate.metadata, Env: candidate.env, Secure: candidate.secure,
		ServiceSecret:      candidate.credentials.ServiceSecret,
		EnvdAccessToken:    candidate.credentials.EnvdAccessToken,
		TrafficAccessToken: candidate.credentials.TrafficAccessToken,
		Builder:            candidate.builder,
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
		Env                  map[string]string
		Secure               bool
		Credentials          *sandboxcfg.Credentials
		MMDSSecrets          map[string]string
	}
	payload, err := json.Marshal(identity{
		BuildID: command.BuildID, TemplateID: command.TemplateRef, Profile: command.Profile,
		APISecretFingerprint: command.APISecretFingerprint,
		ImageRepo:            command.ImageRepo, RegistryAuth: command.RegistryAuth,
		Resources: command.BuildResources, Config: cloneStringMap(command.Config),
		Env: cloneStringMap(command.BuildEnv), Secure: command.BuildSecure,
		Credentials: command.BuildCredentials,
		MMDSSecrets: cloneStringMap(command.BuildMMDSSecrets),
	})
	if err != nil {
		return "", fmt.Errorf("build_register: encode request identity: %w", err)
	}
	digest := hmac.New(sha256.New, []byte(manifestKey))
	_, _ = digest.Write(payload)
	return hex.EncodeToString(digest.Sum(nil)), nil
}
