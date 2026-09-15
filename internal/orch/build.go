package orch

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/remote"
	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

var hexKeyRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validateBuildIDRequest(buildID string) error {
	if err := types.ValidateBuildID(buildID); err != nil {
		return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	return nil
}

// maxCABundlePEMSize bounds the inline CA bundle a build may register. Larger
// bundles and a general build-input upload facility are tracked separately.
const maxCABundlePEMSize = 16 * 1024

// newRegisteredBuild allocates the transient templateID + build id, resolves the
// allowlist, and records a registered build. fromImage is pre-derived from
// builder_image_uri_mask — the convention the e2b CLI pushes its client-built
// image under ({templateID}/{buildID}); the v3 trigger may still override it.
func (o *Orchestrator) newRegisteredBuild(ctx context.Context, apiKey string, spec api.RegisterSpec) (*types.Build, error) {
	request := buildRegisterRequest(spec, "")
	preliminary, err := o.normalizeBuildRegistration(request, spec.MMDSHeader)
	if err != nil {
		return nil, err
	}
	pair, err := o.resolveAllowed(ctx, apiKey)
	if err != nil {
		return nil, err
	}
	if pair.APISecret == "" {
		return nil, api.ErrNotAllowed
	}
	bid, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("orch: new build id: %w", err)
	}
	if err := types.ValidateBuildID(bid.String()); err != nil {
		return nil, fmt.Errorf("orch: generated build id: %w", err)
	}
	tid, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("orch: new template id: %w", err)
	}
	templateID := types.TransientPrefix + tid.String()
	preliminary.templateID = templateID
	final := preliminary
	if o.extensionBuildHook != nil {
		request = buildRegisterRequestFromCandidate(preliminary)
		operation := newBuildOperation(conductorextension.BuildOperationRegister, conductorextension.BuildOriginDirect, bid.String(), nil)
		operation.Register = cloneBuildRegisterRequest(request)
		operationID := operation.ID
		if err := o.callBuildHook(ctx, operation); err != nil {
			return nil, err
		}
		if err := validateBuildOperationEnvelope(operation, operationID, conductorextension.BuildOperationRegister, conductorextension.BuildOriginDirect, bid.String()); err != nil {
			return nil, err
		}
		request = cloneBuildRegisterRequest(operation.Register)
		if request == nil {
			return nil, fmt.Errorf("%w: extension removed build registration candidate", api.ErrBadRequest)
		}
		if request.TemplateID != templateID {
			return nil, fmt.Errorf("%w: extension changed core-owned template identity", api.ErrBadRequest)
		}
		secretHeader, err := mmdsSecretHeader(preliminary.mmds)
		if err != nil {
			return nil, err
		}
		if err := retainBuildRegistrationCredentials(request, preliminary.credentials); err != nil {
			return nil, err
		}
		final, err = o.normalizeBuildRegistration(request, secretHeader)
		if err != nil {
			return nil, err
		}
	}
	final.templateID = templateID
	b := registeredBuildFromCandidate(bid.String(), pair, final, o.imageURIFromMask(templateID, bid.String()))
	b.CreatedUnix = time.Now().Unix()
	initialMMDS, err := o.validateInitialBuildMMDS(b, final.mmds)
	if err != nil {
		return nil, err
	}
	var routesDigest string
	var secretValues store.MMDSRouteSecretValues
	if initialMMDS != nil {
		routesDigest, secretValues = initialMMDS.routesDigest, initialMMDS.values
	}
	b.RegistrationMMDSRoutesDigest = routesDigest
	registrationLimit, err := configresolve.BuilderRegistrationLimit(o.cfg.Builder)
	if err != nil {
		return nil, err
	}
	unlockRetention := o.buildRetention.Lock(b.BuildID)
	defer unlockRetention()
	unlockEvent := o.lockBuildEvent(b.BuildID)
	defer unlockEventFence(unlockEvent)
	registered, inserted, err := o.st.RegisterBuildWithMMDSRouteSecretValues(ctx, b, registrationLimit, routesDigest, secretValues)
	if errors.Is(err, store.ErrBuildRegistrationCapacity) {
		o.recordRegistrationRejection("capacity")
		return nil, fmt.Errorf("%w: %v", api.ErrBuildAdmission, err)
	}
	if err != nil {
		return nil, err
	}
	o.refreshBuildAdmissionGauges(ctx)
	o.buildCapacityChanged()
	if inserted {
		o.observeBuildUpsert(registered)
	}
	return registered, nil
}

// RegisterBuild handles POST /v3/templates (e2b v2 build system): record a
// registered build; the base image + start command arrive at trigger time.
func (o *Orchestrator) RegisterBuild(ctx context.Context, apiKey string, spec api.RegisterSpec) (*types.Build, error) {
	return o.newRegisteredBuild(ctx, apiKey, spec)
}

// TriggerBuild handles POST /v2/templates/{tid}/builds/{bid}: record the base
// image + steps + e2b start command and queue the build for the pool. Bare
// builds reject start/ready commands and always produce an image.
func (o *Orchestrator) TriggerBuild(ctx context.Context, apiKey, tid, bid string, spec api.TriggerSpec, auth api.BuildAuth) error {
	if err := validateBuildIDRequest(bid); err != nil {
		return err
	}
	b, err := o.st.GetBuild(ctx, bid)
	if err != nil {
		return err
	}
	if !ownsBuild(b, apiKey) || b.TemplateID != tid {
		return api.ErrNotFound
	}
	if b.Status != types.BuildRegistered {
		return &api.BuildStateConflictError{State: b.Status}
	}
	if err := validateBuildTriggerDefinition(b, spec); err != nil {
		return err
	}
	if o.extensionBuildHook != nil {
		precondition := buildPrecondition(b)
		operation := newBuildOperation(conductorextension.BuildOperationTrigger, conductorextension.BuildOriginDirect, bid, b)
		operation.Trigger = buildTriggerRequest(spec)
		operationID := operation.ID
		if err := o.callBuildHook(ctx, operation); err != nil {
			return err
		}
		if err := validateBuildOperationEnvelope(operation, operationID, conductorextension.BuildOperationTrigger, conductorextension.BuildOriginDirect, bid); err != nil {
			return err
		}
		request := cloneBuildTriggerRequest(operation.Trigger)
		if request == nil {
			return fmt.Errorf("%w: extension removed build trigger candidate", api.ErrBadRequest)
		}
		spec = internalBuildTriggerRequest(request)
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := o.st.GetBuild(ctx, bid)
		if err != nil {
			return err
		}
		if !ownsBuild(current, apiKey) || current.TemplateID != tid {
			return api.ErrNotFound
		}
		if current.Status != types.BuildRegistered {
			return &api.BuildStateConflictError{State: current.Status}
		}
		if !buildPreconditionMatches(precondition, current) {
			return api.ErrBuildChanged
		}
		b = current
		if err := validateBuildTriggerDefinition(b, spec); err != nil {
			return err
		}
	}
	b = cloneBuildForObservation(b)
	// COPY steps need files_storage configured AND the referenced context
	// already uploaded (client → files endpoint → bucket). Verify both up
	// front so the build fails fast instead of mid-pipeline.
	for i := range spec.Steps {
		if !strings.EqualFold(spec.Steps[i].Type, "COPY") {
			continue
		}
		if o.files == nil {
			return api.ErrFilesUnsupported
		}
		h := spec.Steps[i].FilesHash
		if h == "" {
			return fmt.Errorf("%w: COPY step %d has no filesHash", api.ErrBadRequest, i)
		}
		ok, ferr := o.files.Exists(ctx, b.TemplateID, h)
		if ferr != nil {
			return fmt.Errorf("build: check COPY context %s: %w", h, ferr)
		}
		if !ok {
			return fmt.Errorf("%w: COPY context %s not uploaded (PUT it via the files endpoint first)", api.ErrBadRequest, h)
		}
	}
	switch {
	case spec.FromTemplate != "":
		// Resolve the base template ref to its canonical persist id; the
		// task normalizes SBX/SNP to a cold Sandbox E, while the pipeline
		// materializes its complete root and inherits E start/ready defaults.
		ref := o.resolveTemplateAlias(ctx, apiKey, spec.FromTemplate)
		if _, perr := types.ParseTemplateID(ref); perr != nil {
			return fmt.Errorf("build: fromTemplate %q: not a known template", spec.FromTemplate)
		}
		b.FromTemplate, b.FromImage = ref, ""
	default:
		// The real e2b CLI pushes its client-built image to <mask>/{templateID}:{buildID}
		// and sends no image ref; derive fromImage from the configured mask
		// (which must be reachable from INSIDE a build sandbox — the pull
		// runs in the guest).
		fromImage := spec.FromImage
		if fromImage == "" {
			fromImage = o.imageURIFromMask(b.TemplateID, b.BuildID)
		}
		if fromImage == "" {
			return fmt.Errorf("build: fromImage is required (no builder_image_uri_mask configured)")
		}
		b.FromImage, b.FromTemplate = fromImage, ""
	}
	b.Steps = internalBuildSteps(publicBuildSteps(spec.Steps))
	b.StartCmd = spec.StartCmd
	b.ReadyCmd = spec.ReadyCmd
	if err := validateKnownBuildTargetConfig(b); err != nil {
		return fmt.Errorf("%w: %w", api.ErrBadRequest, err)
	}
	// Node builder.insecure_registry (plain HTTP) and a per-build registry TLS
	// policy address disjoint registry schemes; allowing both would be
	// contradictory, so reject the combination. (fromTemplate + registry.tls
	// is rejected by validateBuildOptions below.)
	if o.cfg.Builder.InsecureRegistry && b.Builder.Registry != nil && b.Builder.Registry.TLS != nil {
		return fmt.Errorf("%w: builder.registry.tls conflicts with node builder.insecure_registry", api.ErrBadRequest)
	}
	if err := o.validateBuildOptions(b.Builder, b.FromTemplate != ""); err != nil {
		return err
	}
	// Kind remains unset until the worker returns a target-consistent terminal
	// artifact. Requested target is immutable registration input.
	b.Kind = ""
	if b.RegistryAuth, err = o.resolveBuildCreds(ctx, b, auth.PullToken, auth.RegistryUsername, auth.RegistryPassword); err != nil {
		return err
	}
	unlockEvent := o.lockBuildEvent(b.BuildID)
	defer unlockEventFence(unlockEvent)
	b.Status = types.BuildWaiting
	b.WaitingUnix = time.Now().Unix()
	committed, err := o.commitBuildTrigger(ctx, b)
	if err != nil {
		return err
	}
	if committed {
		o.publishBuildState(b.BuildID, string(types.BuildWaiting), "", "")
		o.observeBuildUpsert(b)
		o.buildCapacityChanged()
		return nil
	}
	current, err := o.st.GetBuild(ctx, bid)
	if err != nil {
		return err
	}
	if !ownsBuild(current, apiKey) || current.TemplateID != tid {
		return api.ErrNotFound
	}
	return &api.BuildStateConflictError{State: current.Status}
}

func validateBuildTriggerDefinition(build *types.Build, spec api.TriggerSpec) error {
	if err := assertBuildResources(build.Resources, spec.ResourceAssertion); err != nil {
		return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if !build.Profile.Valid() {
		return fmt.Errorf("%w: build has unknown profile %q", api.ErrBadRequest, build.Profile)
	}
	if build.Profile == types.ProfileBare && (spec.StartCmd != "" || spec.ReadyCmd != "") {
		return fmt.Errorf("%w: bare profile builds do not support startCmd or readyCmd", api.ErrBadRequest)
	}
	if build.Builder.Target != nil && build.Builder.Target.Kind == types.BuildTargetImage &&
		(spec.StartCmd != "" || spec.ReadyCmd != "") {
		return fmt.Errorf("%w: explicit image target conflicts with trigger startCmd/readyCmd", api.ErrBadRequest)
	}
	if spec.FromImage != "" && spec.FromTemplate != "" {
		return fmt.Errorf("%w: fromImage and fromTemplate are mutually exclusive", api.ErrBadRequest)
	}
	return nil
}

func assertBuildResources(resources types.BuildResources, assertion buildcfg.ResourcePatch) error {
	for _, leaf := range []struct {
		name string
		got  *int64
		want int64
	}{
		{"cpu", assertion.CPU, resources.CPU},
		{"memory", assertion.Memory, resources.Memory},
		{"storage", assertion.Storage, resources.Storage},
	} {
		if leaf.got != nil && *leaf.got != leaf.want {
			return fmt.Errorf("trigger build.resources.%s=%d does not match registered value %d", leaf.name, *leaf.got, leaf.want)
		}
	}
	return nil
}

func cloneStringMapWithout(in map[string]string, excluded string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in)-1)
	for key, value := range in {
		if key != excluded {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneBuildTarget(target *types.BuildTarget) *types.BuildTarget {
	if target == nil {
		return nil
	}
	copy := *target
	return &copy
}

// buildHasSandboxConfig reports inputs that require a Sandbox artifact.
// Network is also an execution input for image builds; keep it in the parsed
// spec and Sandbox projection without making it require a Sandbox target.
func buildHasSandboxConfig(build *types.Build) bool {
	if build == nil {
		return false
	}
	if len(build.Env) != 0 {
		return true
	}
	for _, namespace := range []string{
		sandboxcfg.NsResource, sandboxcfg.NsTraffic,
		sandboxcfg.NsLaunch, sandboxcfg.NsInit, sandboxcfg.NsMounts,
		sandboxcfg.NsFiles, sandboxcfg.NsMetadata,
	} {
		if _, present := build.Metadata[namespace]; present {
			return true
		}
	}
	return false
}

func buildHasInstanceConfig(build *types.Build) bool {
	if build == nil {
		return false
	}
	return build.Secure || build.ServiceSecret != "" || build.EnvdAccessToken != "" ||
		build.TrafficAccessToken != "" || build.Metadata[sandboxcfg.NsTraffic] != "" ||
		build.Metadata[sandboxcfg.NsMMDS] != "" ||
		build.Metadata[sandboxcfg.NsCheckpoint] != ""
}

func buildSandboxNamespaces(metadata map[string]string) []string {
	var namespaces []string
	for _, namespace := range []string{
		sandboxcfg.NsResource, sandboxcfg.NsTraffic, sandboxcfg.NsNetwork,
		sandboxcfg.NsLaunch, sandboxcfg.NsInit, sandboxcfg.NsMounts,
		sandboxcfg.NsFiles, sandboxcfg.NsMetadata,
	} {
		if _, present := metadata[namespace]; present {
			namespaces = append(namespaces, namespace)
		}
	}
	return namespaces
}

func (o *Orchestrator) validateBuildOptions(opts types.BuildOptions, fromTemplate bool) error {
	if opts.Target != nil {
		if err := opts.Target.Validate(); err != nil {
			return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
		}
	}
	if r := opts.Referer; r != nil {
		explicitReferer := r.Enabled != nil && *r.Enabled
		explicitWriteback := r.Writeback != nil && *r.Writeback
		if fromTemplate && (explicitReferer || explicitWriteback) {
			return fmt.Errorf("%w: builder.referer applies only to fromImage builds", api.ErrBadRequest)
		}
		if r.Enabled != nil && !*r.Enabled && explicitWriteback {
			return fmt.Errorf("%w: builder.referer.writeback=true requires builder.referer.enabled=true", api.ErrBadRequest)
		}
		cfg := o.cfg.Builder.Referer
		if explicitReferer && !cfg.Enabled {
			return fmt.Errorf("%w: builder.referer.enabled=true exceeds node builder.referer.enabled=false", api.ErrBadRequest)
		}
		if explicitWriteback {
			switch {
			case !cfg.Enabled:
				return fmt.Errorf("%w: builder.referer.writeback=true exceeds node builder.referer.enabled=false", api.ErrBadRequest)
			case !cfg.WritebackEnabled():
				return fmt.Errorf("%w: builder.referer.writeback=true exceeds node builder.referer.writeback=false", api.ErrBadRequest)
			}
		}
	}
	if err := validateRegistryTLS(opts.Registry, fromTemplate); err != nil {
		return err
	}
	return nil
}

// validateRegistryTLS checks the per-build registry TLS trust policy. It is
// register-time only (a trigger-time builder.registry is rejected before Merge
// runs); fromTemplate builds must not carry it (their base image is already
// resolved, so there is no source-registry pull to tune).
func validateRegistryTLS(reg *types.BuildRegistryOptions, fromTemplate bool) error {
	if reg == nil || reg.TLS == nil {
		return nil
	}
	tls := reg.TLS
	if tls.CABundlePEM == "" && !tls.InsecureSkipVerify {
		return fmt.Errorf("%w: builder.registry.tls is empty", api.ErrBadRequest)
	}
	if tls.CABundlePEM != "" && tls.InsecureSkipVerify {
		return fmt.Errorf("%w: builder.registry.tls.ca_bundle_pem and builder.registry.tls.insecure_skip_verify are mutually exclusive", api.ErrBadRequest)
	}
	if fromTemplate {
		return fmt.Errorf("%w: builder.registry.tls applies only to fromImage builds", api.ErrBadRequest)
	}
	if tls.CABundlePEM != "" {
		if len(tls.CABundlePEM) > maxCABundlePEMSize {
			return fmt.Errorf("%w: builder.registry.tls.ca_bundle_pem exceeds %d bytes", api.ErrBadRequest, maxCABundlePEMSize)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(tls.CABundlePEM)) {
			return fmt.Errorf("%w: builder.registry.tls.ca_bundle_pem has no parseable X.509 certificate", api.ErrBadRequest)
		}
	}
	return nil
}

func (o *Orchestrator) effectiveImportReferer(b *types.Build) (configsock.BuildImportReferer, error) {
	cfg := o.cfg.Builder.Referer
	if !cfg.Enabled || b.FromImage == "" {
		return configsock.BuildImportReferer{}, nil
	}
	enabled := true
	writeback := cfg.WritebackEnabled()
	if r := b.Builder.Referer; r != nil {
		if r.Enabled != nil {
			enabled = *r.Enabled
		}
		if r.Writeback != nil {
			writeback = *r.Writeback
		}
	}
	if !enabled {
		return configsock.BuildImportReferer{}, nil
	}
	ck, err := manifest.ParseHexKey(b.ManifestKey)
	if err != nil {
		return configsock.BuildImportReferer{}, fmt.Errorf("build: manifest key: %w", err)
	}
	owner := remote.Owner{
		ArtifactType: remote.RefererArtifactType,
		Desc:         cfg.Desc,
		Key:          cfg.Key,
	}.OwnerValue(ck[:])
	return configsock.BuildImportReferer{
		Enabled:   true,
		Fallback:  cfg.FallbackEnabled(),
		Writeback: writeback,
		Owner:     owner,
		Validity:  cfg.Validity,
	}, nil
}

// effectiveRegistryTLS flattens the per-build registry TLS options into the
// resolved form handed to the build unit. Only fromImage builds pull from a
// source registry in Phase A, so fromTemplate builds get no TLS config (their
// base is already resolved). The content (inline PEM) is copied verbatim — no
// host file path crosses the config socket.
func (o *Orchestrator) effectiveRegistryTLS(b *types.Build) *configsock.BuildRegistryTLS {
	if b.FromImage == "" {
		return nil
	}
	r := b.Builder.Registry
	if r == nil || r.TLS == nil {
		return nil
	}
	return &configsock.BuildRegistryTLS{
		CABundlePEM:        r.TLS.CABundlePEM,
		InsecureSkipVerify: r.TLS.InsecureSkipVerify,
	}
}

// resolveBuildCreds picks the registry pull credentials for this build and returns
// them as a regcreds.Creds JSON ("" = anonymous), to be stored encrypted on the build
// row and injected as FLATTEN_REGISTRY_* at flatten time. Precedence: the per-build
// pull token (api_headers, opaque, manifest-key-sealed) > the SDK's fromImageRegistry
// (cleartext username/password) > the tenant default (manifest_keys) > anonymous.
func (o *Orchestrator) resolveBuildCreds(ctx context.Context, b *types.Build, pullToken, regUser, regPass string) (string, error) {
	clusterAuth, isCluster := o.clusterBuildCreds(b)
	var creds regcreds.Creds
	switch {
	case pullToken != "":
		c, err := regcreds.Open(b.ManifestKey, pullToken)
		if err != nil {
			return "", fmt.Errorf("build: pull token: %w", err)
		}
		creds = c
	case regUser != "":
		creds = regcreds.Creds{Username: regUser, Password: regPass}
	case isCluster:
		// Cluster build: use the registry-delivered transient creds (cluster.md),
		// not the node's stored registry_auth_enc.
		creds = regcreds.CredsForImage(clusterAuth, b.FromImage)
	default:
		authJSON, err := o.st.RegistryAuthForKeyPair(ctx, store.KeyPair{
			APISecret: b.APISecret, ManifestKey: b.ManifestKey,
		})
		if err != nil {
			return "", err
		}
		creds = regcreds.CredsForImage(authJSON, b.FromImage)
	}
	if creds.Empty() {
		return "", nil
	}
	js, err := json.Marshal(creds)
	return string(js), err
}

// BuildStatus handles GET /templates/{tid}/builds/{bid}/status.
func (o *Orchestrator) BuildStatus(ctx context.Context, apiKey, tid, bid string) (*types.Build, error) {
	if err := validateBuildIDRequest(bid); err != nil {
		return nil, err
	}
	b, err := o.st.GetBuild(ctx, bid)
	if err != nil {
		return nil, err
	}
	if !ownsBuild(b, apiKey) || b.TemplateID != tid {
		return nil, api.ErrNotFound
	}
	return b, nil
}

// ListTemplates returns the tenant's ready templates (for GET /templates).
func (o *Orchestrator) ListTemplates(ctx context.Context, apiKey string) ([]*types.Build, error) {
	all, err := o.st.BuildsByStatus(ctx, types.BuildReady)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, b := range all {
		if verifyKey(apiKey, b.APISecret) {
			out = append(out, b)
		}
	}
	return out, nil
}

// resolveTemplateAlias accepts a canonical TemplateID directly, then maps a
// retention-bounded reference — the transient register id the SDK reports as
// BuildInfo.template_id, or a build name/alias — to its built persist id. The
// canonical form is self-describing and does not depend on a retained Build row.
// Returns "" if a non-canonical ref has no ready Build owned by this api key.
func (o *Orchestrator) resolveTemplateAlias(ctx context.Context, apiKey, ref string) string {
	if template, err := types.ParseTemplateID(ref); err == nil {
		return template.String()
	}
	if b := o.templateBuild(ctx, apiKey, ref); b != nil {
		return b.PersistID
	}
	return ""
}

// templateBuild resolves a retention-bounded template ref (persist id,
// transient id, name, or alias) to its ready Build row. It supports status-window
// conveniences only; canonical Create never treats this row as template data.
func (o *Orchestrator) templateBuild(ctx context.Context, apiKey, ref string) *types.Build {
	builds, err := o.ListTemplates(ctx, apiKey)
	if err != nil {
		return nil
	}
	for _, b := range builds {
		if ref == b.PersistID || ref == b.TemplateID {
			return b
		}
		for _, n := range append(append([]string{}, b.Names...), b.Aliases...) {
			if n == ref {
				return b
			}
		}
	}
	return nil
}

// --- builder durable admission + scheduler ---

// BuildPool is a wakeup loop only; SQLite is the registration/execution
// authority. Waiting rows are considered in stable FIFO order and a conditional
// DB transition claims their complete resource vector before any unit side
// effect. Temporary aggregate pressure stops the pass; a Build that can never
// fit the current configuration is terminally rejected so it cannot block the
// FIFO head forever after an operator tightens limits.
func (o *Orchestrator) BuildPool(ctx context.Context, interval time.Duration) {
	executionLimit, err := configresolve.BuilderExecutionLimit(o.cfg.Builder)
	if err != nil {
		o.log.Error("builder scheduler disabled", "err", err)
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	runPass := func() {
		now := time.Now()
		if err := o.reapRequestedBuilds(ctx); err != nil {
			o.log.Warn("retry requested build deletion", "err", err)
		}
		o.expireBuildQueues(ctx, now)
		waiting, err := o.st.BuildsByStatus(ctx, types.BuildWaiting)
		if err != nil {
			o.log.Warn("list waiting builds", "err", err)
			return
		}
		for _, candidate := range waiting {
			finish, err := o.buildOps.Begin(ctx)
			if err != nil {
				return
			}
			unlockEvent := o.lockBuildEvent(candidate.BuildID)
			current, readErr := o.st.GetBuild(ctx, candidate.BuildID)
			if readErr != nil || current == nil || current.TemplateID != candidate.TemplateID || current.CancelRequestedUnix != 0 || current.DeleteRequestedUnix != 0 || current.Status != types.BuildWaiting {
				unlockEventFence(unlockEvent)
				finish()
				if readErr != nil {
					o.log.Warn("read waiting build", "err", readErr)
					return
				}
				continue
			}
			executionCtx, cancelExecution := context.WithCancel(ctx)
			owner := &pendingBuild{build: candidate, templateID: candidate.TemplateID, executionCtx: executionCtx, cancelExecution: cancelExecution, done: make(chan struct{})}
			o.pendMu.Lock()
			if o.pend[candidate.BuildID] != nil {
				o.pendMu.Unlock()
				cancelExecution()
				unlockEventFence(unlockEvent)
				finish()
				continue
			}
			o.pend[candidate.BuildID] = owner
			o.pendMu.Unlock()
			won, err := o.st.ClaimBuildExecution(ctx, candidate.BuildID, executionLimit, now)
			if err != nil {
				o.releaseBuildOwner(candidate, owner)
				finish()
				if errors.Is(err, store.ErrBuildExecutionUnfit) {
					reason := "build resources no longer fit configured execution limits"
					expired, expireErr := o.st.ExpireBuild(ctx, candidate.BuildID, candidate.TemplateID, types.BuildWaiting, reason)
					if expireErr != nil {
						unlockEventFence(unlockEvent)
						o.log.Warn("reject permanently unfit build", "bid", candidate.BuildID, "err", expireErr)
						return
					}
					if expired {
						o.recordExecutionRejection("configuration")
						o.refreshBuildAdmissionGauges(ctx)
						failed := cloneBuildForObservation(candidate)
						markBuildRemoved(failed, reason)
						o.publishCommittedBuild(failed)
						o.buildCapacityChanged()
						o.observeBuildRemove(failed)
					}
					unlockEventFence(unlockEvent)
					continue
				}
				unlockEventFence(unlockEvent)
				o.log.Warn("build execution admission", "bid", candidate.BuildID, "err", err)
				return
			}
			if !won {
				o.releaseBuildOwner(candidate, owner)
				unlockEventFence(unlockEvent)
				o.recordExecutionWouldWait(ctx)
				finish()
				return
			}
			o.refreshBuildAdmissionGauges(ctx)
			// The waiting row's immutable work order is already complete. The
			// conditional claim changes only durable lifecycle ownership, so use
			// this exact snapshot and avoid introducing a post-claim read failure
			// window that could strand execution capacity until restart.
			claimed := candidate
			claimed.Status = types.BuildBuilding
			claimed.ExecutionClaimed = true
			claimed.ExecutionClaimedUnix = now.Unix()
			claimed.EnforcementStatus = "pending"
			o.publishBuildState(claimed.BuildID, "building", "", "")
			o.observeBuildUpsert(claimed)
			o.buildCapacityChanged()
			unlockEventFence(unlockEvent)
			go func() {
				defer finish()
				defer o.releaseBuildOwner(claimed, owner)
				res, runErr := o.runBuildUnit(executionCtx, claimed)
				o.completeBuild(ctx, claimed, res, runErr)
			}()
		}
	}
	runPass()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			runPass()
		case <-o.buildWake:
			runPass()
		}
	}
}

func (o *Orchestrator) expireBuildQueues(ctx context.Context, now time.Time) {
	for _, expiry := range []struct {
		state  types.BuildState
		before int64
		reason string
	}{
		{types.BuildRegistered, now.Add(-o.cfg.Builder.RegistrationTTLDur()).Unix(), "build registration expired before trigger"},
		{types.BuildWaiting, now.Add(-o.cfg.Builder.QueueTTLDur()).Unix(), "build execution queue TTL expired"},
	} {
		builds, err := o.st.BuildsByStatus(ctx, expiry.state)
		if err != nil {
			o.log.Warn("scan build TTL", "state", expiry.state, "err", err)
			continue
		}
		for _, build := range builds {
			stamp := build.CreatedUnix
			if expiry.state == types.BuildWaiting {
				stamp = build.WaitingUnix
			}
			if stamp <= 0 || stamp > expiry.before {
				continue
			}
			unlockEvent := o.lockBuildEvent(build.BuildID)
			expired, err := o.st.ExpireBuild(ctx, build.BuildID, build.TemplateID, expiry.state, expiry.reason)
			if err != nil {
				unlockEventFence(unlockEvent)
				o.log.Warn("expire build", "bid", build.BuildID, "err", err)
				continue
			}
			if expired {
				o.recordBuildExpired(expiry.state)
				o.refreshBuildAdmissionGauges(ctx)
				failed := cloneBuildForObservation(build)
				markBuildRemoved(failed, expiry.reason)
				o.publishCommittedBuild(failed)
				o.buildCapacityChanged()
				o.observeBuildRemove(failed)
			}
			unlockEventFence(unlockEvent)
		}
	}
}

type buildResult = configsock.BuildResult

var errBuildCleanupPending = errors.New("build runtime cleanup remains pending")
var errBuildRecoveryInProgress = errors.New("build: conductor recovery is still in progress")

type buildStageError struct {
	stage string
	err   error
}

func (e *buildStageError) Error() string { return e.err.Error() }
func (e *buildStageError) Unwrap() error { return e.err }

func buildFailed(stage string, err error) error {
	if err == nil {
		return nil
	}
	var existing *buildStageError
	if errors.As(err, &existing) {
		return err
	}
	return &buildStageError{stage: stage, err: err}
}

func buildFailureStage(err error) string {
	var staged *buildStageError
	if errors.As(err, &staged) {
		return staged.stage
	}
	return "runtime"
}

type buildCleanupPendingError struct {
	cause     error
	cleanup   error
	unit      string
	port      string
	persisted bool
}

func (e *buildCleanupPendingError) Error() string {
	return errors.Join(errBuildCleanupPending, e.cause, e.cleanup).Error()
}

func (e *buildCleanupPendingError) Unwrap() []error {
	errList := []error{errBuildCleanupPending}
	if e.cause != nil {
		errList = append(errList, e.cause)
	}
	if e.cleanup != nil {
		errList = append(errList, e.cleanup)
	}
	return errList
}

// retainBuildCleanup keeps a pipeline cause distinct from ownership-cleanup
// failures. In particular, a durably accepted worker result has no pipeline
// cause: a transient failure to fence its unit must not turn that result into a
// failed Build once a retry establishes the fence.
func retainBuildCleanup(err, cleanup error, port string, persisted bool) *buildCleanupPendingError {
	pending := &buildCleanupPendingError{
		cause: err, cleanup: cleanup, port: port, persisted: persisted,
	}
	var existing *buildCleanupPendingError
	if !errors.As(err, &existing) {
		return pending
	}
	pending.cause = existing.cause
	pending.cleanup = errors.Join(existing.cleanup, cleanup)
	if pending.port == "" {
		pending.port = existing.port
	}
	pending.persisted = pending.persisted || existing.persisted
	return pending
}

// pendingBuild is the exact-run process-local owner. It starts in preparing
// form before network attachment, then carries the atomically persisted
// network/resources, final handoff, and result channel for the pipeline.
type pendingBuild struct {
	templateID             string
	executionCtx           context.Context
	cancelExecution        context.CancelFunc
	done                   chan struct{}
	build                  *types.Build
	runDir                 string
	baseDir                string
	sourceTemplate         bool
	sourceHasBuildCommands bool
	handoff                *buildTaskHandoff
	spec                   sandboxcfg.SandboxSpec
	network                sandboxcfg.NetworkSpec
	templateNetwork        sandboxcfg.NetworkSpec
	resources              rtconfig.ResourcesConfig
	sandboxResources       rtconfig.ResourcesConfig
	checkpointPolicy       sandboxcfg.SnapshotPolicy
	tapFD                  vswitch.TapFD
	mac                    string
	floating               string
	envdToken              string
	resultMu               sync.Mutex
	resultClosed           bool
	result                 chan configsock.BuildResult
}

func buildTapFD(t vswitch.TapFD) configsock.TapFDConfig {
	return configsock.TapFDConfig{
		Exec:    append([]string(nil), t.Exec...),
		Socket:  t.Socket,
		Request: t.Request,
		Timeout: t.Timeout,
	}
}

// executeBuild runs the target-selected build pipeline in a
// sandbox-builder@<run-id> unit:
// node-ctl run-builder waits for a build assignment, fetches the BuildSpec over
// the config-socket, and drives the required import/materialize/capture sandboxes
// itself (as direct children, in the unit's cgroup). This side owns what spans the unit: the
// BuildRunDir/BuildBaseDir, one vswitch slot the phases reuse sequentially, and —
// for the memory-capture phase under mmds.enabled — a synthetic route entry so the
// build sandbox's FC-mode envd can resolve itself.
func (o *Orchestrator) executeBuild(ctx context.Context, b *types.Build) {
	res, err := o.runBuildUnit(ctx, b)
	o.completeBuild(ctx, b, res, err)
}

func (o *Orchestrator) completeBuild(ctx context.Context, b *types.Build, res *buildResult, err error) {
	o.completeBuildWithPublisher(ctx, b, res, err, func(_, _, _, _ string) { o.publishCommittedBuild(b) })
}

func (o *Orchestrator) completeBuildWithPublisher(
	ctx context.Context,
	b *types.Build,
	res *buildResult,
	err error,
	publish func(buildID, state, templateID, reason string),
) {
	var cleanupPending *buildCleanupPendingError
	if errors.As(err, &cleanupPending) {
		var cleanupErr error
		err, cleanupErr = o.retryBuildCleanup(ctx, b, cleanupPending)
		if cleanupErr != nil {
			// The execution claim remains durable. A live controller keeps retrying;
			// cancellation hands the exact same ownership to startup reconciliation.
			o.log.Error("build cleanup incomplete; execution claim retained", "bid", b.BuildID, "err", cleanupErr)
			return
		}
	}
	o.commitBuildCompletion(ctx, b, res, err, publish)
}

// buildTerminalOutcome computes only result fields. Persistent intent/result
// arbitration happens under the event fence on every commit attempt.
func buildTerminalOutcome(b *types.Build, res *buildResult, runErr error) {
	if runErr == nil && res != nil {
		if err := validateBuildResult(b, *res); err != nil {
			b.Status, b.Reason = types.BuildError, "build produced an invalid result: "+err.Error()
			return
		}
	}
	switch {
	case runErr != nil:
		b.Status, b.Reason = types.BuildError, runErr.Error()
		return
	case res == nil:
		b.Status, b.Reason = types.BuildError, "build produced no result"
		return
	case res.Error != "":
		b.Status, b.Reason = types.BuildError, "build failed; see build logs"
		if res.FailureStage == "artifact_prepare" {
			b.Reason = res.Error
		}
		return
	}
	var ref string
	switch res.Target.ArtifactKind() {
	case types.KindImg:
		ref = res.ImageRef
	case types.KindSbx:
		ref = res.SandboxRef
	case types.KindSnp:
		ref = res.SnapshotRef
	}
	b.Kind = res.Target.ArtifactKind()
	b.PersistID = types.TemplateID{Profile: b.Profile, Kind: b.Kind, Ref: ref}.String()
	if _, err := types.ParseTemplateID(b.PersistID); err != nil {
		b.Status, b.Reason = types.BuildError, "build produced invalid portable ref: "+err.Error()
		return
	}
	b.StartCmd, b.ReadyCmd = res.StartCmd, res.ReadyCmd
	b.Status, b.Reason = types.BuildReady, ""
	b.Names = appendUnique(b.Names, b.PersistID)
	b.Aliases = appendUnique(b.Aliases, b.PersistID)
}

func (o *Orchestrator) publishTerminalBuild(publish func(string, string, string, string), build *types.Build, templateID string) {
	publish(build.BuildID, string(build.Status), templateID, build.Reason)
	if build.Status == types.BuildError {
		o.observeBuildRemove(build)
	} else {
		o.observeBuildUpsert(build)
	}
}

func (o *Orchestrator) persistTerminalBuild(ctx context.Context, build *types.Build) bool {
	return o.commitBuildCompletion(ctx, build, nil, nil, nil)
}

func (o *Orchestrator) commitBuildCompletion(ctx context.Context, build *types.Build, result *buildResult, runErr error, publish func(string, string, string, string)) bool {
	delay := 20 * time.Millisecond
	for attempt := 1; ; attempt++ {
		writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		unlock := o.lockBuildEvent(build.BuildID)
		current, err := o.st.GetBuild(writeCtx, build.BuildID)
		if err == nil && (current == nil || current.TemplateID != build.TemplateID || current.RunID != build.RunID || !current.ExecutionClaimed) {
			unlock()
			cancel()
			o.log.Warn("build terminal ownership lost", "bid", build.BuildID)
			return false
		}
		if err == nil {
			terminal := cloneBuildForObservation(current)
			if publish == nil { // callers already supplied their terminal diagnosis
				terminal.Status, terminal.Reason = build.Status, build.Reason
				terminal.PersistID, terminal.Kind = build.PersistID, build.Kind
				terminal.Names, terminal.Aliases = build.Names, build.Aliases
			} else {
				accepted, cause := result, runErr
				if current.ExecutionResult != nil {
					accepted, cause = current.ExecutionResult, nil
				}
				if current.CancelRequestedUnix != 0 && current.ExecutionResult == nil {
					accepted, cause = nil, errors.New(store.BuildCancelledReason)
				}
				buildTerminalOutcome(terminal, accepted, cause)
				result, runErr = accepted, cause
			}
			err = o.st.PutBuildTerminal(writeCtx, terminal)
			if err == nil {
				terminal.RunID = ""
				terminal.ExecutionClaimed, terminal.ExecutionClaimedUnix = false, 0
				terminal.EnforcementStatus, terminal.Phase, terminal.PhaseSandboxID = "", "", ""
				terminal.ExecutionResult = nil
				terminal.Metadata = cloneStringMapWithout(terminal.Metadata, sandboxcfg.NsMMDS)
				terminal.FinishedUnix = time.Now().Unix()
				*build = *terminal
				if publish != nil {
					o.publishTerminalBuild(publish, terminal, terminal.PersistID)
				}
				if terminal.DeleteRequestedUnix != 0 {
					deleted, deleteErr := o.st.DeleteRequestedBuild(writeCtx, terminal)
					if deleteErr != nil {
						o.log.Warn("delete terminal build; retry scheduled", "bid", build.BuildID, "err", deleteErr)
					} else if deleted {
						o.finishBuildDeletion(terminal)
					}
				}
			}
		}
		unlock()
		cancel()
		if err == nil {
			o.refreshBuildAdmissionGauges(context.Background())
			o.buildCapacityChanged()
			if publish != nil {
				if build.Status == types.BuildReady {
					o.log.Info("build ready", "bid", build.BuildID, "template", build.PersistID)
				} else {
					stage := buildFailureStage(runErr)
					detail := build.Reason
					if result != nil && result.Error != "" {
						stage, detail = result.FailureStage, result.Error
						if stage == "" {
							stage = "runtime"
						}
					}
					if stage == "artifact_prepare" && result == nil {
						o.log.Warn("build failed", "bid", build.BuildID, "failure_stage", stage, "task_artifact_prepare_error_total", 1, "err", detail)
					} else {
						o.log.Warn("build failed", "bid", build.BuildID, "failure_stage", stage, "err", detail)
					}
				}
			}
			return true
		}
		if attempt == 1 {
			o.log.Warn("persist terminal build failed; retaining execution claim and retrying", "bid", build.BuildID, "err", err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

func (o *Orchestrator) runBuildUnit(ctx context.Context, b *types.Build) (result *buildResult, retErr error) {
	if err := types.ValidateBuildID(b.BuildID); err != nil {
		return nil, buildFailed("resource_resolve", err)
	}
	if !b.Profile.Valid() {
		return nil, buildFailed("resource_resolve", fmt.Errorf("build: unknown profile %q", b.Profile))
	}
	if err := ctx.Err(); err != nil {
		return nil, buildFailed("runtime", err)
	}
	sourceTemplate, err := buildUsesSandboxTemplate(b)
	if err != nil {
		return nil, buildFailed("resource_resolve", err)
	}
	if direct, handled, err := directImageBuildResult(b, o.cfg.Checkpoint.Remote.Manifest); handled {
		if err != nil {
			return nil, buildFailed("resource_resolve", err)
		}
		return direct, nil
	}
	runDir := nodepath.BuildRunDir(o.cfg.Paths.RunRoot, b.BuildID)
	baseDir := nodepath.BuildBaseDir(o.cfg.Paths.BaseRoot, b.BuildID)
	var port *vswitch.Port
	var unit string
	runtimePersisted := false
	cleanupSafe := true
	defer func() {
		portID := ""
		if port != nil {
			portID = port.Port
		}
		if !cleanupSafe {
			retErr = retainBuildCleanup(retErr, nil, portID, runtimePersisted)
			return
		}
		// Assignment makes the builder unit the execution owner even before it
		// receives a final BuildSpec. Every exit path must fence that exact unit
		// before detaching its port or removing BuildRunDir/BuildBaseDir.
		if unit != "" {
			if cleanupErr := o.stopBuilderUnit(unit); cleanupErr != nil {
				retErr = &buildCleanupPendingError{cause: retErr, cleanup: cleanupErr, unit: unit, port: portID, persisted: runtimePersisted}
				return
			}
		}
		progress, cleanupErr := o.cleanupBuildRuntimeProgress(b, portID, runtimePersisted)
		if cleanupErr != nil {
			retErr = retainBuildCleanup(retErr, cleanupErr, progress.port, progress.persisted)
		}
	}()
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return nil, buildFailed("artifact_prepare", fmt.Errorf("create BuildRunDir: %w", err))
	}
	if err := os.MkdirAll(nodepath.BuildCheckpointDir(o.cfg.Paths.BaseRoot, b.BuildID), 0o700); err != nil {
		return nil, buildFailed("artifact_prepare", fmt.Errorf("create Build checkpoint directory: %w", err))
	}
	spec, resources, sandboxResources, err := o.resolveBuildRequestInputs(b, sourceTemplate)
	if err != nil {
		return nil, buildFailed("resource_resolve", err)
	}
	o.pendMu.Lock()
	pend := o.pend[b.BuildID]
	localOwner := pend == nil
	if pend != nil && pend.build != b {
		o.pendMu.Unlock()
		return nil, fmt.Errorf("build: duplicate process-local execution owner")
	}
	if pend == nil {
		pend = &pendingBuild{build: b, templateID: b.TemplateID}
		o.pend[b.BuildID] = pend
	}
	pend.runDir, pend.baseDir, pend.sourceTemplate = runDir, baseDir, sourceTemplate
	pend.handoff = newBuildTaskHandoff(sourceTemplate, "")
	pend.spec, pend.resources, pend.sandboxResources = spec, resources, sandboxResources
	pend.result = make(chan configsock.BuildResult, 1)
	o.pendMu.Unlock()
	if localOwner {
		defer o.releaseBuildOwner(b, pend)
	}
	deadline := o.buildExecutionDeadline(b)
	buildCtx, cancelBuild := context.WithDeadline(ctx, deadline)
	defer cancelBuild()

	var mmdsRow *types.Sandbox
	joinCancellation := func() {}
	defer func() { joinCancellation() }()
	if _, err := o.builderRunPool.Assign(buildCtx, b.BuildID, func(runID string) error {
		unit = o.builderUnit(runID)
		joinCancellation = o.stopBuildOnCancellation(buildCtx, b.BuildID, unit)
		_, err := o.prepareBuilderUnit(buildCtx, b, runID)
		return err
	}); err != nil {
		return nil, buildFailed("runtime", err)
	}
	finalPublished := false
	defer func() {
		if retErr != nil && !finalPublished {
			pend.handoff.PublishFinal(nil, retErr)
		}
	}()

	prepareDigest := fastBuildPrepareDigest(b.BuildID)
	var inherited sandboxcfg.NetworkSpec
	if sourceTemplate {
		summary, early, waitErr := o.waitBuildPrepare(buildCtx, pend, unit)
		if early != nil || waitErr != nil {
			if early != nil {
				accepted, fenceErr := o.fenceAcceptedBuildResult(unit, *early)
				if fenceErr != nil {
					cleanupSafe = false
				}
				return accepted, buildFailed("runtime", fenceErr)
			}
			return nil, buildFailed("artifact_prepare", waitErr)
		}
		artifactCapacity, inheritedNetwork, prepareErr := validateBuildPrepareSummary(summary)
		err = prepareErr
		if err != nil {
			return nil, buildFailed("artifact_prepare", err)
		}
		inherited = inheritedNetwork
		pend.sourceHasBuildCommands = summary.HasBuildCommands
		if buildProducesSandbox(b, pend.sourceHasBuildCommands) {
			pend.sandboxResources, err = o.resolveBuildTargetResources(pend.spec, &artifactCapacity)
			if err != nil {
				return nil, buildFailed("resource_resolve", err)
			}
		}
		prepareDigest = summary.ResolutionDigest
		o.log.Info("build task artifact prepared", "bid", b.BuildID, "run_id", b.RunID,
			"task_artifact_ref_count", summary.RequiredRefCount)
	}
	pend.network, pend.templateNetwork, err = o.resolveBuildNetworks(
		b.Profile, inherited, pend.spec.Network, "build-"+shortID(b.BuildID),
	)
	if err != nil {
		return nil, buildFailed("resource_resolve", err)
	}
	if buildProducesMemorySandbox(b, pend.sourceHasBuildCommands) {
		pend.checkpointPolicy, err = o.resolveSnapshotPolicy(b.Metadata, sandboxcfg.SnapshotPolicy{})
		if err != nil {
			return nil, buildFailed("resource_resolve", err)
		}
	}
	port, err = o.attachNetwork(buildCtx, pend.network)
	if err != nil {
		return nil, buildFailed("network_attach", err)
	}
	envdTok := ""
	if b.Profile == types.ProfileE2B && buildProducesMemorySandbox(b, pend.sourceHasBuildCommands) {
		envdTok = b.EnvdAccessToken
		if envdTok == "" {
			envdTok, err = keys.MintToken()
			if err != nil {
				return nil, buildFailed("network_commit", fmt.Errorf("build: mint phase envd token: %w", err))
			}
		}
	}
	durable := buildRuntimePreparation{
		SchemaVersion: buildRuntimePrepareSchemaVersion, PrepareDigest: prepareDigest,
		SourceHasBuildCommands: pend.sourceHasBuildCommands,
		Network:                pend.network, TemplateNetwork: pend.templateNetwork, Resources: pend.resources,
		SandboxResources: pend.sandboxResources,
		CheckpointPolicy: sandboxcfg.CloneSnapshotPolicy(pend.checkpointPolicy),
	}
	prepareJSON, err := encodeBuildRuntimePreparation(durable)
	if err != nil {
		return nil, buildFailed("network_commit", err)
	}
	unlockEvent := o.lockBuildEvent(b.BuildID)
	owned, err := o.st.SetBuildRuntimePreparation(buildCtx, b.BuildID, b.RunID,
		port.Port, port.FloatingIP, port.MAC, envdTok, prepareJSON)
	if err != nil {
		unlockEventFence(unlockEvent)
		return nil, buildFailed("network_commit", err)
	}
	if !owned {
		unlockEventFence(unlockEvent)
		return nil, buildFailed("network_commit", fmt.Errorf("build: exact-run ownership lost before runtime preparation"))
	}
	runtimePersisted = true
	b.RuntimeVswitchPort, b.RuntimeFloatingIP, b.RuntimePortMAC = port.Port, port.FloatingIP, port.MAC
	b.RuntimeEnvdAccessToken, b.RuntimePrepareJSON = envdTok, prepareJSON
	pend.tapFD, pend.mac, pend.floating, pend.envdToken = o.vs.TapFD(port.Port), port.MAC, port.FloatingIP, envdTok

	o.observeBuildUpsert(b)
	unlockEventFence(unlockEvent)
	final, err := o.buildSpecForPending(buildCtx, pend)
	if err != nil {
		return nil, buildFailed("config_write", err)
	}
	unlockEvent = o.lockBuildEvent(b.BuildID)
	allowed, err := o.st.BuildingTaskIdentity(buildCtx, b.BuildID, b.RunID)
	if err != nil || !allowed {
		unlockEventFence(unlockEvent)
		if err == nil {
			err = store.ErrBuildExecutionOwnership
		}
		return nil, buildFailed("config_write", err)
	}
	mmdsRow = o.publishBuildFinal(pend, final)
	unlockEventFence(unlockEvent)
	if mmdsRow != nil {
		defer func() {
			o.uncache(mmdsRow.ID)
			o.publishDelete(mmdsRow.ID)
			o.setMMDSBuildOwner(mmdsRow.ID, "")
		}()
	}
	finalPublished = true
	result, err = o.waitBuildResult(buildCtx, pend, unit)
	if errors.Is(err, errBuildCleanupPending) {
		cleanupSafe = false
	}
	return result, buildFailed("runtime", err)
}

func buildUsesSandboxTemplate(b *types.Build) (bool, error) {
	if b.FromTemplate == "" {
		return false, nil
	}
	tmpl, err := types.ParseTemplateID(b.FromTemplate)
	if err != nil {
		return false, fmt.Errorf("build: fromTemplate %q: %w", b.FromTemplate, err)
	}
	switch tmpl.Kind {
	case types.KindImg:
		return false, nil
	case types.KindSbx, types.KindSnp:
		return true, nil
	default:
		return false, fmt.Errorf("build: unsupported source template kind %q", tmpl.Kind)
	}
}

// directImageBuildResult completes the phase-free identity case without
// assigning a runner or attaching a network. An unchanged Image template is
// already the required top-level artifact; all other sources or targets still
// use the normal durable worker pipeline.
func directImageBuildResult(build *types.Build, imageBundleLocation bool) (*buildResult, bool, error) {
	if imageBundleLocation {
		return nil, false, nil
	}
	if build == nil || build.FromImage != "" || build.FromTemplate == "" || len(build.Steps) != 0 {
		return nil, false, nil
	}
	template, err := types.ParseTemplateID(build.FromTemplate)
	if err != nil || template.Kind != types.KindImg {
		return nil, false, nil
	}
	target := types.ResolveBuildTarget(build.Builder.Target, build.StartCmd, build.ReadyCmd)
	if target.Kind != types.BuildTargetImage {
		return nil, false, nil
	}
	result := &buildResult{
		Target: target, ImageRef: template.Ref,
		StartCmd: build.StartCmd, ReadyCmd: build.ReadyCmd,
	}
	if err := validateBuildResult(build, *result); err != nil {
		return nil, true, err
	}
	return result, true, nil
}

func (o *Orchestrator) buildExecutionDeadline(b *types.Build) time.Time {
	start := time.Unix(b.ExecutionClaimedUnix, 0)
	if b.ExecutionClaimedUnix <= 0 {
		start = time.Now()
	}
	// Source-artifact preparation, host preparation, pipeline execution, and result
	// reporting all consume the configured build budget. Unit fencing and host
	// cleanup use their existing separately bounded contexts; exposing that
	// cleanup headroom here would silently extend tenant execution.
	return start.Add(time.Duration(o.cfg.Builder.TotalTimeoutSec) * time.Second)
}

func (o *Orchestrator) waitBuildPrepare(ctx context.Context, pend *pendingBuild, unit string) (configsock.ArtifactPrepareSummary, *configsock.BuildResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	prepare := make(chan struct {
		summary configsock.ArtifactPrepareSummary
		err     error
	}, 1)
	go func() {
		summary, err := pend.handoff.WaitPrepare(ctx)
		prepare <- struct {
			summary configsock.ArtifactPrepareSummary
			err     error
		}{summary: summary, err: err}
	}()
	for {
		select {
		case got := <-prepare:
			return got.summary, nil, got.err
		case result := <-pend.result:
			return configsock.ArtifactPrepareSummary{}, &result, nil
		case <-ctx.Done():
			if result, ok := takePendingBuildResult(pend); ok {
				return configsock.ArtifactPrepareSummary{}, result, nil
			}
			stopErr := o.stopBuilderUnit(unit)
			result, ok := closePendingBuildResultsAndTake(pend)
			if ok {
				return configsock.ArtifactPrepareSummary{}, result, nil
			}
			if stopErr != nil {
				return configsock.ArtifactPrepareSummary{}, nil, retainBuildCleanup(ctx.Err(), stopErr, "", false)
			}
			return configsock.ArtifactPrepareSummary{}, nil, ctx.Err()
		case <-tick.C:
			if !o.unitActive(ctx, unit) {
				if result, ok := closePendingBuildResultsAndTake(pend); ok {
					return configsock.ArtifactPrepareSummary{}, result, nil
				}
				return configsock.ArtifactPrepareSummary{}, nil, fmt.Errorf("build: unit %s exited during source preparation", unit)
			}
		}
	}
}

func (o *Orchestrator) waitBuildResult(ctx context.Context, pend *pendingBuild, unit string) (*buildResult, error) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case result := <-pend.result:
			return o.fenceAcceptedBuildResult(unit, result)
		case <-pend.handoff.Conflict():
			conflict := pend.handoff.ConflictErr()
			if err := o.stopBuilderUnit(unit); err != nil {
				return nil, retainBuildCleanup(conflict, err, "", false)
			}
			return nil, conflict
		case <-ctx.Done():
			if result, found, err := o.fencePendingBuildResult(pend, unit, false); found {
				return result, err
			}
			stopErr := o.stopBuilderUnit(unit)
			result, found, resultErr := o.fencePendingBuildResult(pend, unit, true)
			if found {
				return result, resultErr
			}
			if stopErr != nil {
				return nil, retainBuildCleanup(ctx.Err(), stopErr, "", false)
			}
			return nil, ctx.Err()
		case <-tick.C:
			if !o.unitActive(ctx, unit) {
				if result, found, err := o.fencePendingBuildResult(pend, unit, true); found {
					return result, err
				}
				return nil, fmt.Errorf("build: unit %s exited without result", unit)
			}
		}
	}
}

func takePendingBuildResult(pend *pendingBuild) (*buildResult, bool) {
	if pend == nil || pend.result == nil {
		return nil, false
	}
	select {
	case result := <-pend.result:
		return &result, true
	default:
		return nil, false
	}
}

func closePendingBuildResultsAndTake(pend *pendingBuild) (*buildResult, bool) {
	if pend == nil {
		return nil, false
	}
	pend.resultMu.Lock()
	defer pend.resultMu.Unlock()
	pend.resultClosed = true
	return takePendingBuildResult(pend)
}

func (o *Orchestrator) fencePendingBuildResult(
	pend *pendingBuild,
	unit string,
	closeResults bool,
) (*buildResult, bool, error) {
	var result *buildResult
	var ok bool
	if closeResults {
		result, ok = closePendingBuildResultsAndTake(pend)
	} else {
		result, ok = takePendingBuildResult(pend)
	}
	if !ok {
		return nil, false, nil
	}
	accepted, err := o.fenceAcceptedBuildResult(unit, *result)
	return accepted, true, err
}

// prepareBuilderUnit is the assignment publication barrier: runtime limits are
// applied and read back before the run-id becomes durable. runPool publishes
// the assignment to run-builder only after this callback returns successfully.
func (o *Orchestrator) prepareBuilderUnit(ctx context.Context, b *types.Build, runID string) (string, error) {
	unit := o.builderUnit(runID)
	properties, err := builderResourceProperties(b.Resources)
	if err != nil {
		return "", err
	}
	if err := o.lc.SetResources(ctx, unit, properties); err != nil {
		return "", err
	}
	effective, err := o.lc.Resources(ctx, unit, "Service")
	if err != nil {
		return "", err
	}
	if effective != properties {
		return "", fmt.Errorf("build: unit %s resource properties effective=%+v want=%+v", unit, effective, properties)
	}
	unlockEvent := o.lockBuildEvent(b.BuildID)
	defer unlockEventFence(unlockEvent)
	bound, err := o.st.BindBuildRun(ctx, b.BuildID, runID, "cpu,memory")
	if err != nil {
		return "", err
	}
	if !bound {
		return "", fmt.Errorf("build: execution ownership lost before run assignment")
	}
	b.RunID = runID
	b.EnforcementStatus = "cpu,memory"
	o.observeBuildUpsert(b)
	return unit, nil
}

// resolveBuildRequestInputs performs only request/policy parsing. Source
// artifact metadata is supplied later by the tenant-bound task and never
// opened here.
func (o *Orchestrator) resolveBuildRequestInputs(b *types.Build, sourceTemplate bool) (sandboxcfg.SandboxSpec, rtconfig.ResourcesConfig, rtconfig.ResourcesConfig, error) {
	spec, err := sandboxcfg.ParseSpec(b.Metadata)
	if err != nil {
		return sandboxcfg.SandboxSpec{}, rtconfig.ResourcesConfig{}, rtconfig.ResourcesConfig{}, err
	}
	phaseResources, err := o.resolveBuildExecutionResources(b.Resources)
	if err != nil {
		return sandboxcfg.SandboxSpec{}, rtconfig.ResourcesConfig{}, rtconfig.ResourcesConfig{}, err
	}
	var targetResources rtconfig.ResourcesConfig
	// A Sandbox source owns the lower-priority portable capacity. Defer target
	// resource resolution until its task-local summary arrives; resolving first
	// against node defaults can incorrectly reject a request that is valid over
	// the source capacity (for example a larger startup allocation).
	if !sourceTemplate && buildProducesSandbox(b, false) {
		targetResources, err = o.resolveBuildTargetResources(spec, nil)
		if err != nil {
			return sandboxcfg.SandboxSpec{}, rtconfig.ResourcesConfig{}, rtconfig.ResourcesConfig{}, err
		}
	}
	return spec, phaseResources, targetResources, nil
}

// buildProducesSandbox is used once command defaults are known, either because
// there is no Sandbox source or its task-local preparation supplied a summary.
func buildProducesSandbox(build *types.Build, sourceHasCommands bool) bool {
	if build.Builder.Target != nil {
		return build.Builder.Target.Kind == types.BuildTargetSandbox
	}
	return build.Profile == types.ProfileE2B && (sourceHasCommands || build.StartCmd != "" || build.ReadyCmd != "")
}

// Only memory targets consume checkpoint policy and instance credentials.
func buildProducesMemorySandbox(build *types.Build, sourceHasCommands bool) bool {
	if build.Builder.Target != nil {
		return build.Builder.Target.Kind == types.BuildTargetSandbox && build.Builder.Target.Memory
	}
	return buildProducesSandbox(build, sourceHasCommands)
}

// resolveBuildExecutionResources derives A/B VM sizing solely from immutable
// Build admission resources. Sandbox target resource metadata never controls
// the execution sandboxes.
func (o *Orchestrator) resolveBuildExecutionResources(build types.BuildResources) (rtconfig.ResourcesConfig, error) {
	cores := int(math.Ceil(float64(build.CPU) / 1000))
	allocCPU := float64(build.CPU) / 1000
	memory := fmt.Sprintf("%dB", build.Memory)
	patch := sandboxcfg.ResourcePatch{
		Capacity:    &sandboxcfg.CapacityPatch{CPU: &cores, Memory: &memory},
		Allocatable: &sandboxcfg.AllocatablePatch{CPU: &allocCPU, Memory: &memory},
	}
	return o.resolveBuildResources(patch, nil)
}

// resolveBuildTargetResources layers Register Create resource options over a
// source E's portable defaults. A nil source uses ordinary node defaults.
func (o *Orchestrator) resolveBuildTargetResources(spec sandboxcfg.SandboxSpec, source *configsock.ArtifactCapacity) (rtconfig.ResourcesConfig, error) {
	patch := spec.Resource
	if source != nil {
		cpu, memory := source.CPU, source.Memory
		allocCPU, allocMemory := source.AllocatableCPU, source.AllocatableMemory
		base := sandboxcfg.ResourcePatch{
			Capacity:    &sandboxcfg.CapacityPatch{CPU: &cpu, Memory: &memory},
			Allocatable: &sandboxcfg.AllocatablePatch{CPU: &allocCPU, Memory: &allocMemory},
		}
		var err error
		patch, err = sandboxcfg.MergeResourcePatch(base, patch)
		if err != nil {
			return rtconfig.ResourcesConfig{}, fmt.Errorf("%w: target Sandbox resources: %v", api.ErrBadRequest, err)
		}
	}
	resources, err := o.resolveBuildResources(patch, source)
	if err != nil {
		return rtconfig.ResourcesConfig{}, err
	}
	if source != nil && source.DeflateOnOOM != nil {
		resolved := resources.Allocatable.DeflateOnOOM
		value := *source.DeflateOnOOM
		resources.Allocatable.DeflateOnOOM = &value
		dynamic := o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled
		if err := sandboxcfg.ValidateResolvedResources(resources, dynamic); err != nil {
			// A registration override can make the inherited false value
			// incompatible (for example by lowering allocatable memory). In that
			// case the higher-priority resolved allocation keeps its derived true.
			resources.Allocatable.DeflateOnOOM = resolved
		}
	}
	return resources, nil
}

func (o *Orchestrator) resolveBuildResources(patch sandboxcfg.ResourcePatch, _ *configsock.ArtifactCapacity) (rtconfig.ResourcesConfig, error) {
	dynamic := o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled
	resources, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node:                     configresolve.SandboxResources(o.cfg.Sandbox.Resources),
		Patch:                    patch,
		Dynamic:                  dynamic,
		ControllerSocketIdentity: o.resourceControllerSocketIdentity,
	})
	if err != nil {
		if errors.Is(err, sandboxcfg.ErrInvalidResourceRequest) {
			return rtconfig.ResourcesConfig{}, fmt.Errorf("%w: Sandbox resources: %v", api.ErrBadRequest, err)
		}
		return rtconfig.ResourcesConfig{}, fmt.Errorf("build: resolve Sandbox resources: %w", err)
	}
	return resources, nil
}

func (o *Orchestrator) cleanupBuildRuntime(b *types.Build, port string, persisted bool) error {
	_, err := o.cleanupBuildRuntimeProgress(b, port, persisted)
	return err
}

type buildRuntimeCleanupProgress struct {
	port      string
	persisted bool
}

func (o *Orchestrator) cleanupBuildRuntimeProgress(b *types.Build, port string, persisted bool) (buildRuntimeCleanupProgress, error) {
	progress := buildRuntimeCleanupProgress{port: port, persisted: persisted}
	var cleanupErr error
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	func() {
		o.networkAllocationMu.Lock()
		defer o.networkAllocationMu.Unlock()
		if port != "" {
			if err := o.vs.Detach(cleanupCtx, port); err != nil && !errors.Is(err, vswitch.ErrPortNotAttached) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("detach build port %s: %w", port, err))
			} else if persisted {
				if o.detachedPortsPending == nil {
					o.detachedPortsPending = make(map[string]struct{})
				}
				o.detachedPortsPending[port] = struct{}{}
			} else {
				progress.port = ""
			}
		}
		if cleanupErr == nil && persisted {
			unlockEvent := o.lockBuildEvent(b.BuildID)
			defer unlockEventFence(unlockEvent)
			cleared, err := o.st.ClearBuildRuntimeOwnership(cleanupCtx, b.BuildID, b.RunID, port)
			if err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
			} else if !cleared {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("build execution ownership lost during cleanup"))
			} else {
				delete(o.detachedPortsPending, port)
				b.RuntimeVswitchPort, b.RuntimeFloatingIP, b.RuntimePortMAC, b.RuntimeEnvdAccessToken = "", "", "", ""
				b.RuntimePrepareJSON = ""
				progress.port, progress.persisted = "", false
				if current, err := o.st.GetBuild(cleanupCtx, b.BuildID); err == nil && current != nil && current.TemplateID == b.TemplateID {
					o.observeBuildUpsert(current)
				}
			}
		}
	}()
	if cleanupErr != nil {
		return progress, cleanupErr
	}
	removeRunDir := o.removeBuildRunDir
	if removeRunDir == nil {
		removeRunDir = os.RemoveAll
	}
	if err := removeRunDir(nodepath.BuildRunDir(o.cfg.Paths.RunRoot, b.BuildID)); err != nil {
		return progress, fmt.Errorf("remove BuildRunDir: %w", err)
	}
	removeBaseDir := o.removeBuildBaseDir
	if removeBaseDir == nil {
		removeBaseDir = os.RemoveAll
	}
	if err := removeBaseDir(nodepath.BuildBaseDir(o.cfg.Paths.BaseRoot, b.BuildID)); err != nil {
		return progress, fmt.Errorf("remove BuildBaseDir: %w", err)
	}
	return progress, nil
}

// retryBuildCleanup keeps a live controller making progress after a transient
// unit, connector, store, or filesystem cleanup failure. It never releases the
// durable execution claim until the unit is fenced and all exact runtime
// ownership is gone. Cancellation leaves the row for startup reconciliation.
func (o *Orchestrator) retryBuildCleanup(ctx context.Context, b *types.Build, pending *buildCleanupPendingError) (error, error) {
	cause := pending.cause
	unit := pending.unit
	if unit == "" && b.RunID != "" {
		unit = o.builderUnit(b.RunID)
	}
	port := pending.port
	persisted := pending.persisted
	if port == "" && b.RuntimeVswitchPort != "" {
		port = b.RuntimeVswitchPort
		persisted = true
	}
	delay := 20 * time.Millisecond
	for attempt := 1; ; attempt++ {
		var cleanupErr error
		if unit != "" {
			cleanupErr = o.stopBuilderUnit(unit)
		}
		if cleanupErr == nil {
			var progress buildRuntimeCleanupProgress
			progress, cleanupErr = o.cleanupBuildRuntimeProgress(b, port, persisted)
			port, persisted = progress.port, progress.persisted
		}
		if cleanupErr == nil {
			return cause, nil
		}
		if attempt == 1 {
			o.log.Warn("retry retained build cleanup", "bid", b.BuildID, "err", cleanupErr)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return cause, &buildCleanupPendingError{
				cause: cause, cleanup: cleanupErr, unit: unit,
				port: port, persisted: persisted,
			}
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

func builderResourceProperties(resources types.BuildResources) (launcher.ResourceProperties, error) {
	if resources.CPU < 0 || resources.Memory < 0 {
		return launcher.ResourceProperties{}, fmt.Errorf("negative build resource property")
	}
	if resources.CPU > int64(^uint64(0)/1000) {
		return launcher.ResourceProperties{}, fmt.Errorf("build resources CPU overflows systemd quota")
	}
	return launcher.ResourceProperties{
		CPUQuotaPerSecUSec: uint64(resources.CPU) * 1000,
		MemoryMax:          uint64(resources.Memory),
	}, nil
}

func (o *Orchestrator) waitBuilderUnitExit(ctx context.Context, unit string, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !o.unitActive(ctx, unit) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("build: unit %s remained active after result", unit)
		case <-ticker.C:
		}
	}
}

// fenceAcceptedBuildResult preserves the durable worker result across
// controller shutdown. Waiting uses its own bounded lifetime; if the worker
// does not exit after the acknowledged report, a successful Stop is an
// equivalent execution-release fence and the accepted result still wins.
func (o *Orchestrator) fenceAcceptedBuildResult(unit string, result buildResult) (*buildResult, error) {
	return o.fenceAcceptedBuildResultWithin(unit, result, 20*time.Second)
}

func (o *Orchestrator) fenceAcceptedBuildResultWithin(unit string, result buildResult, timeout time.Duration) (*buildResult, error) {
	if err := o.waitBuilderUnitExit(context.Background(), unit, timeout); err != nil {
		if stopErr := o.stopBuilderUnit(unit); stopErr != nil {
			return &result, retainBuildCleanup(nil, errors.Join(err, stopErr), "", false)
		}
	}
	return &result, nil
}

// stopBuilderUnit is the execution-release fence. A successful systemd stop job
// and an inactive readback prove that the old process can no longer consume the
// runtime ownership guarded by the durable execution claim.
func (o *Orchestrator) stopBuilderUnit(unit string) error {
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := o.lc.Stop(stopCtx, unit); err != nil {
		if !o.unitActive(stopCtx, unit) {
			// CollectMode may have removed the transient instance between List and
			// Stop. A successful inactive/missing readback is the equivalent fence.
			return nil
		}
		return fmt.Errorf("build: stop unit %s before releasing execution claim: %w", unit, err)
	}
	if o.unitActive(stopCtx, unit) {
		return fmt.Errorf("build: unit %s remained active after stop", unit)
	}
	return nil
}

// --- configsock.Provider (build) ---

// BuildTaskAuth is deliberately non-secret. The exact durable run check is
// performed before configsock reads the task pidfile and before BuildTaskSpecFor
// can expose MANIFEST_KEY or registry credentials.
func (o *Orchestrator) BuildTaskAuth(ctx context.Context, buildID, runID string) (configsock.BuildTaskAuth, bool, error) {
	if err := types.ValidateBuildID(buildID); err != nil {
		return configsock.BuildTaskAuth{}, false, err
	}
	found, err := o.st.BuildingTaskIdentity(ctx, buildID, runID)
	if err != nil || !found {
		return configsock.BuildTaskAuth{}, found, err
	}
	return configsock.BuildTaskAuth{PidFile: filepath.Join(nodepath.BuildRunDir(o.cfg.Paths.RunRoot, buildID), "builder.pid")}, true, nil
}

func (o *Orchestrator) BuildTaskSpecFor(ctx context.Context, buildID, runID string) (*configsock.BuildTaskSpec, bool, error) {
	if !o.buildRecoveryReadyNow() {
		return nil, false, errBuildRecoveryInProgress
	}
	o.pendMu.Lock()
	pend := o.pend[buildID]
	o.pendMu.Unlock()
	if pend == nil || pend.handoff == nil {
		return nil, false, nil
	}
	b, err := o.st.GetBuild(ctx, buildID)
	if err != nil {
		return nil, false, err
	}
	if b == nil || b.RunID != runID || !b.ExecutionClaimed || b.Status != types.BuildBuilding || (pend.templateID != "" && pend.templateID != b.TemplateID) || b.CancelRequestedUnix != 0 || b.DeleteRequestedUnix != 0 {
		return nil, false, nil
	}
	response := &configsock.BuildTaskSpec{BuildID: buildID, RunID: runID, Env: buildTaskEnv(b)}
	if !pend.sourceTemplate {
		final, err := pend.handoff.WaitFinal(ctx)
		if err != nil {
			return nil, false, err
		}
		allowed, err := o.st.BuildingTaskIdentity(ctx, buildID, runID)
		if err != nil || !allowed {
			return nil, false, err
		}
		response.Final = final
		return response, true, nil
	}
	tmpl, err := types.ParseTemplateID(b.FromTemplate)
	if err != nil || (tmpl.Kind != types.KindSbx && tmpl.Kind != types.KindSnp) {
		return nil, false, fmt.Errorf("build: invalid Sandbox/Snapshot template %q", b.FromTemplate)
	}
	manifestConfig := o.cfg.ManifestConfig
	if manifestConfig != "" && !filepath.IsAbs(manifestConfig) {
		manifestConfig, err = filepath.Abs(manifestConfig)
		if err != nil {
			return nil, false, fmt.Errorf("build: absolute manifest config path: %w", err)
		}
	}
	checkpointDir := filepath.Join(pend.baseDir, "checkpoint")
	rootRef, err := normalizeSandboxTaskRootRef(tmpl.Ref, checkpointDir)
	if err != nil {
		return nil, false, err
	}
	sourceKind := types.ResumeSourceSandbox
	if tmpl.Kind == types.KindSnp {
		sourceKind = types.ResumeSourceSnapshot
	}
	response.Prepare = &configsock.ArtifactPrepareSpec{
		RootSourceKind: string(sourceKind),
		RootRef:        rootRef, LaunchMode: string(types.LaunchCold), ManifestConfig: manifestConfig,
		RefLocationParent: o.cfg.Checkpoint.Remote.RefLocationParent,
		RelativeDir:       checkpointDir, MaxRefs: maxRequiredArtifactRefs, ReadSourceImageConfig: true,
		PreflightImageBundle:     o.cfg.Checkpoint.Remote.Manifest,
		AbsoluteDeadlineUnixNano: o.buildExecutionDeadline(b).UnixNano(),
	}
	return response, true, nil
}

func (o *Orchestrator) CompleteBuildPrepare(ctx context.Context, buildID, runID string, summary configsock.ArtifactPrepareSummary) (*configsock.BuildSpec, error) {
	if !o.buildRecoveryReadyNow() {
		return nil, errBuildRecoveryInProgress
	}
	o.pendMu.Lock()
	pend := o.pend[buildID]
	o.pendMu.Unlock()
	if pend == nil || pend.handoff == nil {
		return nil, configsock.RejectBuildPrepare(fmt.Errorf("build: exact-run preparation owner not found"))
	}
	allowed, err := o.st.BuildingTaskIdentity(ctx, buildID, runID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, configsock.RejectBuildPrepare(store.ErrBuildExecutionOwnership)
	}
	replay, err := pend.handoff.Submit(summary)
	if err != nil {
		return nil, configsock.RejectBuildPrepare(err)
	}
	if replay {
		o.log.Info("build task artifact prepare replay", "bid", buildID, "run_id", runID,
			"task_artifact_prepare_replay_total", 1)
	}
	final, err := pend.handoff.WaitFinal(ctx)
	if err != nil {
		return nil, err
	}
	allowed, err = o.st.BuildingTaskIdentity(ctx, buildID, runID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, configsock.RejectBuildPrepare(store.ErrBuildExecutionOwnership)
	}
	return final, nil
}

func buildTaskEnv(b *types.Build) map[string]string {
	env := map[string]string{"MANIFEST_KEY": b.ManifestKey}
	if b.RegistryAuth != "" {
		var creds regcreds.Creds
		if json.Unmarshal([]byte(b.RegistryAuth), &creds) == nil {
			for key, value := range creds.FlattenEnv() {
				env[key] = value
			}
		}
	}
	return env
}

// BuildSpecFor constructs a final work order exclusively from request data and
// already-resolved pending host preparation. It never opens an artifact.
func (o *Orchestrator) BuildSpecFor(ctx context.Context, configID string) (*configsock.BuildSpec, string, bool, error) {
	kind, bid, found := strings.Cut(configID, ":")
	if !found || kind != "build" {
		return nil, "", false, nil
	}
	if err := o.waitBuildRecoveryReady(ctx); err != nil {
		return nil, "", false, err
	}
	o.pendMu.Lock()
	pend := o.pend[bid]
	o.pendMu.Unlock()
	if pend == nil {
		return nil, "", false, nil
	}
	spec, err := o.buildSpecForPending(ctx, pend)
	if err != nil {
		return nil, "", false, err
	}
	return spec, filepath.Join(nodepath.BuildRunDir(o.cfg.Paths.RunRoot, pend.build.BuildID), "builder.pid"), true, nil
}

func (o *Orchestrator) buildSpecForPending(ctx context.Context, pend *pendingBuild) (*configsock.BuildSpec, error) {
	b := pend.build
	if !b.Profile.Valid() {
		return nil, fmt.Errorf("build: unknown profile %q", b.Profile)
	}

	// Presign each COPY context for the build's whole lifetime (it is signed
	// now, when the unit starts and dials for the spec).
	getTTL := time.Duration(o.cfg.Builder.TotalTimeoutSec+300) * time.Second
	var steps []configsock.BuildStep
	for _, s := range b.Steps {
		bs := configsock.BuildStep{Type: s.Type, Args: s.Args, FilesHash: s.FilesHash}
		if s.FilesHash != "" && o.files != nil {
			url, err := o.files.PresignGet(ctx, b.TemplateID, s.FilesHash, getTTL)
			if err != nil {
				return nil, fmt.Errorf("presign COPY context %s: %w", s.FilesHash, err)
			}
			bs.FilesURL = url
		}
		steps = append(steps, bs)
	}
	fromTemplateRef, fromTemplateKind := "", ""
	var refLocations map[string]string
	if b.FromTemplate != "" {
		t, err := types.ParseTemplateID(b.FromTemplate)
		if err != nil {
			return nil, err
		}
		fromTemplateRef, fromTemplateKind = t.Ref, string(t.Kind)
		if t.Kind == types.KindImg {
			refLocations, err = o.singleRefLocations(t.Ref)
			if err != nil {
				return nil, err
			}
		}
	}
	// Publication names/URIs are NOT derived here: the builder mints the
	// bare-build-id name for each actual image-class or checkpoint-class
	// publication, so every publication of one build converges on one
	// directory.
	checkpointParent := o.cfg.Checkpoint.Remote.RefLocationParent

	importReferer, err := o.effectiveImportReferer(b)
	if err != nil {
		return nil, err
	}

	spec := &configsock.BuildSpec{
		BuildID:                     b.BuildID,
		Profile:                     string(b.Profile),
		RunID:                       b.RunID,
		RunDir:                      pend.runDir,
		BaseDir:                     pend.baseDir,
		FromImage:                   b.FromImage,
		FromTemplateRef:             fromTemplateRef,
		FromTemplateKind:            fromTemplateKind,
		RequestedTarget:             cloneBuildTarget(b.Builder.Target),
		RefLocations:                refLocations,
		CheckpointMode:              o.cfg.Checkpoint.Mode,
		CheckpointRefLocationParent: checkpointParent,
		CheckpointRemoteManifest:    o.cfg.Checkpoint.Remote.Manifest,
		Steps:                       steps,
		StartCmd:                    b.StartCmd,
		ReadyCmd:                    b.ReadyCmd,
		Env:                         nil,
		Paths: configsock.BuildPaths{
			Kernel:         o.cfg.Sandbox.Boot.Kernel,
			Runtime:        o.cfg.Sandbox.Boot.Runtime,
			OverlayDiffTpl: o.cfg.Sandbox.Boot.OverlayDiffTemplate,
			BuilderDiffTpl: o.cfg.Builder.DiffTemplate,
			SandboxCtl:     o.executables.SandboxCtl(),
			FlattenCtl:     o.executables.FlattenCtl(),
			ManifestConfig: o.cfg.ManifestConfig,
		},
		Net: configsock.BuildNet{
			TapFD:    buildTapFD(pend.tapFD),
			MAC:      pend.mac,
			InnerIP:  pend.network.InnerIP,
			Nexthop:  pend.network.Nexthop,
			Hostname: pend.network.Hostname,
			DNS:      pend.network.DNS,
		},
		TemplateNetwork:   pend.templateNetwork,
		Resources:         pend.resources,
		SandboxResources:  pend.sandboxResources,
		SandboxSpec:       pend.spec,
		SandboxNamespaces: buildSandboxNamespaces(b.Metadata),
		SandboxEnv:        cloneStringMap(b.Env),
		HasSandboxConfig:  buildHasSandboxConfig(b),
		HasInstanceConfig: buildHasInstanceConfig(b),
		CheckpointPolicy:  sandboxcfg.CloneSnapshotPolicy(pend.checkpointPolicy),
		MMDSEnabled:       b.Profile == types.ProfileE2B && o.cfg.MMDS.Enabled,
		EnvdToken:         pend.envdToken,
		Insecure:          o.cfg.Builder.InsecureRegistry,
		Platform:          o.cfg.Builder.Platform,
		ImportReferer:     importReferer,
		RegistryTLS:       o.effectiveRegistryTLS(b),
		Timeouts: configsock.BuildTimeouts{
			PullSec:                  o.cfg.Builder.PullTimeoutSec,
			StepSec:                  o.cfg.Builder.StepTimeoutSec,
			ReadySec:                 o.cfg.Builder.ReadyTimeoutSec,
			TotalSec:                 o.cfg.Builder.TotalTimeoutSec,
			AbsoluteDeadlineUnixNano: o.buildExecutionDeadline(b).UnixNano(),
		},
	}
	return spec, nil
}

// publishBuildFinal installs the optional MMDS route before making the final
// spec observable. A task may start phase C as soon as WaitFinal returns, so
// reversing these operations creates a real initialization race.
func (o *Orchestrator) publishBuildFinal(pend *pendingBuild, spec *configsock.BuildSpec) *types.Sandbox {
	var mmdsRow *types.Sandbox
	if pend.build.Profile == types.ProfileE2B && o.cfg.MMDS.Enabled &&
		buildProducesMemorySandbox(pend.build, pend.sourceHasBuildCommands) {
		mmdsRow = o.publishRecoveredBuildMMDS(pend.build)
	}
	pend.handoff.PublishFinal(spec, nil)
	return mmdsRow
}

// resolveBuildNetworks derives two roles from the same merged logical network:
// the temporary build VMs use buildHostname when no hostname was declared, while
// the produced template uses the normal sandbox hostname default. Both preserve
// current-build fields over source Sandbox fields and share every other default.
func (o *Orchestrator) resolveBuildNetworks(
	profile types.Profile,
	inherited sandboxcfg.NetworkSpec,
	specified sandboxcfg.NetworkSpec,
	buildHostname string,
) (sandboxcfg.NetworkSpec, sandboxcfg.NetworkSpec, error) {
	merged := sandboxcfg.MergeNetwork(inherited, specified)
	buildNetwork, err := o.resolveNetwork(profile, merged, buildHostname)
	if err != nil {
		return sandboxcfg.NetworkSpec{}, sandboxcfg.NetworkSpec{}, err
	}
	templateNetwork, err := o.resolveNetwork(profile, merged, o.cfg.Sandbox.Network.Hostname)
	if err != nil {
		return sandboxcfg.NetworkSpec{}, sandboxcfg.NetworkSpec{}, err
	}
	return buildNetwork, templateNetwork, nil
}

// shortID returns the first 8 chars (hostname-friendly handle).
func shortID(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// imageURIFromMask renders builder_image_uri_mask with the build's templateID +
// buildID (the convention the e2b CLI pushed its client-built image under).
func (o *Orchestrator) imageURIFromMask(templateID, buildID string) string {
	m := o.cfg.Builder.ImageURIMask
	if m == "" {
		return ""
	}
	m = strings.ReplaceAll(m, "{templateID}", templateID)
	m = strings.ReplaceAll(m, "{buildID}", buildID)
	return m
}

func nonEmpty(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

func appendUnique(xs []string, s string) []string {
	for _, x := range xs {
		if x == s {
			return xs
		}
	}
	return append(xs, s)
}
