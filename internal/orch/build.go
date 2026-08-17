package orch

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/remote"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

var hexKeyRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// maxCABundlePEMSize bounds the inline CA bundle a build may register. Larger
// bundles and a general build-input upload facility are tracked separately.
const maxCABundlePEMSize = 16 * 1024

// newRegisteredBuild allocates the transient templateID + build id, resolves the
// allowlist, and records a registered build. fromImage is pre-derived from
// builder_image_uri_mask — the convention the e2b CLI pushes its client-built
// image under ({templateID}/{buildID}); the v3 trigger may still override it.
func (o *Orchestrator) newRegisteredBuild(ctx context.Context, apiKey string, spec api.RegisterSpec) (*types.Build, error) {
	if !spec.Profile.Valid() {
		return nil, fmt.Errorf("%w: unknown build profile %q", api.ErrBadRequest, spec.Profile)
	}
	if err := spec.Resources.ValidateRequired(); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	executionLimit, err := o.cfg.Builder.ExecutionLimit()
	if err != nil {
		return nil, err
	}
	if !executionLimit.AllowsOne(spec.Resources) {
		o.recordRegistrationRejection("execution_fit")
		return nil, fmt.Errorf("%w: build resources cannot fit builder.admission.execution", api.ErrBadRequest)
	}
	metadata := spec.Metadata
	if _, present := metadata[clusterstate.ObjectMetadataKey]; present {
		return nil, fmt.Errorf("%w: %s is node-managed cluster context", api.ErrBadRequest, clusterstate.ObjectMetadataKey)
	}
	builderOpts := spec.Builder
	builderOpts.Resources = nil
	mmdsDoc, metadata, err := sandboxcfg.ExtractMMDS(metadata, spec.MMDSHeader, o.mmdsPolicy())
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
	if err := o.validateBuildOptions(builderOpts, false); err != nil {
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
	tid, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("orch: new template id: %w", err)
	}
	templateID := types.TransientPrefix + tid.String()
	b := &types.Build{
		BuildID:            bid.String(),
		TemplateID:         templateID,
		APISecret:          pair.APISecret,
		ManifestKey:        pair.ManifestKey,
		Profile:            spec.Profile,
		Kind:               types.KindImg,
		Status:             types.BuildRegistered,
		FromImage:          o.imageURIFromMask(templateID, bid.String()),
		Names:              nonEmpty(spec.Name),
		Aliases:            append([]string{}, spec.Tags...),
		Resources:          spec.Resources,
		PhaseResourcePatch: phaseResourcePatch,
		Metadata:           metadata,
		Builder:            builderOpts,
		CreatedUnix:        time.Now().Unix(),
	}
	initialMMDS := initialMMDSRouteSecretValues(mmdsDoc)
	if initialMMDS != nil {
		transportRow := &types.Sandbox{
			ID: "build-" + b.BuildID, Profile: b.Profile, TemplateID: b.TemplateID,
			State: types.StateRunning, RunID: "build-registration-check",
			APISecret: b.APISecret, ManifestKey: b.ManifestKey, Metadata: b.Metadata,
		}
		if err := validateInitialMMDSRouteEntry(transportRow, initialMMDS); err != nil {
			return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
		}
	}
	var routesDigest string
	var secretValues store.MMDSRouteSecretValues
	if initialMMDS != nil {
		routesDigest, secretValues = initialMMDS.routesDigest, initialMMDS.values
	}
	b.RegistrationMMDSRoutesDigest = routesDigest
	registrationLimit, err := o.cfg.Builder.RegistrationLimit()
	if err != nil {
		return nil, err
	}
	registered, _, err := o.st.RegisterBuildWithMMDSRouteSecretValues(ctx, b, registrationLimit, routesDigest, secretValues)
	if errors.Is(err, store.ErrBuildRegistrationCapacity) {
		o.recordRegistrationRejection("capacity")
		return nil, fmt.Errorf("%w: %v", api.ErrBuildAdmission, err)
	}
	if err != nil {
		return nil, err
	}
	o.refreshBuildAdmissionGauges(ctx)
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
	if err := assertBuildResources(b.Resources, spec.ResourceAssertion); err != nil {
		return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if !b.Profile.Valid() {
		return fmt.Errorf("%w: build has unknown profile %q", api.ErrBadRequest, b.Profile)
	}
	if b.Profile == types.ProfileBare && (spec.StartCmd != "" || spec.ReadyCmd != "") {
		return fmt.Errorf("%w: bare profile builds do not support startCmd or readyCmd", api.ErrBadRequest)
	}
	if spec.FromImage != "" && spec.FromTemplate != "" {
		return fmt.Errorf("build: fromImage and fromTemplate are mutually exclusive")
	}
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
		// pipeline extracts the base image (and inherits start/ready) from
		// its snapshot.cfg.
		ref := o.resolveTemplateAlias(ctx, apiKey, spec.FromTemplate)
		if _, perr := types.ParseTemplateID(ref); perr != nil {
			return fmt.Errorf("build: fromTemplate %q: not a known template", spec.FromTemplate)
		}
		b.FromTemplate, b.FromImage = ref, ""
		if len(spec.Steps) == 0 && spec.StartCmd == "" {
			return fmt.Errorf("build: fromTemplate without steps or startCmd has nothing to do")
		}
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
	b.Steps = spec.Steps
	b.StartCmd = spec.StartCmd
	b.ReadyCmd = spec.ReadyCmd
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
	b.Kind = types.KindImg
	if b.Profile == types.ProfileE2B && (spec.StartCmd != "" || b.FromTemplate != "") {
		// snp is provisional: the pipeline reports what it actually
		// produced (fromTemplate may inherit start/ready) and the
		// finalizer recomputes the kind from the result.
		b.Kind = types.KindSnp
	}
	if b.RegistryAuth, err = o.resolveBuildCreds(ctx, b, auth.PullToken, auth.RegistryUsername, auth.RegistryPassword); err != nil {
		return err
	}
	b.Status = types.BuildWaiting
	b.WaitingUnix = time.Now().Unix()
	committed, err := o.commitBuildTrigger(ctx, b)
	if err != nil || committed {
		return err
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

// validateBuildPhaseResources applies the selected node's ordinary Sandbox
// policy before registration consumes admission capacity. Execution resolves
// the same immutable patch again to build the final YAML; this early pass has
// no network, cgroup, controller, or systemd side effects.
func (o *Orchestrator) validateBuildPhaseResources(raw string) error {
	var patch sandboxcfg.ResourcePatch
	var err error
	if raw != "" {
		patch, err = sandboxcfg.ParseResourcePatch(raw)
		if err != nil {
			return fmt.Errorf("%w: phase sandbox resources: %v", api.ErrBadRequest, err)
		}
	}
	dynamic := o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled
	_, err = sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node:                     o.cfg.Sandbox.Resources.Policy(),
		Patch:                    patch,
		Dynamic:                  dynamic,
		ControllerSocketIdentity: o.resourceControllerSocketIdentity,
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, sandboxcfg.ErrInvalidResourceRequest) {
		return fmt.Errorf("%w: phase sandbox resources: %v", api.ErrBadRequest, err)
	}
	return fmt.Errorf("build: resolve phase sandbox resources: %w", err)
}

func (o *Orchestrator) validateBuildOptions(opts types.BuildOptions, fromTemplate bool) error {
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

// resolveTemplateAlias maps a non-persist template reference — the transient register
// id the SDK reports as BuildInfo.template_id, or a build name/alias — to its built
// persist id. Returns "" if no ready build owned by this api key matches.
func (o *Orchestrator) resolveTemplateAlias(ctx context.Context, apiKey, ref string) string {
	if b := o.templateBuild(ctx, apiKey, ref); b != nil {
		return b.PersistID
	}
	return ""
}

// templateBuild resolves a template ref (persist id, transient id, name, or alias)
// to its build record within the tenant's templates, or nil if none matches. Used
// to recover a template's declared config (builds.metadata_json) at create time.
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
	executionLimit, err := o.cfg.Builder.ExecutionLimit()
	if err != nil {
		o.log.Error("builder scheduler disabled", "err", err)
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	runPass := func() {
		now := time.Now()
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
			won, err := o.st.ClaimBuildExecution(ctx, candidate.BuildID, executionLimit, now)
			if err != nil {
				finish()
				if errors.Is(err, store.ErrBuildExecutionUnfit) {
					reason := "build resources no longer fit configured execution limits"
					expired, expireErr := o.st.ExpireBuild(ctx, candidate.BuildID, types.BuildWaiting, reason)
					if expireErr != nil {
						o.log.Warn("reject permanently unfit build", "bid", candidate.BuildID, "err", expireErr)
						return
					}
					if expired {
						o.recordExecutionRejection("configuration")
						o.refreshBuildAdmissionGauges(ctx)
						o.publishBuildState(candidate.BuildID, "error", "", reason)
					}
					continue
				}
				o.log.Warn("build execution admission", "bid", candidate.BuildID, "err", err)
				return
			}
			if !won {
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
			go func() {
				defer finish()
				o.executeBuild(ctx, claimed)
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
			expired, err := o.st.ExpireBuild(ctx, build.BuildID, expiry.state, expiry.reason)
			if err != nil {
				o.log.Warn("expire build", "bid", build.BuildID, "err", err)
				continue
			}
			if expired {
				o.recordBuildExpired(expiry.state)
				o.refreshBuildAdmissionGauges(ctx)
				o.publishBuildState(build.BuildID, "error", "", expiry.reason)
			}
		}
	}
}

type buildResult = configsock.BuildResult

var errBuildCleanupPending = errors.New("build runtime cleanup remains pending")

type buildCleanupPendingError struct {
	cause   error
	cleanup error
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

// pendingBuild is the per-execution state BuildSpecFor serves while the
// build run-id unit executes: the pre-attached network slot, the resolved
// temporary-VM and persistent-template network roles, minted envd token, and
// result channel.
type pendingBuild struct {
	build           *types.Build
	workdir         string
	spec            sandboxcfg.SandboxSpec
	network         sandboxcfg.NetworkSpec
	templateNetwork sandboxcfg.NetworkSpec
	resources       rtconfig.ResourcesConfig
	tapFD           vswitch.TapFD
	mac             string
	floating        string
	envdToken       string
	result          chan configsock.BuildResult
}

func buildTapFD(t vswitch.TapFD) configsock.TapFDConfig {
	return configsock.TapFDConfig{
		Exec:    append([]string(nil), t.Exec...),
		Socket:  t.Socket,
		Request: t.Request,
		Timeout: t.Timeout,
	}
}

// executeBuild runs the three-phase pipeline in a sandbox-builder@<run-id> unit:
// node-ctl run-builder waits for a build assignment, fetches the BuildSpec over
// the config-socket, and drives import/steps/template sandboxes itself (as direct
// children, in the unit's cgroup). This side owns what spans the unit: the
// workdir, one vswitch slot the phases reuse sequentially, and — for the template
// phase under mmds.enabled — a synthetic route entry so the build sandbox's
// FC-mode envd can resolve itself.
func (o *Orchestrator) executeBuild(ctx context.Context, b *types.Build) {
	res, err := o.runBuildUnit(ctx, b)
	o.completeBuild(ctx, b, res, err)
}

func (o *Orchestrator) completeBuild(ctx context.Context, b *types.Build, res *buildResult, err error) {
	var cleanupPending *buildCleanupPendingError
	if errors.As(err, &cleanupPending) {
		var cleanupErr error
		err, cleanupErr = o.retryBuildCleanup(ctx, b, cleanupPending.cause)
		if cleanupErr != nil {
			// The execution claim remains durable. A live controller keeps retrying;
			// cancellation hands the exact same ownership to startup reconciliation.
			o.log.Error("build cleanup incomplete; execution claim retained", "bid", b.BuildID, "err", cleanupErr)
			return
		}
	}
	switch {
	case err == nil && res != nil && res.Error != "":
		// The pipeline ran and reported its own failure. run-builder's fail()
		// already wrote "build failed: <detail>" to the build log stream (tag
		// build), which the SDK is streaming — so reason.message stays generic
		// and the detail lives in the log, not a duplicated BuildException tail.
		b.Status, b.Reason = types.BuildError, "build failed; see build logs"
		if o.persistTerminalBuild(ctx, b) {
			o.publishBuildState(b.BuildID, "error", "", b.Reason)
		}
		o.log.Warn("build failed", "bid", b.BuildID, "err", res.Error)
		return
	case err != nil:
		// Infrastructure failure: the pipeline never ran (or produced no
		// result), so there is NO build log for it — surface the orchestrator-
		// side error directly, it is the only signal.
		b.Status, b.Reason = types.BuildError, err.Error()
		if o.persistTerminalBuild(ctx, b) {
			o.publishBuildState(b.BuildID, "error", "", b.Reason)
		}
		o.log.Warn("build failed", "bid", b.BuildID, "err", err)
		return
	}
	if res == nil {
		b.Status, b.Reason = types.BuildError, "build produced no result"
		if o.persistTerminalBuild(ctx, b) {
			o.publishBuildState(b.BuildID, "error", "", b.Reason)
		}
		return
	}
	if b.Profile == types.ProfileBare && (res.SnapshotRef != "" || res.StartCmd != "" || res.ReadyCmd != "") {
		b.Status, b.Reason = types.BuildError, "bare build produced non-image output"
		if o.persistTerminalBuild(ctx, b) {
			o.publishBuildState(b.BuildID, "error", "", b.Reason)
		}
		return
	}
	switch {
	case res.SnapshotRef != "":
		b.Kind = types.KindSnp
		b.PersistID = types.TemplateID{Profile: b.Profile, Kind: types.KindSnp, Ref: res.SnapshotRef}.String()
	case res.ImageRef != "":
		b.Kind = types.KindImg
		b.PersistID = types.TemplateID{Profile: b.Profile, Kind: types.KindImg, Ref: res.ImageRef}.String()
	default:
		b.Status, b.Reason = types.BuildError, "build produced no artifact"
		if o.persistTerminalBuild(ctx, b) {
			o.publishBuildState(b.BuildID, "error", "", b.Reason)
		}
		return
	}
	if _, err := types.ParseTemplateID(b.PersistID); err != nil {
		b.Status, b.Reason = types.BuildError, "build produced invalid portable ref: "+err.Error()
		if o.persistTerminalBuild(ctx, b) {
			o.publishBuildState(b.BuildID, "error", "", b.Reason)
		}
		return
	}
	b.StartCmd, b.ReadyCmd = res.StartCmd, res.ReadyCmd
	b.Status = types.BuildReady
	b.Names = appendUnique(b.Names, b.PersistID)
	b.Aliases = appendUnique(b.Aliases, b.PersistID)
	if !o.persistTerminalBuild(ctx, b) {
		return
	}
	o.publishBuildState(b.BuildID, "ready", b.PersistID, "")
	o.log.Info("build ready", "bid", b.BuildID, "template", b.PersistID)
}

func (o *Orchestrator) persistTerminalBuild(ctx context.Context, build *types.Build) bool {
	delay := 20 * time.Millisecond
	for attempt := 1; ; attempt++ {
		writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := o.st.PutBuildTerminal(writeCtx, build)
		cancel()
		if err == nil {
			build.ExecutionClaimed = false
			build.ExecutionClaimedUnix = 0
			o.refreshBuildAdmissionGauges(context.Background())
			return true
		}
		if attempt == 1 {
			o.log.Warn("persist terminal build failed; retaining execution claim and retrying", "bid", build.BuildID, "err", err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			o.log.Error("persist terminal build canceled; execution claim retained", "bid", build.BuildID, "err", err)
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
	if !b.Profile.Valid() {
		return nil, fmt.Errorf("build: unknown profile %q", b.Profile)
	}
	dir := buildRuntimeDir(o.cfg.Paths.RunRoot, b.BuildID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	var port *vswitch.Port
	runtimePersisted := false
	cleanupSafe := true
	defer func() {
		if !cleanupSafe {
			retErr = &buildCleanupPendingError{cause: retErr}
			return
		}
		portID := ""
		if port != nil {
			portID = port.Port
		}
		cleanupErr := o.cleanupBuildRuntime(b, portID, dir, runtimePersisted)
		if cleanupErr != nil {
			retErr = &buildCleanupPendingError{cause: retErr, cleanup: cleanupErr}
		}
	}()
	spec, network, templateNetwork, resources, err := o.resolveBuildPhaseInputs(ctx, b)
	if err != nil {
		return nil, err
	}
	port, err = o.attachNetwork(ctx, network)
	if err != nil {
		return nil, err
	}

	envdTok := ""
	if b.Profile == types.ProfileE2B {
		envdTok, err = keys.MintToken()
		if err != nil {
			return nil, fmt.Errorf("build: mint phase envd token: %w", err)
		}
	}
	owned, err := o.st.SetBuildRuntimeOwnership(ctx, b.BuildID, port.Port, port.FloatingIP, port.MAC, envdTok)
	if err != nil {
		return nil, err
	}
	if !owned {
		return nil, fmt.Errorf("build: execution ownership lost before runtime allocation")
	}
	runtimePersisted = true
	b.RuntimeVswitchPort, b.RuntimeFloatingIP, b.RuntimePortMAC, b.RuntimeEnvdAccessToken = port.Port, port.FloatingIP, port.MAC, envdTok
	pend := &pendingBuild{
		build: b, workdir: dir, spec: spec,
		network: network, templateNetwork: templateNetwork,
		resources: resources,
		tapFD:     o.vs.TapFD(port.Port), mac: port.MAC,
		floating: port.FloatingIP, envdToken: envdTok,
		result: make(chan configsock.BuildResult, 1),
	}
	o.pendMu.Lock()
	o.pend[b.BuildID] = pend
	o.pendMu.Unlock()
	defer func() {
		o.pendMu.Lock()
		delete(o.pend, b.BuildID)
		o.pendMu.Unlock()
	}()

	var unit string
	var mmdsRow *types.Sandbox
	if _, err := o.builderRunPool.Assign(ctx, b.BuildID, func(runID string) error {
		var err error
		unit, err = o.prepareBuilderUnit(ctx, b, runID)
		if err != nil {
			return err
		}
		// Publish only after the real run id is durably assigned. The worker
		// cannot mint an unbound MMDSv2 token, and the builder's registered
		// routes/values are now projected under the synthetic sandbox id.
		if b.Profile == types.ProfileE2B && o.cfg.MMDS.Enabled {
			mmdsRow = &types.Sandbox{
				ID: "build-" + b.BuildID, Profile: b.Profile, TemplateID: b.TemplateID,
				State: types.StateRunning, RunID: runID, FloatingIP: port.FloatingIP,
				EnvdAccessToken: envdTok, APISecret: b.APISecret, ManifestKey: b.ManifestKey,
				Metadata: b.Metadata, CreatedUnix: time.Now().Unix(),
			}
			o.setMMDSBuildOwner(mmdsRow.ID, b.BuildID)
			o.cache(mmdsRow)
			o.publishUpsert(mmdsRow)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if mmdsRow != nil {
		defer func() {
			o.uncache(mmdsRow.ID)
			o.publishDelete(mmdsRow.ID)
			o.setMMDSBuildOwner(mmdsRow.ID, "")
		}()
	}
	timeout := time.Duration(o.cfg.Builder.TotalTimeoutSec+60) * time.Second
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case res := <-pend.result:
			if err := o.waitBuilderUnitExit(ctx, unit, 20*time.Second); err != nil {
				if stopErr := o.stopBuilderUnit(unit); stopErr != nil {
					cleanupSafe = false
					return nil, errors.Join(err, stopErr)
				}
				return nil, err
			}
			return &res, nil
		case <-ctx.Done():
			if err := o.stopBuilderUnit(unit); err != nil {
				cleanupSafe = false
				return nil, errors.Join(ctx.Err(), err)
			}
			return nil, ctx.Err()
		case <-timer.C:
			if err := o.stopBuilderUnit(unit); err != nil {
				cleanupSafe = false
				return nil, errors.Join(fmt.Errorf("build: result timeout after %s", timeout), err)
			}
			return nil, fmt.Errorf("build: result timeout after %s", timeout)
		case <-tick.C:
			select {
			case res := <-pend.result:
				if err := o.waitBuilderUnitExit(ctx, unit, 20*time.Second); err != nil {
					if stopErr := o.stopBuilderUnit(unit); stopErr != nil {
						cleanupSafe = false
						return nil, errors.Join(err, stopErr)
					}
					return nil, err
				}
				return &res, nil
			default:
			}
			if !o.unitActive(ctx, unit) {
				return nil, fmt.Errorf("build: unit %s exited without result", unit)
			}
		}
	}
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
	bound, err := o.st.BindBuildRun(ctx, b.BuildID, runID, "cpu,memory")
	if err != nil {
		return "", err
	}
	if !bound {
		return "", fmt.Errorf("build: execution ownership lost before run assignment")
	}
	b.RunID = runID
	b.EnforcementStatus = "cpu,memory"
	return unit, nil
}

// resolveBuildPhaseInputs is the sole node-policy + persisted ResourcePatch
// resolver for both fresh execution and restart adoption. It performs no host
// resource side effects.
func (o *Orchestrator) resolveBuildPhaseInputs(ctx context.Context, b *types.Build) (sandboxcfg.SandboxSpec, sandboxcfg.NetworkSpec, sandboxcfg.NetworkSpec, rtconfig.ResourcesConfig, error) {
	phaseMetadata := cloneStringMapWithout(b.Metadata, "")
	if phaseMetadata == nil {
		phaseMetadata = map[string]string{}
	}
	if b.PhaseResourcePatch != "" {
		phaseMetadata[sandboxcfg.NsResource] = b.PhaseResourcePatch
	}
	spec, err := sandboxcfg.ParseSpec(phaseMetadata)
	if err != nil {
		return sandboxcfg.SandboxSpec{}, sandboxcfg.NetworkSpec{}, sandboxcfg.NetworkSpec{}, rtconfig.ResourcesConfig{}, err
	}
	dynamic := o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled
	resources, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node:                     o.cfg.Sandbox.Resources.Policy(),
		Patch:                    spec.Resource,
		Dynamic:                  dynamic,
		ControllerSocketIdentity: o.resourceControllerSocketIdentity,
	})
	if err != nil {
		if errors.Is(err, sandboxcfg.ErrInvalidResourceRequest) {
			return sandboxcfg.SandboxSpec{}, sandboxcfg.NetworkSpec{}, sandboxcfg.NetworkSpec{}, rtconfig.ResourcesConfig{}, fmt.Errorf("%w: phase sandbox resources: %v", api.ErrBadRequest, err)
		}
		return sandboxcfg.SandboxSpec{}, sandboxcfg.NetworkSpec{}, sandboxcfg.NetworkSpec{}, rtconfig.ResourcesConfig{}, fmt.Errorf("build: resolve phase sandbox resources: %w", err)
	}
	inherited, err := o.sourceTemplateNetwork(ctx, b)
	if err != nil {
		return sandboxcfg.SandboxSpec{}, sandboxcfg.NetworkSpec{}, sandboxcfg.NetworkSpec{}, rtconfig.ResourcesConfig{}, err
	}
	network, templateNetwork, err := o.resolveBuildNetworks(b.Profile, inherited, spec.Network, "build-"+shortID(b.BuildID))
	if err != nil {
		return sandboxcfg.SandboxSpec{}, sandboxcfg.NetworkSpec{}, sandboxcfg.NetworkSpec{}, rtconfig.ResourcesConfig{}, err
	}
	return spec, network, templateNetwork, resources, nil
}

func (o *Orchestrator) cleanupBuildRuntime(b *types.Build, port, dir string, persisted bool) error {
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
				if o.detachedBuildPortsPending == nil {
					o.detachedBuildPortsPending = make(map[string]struct{})
				}
				o.detachedBuildPortsPending[port] = struct{}{}
			}
		}
		if cleanupErr == nil && persisted {
			cleared, err := o.st.ClearBuildRuntimeOwnership(cleanupCtx, b.BuildID)
			if err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
			} else if !cleared {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("build execution ownership lost during cleanup"))
			} else {
				delete(o.detachedBuildPortsPending, port)
				b.RuntimeVswitchPort, b.RuntimeFloatingIP, b.RuntimePortMAC, b.RuntimeEnvdAccessToken = "", "", "", ""
			}
		}
	}()
	if err := os.RemoveAll(dir); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove build workdir: %w", err))
	}
	return cleanupErr
}

// retryBuildCleanup keeps a live controller making progress after a transient
// unit, connector, store, or filesystem cleanup failure. It never releases the
// durable execution claim until the unit is fenced and all exact runtime
// ownership is gone. Cancellation leaves the row for startup reconciliation.
func (o *Orchestrator) retryBuildCleanup(ctx context.Context, b *types.Build, cause error) (error, error) {
	delay := 20 * time.Millisecond
	for attempt := 1; ; attempt++ {
		var cleanupErr error
		if b.RunID != "" {
			cleanupErr = o.stopBuilderUnit(o.builderUnit(b.RunID))
		}
		if cleanupErr == nil {
			port := b.RuntimeVswitchPort
			cleanupErr = o.cleanupBuildRuntime(b, port,
				buildRuntimeDir(o.cfg.Paths.RunRoot, b.BuildID), port != "")
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
			return cause, &buildCleanupPendingError{cause: cause, cleanup: cleanupErr}
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

// BuildSpecFor resolves "build:<bid>" to the pipeline work order. Only
// valid while executeBuild has the build pending (the unit is running).
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
	b := pend.build
	if !b.Profile.Valid() {
		return nil, "", false, fmt.Errorf("build: unknown profile %q", b.Profile)
	}

	env := map[string]string{"MANIFEST_KEY": b.ManifestKey}
	if b.RegistryAuth != "" {
		var c regcreds.Creds
		if json.Unmarshal([]byte(b.RegistryAuth), &c) == nil {
			for k, v := range c.FlattenEnv() {
				env[k] = v
			}
		}
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
				return nil, "", false, fmt.Errorf("presign COPY context %s: %w", s.FilesHash, err)
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
			return nil, "", false, err
		}
		fromTemplateRef, fromTemplateKind = t.Ref, string(t.Kind)
		refLocations, err = o.templateRefLocations(ctx, b.ManifestKey, t)
		if err != nil {
			return nil, "", false, err
		}
	}
	toRefLocation := ""
	if o.cfg.Checkpoint.Remote.RefLocationParent != "" {
		uri, err := o.cfg.Checkpoint.RefLocationURI(b.BuildID)
		if err != nil {
			return nil, "", false, err
		}
		toRefLocation = b.BuildID + "=" + uri
	}

	importReferer, err := o.effectiveImportReferer(b)
	if err != nil {
		return nil, "", false, err
	}

	spec := &configsock.BuildSpec{
		BuildID:          b.BuildID,
		Profile:          string(b.Profile),
		RunID:            b.RunID,
		Workdir:          pend.workdir,
		FromImage:        b.FromImage,
		FromTemplateRef:  fromTemplateRef,
		FromTemplateKind: fromTemplateKind,
		RefLocations:     refLocations,
		ToRefLocation:    toRefLocation,
		Steps:            steps,
		StartCmd:         b.StartCmd,
		ReadyCmd:         b.ReadyCmd,
		Env:              env,
		Paths: configsock.BuildPaths{
			Kernel:         o.cfg.Sandbox.Boot.Kernel,
			Runtime:        o.cfg.Sandbox.Boot.Runtime,
			OverlayDiffTpl: o.cfg.Sandbox.Boot.OverlayDiffTemplate,
			BuilderDiffTpl: o.cfg.Builder.DiffTemplate,
			SandboxCtl:     o.cfg.SandboxCtl(),
			FlattenCtl:     o.cfg.FlattenCtl(),
			ManifestCtl:    o.cfg.ManifestCtl(),
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
		TemplateNetwork: pend.templateNetwork,
		Resources:       pend.resources,
		MMDSEnabled:     b.Profile == types.ProfileE2B && o.cfg.MMDS.Enabled,
		EnvdToken:       pend.envdToken,
		Insecure:        o.cfg.Builder.InsecureRegistry,
		Platform:        o.cfg.Builder.Platform,
		ImportReferer:   importReferer,
		RegistryTLS:     o.effectiveRegistryTLS(b),
		Timeouts: configsock.BuildTimeouts{
			PullSec:  o.cfg.Builder.PullTimeoutSec,
			StepSec:  o.cfg.Builder.StepTimeoutSec,
			ReadySec: o.cfg.Builder.ReadyTimeoutSec,
			TotalSec: o.cfg.Builder.TotalTimeoutSec,
		},
	}
	return spec, configsock.BuildPidfile(o.cfg.Paths.RunRoot, b.BuildID), true, nil
}

// resolveBuildNetworks derives two roles from the same merged logical network:
// the temporary build VMs use buildHostname when no hostname was declared, while
// the produced template uses the normal sandbox hostname default. Both preserve
// current-build fields over source snapshot fields and share every other default.
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

// sourceTemplateNetwork returns the self-described network from a snapshot
// source. Image templates have no snapshot metadata channel, so they intentionally
// contribute no inherited network. A malformed or unreadable snapshot fails the
// build before host attachment instead of silently changing network semantics.
func (o *Orchestrator) sourceTemplateNetwork(ctx context.Context, b *types.Build) (sandboxcfg.NetworkSpec, error) {
	if b.FromTemplate == "" {
		return sandboxcfg.NetworkSpec{}, nil
	}
	tmpl, err := types.ParseTemplateID(b.FromTemplate)
	if err != nil {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("build: fromTemplate %q: %w", b.FromTemplate, err)
	}
	if tmpl.Kind != types.KindSnp {
		return sandboxcfg.NetworkSpec{}, nil
	}
	cfg, err := o.inspectSnapshotConfig(ctx, b.ManifestKey, tmpl.Ref)
	if err != nil {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("build: fromTemplate network: %w", err)
	}
	raw := strings.TrimSpace(cfg.Metadata[sandboxcfg.NsNetwork])
	if raw == "" {
		return sandboxcfg.NetworkSpec{}, nil
	}
	spec, err := sandboxcfg.ParseSpec(map[string]string{sandboxcfg.NsNetwork: raw})
	if err != nil {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("build: fromTemplate network: %w", err)
	}
	return spec.Network, nil
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
