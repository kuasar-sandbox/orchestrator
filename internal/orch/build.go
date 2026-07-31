package orch

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/remote"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

var hexKeyRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// newRegisteredBuild allocates the transient templateID + build id, resolves the
// allowlist, and records a registered build. fromImage is pre-derived from
// builder_image_uri_mask — the convention the e2b CLI pushes its client-built
// image under ({templateID}/{buildID}); the v3 trigger may still override it.
func (o *Orchestrator) newRegisteredBuild(ctx context.Context, apiKey string, spec api.RegisterSpec) (*types.Build, error) {
	if !spec.Profile.Valid() {
		return nil, fmt.Errorf("%w: unknown build profile %q", api.ErrBadRequest, spec.Profile)
	}
	metadata, builderOpts, err := buildcfg.Extract(spec.Metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if _, err := sandboxcfg.ParseSpec(metadata); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
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
		BuildID:     bid.String(),
		TemplateID:  templateID,
		APISecret:   pair.APISecret,
		ManifestKey: pair.ManifestKey,
		Profile:     spec.Profile,
		Kind:        types.KindImg,
		Status:      types.BuildRegistered,
		FromImage:   o.imageURIFromMask(templateID, bid.String()),
		Names:       nonEmpty(spec.Name),
		Aliases:     append([]string{}, spec.Tags...),
		Metadata:    metadata,
		Builder:     builderOpts,
		CreatedUnix: time.Now().Unix(),
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		return nil, err
	}
	return b, nil
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
	if !b.Profile.Valid() {
		return fmt.Errorf("%w: build has unknown profile %q", api.ErrBadRequest, b.Profile)
	}
	if b.Profile == types.ProfileBare && (spec.StartCmd != "" || spec.ReadyCmd != "") {
		return fmt.Errorf("%w: bare profile builds do not support startCmd or readyCmd", api.ErrBadRequest)
	}
	if spec.FromImage != "" && spec.FromTemplate != "" {
		return fmt.Errorf("build: fromImage and fromTemplate are mutually exclusive")
	}
	triggerMeta, triggerBuilder, err := buildcfg.Extract(spec.Metadata)
	if err != nil {
		return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
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
	mergedMetadata := sandboxcfg.MergeMetadata(b.Metadata, triggerMeta) // trigger overrides register
	if _, err := sandboxcfg.ParseSpec(mergedMetadata); err != nil {
		return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	b.Metadata = mergedMetadata
	b.Builder = buildcfg.Merge(b.Builder, triggerBuilder)
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
	return o.st.PutBuild(ctx, b)
}

func (o *Orchestrator) validateBuildOptions(opts types.BuildOptions, fromTemplate bool) error {
	r := opts.Referer
	if r == nil {
		return nil
	}
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

// resolveBuildCreds picks the registry pull credentials for this build and returns
// them as a regcreds.Creds JSON ("" = anonymous), to be stored encrypted on the build
// row and injected as FLATTEN_REGISTRY_* at flatten time. Precedence: the per-build
// pull token (api_headers, opaque, manifest-key-sealed) > the SDK's fromImageRegistry
// (cleartext username/password) > the tenant default (manifest_keys) > anonymous.
func (o *Orchestrator) resolveBuildCreds(ctx context.Context, b *types.Build, pullToken, regUser, regPass string) (string, error) {
	clusterAuth, isCluster := o.clusterBuildCreds(b.BuildID)
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

// --- builder resource pool ---

// BuildPool admits queued builds up to BuilderMaxConcurrent and executes them.
// Admission is a counting semaphore; the CAS on the build row lets a restarted
// orchestrator (or a future multi-worker) race safely for each build.
func (o *Orchestrator) BuildPool(ctx context.Context, interval time.Duration) {
	sem := make(chan struct{}, o.cfg.Builder.MaxConcurrent)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			waiting, _ := o.st.BuildsByStatus(ctx, types.BuildWaiting)
		admit:
			for _, b := range waiting {
				select {
				case sem <- struct{}{}:
				default:
					break admit // pool full this tick
				}
				won, err := o.claimWaitingBuild(ctx, b)
				if err != nil {
					<-sem
					o.log.Warn("build admission", "bid", b.BuildID, "err", err)
					continue
				}
				if !won {
					<-sem
					continue
				}
				o.publishBuildState(b.BuildID, "building", "", "")
				go func(b *types.Build) {
					defer func() { <-sem }()
					o.executeBuild(ctx, b)
				}(b)
			}
		}
	}
}

// claimWaitingBuild keeps the claimed row and the in-memory value aligned for
// the execution path after admission.
func (o *Orchestrator) claimWaitingBuild(ctx context.Context, b *types.Build) (bool, error) {
	won, err := o.st.CASBuildStatus(ctx, b.BuildID, types.BuildWaiting, types.BuildBuilding)
	if err != nil || !won {
		return won, err
	}
	b.Status = types.BuildBuilding
	return true, nil
}

type buildResult = configsock.BuildResult

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
	switch {
	case err == nil && res.Error != "":
		// The pipeline ran and reported its own failure. run-builder's fail()
		// already wrote "build failed: <detail>" to the build log stream (tag
		// build), which the SDK is streaming — so reason.message stays generic
		// and the detail lives in the log, not a duplicated BuildException tail.
		b.Status, b.Reason = types.BuildError, "build failed; see build logs"
		_ = o.st.PutBuild(ctx, b)
		o.publishBuildState(b.BuildID, "error", "", b.Reason)
		o.log.Warn("build failed", "bid", b.BuildID, "err", res.Error)
		return
	case err != nil:
		// Infrastructure failure: the pipeline never ran (or produced no
		// result), so there is NO build log for it — surface the orchestrator-
		// side error directly, it is the only signal.
		b.Status, b.Reason = types.BuildError, err.Error()
		_ = o.st.PutBuild(ctx, b)
		o.publishBuildState(b.BuildID, "error", "", b.Reason)
		o.log.Warn("build failed", "bid", b.BuildID, "err", err)
		return
	}
	if b.Profile == types.ProfileBare && (res.SnapshotRef != "" || res.StartCmd != "" || res.ReadyCmd != "") {
		b.Status, b.Reason = types.BuildError, "bare build produced non-image output"
		_ = o.st.PutBuild(ctx, b)
		o.publishBuildState(b.BuildID, "error", "", b.Reason)
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
		_ = o.st.PutBuild(ctx, b)
		o.publishBuildState(b.BuildID, "error", "", b.Reason)
		return
	}
	if _, err := types.ParseTemplateID(b.PersistID); err != nil {
		b.Status, b.Reason = types.BuildError, "build produced invalid portable ref: "+err.Error()
		_ = o.st.PutBuild(ctx, b)
		o.publishBuildState(b.BuildID, "error", "", b.Reason)
		return
	}
	b.StartCmd, b.ReadyCmd = res.StartCmd, res.ReadyCmd
	b.Status = types.BuildReady
	b.Names = appendUnique(b.Names, b.PersistID)
	b.Aliases = appendUnique(b.Aliases, b.PersistID)
	_ = o.st.PutBuild(ctx, b)
	o.publishBuildState(b.BuildID, "ready", b.PersistID, "")
	o.log.Info("build ready", "bid", b.BuildID, "template", b.PersistID)
}

func (o *Orchestrator) runBuildUnit(ctx context.Context, b *types.Build) (*buildResult, error) {
	if !b.Profile.Valid() {
		return nil, fmt.Errorf("build: unknown profile %q", b.Profile)
	}
	dir := filepath.Join(o.cfg.Paths.RunRoot, b.BuildID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	// One resolved NetworkSpec feeds both the host-side attachment and the guest
	// build config. Register/Trigger validate new records; this defensive parse
	// keeps corrupt legacy rows from degrading into a misleading network timeout.
	spec, err := sandboxcfg.ParseSpec(b.Metadata)
	if err != nil {
		return nil, err
	}
	inherited, err := o.sourceTemplateNetwork(ctx, b)
	if err != nil {
		return nil, err
	}
	network, templateNetwork, err := o.resolveBuildNetworks(
		b.Profile,
		inherited,
		spec.Network,
		"build-"+shortID(b.BuildID),
	)
	if err != nil {
		return nil, err
	}
	port, err := o.attachNetwork(ctx, network)
	if err != nil {
		return nil, err
	}
	defer func() { _ = o.vs.Detach(context.Background(), port.Port) }()

	envdTok := ""
	if b.Profile == types.ProfileE2B {
		envdTok, _ = keys.MintToken()
	}
	pend := &pendingBuild{
		build: b, workdir: dir, spec: spec,
		network: network, templateNetwork: templateNetwork,
		tapFD: o.vs.TapFD(port.Port), mac: port.MAC,
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

	// MMDS visibility for the template phase: a synthetic running route
	// (FC-mode envd resolves {id, token-hash} by its floating IP).
	if b.Profile == types.ProfileE2B && o.cfg.MMDS.Enabled {
		row := &types.Sandbox{
			ID: "build-" + b.BuildID, Profile: b.Profile, TemplateID: b.TemplateID,
			State: types.StateRunning, FloatingIP: port.FloatingIP,
			EnvdAccessToken: envdTok, APISecret: b.APISecret, ManifestKey: b.ManifestKey,
			CreatedUnix: time.Now().Unix(),
		}
		o.cache(row)
		o.publishUpsert(row)
		defer func() {
			o.uncache(row.ID)
			o.publishDelete(row.ID)
		}()
	}

	var unit string
	if _, err := o.builderRunPool.Assign(ctx, b.BuildID, func(runID string) error {
		b.RunID = runID
		unit = o.builderUnit(runID)
		return o.st.SetBuildRunID(ctx, b.BuildID, runID)
	}); err != nil {
		return nil, err
	}
	defer func() { _ = o.lc.ResetFailed(context.Background(), unit) }()

	timeout := time.Duration(o.cfg.Builder.TotalTimeoutSec+60) * time.Second
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case res := <-pend.result:
			return &res, nil
		case <-ctx.Done():
			_ = o.lc.Stop(context.Background(), unit)
			return nil, ctx.Err()
		case <-timer.C:
			_ = o.lc.Stop(context.Background(), unit)
			return nil, fmt.Errorf("build: result timeout after %s", timeout)
		case <-tick.C:
			select {
			case res := <-pend.result:
				return &res, nil
			default:
			}
			if !o.unitActive(ctx, unit) {
				return nil, fmt.Errorf("build: unit %s exited without result", unit)
			}
		}
	}
}

// --- configsock.Provider (build) ---

// BuildSpecFor resolves "build:<bid>" to the pipeline work order. Only
// valid while executeBuild has the build pending (the unit is running).
func (o *Orchestrator) BuildSpecFor(ctx context.Context, configID string) (*configsock.BuildSpec, string, bool, error) {
	kind, bid, found := strings.Cut(configID, ":")
	if !found || kind != "build" {
		return nil, "", false, nil
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

	// Phase-C VM capacity: the template's declared resource.capacity (register
	// cpuCount/memoryMB or a resource header) else the node builder default. This
	// pins snapshot.cfg.resources.capacity, which a snp-template create inherits.
	vcpu, mem := o.cfg.Builder.VCPU, o.cfg.Builder.Memory
	if capacity := pend.spec.Resource.Capacity; capacity != nil {
		if capacity.CPU > 0 {
			vcpu = capacity.CPU
		}
		if capacity.Memory != "" {
			mem = capacity.Memory
		}
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
		VCPU:            vcpu,
		Memory:          mem,
		MMDSEnabled:     b.Profile == types.ProfileE2B && o.cfg.MMDS.Enabled,
		EnvdToken:       pend.envdToken,
		Insecure:        o.cfg.Builder.InsecureRegistry,
		Platform:        o.cfg.Builder.Platform,
		ImportReferer:   importReferer,
		Timeouts: configsock.BuildTimeouts{
			PullSec:  o.cfg.Builder.PullTimeoutSec,
			StepSec:  o.cfg.Builder.StepTimeoutSec,
			ReadySec: o.cfg.Builder.ReadyTimeoutSec,
			TotalSec: o.cfg.Builder.TotalTimeoutSec,
		},
	}
	return spec, filepath.Join(pend.workdir, b.BuildID+".pid"), true, nil
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
