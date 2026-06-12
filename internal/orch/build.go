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

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/api"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/keys"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/vswitch"
)

var hexKeyRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// newRegisteredBuild allocates the transient templateID + build id, resolves the
// allowlist, and records a registered build. fromImage is pre-derived from
// builder_image_uri_mask — the convention the e2b CLI pushes its client-built
// image under ({templateID}/{buildID}); the v3 trigger may still override it.
func (o *Orchestrator) newRegisteredBuild(ctx context.Context, apiKey, name string, tags []string) (*types.Build, error) {
	manifestKey, err := o.resolveAllowed(ctx, apiKey)
	if err != nil {
		return nil, err
	}
	if manifestKey == "" {
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
		ManifestKey: manifestKey,
		Profile:     types.ProfileE2B,
		Kind:        types.KindImg,
		Status:      types.BuildRegistered,
		FromImage:   o.imageURIFromMask(templateID, bid.String()),
		Names:       nonEmpty(name),
		Aliases:     append([]string{}, tags...),
		CreatedUnix: time.Now().Unix(),
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		return nil, err
	}
	return b, nil
}

// RegisterBuild handles POST /v3/templates (e2b v2 build system): record a
// registered build; the base image + start command arrive at trigger time.
func (o *Orchestrator) RegisterBuild(ctx context.Context, apiKey, name string, tags []string) (*types.Build, error) {
	return o.newRegisteredBuild(ctx, apiKey, name, tags)
}

// TriggerBuild handles POST /v2/templates/{tid}/builds/{bid}: record the base
// image + start command, pick the kind (snp if a start command is set, else img),
// and queue the build for the pool. Build steps are intentionally unsupported.
func (o *Orchestrator) TriggerBuild(ctx context.Context, apiKey, tid, bid string, spec api.TriggerSpec, auth api.BuildAuth) error {
	b, err := o.st.GetBuild(ctx, bid)
	if err != nil {
		return err
	}
	if !ownsBuild(b, apiKey) || b.TemplateID != tid {
		return api.ErrNotFound
	}
	if spec.FromImage != "" && spec.FromTemplate != "" {
		return fmt.Errorf("build: fromImage and fromTemplate are mutually exclusive")
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
	b.Kind = types.KindImg
	if spec.StartCmd != "" || b.FromTemplate != "" {
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

// resolveBuildCreds picks the registry pull credentials for this build and returns
// them as a regcreds.Creds JSON ("" = anonymous), to be stored encrypted on the build
// row and injected as FLATTEN_REGISTRY_* at flatten time. Precedence: the per-build
// pull token (api_headers, opaque, manifest-key-sealed) > the SDK's fromImageRegistry
// (cleartext username/password) > the tenant default (manifest_keys) > anonymous.
func (o *Orchestrator) resolveBuildCreds(ctx context.Context, b *types.Build, pullToken, regUser, regPass string) (string, error) {
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
	default:
		authJSON, err := o.st.RegistryAuthForKey(ctx, b.ManifestKey)
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
		if verifyKey(apiKey, b.ManifestKey) {
			out = append(out, b)
		}
	}
	return out, nil
}

// resolveTemplateAlias maps a non-persist template reference — the transient register
// id the SDK reports as BuildInfo.template_id, or a build name/alias — to its built
// persist id. Returns "" if no ready build owned by this api key matches.
func (o *Orchestrator) resolveTemplateAlias(ctx context.Context, apiKey, ref string) string {
	builds, err := o.ListTemplates(ctx, apiKey)
	if err != nil {
		return ""
	}
	for _, b := range builds {
		if ref == b.PersistID || ref == b.TemplateID {
			return b.PersistID
		}
		for _, n := range append(append([]string{}, b.Names...), b.Aliases...) {
			if n == ref {
				return b.PersistID
			}
		}
	}
	return ""
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
				won, _ := o.st.CASBuildStatus(ctx, b.BuildID, types.BuildWaiting, types.BuildBuilding)
				if !won {
					<-sem
					continue
				}
				go func(b *types.Build) {
					defer func() { <-sem }()
					o.executeBuild(ctx, b)
				}(b)
			}
		}
	}
}

// buildResult is what run-builder prints on stdout (captured to
// <bid>.result by the unit): the manifest keys of what the pipeline
// actually produced, plus the effective start/ready commands
// (fromTemplate inheritance resolves inside the pipeline).
type buildResult struct {
	ImageKey    string `json:"image_key,omitempty"`
	SnapshotKey string `json:"snapshot_key,omitempty"`
	StartCmd    string `json:"start_cmd,omitempty"`
	ReadyCmd    string `json:"ready_cmd,omitempty"`
	Error       string `json:"error,omitempty"`
}

// pendingBuild is the per-execution state BuildSpecFor serves while the
// build unit runs: the pre-attached network slot + minted envd token.
type pendingBuild struct {
	build     *types.Build
	workdir   string
	tapExec   []string
	mac       string
	innerIP   string // CIDR
	floating  string
	envdToken string
}

// executeBuild runs the three-phase pipeline in a sandbox-builder@<bid>
// unit: orchestrator-ctl run-builder fetches the BuildSpec over the
// config-socket and drives import/steps/template sandboxes itself (as
// direct children, in the unit's cgroup). This side owns what spans the
// unit: the workdir, ONE vswitch slot the phases reuse sequentially,
// and — for the template phase under mmds.enabled — a synthetic route
// entry so the build sandbox's FC-mode envd can resolve itself.
func (o *Orchestrator) executeBuild(ctx context.Context, b *types.Build) {
	res, err := o.runBuildUnit(ctx, b)
	if err == nil && res.Error != "" {
		err = fmt.Errorf("%s", res.Error)
	}
	if err != nil {
		b.Status, b.Reason = types.BuildError, err.Error()
		_ = o.st.PutBuild(ctx, b)
		o.log.Warn("build failed", "bid", b.BuildID, "err", err)
		return
	}
	switch {
	case res.SnapshotKey != "":
		b.Kind = types.KindSnp
		b.PersistID = types.TemplateID{Profile: b.Profile, Kind: types.KindSnp, Key: res.SnapshotKey}.String()
	case res.ImageKey != "":
		b.Kind = types.KindImg
		b.PersistID = types.TemplateID{Profile: b.Profile, Kind: types.KindImg, Key: res.ImageKey}.String()
	default:
		b.Status, b.Reason = types.BuildError, "build produced no artifact"
		_ = o.st.PutBuild(ctx, b)
		return
	}
	b.StartCmd, b.ReadyCmd = res.StartCmd, res.ReadyCmd
	b.Status = types.BuildReady
	b.Names = appendUnique(b.Names, b.PersistID)
	b.Aliases = appendUnique(b.Aliases, b.PersistID)
	_ = o.st.PutBuild(ctx, b)
	o.log.Info("build ready", "bid", b.BuildID, "template", b.PersistID)
}

func (o *Orchestrator) runBuildUnit(ctx context.Context, b *types.Build) (*buildResult, error) {
	dir := filepath.Join(o.cfg.Paths.RunRoot, b.BuildID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	// One network slot for the whole build; the phase sandboxes reuse it
	// sequentially (tapfd handoff re-acquires the queue fd each boot).
	plainIP, cidrIP, err := o.allocInnerIP(types.ProfileE2B, "")
	if err != nil {
		return nil, err
	}
	port, err := o.vs.Attach(ctx, vswitch.AttachReq{InnerIP: plainIP})
	if err != nil {
		return nil, err
	}
	defer func() { _ = o.vs.Detach(context.Background(), port.Port) }()

	envdTok, _ := keys.MintToken()
	pend := &pendingBuild{
		build: b, workdir: dir,
		tapExec: o.vs.TapFDExec(port.Port), mac: port.MAC,
		innerIP: cidrIP, floating: port.FloatingIP, envdToken: envdTok,
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
	if o.cfg.MMDS.Enabled {
		row := &types.Sandbox{
			ID: "build-" + b.BuildID, TemplateID: b.TemplateID,
			State: types.StateRunning, FloatingIP: port.FloatingIP,
			EnvdAccessToken: envdTok, ManifestKey: b.ManifestKey,
			CreatedUnix: time.Now().Unix(),
		}
		o.cache(row)
		o.publishUpsert(row)
		defer func() {
			o.uncache(row.ID)
			o.publishDelete(row.ID)
		}()
	}

	unit := o.builderUnit(b.BuildID)
	_ = o.lc.ResetFailed(ctx, unit)
	startErr := o.lc.Start(ctx, unit) // Type=oneshot: returns when run-builder exits
	out, readErr := os.ReadFile(filepath.Join(dir, b.BuildID+".result"))
	_ = o.lc.Stop(ctx, unit)
	_ = o.lc.ResetFailed(ctx, unit)

	var res buildResult
	if len(out) > 0 {
		if jerr := json.Unmarshal(out, &res); jerr != nil {
			return nil, fmt.Errorf("build: result file: %w (unit err: %v)", jerr, startErr)
		}
	}
	if startErr != nil {
		if res.Error != "" {
			return &res, nil // the pipeline reported its own failure
		}
		return nil, startErr
	}
	if readErr != nil {
		return nil, fmt.Errorf("build: no result file: %w", readErr)
	}
	return &res, nil
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

	env := map[string]string{"MANIFEST_KEY": b.ManifestKey}
	if b.RegistryAuth != "" {
		var c regcreds.Creds
		if json.Unmarshal([]byte(b.RegistryAuth), &c) == nil {
			for k, v := range c.FlattenEnv() {
				env[k] = v
			}
		}
	}

	var steps []configsock.BuildStep
	for _, s := range b.Steps {
		steps = append(steps, configsock.BuildStep{Type: s.Type, Args: s.Args})
	}
	fromTemplate, fromTemplateKind := "", ""
	if b.FromTemplate != "" {
		t, err := types.ParseTemplateID(b.FromTemplate)
		if err != nil {
			return nil, "", false, err
		}
		fromTemplate, fromTemplateKind = t.Key, string(t.Kind)
	}

	spec := &configsock.BuildSpec{
		BuildID:          b.BuildID,
		Workdir:          pend.workdir,
		FromImage:        b.FromImage,
		FromTemplate:     fromTemplate,
		FromTemplateKind: fromTemplateKind,
		Steps:            steps,
		StartCmd:         b.StartCmd,
		ReadyCmd:         b.ReadyCmd,
		Env:              env,
		Paths: configsock.BuildPaths{
			Kernel:         o.cfg.Sandbox.Boot.Kernel,
			RuntimeE2B:     o.cfg.Sandbox.Boot.RuntimeE2B,
			RuntimeBuilder: o.cfg.Builder.RuntimeBuilder,
			OverlayDiffTpl: o.cfg.Sandbox.Boot.OverlayDiffTemplate,
			BuilderDiffTpl: o.cfg.Builder.DiffTemplate,
			SandboxCtl:     o.cfg.SandboxCtl(),
			FlattenCtl:     o.cfg.FlattenCtl(),
			ManifestCtl:    o.cfg.ManifestCtl(),
			ManifestConfig: o.cfg.ManifestConfig,
		},
		Net: configsock.BuildNet{
			TapFDExec: pend.tapExec,
			MAC:       pend.mac,
			InnerIP:   pend.innerIP,
			Nexthop:   o.innerGateway(types.ProfileE2B),
			Hostname:  "build-" + shortID(b.BuildID),
			DNS:       o.cfg.Sandbox.Network.DNS,
		},
		VCPU:        o.cfg.Builder.VCPU,
		Memory:      o.cfg.Builder.Memory,
		MMDSEnabled: o.cfg.MMDS.Enabled,
		EnvdToken:   pend.envdToken,
		Insecure:    o.cfg.Builder.InsecureRegistry,
		Platform:    o.cfg.Builder.Platform,
		Timeouts: configsock.BuildTimeouts{
			PullSec:  o.cfg.Builder.PullTimeoutSec,
			StepSec:  o.cfg.Builder.StepTimeoutSec,
			ReadySec: o.cfg.Builder.ReadyTimeoutSec,
			TotalSec: o.cfg.Builder.TotalTimeoutSec,
		},
	}
	return spec, filepath.Join(pend.workdir, b.BuildID+".pid"), true, nil
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
