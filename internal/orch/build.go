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
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
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
func (o *Orchestrator) TriggerBuild(ctx context.Context, apiKey, tid, bid, fromImage, startCmd string, auth api.BuildAuth) error {
	b, err := o.st.GetBuild(ctx, bid)
	if err != nil {
		return err
	}
	if !ownsBuild(b, apiKey) || b.TemplateID != tid {
		return api.ErrNotFound
	}
	// The real e2b CLI pushes its client-built image to <mask>/{templateID}:{buildID}
	// and sends no image ref; derive fromImage from the configured mask.
	if fromImage == "" {
		fromImage = o.imageURIFromMask(b.TemplateID, b.BuildID)
	}
	if fromImage == "" {
		return fmt.Errorf("build: fromImage is required (no builder_image_uri_mask configured)")
	}
	b.FromImage = fromImage
	b.StartCmd = startCmd
	b.Kind = types.KindImg
	if startCmd != "" {
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

func (o *Orchestrator) executeBuild(ctx context.Context, b *types.Build) {
	key, err := o.executeImage(ctx, b)
	if err == nil && b.Kind == types.KindSnp {
		key, err = o.executeSnapshot(ctx, b, key)
	}
	if err != nil {
		b.Status, b.Reason = types.BuildError, err.Error()
		_ = o.st.PutBuild(ctx, b)
		o.log.Warn("build failed", "bid", b.BuildID, "err", err)
		return
	}
	b.Status = types.BuildReady
	b.PersistID = types.TemplateID{Profile: b.Profile, Kind: b.Kind, Key: key}.String()
	b.Names = appendUnique(b.Names, b.PersistID)
	b.Aliases = appendUnique(b.Aliases, b.PersistID)
	_ = o.st.PutBuild(ctx, b)
	o.log.Info("build ready", "bid", b.BuildID, "template", b.PersistID)
}

// executeImage runs the image build in a sandbox-builder@<bid> unit (cgroup
// accounting under sandbox-builder.slice). The unit's run-builder pulls the build
// LaunchSpec over the config-socket and execs flatten-ctl; flatten-ctl's stdout
// (the manifest key) is captured by the unit's StandardOutput=file into
// <bid>.result, read back here.
func (o *Orchestrator) executeImage(ctx context.Context, b *types.Build) (string, error) {
	dir := filepath.Join(o.cfg.Paths.RunRoot, b.BuildID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	if err := os.WriteFile(filepath.Join(dir, "flatten.yaml"), []byte(o.flattenConfigYAML(b, dir)), 0o600); err != nil {
		return "", err
	}
	unit := o.builderUnit(b.BuildID)
	_ = o.lc.ResetFailed(ctx, unit)
	startErr := o.lc.Start(ctx, unit)
	out, _ := os.ReadFile(filepath.Join(dir, b.BuildID+".result"))
	key := strings.TrimSpace(string(out))
	_ = o.lc.Stop(ctx, unit)
	_ = o.lc.ResetFailed(ctx, unit)
	if startErr != nil {
		return "", startErr
	}
	if !hexKeyRe.MatchString(key) {
		return "", fmt.Errorf("build: flatten-ctl output %q (want 64-hex key)", key)
	}
	return key, nil
}

// executeSnapshot boots the freshly-built image as an e2b sandbox keyed by the
// build id (sandbox-runner@<bid>), waits for envd readiness, snapshots it and
// returns the snapshot manifest key. The transient build sandbox is then removed.
//
// NOTE: warming the snapshot with the template start command depends on the
// envd start-command contract; it is passed through the guest env (E2B_START_CMD)
// for envd to pick up and should be verified against the running envd.
func (o *Orchestrator) executeSnapshot(ctx context.Context, b *types.Build, imgKey string) (string, error) {
	imgTmpl := types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Key: imgKey}
	sb := &types.Sandbox{
		ID:          b.BuildID,
		TemplateID:  imgTmpl.String(),
		State:       types.StateRunning,
		RunDir:      o.cfg.Paths.RunRoot + "/" + b.BuildID,
		BaseDir:     o.cfg.Paths.BaseRoot + "/" + b.BuildID,
		ManifestKey: b.ManifestKey,
		Env:         map[string]string{},
		EnvdUDS:     o.cfg.Paths.RunRoot + "/" + b.BuildID + "/envd.sock",
		CiUDS:       o.cfg.Paths.RunRoot + "/" + b.BuildID + "/ci.sock",
		CreatedUnix: time.Now().Unix(),
	}
	if b.StartCmd != "" {
		sb.Env["E2B_START_CMD"] = b.StartCmd
	}
	if err := o.launch(ctx, sb, imgTmpl); err != nil {
		o.teardown(context.Background(), sb)
		return "", err
	}
	key, err := o.snapshotRemote(ctx, sb)
	o.teardown(context.Background(), sb)
	_ = o.st.Delete(context.Background(), sb.ID)
	if err != nil {
		return "", err
	}
	if !hexKeyRe.MatchString(key) {
		return "", fmt.Errorf("build: snapshot returned %q (want 64-hex key)", key)
	}
	return key, nil
}

// --- configsock.Provider (build) ---

// buildLaunchSpec resolves "build:<bid>" to the flatten-ctl export LaunchSpec.
// The manifest key is the build owner (owner == SHA256(api_key) == manifest key);
// the flatten config (referer keyed by the base image, ephemeral scratch) is
// written under the build run dir by executeImage before the unit starts.
func (o *Orchestrator) buildLaunchSpec(ctx context.Context, bid string) (*configsock.LaunchSpec, string, bool, error) {
	b, err := o.st.GetBuild(ctx, bid)
	if err != nil || b == nil {
		return nil, "", false, err
	}
	dir := filepath.Join(o.cfg.Paths.RunRoot, bid)
	env := map[string]string{"MANIFEST_KEY": b.ManifestKey}
	// Inject the resolved registry pull creds as FLATTEN_REGISTRY_* (token or basic)
	// so flatten-ctl can pull from the tenant's (possibly private) registry.
	if b.RegistryAuth != "" {
		var c regcreds.Creds
		if json.Unmarshal([]byte(b.RegistryAuth), &c) == nil {
			for k, v := range c.FlattenEnv() {
				env[k] = v
			}
		}
	}
	spec := &configsock.LaunchSpec{
		Exec: o.cfg.FlattenCtl(),
		Args: []string{
			"export",
			"--config", filepath.Join(dir, "flatten.yaml"),
			"--manifest-config", o.cfg.ManifestConfig,
			"--upload", b.FromImage,
		},
		Workdir: dir,
		Env:     env,
	}
	return spec, filepath.Join(dir, bid+".pid"), true, nil
}

// --- helpers ---

// flattenConfigYAML renders the per-build flatten config (FLATTEN_CONFIG): the
// idempotent OCI-Referrers flow keyed by the base image, with scratch + the
// ephemeral blob cache under the build run dir (cleaned with the dir). Registry
// pull settings (insecure HTTP, platform) come from the orchestrator config.
func (o *Orchestrator) flattenConfigYAML(b *types.Build, runDir string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "tmpdir: %q\n", runDir)
	if o.cfg.Builder.InsecureRegistry {
		sb.WriteString("insecure: true\n")
	}
	if o.cfg.Builder.Platform != "" {
		fmt.Fprintf(&sb, "platform: %q\n", o.cfg.Builder.Platform)
	}
	fmt.Fprintf(&sb, "referer:\n  enabled: true\n  key: %q\n  desc: %q\n", b.FromImage, b.FromImage)
	return sb.String()
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
