package builder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	nodeconfig "github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// --- phase C: template snapshot ---------------------------------------------

// prepareBundleTemplateBase preserves the existing platform base_ref policy
// without relying on upload-snapshot to rewrite snapshot.cfg. Bundle exact
// upload is intentionally byte-preserving, so a newly exported local image
// must enter the Store before phase C and the snapshot must record that
// manifest identity itself. Tarstream publication keeps its existing rewrite
// path, and an already remote base needs no work.
func (p *buildPipeline) prepareBundleTemplateBase() error {
	if p.spec.CheckpointMode != nodeconfig.CheckpointBundle || p.imagePath == "" {
		return nil
	}
	ref, err := p.uploadImage()
	if err != nil {
		return fmt.Errorf("publish platform image: %w", err)
	}
	p.baseRef = ref
	return nil
}

func (p *buildPipeline) phaseTemplate() (result string, retErr error) {
	s := p.spec
	envdUDS := filepath.Join(s.Workdir, "envd.sock")
	doc, err := p.templateYAML()
	if err != nil {
		return "", err
	}
	sb, err := p.startSandbox("c", doc,
		[]string{envdUDS + ":127.0.0.1:49983"})
	if err != nil {
		return "", err
	}
	defer sb.joinTeardownError(&retErr)

	bootCtx, cancelBoot := context.WithTimeout(p.ctx, 90*time.Second)
	defer cancelBoot()
	if err := sb.waitRuntimeReady(bootCtx); err != nil {
		return "", err
	}
	if err := p.waitEnvd(bootCtx, envdUDS); err != nil {
		return "", err
	}
	cancelBoot()
	if err := p.envdInit(envdUDS); err != nil {
		return "", fmt.Errorf("envd /init: %w", err)
	}

	// startCmd through envd, the e2b way: launch, hold the stream while
	// readiness is probed, then drop it — envd never kills a process for
	// a lost stream, so the snapshot freezes it as an envd-MANAGED
	// process (visible/connectable from sandboxes spawned off the
	// template). e2b defaults: user "user", /home/user.
	sess := &envdExec{uds: envdUDS, log: p.log, out: p.out}
	if s.MMDSEnabled && s.EnvdToken != "" {
		sess.token = s.EnvdToken // /init armed envd; RPCs need the token now
	}
	p.progress("template: starting %s", p.startCmd)
	sc, err := sess.start(p.ctx, "user", "/home/user", nil, p.startCmd)
	if err != nil {
		return "", fmt.Errorf("startCmd: %w", err)
	}

	// readyCmd: poll every 2s within the ready budget (e2b semantics; the
	// default when a start command exists is e2b's `sleep 20`).
	ready := p.readyCmd
	if ready == "" {
		ready = "sleep 20"
	}
	deadline := time.Now().Add(time.Duration(s.Timeouts.ReadySec) * time.Second)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			_ = sc.stop()
			return "", fmt.Errorf("readyCmd never succeeded within %ds", s.Timeouts.ReadySec)
		}
		if err := sess.run(p.ctx, "user", "/home/user", nil, ready, remaining, true); err == nil {
			break
		}
		select {
		case <-p.ctx.Done():
			_ = sc.stop()
			return "", p.ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	p.progress("template: ready")
	// Release the stream; a start command that already FAILED fails the
	// build (a clean early exit is fine — one-shot start commands).
	if err := sc.stop(); err != nil {
		return "", err
	}

	out, err := p.hostCmdEnv(s.Env, s.Paths.SandboxCtl, templateSnapshotArgs(sb.sid, s.Workdir, sb.runRoot, s.CheckpointMode)...)
	if err != nil {
		return "", fmt.Errorf("snapshot: %w (%s)", err, firstLine(out))
	}
	bundle := filepath.Join(s.Workdir, sb.sid+".snapshot")
	if _, err := os.Stat(bundle); err != nil {
		return "", fmt.Errorf("snapshot bundle missing: %w", err)
	}
	p.progress("template: snapshot taken")
	return bundle, nil
}

func templateSnapshotArgs(sandboxID, output, runRoot, mode string) []string {
	if mode == "" {
		mode = "local"
	}
	return []string{"snapshot", "--sandbox-id", sandboxID, "--output", output, "--mode", mode, "--run-root", runRoot}
}

// waitEnvd polls envd's /health within the phase boot context. Runtime events
// and envd therefore share one deadline instead of receiving serial budgets.
func (p *buildPipeline) waitEnvd(ctx context.Context, uds string) error {
	cl := udsHTTP(uds)
	defer cl.CloseIdleConnections()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://envd/health", nil)
		if resp, err := cl.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 || resp.StatusCode == 204 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("envd not ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// envdInit provisions envd before the snapshot, matching the deployment
// posture: with MMDS the access token freezes into the template (children
// re-key via MMDS at create); without it envd stays non-secure.
func (p *buildPipeline) envdInit(uds string) error {
	payload := map[string]any{
		"envVars":        map[string]string{},
		"defaultUser":    "user",
		"defaultWorkdir": "/home/user",
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	}
	if p.spec.MMDSEnabled && p.spec.EnvdToken != "" {
		payload["accessToken"] = p.spec.EnvdToken
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(p.ctx, http.MethodPost, "http://envd/init", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := udsHTTP(uds).Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func udsHTTP(uds string) *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", uds)
			},
		},
	}
}

// --- finale: uploads ---------------------------------------------------------

func (p *buildPipeline) uploadImage() (string, error) {
	if p.baseImageRef != "" {
		return p.baseImageRef, nil
	}
	p.progress("uploading image to the content store")
	out, err := p.hostCmdEnv(p.spec.Env, p.spec.Paths.ManifestCtl,
		"store", "--no-progress", "--manifest-config", p.spec.Paths.ManifestConfig, p.imagePath)
	if err != nil {
		return "", fmt.Errorf("%w (%s)", err, firstLine(out))
	}
	key := strings.TrimSpace(string(out))
	if len(key) != 64 {
		return "", fmt.Errorf("manifest-ctl store output %q (want 64-hex key)", key)
	}
	p.baseImageRef = "manifest://" + key
	p.progress("uploaded image: %s", key)
	return p.baseImageRef, nil
}

func (p *buildPipeline) uploadSnapshot(bundle string) (string, error) {
	// Bundle mode exact-uploads snapshot layers without rewriting snapshot.cfg;
	// prepareBundleTemplateBase has already published a newly built platform
	// base. Tarstream mode retains upload-snapshot's existing graph rewrite.
	p.progress("uploading template snapshot to the content store")
	args, err := uploadSnapshotArgs(p.spec, bundle, p.now())
	if err != nil {
		return "", err
	}
	out, err := p.hostCmdEnv(p.spec.Env, p.spec.Paths.SandboxCtl, args...)
	if err != nil {
		return "", fmt.Errorf("%w (%s)", err, firstLine(out))
	}
	ref := strings.TrimSpace(string(out))
	if _, err := types.ParsePortableRef(ref); err != nil {
		return "", fmt.Errorf("upload-snapshot output %q: %w", ref, err)
	}
	p.progress("uploaded template snapshot: %s", ref)
	return ref, nil
}

// uploadSnapshotArgs builds the sandbox-ctl upload-snapshot argv. The
// publication name is minted HERE, at upload time, not at spec-resolution
// time: a build that spans UTC midnight publishes into the day it actually
// uploads, not the day the orchestrator resolved the spec. now is a parameter
// so tests can pin the clock across midnight.
func uploadSnapshotArgs(spec *configsock.BuildSpec, bundle string, now time.Time) ([]string, error) {
	args := []string{"upload-snapshot", "--quiet", "--manifest-config", spec.Paths.ManifestConfig}
	if spec.PublishLocationParent != "" {
		locName := reflocation.PublicationName(spec.BuildID, now)
		location, err := reflocation.Resolve(spec.PublishLocationParent, locName)
		if err != nil {
			return nil, fmt.Errorf("publish location for %s: %w", locName, err)
		}
		args = append(args, "--to-ref-location", locName+"="+location.URI)
	}
	return append(args, bundle), nil
}
