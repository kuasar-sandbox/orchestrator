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

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// --- phase C: template snapshot ---------------------------------------------

func (p *buildPipeline) phaseTemplate() (string, error) {
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
	defer sb.teardown()

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

	out, err := p.hostCmdEnv(s.Env, s.Paths.SandboxCtl, "snapshot",
		"--sandbox-id", sb.sid, "--output", s.Workdir, "--run-root", sb.runRoot)
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
	// upload-snapshot publishes every local artifact the snapshot.cfg
	// references (the base image is a bundle-dir sibling) to the configured
	// portable backend — one command finishes the build.
	p.progress("uploading template snapshot to the content store")
	args := []string{"upload-snapshot", "--quiet"}
	if p.spec.ToRefLocation != "" {
		args = append(args, "--to-ref-location", p.spec.ToRefLocation)
	} else {
		args = append(args, "--manifest-config", p.spec.Paths.ManifestConfig)
	}
	args = append(args, bundle)
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
