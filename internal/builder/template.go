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

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// --- phase C: template snapshot ---------------------------------------------

func (p *buildPipeline) phaseTemplate() (result string, retErr error) {
	s := p.spec
	envdUDS := filepath.Join(p.phaseRunDir("c"), "envd.sock")
	document, err := p.buildColdConfigYAML()
	if err != nil {
		return "", err
	}
	var connect []string
	if p.profile == types.ProfileE2B {
		connect = []string{envdUDS + ":127.0.0.1:49983"}
	}
	sb, err := p.startSandboxYAML("c", document, connect,
		s.SourceSandboxRef, s.SourceSandboxRef != "")
	if err != nil {
		return "", err
	}
	defer sb.joinTeardownError(&retErr)

	bootCtx, cancelBoot := context.WithTimeout(p.ctx, 90*time.Second)
	defer cancelBoot()
	if err := sb.waitRuntimeReady(bootCtx); err != nil {
		return "", err
	}
	if p.profile == types.ProfileE2B {
		if err := p.waitEnvd(bootCtx, envdUDS); err != nil {
			return "", err
		}
		cancelBoot()
		if err := p.envdInit(envdUDS); err != nil {
			return "", fmt.Errorf("envd /init: %w", err)
		}
		if err := p.waitTemplateCommands(envdUDS); err != nil {
			return "", err
		}
	} else {
		cancelBoot()
		if err := waitFixedReadiness(p.ctx, 20*time.Second); err != nil {
			return "", err
		}
	}

	checkpointDir := p.checkpointDir()
	out, err := p.hostCmdEnv(s.Env, s.Paths.SandboxCtl,
		templateSnapshotArgs(sb.pathID, checkpointDir, sb.runRoot, s.CheckpointMode, s.CheckpointPolicy)...)
	if err != nil {
		return "", fmt.Errorf("snapshot: %w (%s)", err, firstLine(out))
	}
	bundle := filepath.Join(checkpointDir, sb.sid+".snapshot")
	if _, err := os.Stat(bundle); err != nil {
		return "", fmt.Errorf("snapshot bundle missing: %w", err)
	}
	p.progress("template: snapshot taken")
	return bundle, nil
}

func (p *buildPipeline) waitTemplateCommands(envdUDS string) error {
	s := p.spec
	sess := &envdExec{uds: envdUDS, log: p.log, out: p.out}
	if s.MMDSEnabled && s.EnvdToken != "" {
		sess.token = s.EnvdToken
	}
	var start *startedCmd
	if p.startCmd != "" {
		p.progress("template: starting %s", p.startCmd)
		stream, err := sess.start(p.ctx, "user", "/home/user", nil, p.startCmd)
		if err != nil {
			return fmt.Errorf("startCmd: %w", err)
		}
		start = stream
		defer func() {
			if start != nil {
				_ = start.stop()
			}
		}()
	}
	if p.readyCmd == "" {
		if err := waitFixedReadiness(p.ctx, 20*time.Second); err != nil {
			return err
		}
	} else {
		deadline := time.Now().Add(time.Duration(s.Timeouts.ReadySec) * time.Second)
		for {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("readyCmd never succeeded within %ds", s.Timeouts.ReadySec)
			}
			if err := sess.run(p.ctx, "user", "/home/user", nil, p.readyCmd, remaining, true); err == nil {
				break
			}
			remaining = time.Until(deadline)
			if remaining <= 0 {
				continue
			}
			interval := 2 * time.Second
			if remaining < interval {
				interval = remaining
			}
			timer := time.NewTimer(interval)
			select {
			case <-p.ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return p.ctx.Err()
			case <-timer.C:
			}
		}
	}
	p.progress("template: ready")
	if start != nil {
		if err := start.stop(); err != nil {
			return err
		}
		start = nil
	}
	return nil
}

func waitFixedReadiness(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func templateSnapshotArgs(pathID, output, runRoot, mode string, policy sandboxcfg.SnapshotPolicy) []string {
	if mode == "" {
		mode = "local"
	}
	args := []string{"snapshot", "--path-id", pathID, "--output", output, "--mode", mode, "--run-root", runRoot}
	if policy.MergeRef != nil {
		args = append(args, fmt.Sprintf("--merge-ref=%t", *policy.MergeRef))
	}
	if policy.DropCaches != nil {
		args = append(args, fmt.Sprintf("--drop-caches=%t", *policy.DropCaches))
	}
	return args
}

func (p *buildPipeline) buildColdConfigYAML() ([]byte, error) {
	cfg, err := p.buildColdConfig()
	if err != nil {
		return nil, err
	}
	return sandboxcfg.MarshalBuildColdConfig(cfg)
}

func (p *buildPipeline) buildColdConfig() (*rtconfig.SandboxConfig, error) {
	namespaces := make(map[string]bool, len(p.spec.SandboxNamespaces))
	for _, namespace := range p.spec.SandboxNamespaces {
		namespaces[namespace] = true
	}
	cfg, err := (sandboxcfg.BuildColdParams{
		Profile: p.profile, ImageRef: p.baseRef, Source: p.spec.SourceSandboxConfig,
		Kernel: p.spec.Paths.Kernel, Runtime: p.spec.Paths.Runtime,
		OverlayDiffTpl: p.spec.Paths.OverlayDiffTpl,
		TapFD: sandboxcfg.TapFD{
			Exec: append([]string(nil), p.spec.Net.TapFD.Exec...), Socket: p.spec.Net.TapFD.Socket,
			Request: p.spec.Net.TapFD.Request, Timeout: p.spec.Net.TapFD.Timeout,
		},
		PortMAC: p.spec.Net.MAC, InnerIP: p.spec.Net.InnerIP,
		EnvVars: p.spec.SandboxEnv, Resources: p.spec.SandboxResources,
		Network: p.spec.TemplateNetwork, MMDSEnabled: p.spec.MMDSEnabled,
		Spec: p.spec.SandboxSpec, NamespacePresent: namespaces,
		StartCmd: p.startCmd, ReadyCmd: p.readyCmd,
	}).BuildColdConfig()
	if err != nil {
		return nil, err
	}
	// The former CLI path loaded this document through config.LoadMerged,
	// which applied canonical defaults before image-default materialization.
	// Direct package assembly receives the typed value, so apply the identical
	// defaults here for both top-level E assembly and Phase C.
	cfg.ApplyDefaults()
	return cfg, nil
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
	payload := p.envdInitPayload()
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

func (p *buildPipeline) envdInitPayload() map[string]any {
	payload := map[string]any{
		"envVars":        p.spec.SandboxEnv,
		"defaultUser":    "user",
		"defaultWorkdir": "/home/user",
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	}
	if p.spec.MMDSEnabled && p.spec.EnvdToken != "" {
		payload["accessToken"] = p.spec.EnvdToken
	}
	return payload
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

// --- finale: checkpoint publication -----------------------------------------

func (p *buildPipeline) publishSnapshot(bundle string) (string, error) {
	// Bundle mode preserves snapshot.cfg and any already-portable located image
	// Bundle reference. Local mode rewrites only the checkpoint-class graph into
	// role-specific tarstreams before applying the same checkpoint destination.
	p.progress("publishing template checkpoint snapshot")
	args, err := publishCheckpointArtifactArgs(p.spec, p.publication.CheckpointClassTarget, bundle, p.now())
	if err != nil {
		return "", err
	}
	out, err := p.hostCmdEnv(p.spec.Env, p.spec.Paths.SandboxCtl, args...)
	if err != nil {
		return "", fmt.Errorf("%w (%s)", err, firstLine(out))
	}
	ref := strings.TrimSpace(string(out))
	if _, err := types.ParsePortableRef(ref); err != nil {
		return "", fmt.Errorf("publish output %q: %w", ref, err)
	}
	p.progress("published template checkpoint snapshot: %s", ref)
	return ref, nil
}

// publishCheckpointArtifactArgs builds the sandbox-ctl argv for the Phase-C
// checkpoint-class graph. The name is minted at this publication, not while the
// BuildSpec is resolved; image and checkpoint publications may therefore land
// in different UTC date buckets. now is explicit so tests can cross midnight.
func publishCheckpointArtifactArgs(spec *configsock.BuildSpec, target CheckpointClassPublicationTarget, artifact string, now time.Time) ([]string, error) {
	args := []string{"publish", "--quiet", "--manifest-config", spec.Paths.ManifestConfig}
	switch target {
	case CheckpointClassManifestStore:
	case CheckpointClassRefLocation:
		locName := reflocation.PublicationName(spec.BuildID, now)
		location, err := reflocation.Resolve(spec.CheckpointRefLocationParent, locName)
		if err != nil {
			return nil, fmt.Errorf("checkpoint publication location for %s: %w", locName, err)
		}
		args = append(args, "--to-ref-location", locName+"="+location.URI)
	default:
		return nil, fmt.Errorf("unsupported checkpoint-class publication target %q", target)
	}
	args = appendRefLocationArgs(args, spec.RefLocations)
	return append(args, artifact), nil
}
