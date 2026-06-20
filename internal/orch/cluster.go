package orch

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/keys"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
)

// This file makes the orchestrator the node side of the cluster node-link
// (nodelink.Node): it executes the registry's lifecycle commands and reports the
// terminal state on the route stream (routes.go already carries the cluster
// (group, route_key) identity, so a registry Reserve converges on it).

// HandleCommand executes a registry node-link command. The terminal sandbox
// state is reported back via the route stream, which a registry Reserve waits on
// (not on an ack), so this need only kick off the action.
func (o *Orchestrator) HandleCommand(ctx context.Context, cmd *routesync.Command) {
	switch cmd.Kind {
	case routesync.CmdCreate:
		if _, err := o.CreateCluster(ctx, cmd); err != nil {
			o.log.Error("cluster create", "sid", cmd.SID, "group", cmd.Group, "err", err)
		}
	case routesync.CmdConnect:
		if err := o.connectCluster(ctx, cmd.SID); err != nil {
			o.log.Error("cluster connect", "sid", cmd.SID, "err", err)
		}
	case routesync.CmdDelete:
		if err := o.deleteCluster(ctx, cmd.SID); err != nil {
			o.log.Error("cluster delete", "sid", cmd.SID, "err", err)
		}
	case routesync.CmdKeyPut, routesync.CmdKeyRenew:
		// Key distribution (cluster.md §7.6): the registry pushes the group's
		// manifest key into the node's allowlist (a TTL lease) so create can
		// resolve it by fingerprint.
		if cmd.ManifestKey != "" {
			var ttl int64
			if cmd.ExpiresUnix > 0 {
				if ttl = cmd.ExpiresUnix - time.Now().Unix(); ttl <= 0 {
					ttl = 1
				}
			}
			if _, err := o.st.AddManifestKey(ctx, cmd.ManifestKey, "cluster", ttl, ""); err != nil {
				o.log.Error("cluster key_put", "fp", cmd.KeyFingerprint, "err", err)
			}
		}
	case routesync.CmdKeyDrop:
		// the node's lazy lease expiry reclaims the key; explicit drop-by-fingerprint is Phase 7.
		o.log.Debug("cluster key_drop", "fp", cmd.KeyFingerprint)
	default:
		o.log.Warn("cluster: unhandled command", "kind", cmd.Kind, "sid", cmd.SID)
	}
}

// CreateCluster boots a sandbox the registry placed here (node-link create): the
// registry-minted sid, the group's snapshot template, the manifest key the node
// holds (matched by fingerprint), and the cluster (group, route_key) metadata. It
// mirrors Create but with an external sid + a fingerprint-resolved key (no api
// key — the registry already authorized; the key arrives via predistribution).
func (o *Orchestrator) CreateCluster(ctx context.Context, cmd *routesync.Command) (*types.Sandbox, error) {
	manifestKey, err := o.resolveByFingerprint(ctx, cmd.KeyFingerprint)
	if err != nil {
		return nil, err
	}
	tmpl, err := types.ParseTemplateID(cmd.TemplateRef)
	if err != nil {
		return nil, fmt.Errorf("cluster create: template %q: %w", cmd.TemplateRef, err)
	}
	envdTok, _ := keys.MintToken()
	trafTok, _ := keys.MintToken()

	meta := map[string]string{}
	for k, v := range cmd.Config {
		meta[k] = v
	}
	cm, _ := json.Marshal(struct {
		Group    string `json:"group"`
		RouteKey string `json:"route_key"`
	}{cmd.Group, cmd.RouteKey})
	meta[sandboxcfg.NsCluster] = string(cm)

	sb := &types.Sandbox{
		ID:                 cmd.SID,
		TemplateID:         tmpl.String(),
		State:              types.StateRunning,
		RunDir:             o.cfg.Paths.RunRoot + "/" + cmd.SID,
		BaseDir:            o.cfg.Paths.BaseRoot + "/" + cmd.SID,
		ManifestKey:        manifestKey,
		EnvdAccessToken:    envdTok,
		TrafficAccessToken: trafTok,
		Metadata:           meta,
		CreatedUnix:        time.Now().Unix(),
		DeadlineUnix:       time.Now().Add(time.Duration(o.cfg.Sandbox.TimeoutSec) * time.Second).Unix(),
	}
	if tmpl.Profile == types.ProfileE2B {
		sb.EnvdUDS = sb.RunDir + "/envd.sock"
		sb.CiUDS = sb.RunDir + "/ci.sock"
	}
	if err := o.launch(ctx, sb, tmpl); err != nil {
		o.teardown(context.Background(), sb)
		return nil, err
	}
	o.publishUpsert(sb)
	return sb, nil
}

func (o *Orchestrator) resolveByFingerprint(ctx context.Context, fp string) (string, error) {
	if fp == "" {
		return "", fmt.Errorf("cluster create: empty key fingerprint")
	}
	candidates, err := o.st.AllowedManifestKeysByHash(ctx, fp)
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("cluster: no allowlisted manifest key for fingerprint %s (key not distributed?)", fp)
	}
	return candidates[0], nil
}

// connectCluster resumes a node-local PAUSED sandbox by sid, reusing the
// api-key-gated Connect with a key derived from the sandbox's own manifest key.
func (o *Orchestrator) connectCluster(ctx context.Context, sid string) error {
	apiKey, err := o.deriveSandboxAPIKey(ctx, sid)
	if err != nil {
		return err
	}
	_, err = o.Connect(ctx, sid, apiKey, "", 0)
	return err
}

func (o *Orchestrator) deleteCluster(ctx context.Context, sid string) error {
	apiKey, err := o.deriveSandboxAPIKey(ctx, sid)
	if err != nil {
		return err
	}
	_, err = o.Kill(ctx, sid, apiKey)
	return err
}

// deriveSandboxAPIKey mints the api key for a sandbox's own manifest key so the
// cluster command can reuse the api-key-gated Connect/Kill (the node already
// trusts the registry's command; this just satisfies the local auth path).
func (o *Orchestrator) deriveSandboxAPIKey(ctx context.Context, sid string) (string, error) {
	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return "", err
	}
	if sb == nil {
		return "", fmt.Errorf("cluster: sandbox %s not found", sid)
	}
	raw, err := hex.DecodeString(sb.ManifestKey)
	if err != nil {
		return "", err
	}
	return apikey.Mint(raw)
}
