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

// HandleCommand executes a registry node-link command and returns a receipt ack
// (cluster.md §5.1): accepted once the synchronous preconditions hold (key
// installed, template valid), rejected otherwise. Slow work (a boot/resume/
// teardown) runs asynchronously so the ack is prompt and the node-link reader
// isn't blocked; the terminal sandbox state is reported on the route stream,
// which a Reserve waits on.
func (o *Orchestrator) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	switch cmd.Kind {
	case routesync.CmdCreate:
		manifestKey, tmpl, err := o.precheckCluster(ctx, cmd)
		if err != nil {
			return reject(cmd, err)
		}
		go func() {
			if _, err := o.bootCluster(context.Background(), cmd, manifestKey, tmpl); err != nil {
				o.log.Error("cluster create", "sid", cmd.SID, "group", cmd.Group, "err", err)
			}
		}()
		return accept(cmd)
	case routesync.CmdConnect:
		go func() {
			if err := o.connectCluster(context.Background(), cmd.SID); err != nil {
				o.log.Error("cluster connect", "sid", cmd.SID, "err", err)
			}
		}()
		return accept(cmd)
	case routesync.CmdDelete:
		go func() {
			if err := o.deleteCluster(context.Background(), cmd.SID); err != nil {
				o.log.Error("cluster delete", "sid", cmd.SID, "err", err)
			}
		}()
		return accept(cmd)
	case routesync.CmdKeyPut, routesync.CmdKeyRenew:
		// Key distribution (cluster.md §7.6): install the group's manifest key into
		// the node's allowlist (a TTL lease) so create can resolve it by fingerprint.
		// The ack confirms installation — a build forward (Reserve) gates on it.
		if cmd.ManifestKey != "" {
			var ttl int64
			if cmd.ExpiresUnix > 0 {
				if ttl = cmd.ExpiresUnix - time.Now().Unix(); ttl <= 0 {
					ttl = 1
				}
			}
			if _, err := o.st.AddManifestKey(ctx, cmd.ManifestKey, "cluster", ttl, ""); err != nil {
				return reject(cmd, err)
			}
		}
		return accept(cmd)
	case routesync.CmdKeyDrop:
		if err := o.dropClusterKey(ctx, cmd.KeyFingerprint); err != nil {
			return reject(cmd, err)
		}
		return accept(cmd)
	default:
		return reject(cmd, fmt.Errorf("unhandled command kind %q", cmd.Kind))
	}
}

func accept(cmd *routesync.Command) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}
}

func reject(cmd *routesync.Command, err error) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: err.Error()}
}

// CreateCluster is the synchronous precheck + boot of a node-link create. The
// async path (HandleCommand) splits it so the ack is prompt; callers/tests that
// want the result synchronously use this.
func (o *Orchestrator) CreateCluster(ctx context.Context, cmd *routesync.Command) (*types.Sandbox, error) {
	manifestKey, tmpl, err := o.precheckCluster(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return o.bootCluster(ctx, cmd, manifestKey, tmpl)
}

// precheckCluster resolves the manifest key (by the fingerprint the registry
// predistributed) and the snapshot template — the fast, synchronous preconditions
// whose failure is a rejected ack (rather than a slow create that fails only by
// Reserve timeout).
func (o *Orchestrator) precheckCluster(ctx context.Context, cmd *routesync.Command) (string, types.TemplateID, error) {
	manifestKey, err := o.resolveByFingerprint(ctx, cmd.KeyFingerprint)
	if err != nil {
		return "", types.TemplateID{}, err
	}
	tmpl, err := types.ParseTemplateID(cmd.TemplateRef)
	if err != nil {
		return "", types.TemplateID{}, fmt.Errorf("cluster create: template %q: %w", cmd.TemplateRef, err)
	}
	return manifestKey, tmpl, nil
}

// bootCluster builds + launches the sandbox (registry-minted sid, resolved key,
// cluster (group, route_key) metadata) and publishes its route — which satisfies
// the registry's Reserve.
func (o *Orchestrator) bootCluster(ctx context.Context, cmd *routesync.Command, manifestKey string, tmpl types.TemplateID) (*types.Sandbox, error) {
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

// dropClusterKey removes a predistributed manifest key from the node's allowlist
// by fingerprint (cluster.md §7.6 lease withdrawal). 7a relies on the node's lazy
// TTL-lease expiry to reclaim it; 7c wires explicit removal-by-fingerprint.
func (o *Orchestrator) dropClusterKey(ctx context.Context, fingerprint string) error {
	o.log.Debug("cluster key_drop (lazy lease expiry reclaims; explicit drop is 7c)", "fp", fingerprint)
	return nil
}
