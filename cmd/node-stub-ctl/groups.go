package main

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// The stub uses the actual Node Group service and SQLite transactions. Only
// sandbox execution is simulated; there is no second Group implementation.
func (n *stubNode) groupService() (*orch.Orchestrator, *store.Store, error) {
	n.groupMu.Lock()
	defer n.groupMu.Unlock()
	if n.groups != nil {
		return n.groups, n.groupDB, nil
	}
	dir, err := os.MkdirTemp("", "kuasar-node-stub-group-")
	if err != nil {
		return nil, nil, err
	}
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, err
	}
	db, err := store.Open(filepath.Join(dir, "node.db"), box)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, err
	}
	n.mu.Lock()
	pairs := make([]stubKeyPair, 0, len(n.keyPairs))
	for _, pair := range n.keyPairs {
		pairs = append(pairs, pair)
	}
	n.mu.Unlock()
	for _, pair := range pairs {
		if pair.APISecretType != "inline" || pair.ManifestKeyType != "inline" {
			continue
		}
		ttl := int64(0)
		if pair.ExpiresUnix != 0 {
			ttl = pair.ExpiresUnix - time.Now().Unix()
			if ttl <= 0 {
				continue
			}
		}
		if _, err := db.AddKeyPair(context.Background(), store.KeyPair{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey}, "stub", ttl, ""); err != nil {
			_ = db.Close()
			_ = os.RemoveAll(dir)
			return nil, nil, err
		}
	}
	n.groups = orch.New(&config.Config{}, db, nil, nil, n.log)
	n.groupDB, n.groupDir = db, dir
	return n.groups, db, nil
}

func (n *stubNode) closeGroups() {
	n.groupMu.Lock()
	defer n.groupMu.Unlock()
	if n.groupDB != nil {
		_ = n.groupDB.Close()
	}
	if n.groupDir != "" {
		_ = os.RemoveAll(n.groupDir)
	}
	n.groups, n.groupDB, n.groupDir = nil, nil, ""
}

func (n *stubNode) GroupSnapshot(ctx context.Context) (int64, []nodegroup.Support, error) {
	core, _, err := n.groupService()
	if err != nil {
		return 0, nil, err
	}
	return core.GroupSnapshot(ctx)
}
func (n *stubNode) SubscribeGroups() (<-chan routesync.GroupEvent, func()) {
	core, _, err := n.groupService()
	if err != nil {
		ch := make(chan routesync.GroupEvent)
		close(ch)
		return ch, func() {}
	}
	return core.SubscribeGroups()
}

func (n *stubNode) handleGroup(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	core, _, err := n.groupService()
	if err != nil {
		return stubGroupError(cmd, err, false)
	}
	result := core.HandleCommand(ctx, cmd)
	if result.Status == routesync.AckAccepted && cmd.Kind == routesync.CmdGroupPut && cmd.Group.KeyPut != nil {
		put := cmd.Group.KeyPut
		apiFP, _ := store.APISecretHash(put.APISecret)
		manifestFP, _ := store.ManifestKeyHash(put.ManifestKey)
		pair := stubKeyPair{APISecretFingerprint: apiFP, ManifestKeyFingerprint: manifestFP, APISecretType: "inline", ManifestKeyType: "inline",
			APISecret: put.APISecret, ManifestKey: put.ManifestKey, ExpiresUnix: put.ExpiresUnix}
		n.mu.Lock()
		if old, ok := n.keyPairs[apiFP]; ok && (old.ExpiresUnix == 0 || old.ExpiresUnix > pair.ExpiresUnix && pair.ExpiresUnix != 0) {
			pair.ExpiresUnix = old.ExpiresUnix
		}
		n.keyPairs[apiFP] = pair
		n.mu.Unlock()
	}
	return result
}

func stubGroupError(cmd *routesync.Command, err error, admission bool) *routesync.CmdAck {
	out := ack(cmd, routesync.AckRejected, "Group operation failed")
	out.HTTPStatus = nodegroup.HTTPStatus(err)
	if admission && errors.Is(err, nodegroup.ErrConflict) {
		out.ErrorCode = routesync.ErrorGroupVersionConflict
	}
	return out
}

func stubOwns(root, key string) bool {
	raw, err := hex.DecodeString(root)
	if err != nil {
		return false
	}
	defer clear(raw)
	parsed, err := apikey.Parse(key)
	return err == nil && apikey.Verify(parsed, raw)
}

func (n *stubNode) groupCreate(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	n.mu.Lock()
	old := n.sandboxes[cmd.SID]
	if old != nil {
		valid := stubOwns(old.APISecret, cmd.GroupAPIKey) && old.APISecretFingerprint == cmd.APISecretFingerprint && sameStubClusterContext(old.Cluster, cmd.Cluster)
		entry := old.routeEntry()
		n.mu.Unlock()
		if !valid {
			return stubGroupError(cmd, nodegroup.ErrForbidden, false)
		}
		n.publishRoute(entry)
		return ack(cmd, routesync.AckAccepted, "")
	}
	n.mu.Unlock()
	if cmd.Cluster == nil {
		return stubGroupError(cmd, nodegroup.ErrInvalid, false)
	}
	_, db, err := n.groupService()
	if err != nil {
		return stubGroupError(cmd, err, false)
	}
	detail, pair, err := db.GroupAdmit(ctx, cmd.GroupAPIKey, cmd.Cluster.Group, cmd.ExpectedGroupVersion)
	if err != nil {
		return stubGroupError(cmd, err, true)
	}
	fp, _ := store.APISecretHash(pair.APISecret)
	if fp != cmd.APISecretFingerprint {
		return stubGroupError(cmd, nodegroup.ErrForbidden, false)
	}
	input := *cmd
	if input.TemplateRef == "" {
		if detail.Context.TemplateID == nil {
			return stubGroupError(cmd, nodegroup.ErrInvalid, false)
		}
		input.TemplateRef = *detail.Context.TemplateID
		input.Config, err = sandboxcfg.MergeCreateMetadata(detail.Context.SandboxConfig, cmd.Config)
		if err != nil {
			return stubGroupError(cmd, nodegroup.ErrInvalid, false)
		}
	}
	return n.handleCreate(&input)
}

func (n *stubNode) groupBuild(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	location, err := clusterstate.ObjectLocationFromMetadata(cmd.Config)
	if err != nil {
		return stubGroupError(cmd, nodegroup.ErrInvalid, false)
	}
	input := *cmd
	n.mu.Lock()
	old := n.builds[cmd.BuildID]
	root := ""
	if old != nil {
		root = old.GroupAPISecret
		input.ImageRepo, input.RegistryAuth = old.RegistrationImageRepo, old.RegistrationRegistryAuth
	}
	n.mu.Unlock()
	if old != nil {
		if !stubOwns(root, cmd.GroupAPIKey) {
			return stubGroupError(cmd, nodegroup.ErrForbidden, false)
		}
	} else {
		_, db, err := n.groupService()
		if err != nil {
			return stubGroupError(cmd, err, false)
		}
		detail, pair, err := db.GroupAdmit(ctx, cmd.GroupAPIKey, location.Group, cmd.ExpectedGroupVersion)
		if err != nil {
			return stubGroupError(cmd, err, true)
		}
		fp, _ := store.APISecretHash(pair.APISecret)
		if fp != cmd.APISecretFingerprint {
			return stubGroupError(cmd, nodegroup.ErrForbidden, false)
		}
		root = pair.APISecret
		input.ImageRepo = detail.Context.ImageRepo
		if detail.Context.RegistryAuth != nil {
			input.RegistryAuth = detail.Context.RegistryAuth.Value
		}
	}
	result := n.handleBuildRegisterContext(ctx, &input)
	if result.Status == routesync.AckAccepted {
		n.mu.Lock()
		if build := n.builds[cmd.BuildID]; build != nil {
			build.GroupAPISecret = root
		}
		n.mu.Unlock()
	}
	return result
}

func (n *stubNode) updateGroupPair(ctx context.Context, pair stubKeyPair) error {
	n.groupMu.Lock()
	defer n.groupMu.Unlock()
	if n.groupDB == nil {
		return nil
	}
	ttl := int64(0)
	if pair.ExpiresUnix != 0 {
		ttl = pair.ExpiresUnix - time.Now().Unix()
		if ttl <= 0 {
			return nodegroup.ErrInvalid
		}
	}
	_, err := n.groupDB.AddKeyPair(ctx, store.KeyPair{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey}, "stub", ttl, "")
	return err
}

func (n *stubNode) dropGroupPair(ctx context.Context, fp string) error {
	n.groupMu.Lock()
	defer n.groupMu.Unlock()
	if n.groupDB == nil {
		return nil
	}
	_, err := n.groupDB.RemoveKeyPairByAPISecretFingerprint(ctx, fp)
	return err
}
