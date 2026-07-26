package registry

import (
	"context"
	"errors"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type NodeOwner interface {
	Connected(ctx context.Context, nodeID string) error
	PutKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error
	DropKeyPair(ctx context.Context, nodeID, apiSecretFingerprint string) error
	AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool
	ReleaseBuild(ctx context.Context, nodeID, buildID string)
	Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error)
	DeleteSandbox(ctx context.Context, nodeID, sid, apiSecretFingerprint string) error
	SendCommand(ctx context.Context, nodeID string, cmd *routesync.Command) error
	SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error)
}

type localNodeOwner struct {
	reg    *Registry
	leases *buildAdmissionManager
}

func newLocalNodeOwner(reg *Registry) *localNodeOwner {
	return &localNodeOwner{reg: reg, leases: newBuildAdmissionManager()}
}

func (o *localNodeOwner) Connected(ctx context.Context, nodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, live := o.reg.node(nodeID); !live {
		return ErrNodeGone
	}
	return nil
}

func (o *localNodeOwner) PutKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	return o.reg.stores.UpsertNodeKeyPair(ctx, nodeID, pair)
}

func (o *localNodeOwner) DropKeyPair(ctx context.Context, nodeID, apiSecretFingerprint string) error {
	return o.reg.stores.DropNodeKeyPair(ctx, nodeID, apiSecretFingerprint)
}

func (o *localNodeOwner) RefreshKeyPairs(ctx context.Context, nodeID string, pairs []clusterstate.NodeKeyPair) {
	now := time.Now().Unix()
	for _, pair := range pairs {
		if pair.APISecretFingerprint == "" || (pair.ExpiresUnix > 0 && pair.ExpiresUnix <= now) {
			continue
		}
		if pair.AckedExpiresUnix-now > int64(keyRenewBefore.Seconds()) {
			continue
		}
		cmd := &routesync.Command{
			CmdID:                  newID(),
			Kind:                   routesync.CmdKeyPut,
			APISecretFingerprint:   pair.APISecretFingerprint,
			APISecretType:          pair.APISecretType,
			APISecret:              pair.APISecret,
			APISecretRef:           pair.APISecretRef,
			ManifestKeyFingerprint: pair.ManifestKeyFingerprint,
			ManifestKeyType:        pair.ManifestKeyType,
			ManifestKey:            pair.ManifestKey,
			ManifestKeyRef:         pair.ManifestKeyRef,
			ExpiresUnix:            pair.ExpiresUnix,
		}
		ack, err := o.SendCommandAndWait(ctx, nodeID, cmd, keyAckTimeout)
		if err != nil || ack == nil {
			o.reg.log.Debug("node-link: key-pair acknowledgement missing",
				"node", nodeID, "cmd_id", cmd.CmdID, "err", err)
			return
		}
		if ack.Status != routesync.AckAccepted {
			o.reg.log.Warn("node-link: key pair rejected",
				"node", nodeID, "cmd_id", cmd.CmdID, "reason", ack.Reason)
			continue
		}
		marked, err := o.reg.stores.MarkNodeKeyPairAcked(ctx, nodeID, pair)
		if err != nil {
			o.reg.log.Warn("node-link: record key-pair acknowledgement",
				"node", nodeID, "cmd_id", cmd.CmdID, "err", err)
			continue
		}
		if !marked {
			o.reg.log.Debug("node-link: key-pair desired lease changed before acknowledgement",
				"node", nodeID, "cmd_id", cmd.CmdID)
		}
	}
}

func (o *localNodeOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	node, found, err := o.Runtime(ctx, nodeID)
	if err != nil || !found {
		return false
	}
	return o.leases.admit(nodeID, buildID, node.BuildCapacity, want)
}

func (o *localNodeOwner) ReleaseBuild(ctx context.Context, nodeID, buildID string) {
	o.leases.release(nodeID, buildID)
}

func (o *localNodeOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	if err := o.Connected(ctx, nodeID); err != nil {
		return nil, false, err
	}
	return o.reg.stores.GetNodeProfile(ctx, nodeID)
}

func (o *localNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid, apiSecretFingerprint string) error {
	return o.SendCommand(ctx, nodeID, &routesync.Command{
		CmdID: newID(), Kind: routesync.CmdDelete, SID: sid,
		APISecretFingerprint: apiSecretFingerprint,
	})
}

func (o *localNodeOwner) SendCommand(ctx context.Context, nodeID string, cmd *routesync.Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	conn, live := o.reg.node(nodeID)
	if !live {
		return ErrNodeGone
	}
	if cmd == nil {
		return errors.New("registry: node command is required")
	}
	if cmd.CmdID == "" {
		cmd.CmdID = newID()
	}
	return conn.send(cmd)
}

func (o *localNodeOwner) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, live := o.reg.node(nodeID)
	if !live {
		return nil, ErrNodeGone
	}
	if cmd == nil {
		return nil, errors.New("registry: node command is required")
	}
	if cmd.CmdID == "" {
		cmd.CmdID = newID()
	}
	return o.reg.sendAndWait(ctx, conn, cmd, timeout)
}

type routingNodeOwner struct {
	reg     *Registry
	local   NodeOwner
	remotes map[string]NodeOwner
}

func newRoutingNodeOwner(reg *Registry, local NodeOwner, remotes map[string]NodeOwner) *routingNodeOwner {
	cp := make(map[string]NodeOwner, len(remotes))
	for id, owner := range remotes {
		if id != "" && owner != nil {
			cp[id] = owner
		}
	}
	return &routingNodeOwner{reg: reg, local: local, remotes: cp}
}

func (o *routingNodeOwner) ownerFor(ctx context.Context, nodeID string) NodeOwner {
	node, found, err := o.reg.stores.GetNodeProfile(ctx, nodeID)
	if err == nil && found && node.LinkOwner != "" {
		return o.ownerByMember(node.LinkOwner)
	}
	return o.ownerForShard(ctx, nodeID)
}

func (o *routingNodeOwner) ownerByMember(memberID string) NodeOwner {
	if memberID == o.reg.stores.WriterID() {
		return o.local
	}
	if remote := o.remotes[memberID]; remote != nil {
		return remote
	}
	return missingNodeOwner{memberID: memberID}
}

func (o *routingNodeOwner) ownerForShard(ctx context.Context, nodeID string) NodeOwner {
	owners, err := o.reg.stores.NodeOwnerCandidates(ctx, nodeID)
	if err != nil || len(owners) == 0 {
		return o.local
	}
	var fallback NodeOwner
	var uncertain NodeOwner
	for _, memberID := range owners {
		if memberID == "" {
			continue
		}
		candidate := o.ownerByMember(memberID)
		if fallback == nil {
			fallback = candidate
		}
		probeErr := candidate.Connected(ctx, nodeID)
		if probeErr == nil {
			return candidate
		}
		if !errors.Is(probeErr, ErrNodeGone) && uncertain == nil {
			uncertain = candidate
		}
	}
	if uncertain != nil {
		return uncertain
	}
	if fallback != nil {
		return fallback
	}
	return o.local
}

func (o *routingNodeOwner) Connected(ctx context.Context, nodeID string) error {
	node, found, err := o.reg.stores.GetNodeProfile(ctx, nodeID)
	if err == nil && found && node.LinkOwner != "" {
		return o.ownerByMember(node.LinkOwner).Connected(ctx, nodeID)
	}
	owners, ownerErr := o.reg.stores.NodeOwnerCandidates(ctx, nodeID)
	if ownerErr != nil {
		if err != nil {
			return err
		}
		return ownerErr
	}
	var uncertain error
	for _, memberID := range owners {
		if memberID == "" {
			continue
		}
		probeErr := o.ownerByMember(memberID).Connected(ctx, nodeID)
		if probeErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errors.Is(probeErr, ErrNodeGone) && uncertain == nil {
			uncertain = probeErr
		}
	}
	if uncertain != nil {
		return uncertain
	}
	return ErrNodeGone
}

func (o *routingNodeOwner) PutKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	return o.ownerFor(ctx, nodeID).PutKeyPair(ctx, nodeID, pair)
}

func (o *routingNodeOwner) DropKeyPair(ctx context.Context, nodeID, apiSecretFingerprint string) error {
	return o.ownerFor(ctx, nodeID).DropKeyPair(ctx, nodeID, apiSecretFingerprint)
}

func (o *routingNodeOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	return o.ownerFor(ctx, nodeID).AdmitBuild(ctx, nodeID, buildID, want)
}

func (o *routingNodeOwner) ReleaseBuild(ctx context.Context, nodeID, buildID string) {
	o.local.ReleaseBuild(ctx, nodeID, buildID)
	for _, remote := range o.remotes {
		remote.ReleaseBuild(ctx, nodeID, buildID)
	}
}

func (o *routingNodeOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return o.ownerFor(ctx, nodeID).Runtime(ctx, nodeID)
}

func (o *routingNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid, apiSecretFingerprint string) error {
	return o.ownerFor(ctx, nodeID).DeleteSandbox(ctx, nodeID, sid, apiSecretFingerprint)
}

func (o *routingNodeOwner) SendCommand(ctx context.Context, nodeID string, cmd *routesync.Command) error {
	return o.ownerFor(ctx, nodeID).SendCommand(ctx, nodeID, cmd)
}

func (o *routingNodeOwner) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	return o.ownerFor(ctx, nodeID).SendCommandAndWait(ctx, nodeID, cmd, timeout)
}

type missingNodeOwner struct{ memberID string }

func (o missingNodeOwner) err() error { return ErrNodeGone }

func (o missingNodeOwner) Connected(context.Context, string) error { return o.err() }

func (o missingNodeOwner) PutKeyPair(context.Context, string, clusterstate.NodeKeyPair) error {
	return o.err()
}

func (o missingNodeOwner) DropKeyPair(context.Context, string, string) error { return o.err() }

func (o missingNodeOwner) AdmitBuild(context.Context, string, string, *routesync.BuildResources) bool {
	return false
}

func (o missingNodeOwner) ReleaseBuild(context.Context, string, string) {}

func (o missingNodeOwner) Runtime(context.Context, string) (*NodeRecord, bool, error) {
	return nil, false, o.err()
}

func (o missingNodeOwner) DeleteSandbox(context.Context, string, string, string) error {
	return o.err()
}

func (o missingNodeOwner) SendCommand(context.Context, string, *routesync.Command) error {
	return o.err()
}

func (o missingNodeOwner) SendCommandAndWait(context.Context, string, *routesync.Command, time.Duration) (*routesync.CmdAck, error) {
	return nil, o.err()
}

// buildAdmissionManager is the local node owner build-budget state.
// It is intentionally volatile: a node/registry restart clears execution leases,
// matching the cluster rule that node execution state is not restored from disk.
type buildAdmissionManager struct {
	mu     sync.Mutex
	byNode map[string]map[string]*routesync.BuildResources
}

func newBuildAdmissionManager() *buildAdmissionManager {
	return &buildAdmissionManager{
		byNode: map[string]map[string]*routesync.BuildResources{},
	}
}

func (m *buildAdmissionManager) admit(nodeID, buildID string, cap, want *routesync.BuildResources) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nodeID == "" || buildID == "" {
		return false
	}
	if _, exists := m.byNode[nodeID][buildID]; exists {
		return true
	}
	if cap != nil {
		var used routesync.BuildResources
		for _, r := range m.byNode[nodeID] {
			addBuildResources(&used, r)
		}
		addBuildResources(&used, want)
		if exceedsBuildResources(&used, cap) {
			return false
		}
	}
	if m.byNode[nodeID] == nil {
		m.byNode[nodeID] = map[string]*routesync.BuildResources{}
	}
	m.byNode[nodeID][buildID] = cloneBuildResources(want)
	return true
}

func (m *buildAdmissionManager) release(nodeID, buildID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nodeID == "" || buildID == "" {
		return
	}
	delete(m.byNode[nodeID], buildID)
	if len(m.byNode[nodeID]) == 0 {
		delete(m.byNode, nodeID)
	}
}

func addBuildResources(dst *routesync.BuildResources, src *routesync.BuildResources) {
	if src == nil {
		return
	}
	dst.CPU += src.CPU
	dst.Mem += src.Mem
	dst.Storage += src.Storage
}

func exceedsBuildResources(used, cap *routesync.BuildResources) bool {
	return (cap.CPU > 0 && used.CPU > cap.CPU) ||
		(cap.Mem > 0 && used.Mem > cap.Mem) ||
		(cap.Storage > 0 && used.Storage > cap.Storage)
}

func cloneBuildResources(in *routesync.BuildResources) *routesync.BuildResources {
	if in == nil {
		return nil
	}
	cp := *in
	return &cp
}
