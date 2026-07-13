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
	PutManifestKey(ctx context.Context, nodeID, fingerprint, keyType, keyValue string, expiresUnix int64) error
	DropManifestKey(ctx context.Context, nodeID, fingerprint string) error
	AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool
	ReleaseBuild(ctx context.Context, buildID string)
	Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error)
	DeleteSandbox(ctx context.Context, nodeID, sid string) error
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

func (o *localNodeOwner) PutManifestKey(ctx context.Context, nodeID, fingerprint, keyType, keyValue string, expiresUnix int64) error {
	if keyType == "" {
		keyType = clusterstate.SecretInline
	}
	key := clusterstate.NodeManifestKey{Fingerprint: fingerprint, Type: keyType, ExpiresUnix: expiresUnix}
	if keyType == "ref" {
		key.Ref = keyValue
	} else {
		key.Value = keyValue
	}
	return o.reg.stores.UpsertNodeManifestKey(ctx, nodeID, key)
}

func (o *localNodeOwner) DropManifestKey(ctx context.Context, nodeID, fingerprint string) error {
	return o.reg.stores.DropNodeManifestKey(ctx, nodeID, fingerprint)
}

func (o *localNodeOwner) RefreshManifestKeys(ctx context.Context, nodeID string, keys []clusterstate.NodeManifestKey) {
	now := time.Now().Unix()
	for _, key := range keys {
		if key.Fingerprint == "" || (key.ExpiresUnix > 0 && key.ExpiresUnix <= now) {
			continue
		}
		if key.AckedExpiresUnix-now > int64(keyRenewBefore.Seconds()) {
			continue
		}
		cmd := &routesync.Command{
			CmdID:           newID(),
			Kind:            routesync.CmdKeyPut,
			KeyFingerprint:  key.Fingerprint,
			ManifestKeyType: key.Type,
			ManifestKey:     key.Value,
			ManifestKeyRef:  key.Ref,
			ExpiresUnix:     key.ExpiresUnix,
		}
		if cmd.ManifestKeyType == "" {
			cmd.ManifestKeyType = clusterstate.SecretInline
		}
		ack, err := o.SendCommandAndWait(ctx, nodeID, cmd, keyAckTimeout)
		if err != nil || ack == nil {
			o.reg.log.Debug("node-link: manifest key acknowledgement missing",
				"node", nodeID, "fingerprint", key.Fingerprint, "cmd_id", cmd.CmdID, "err", err)
			return
		}
		if ack.Status != routesync.AckAccepted {
			o.reg.log.Warn("node-link: manifest key rejected",
				"node", nodeID, "fingerprint", key.Fingerprint, "cmd_id", cmd.CmdID, "reason", ack.Reason)
			continue
		}
		marked, err := o.reg.stores.MarkNodeManifestKeyAcked(ctx, nodeID, key)
		if err != nil {
			o.reg.log.Warn("node-link: record manifest key acknowledgement",
				"node", nodeID, "fingerprint", key.Fingerprint, "cmd_id", cmd.CmdID, "err", err)
			continue
		}
		if !marked {
			o.reg.log.Debug("node-link: manifest key desired lease changed before acknowledgement",
				"node", nodeID, "fingerprint", key.Fingerprint, "cmd_id", cmd.CmdID)
		}
	}
}

func (o *localNodeOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	node, found, err := o.reg.stores.GetNodeProfile(ctx, nodeID)
	if err != nil || !found {
		return false
	}
	return o.leases.admit(nodeID, buildID, node.BuildCapacity, want)
}

func (o *localNodeOwner) ReleaseBuild(ctx context.Context, buildID string) {
	o.leases.release(buildID)
}

func (o *localNodeOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return o.reg.stores.GetNodeProfile(ctx, nodeID)
}

func (o *localNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid string) error {
	return o.SendCommand(ctx, nodeID, &routesync.Command{CmdID: newID(), Kind: routesync.CmdDelete, SID: sid})
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
		if node.LinkOwner == o.reg.stores.WriterID() {
			return o.local
		}
		if remote := o.remotes[node.LinkOwner]; remote != nil {
			return remote
		}
		return missingNodeOwner{memberID: node.LinkOwner}
	}
	return o.ownerForShard(ctx, nodeID)
}

func (o *routingNodeOwner) ownerForShard(ctx context.Context, nodeID string) NodeOwner {
	owners, err := o.reg.stores.NodeOwnerCandidates(ctx, nodeID)
	if err != nil || len(owners) == 0 {
		return o.local
	}
	local := o.reg.stores.WriterID()
	for _, owner := range owners {
		if owner == local {
			return o.local
		}
	}
	for _, owner := range owners {
		if owner == "" {
			continue
		}
		if remote := o.remotes[owner]; remote != nil {
			return remote
		}
		return missingNodeOwner{memberID: owner}
	}
	return o.local
}

func (o *routingNodeOwner) PutManifestKey(ctx context.Context, nodeID, fingerprint, keyType, keyValue string, expiresUnix int64) error {
	return o.ownerFor(ctx, nodeID).PutManifestKey(ctx, nodeID, fingerprint, keyType, keyValue, expiresUnix)
}

func (o *routingNodeOwner) DropManifestKey(ctx context.Context, nodeID, fingerprint string) error {
	return o.ownerFor(ctx, nodeID).DropManifestKey(ctx, nodeID, fingerprint)
}

func (o *routingNodeOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	return o.ownerFor(ctx, nodeID).AdmitBuild(ctx, nodeID, buildID, want)
}

func (o *routingNodeOwner) ReleaseBuild(ctx context.Context, buildID string) {
	o.local.ReleaseBuild(ctx, buildID)
	for _, remote := range o.remotes {
		remote.ReleaseBuild(ctx, buildID)
	}
}

func (o *routingNodeOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return o.ownerFor(ctx, nodeID).Runtime(ctx, nodeID)
}

func (o *routingNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid string) error {
	return o.ownerFor(ctx, nodeID).DeleteSandbox(ctx, nodeID, sid)
}

func (o *routingNodeOwner) SendCommand(ctx context.Context, nodeID string, cmd *routesync.Command) error {
	return o.ownerFor(ctx, nodeID).SendCommand(ctx, nodeID, cmd)
}

func (o *routingNodeOwner) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	return o.ownerFor(ctx, nodeID).SendCommandAndWait(ctx, nodeID, cmd, timeout)
}

type missingNodeOwner struct{ memberID string }

func (o missingNodeOwner) err() error { return ErrNodeGone }

func (o missingNodeOwner) PutManifestKey(context.Context, string, string, string, string, int64) error {
	return o.err()
}

func (o missingNodeOwner) DropManifestKey(context.Context, string, string) error { return o.err() }

func (o missingNodeOwner) AdmitBuild(context.Context, string, string, *routesync.BuildResources) bool {
	return false
}

func (o missingNodeOwner) ReleaseBuild(context.Context, string) {}

func (o missingNodeOwner) Runtime(context.Context, string) (*NodeRecord, bool, error) {
	return nil, false, o.err()
}

func (o missingNodeOwner) DeleteSandbox(context.Context, string, string) error { return o.err() }

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
	mu      sync.Mutex
	byNode  map[string]map[string]*routesync.BuildResources
	byBuild map[string]string
}

func newBuildAdmissionManager() *buildAdmissionManager {
	return &buildAdmissionManager{
		byNode:  map[string]map[string]*routesync.BuildResources{},
		byBuild: map[string]string{},
	}
}

func (m *buildAdmissionManager) admit(nodeID, buildID string, cap, want *routesync.BuildResources) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nodeID == "" || buildID == "" {
		return false
	}
	if prev := m.byBuild[buildID]; prev != "" {
		return prev == nodeID
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
	m.byBuild[buildID] = nodeID
	return true
}

func (m *buildAdmissionManager) release(buildID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodeID := m.byBuild[buildID]
	if nodeID == "" {
		return
	}
	delete(m.byBuild, buildID)
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
