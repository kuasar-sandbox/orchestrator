package registry

import (
	"context"
	"sync"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

type NodeOwner interface {
	PutManifestKey(ctx context.Context, nodeID, fingerprint, manifestKey string, expiresUnix int64) error
	DropManifestKey(ctx context.Context, nodeID, fingerprint string) error
	AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool
	ReleaseBuild(ctx context.Context, buildID string)
	Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error)
	DeleteSandbox(ctx context.Context, nodeID, sid string) error
}

type localNodeOwner struct {
	reg    *Registry
	leases *buildAdmissionManager
}

func newLocalNodeOwner(reg *Registry) *localNodeOwner {
	return &localNodeOwner{reg: reg, leases: newBuildAdmissionManager()}
}

func (o *localNodeOwner) PutManifestKey(ctx context.Context, nodeID, fingerprint, manifestKey string, expiresUnix int64) error {
	conn, live := o.reg.node(nodeID)
	if !live {
		return ErrNodeGone
	}
	return conn.send(&routesync.Command{CmdID: newID(), Kind: routesync.CmdKeyPut, KeyFingerprint: fingerprint, ManifestKey: manifestKey, ExpiresUnix: expiresUnix})
}

func (o *localNodeOwner) DropManifestKey(ctx context.Context, nodeID, fingerprint string) error {
	conn, live := o.reg.node(nodeID)
	if !live {
		return ErrNodeGone
	}
	return conn.send(&routesync.Command{CmdID: newID(), Kind: routesync.CmdKeyDrop, KeyFingerprint: fingerprint})
}

func (o *localNodeOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	node, found, err := o.reg.stores.GetNode(ctx, nodeID)
	if err != nil || !found {
		return false
	}
	return o.leases.admit(nodeID, buildID, node.BuildCapacity, want)
}

func (o *localNodeOwner) ReleaseBuild(ctx context.Context, buildID string) {
	o.leases.release(buildID)
}

func (o *localNodeOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return o.reg.stores.GetNode(ctx, nodeID)
}

func (o *localNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid string) error {
	conn, live := o.reg.node(nodeID)
	if !live {
		return ErrNodeGone
	}
	return conn.send(&routesync.Command{CmdID: newID(), Kind: routesync.CmdDelete, SID: sid})
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
