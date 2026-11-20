package registry

import (
	"context"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/testutil/metadatastore"
	"testing"
)

func newTestStores() *Stores {
	stores, err := NewStores(metadatastore.New().Repositories(), metadatastore.Owners{ID: "registry"})
	if err != nil {
		panic(err)
	}
	return stores
}
func newTestStoreCluster(t *testing.T, members []string, routeOwners, nodeOwners, scaleOwners, nodeListOwners int) map[string]*Stores {
	t.Helper()
	db := metadatastore.New()
	out := map[string]*Stores{}
	for _, id := range members {
		s, err := NewStores(db.Repositories(), metadatastore.Owners{ID: id, Members: members, Count: nodeOwners})
		if err != nil {
			t.Fatal(err)
		}
		out[id] = s
	}
	return out
}

// Seed aggregate children explicitly in fixtures. Production registration never
// rewrites ownership or key-pair data from a caller's stale aggregate snapshot.
func putTestNode(s *Stores, ctx context.Context, n *NodeRecord) error {
	if err := s.PutNode(ctx, n); err != nil {
		return err
	}
	for _, ref := range n.Sandboxes {
		if err := s.AddNodeSandboxRef(ctx, n.NodeID, ref); err != nil {
			return err
		}
	}
	for _, ref := range n.Builds {
		if err := s.AddNodeBuildRef(ctx, n.NodeID, ref); err != nil {
			return err
		}
	}
	for _, pair := range n.KeyPairs {
		if err := s.UpsertNodeKeyPair(ctx, n.NodeID, pair); err != nil {
			return err
		}
	}
	return nil
}
func testNodeSession(ctx context.Context, s *Stores, node string) context.Context {
	snapshot, err := s.nodes.ReadNodeProfile(ctx, node)
	if err != nil || !snapshot.Exists {
		return ctx
	}
	return withNodeRegistration(ctx, snapshot.Registration)
}
func testVersion(n uint64) metadata.Version { return metadatastore.Version(n) }

func testReadNodeKeyPair(s *Stores, ctx context.Context, node, fingerprint string) (metadata.NodeKeyPair, metadata.Version, bool, error) {
	for cursor := metadata.Cursor(""); ; {
		page, err := s.keyPairs.ListKeyPairs(ctx, node, cursor, 128)
		if err != nil {
			return metadata.NodeKeyPair{}, metadata.Version{}, false, err
		}
		for _, p := range page.Items {
			if p.Value.APISecretFingerprint == fingerprint {
				return p.Value, p.Version, true, nil
			}
		}
		if page.Next == "" {
			return metadata.NodeKeyPair{}, metadata.Version{}, false, nil
		}
		cursor = page.Next
	}
}

func seedTestNode(t *testing.T, s *Stores, id string) {
	t.Helper()
	if _, found, err := s.GetNodeProfile(context.Background(), id); err != nil {
		t.Fatal(err)
	} else if found {
		return
	}
	if err := s.PutNode(context.Background(), &NodeRecord{NodeID: id}); err != nil {
		t.Fatal(err)
	}
}

// testReportContext models the registration that always precedes a node frame.
// A test of a stale stream must capture withNodeRegistration itself, then call
// the production entry point so this helper cannot refresh its authority.
func testReportContext(ctx context.Context, s *Stores, node string) context.Context {
	snapshot, err := s.nodes.ReadNodeProfile(ctx, node)
	if err != nil {
		panic(err)
	}
	if !snapshot.Exists {
		if err := s.PutNode(ctx, &NodeRecord{NodeID: node}); err != nil {
			panic(err)
		}
		snapshot, err = s.nodes.ReadNodeProfile(ctx, node)
		if err != nil {
			panic(err)
		}
	}
	return withNodeRegistration(ctx, snapshot.Registration)
}
func testApplyRoute(r *Registry, ctx context.Context, node string, event *routesync.RouteEntry) {
	r.applyRoute(testReportContext(ctx, r.stores, node), node, event)
}
func testApplyDelete(r *Registry, ctx context.Context, node, sid, group, key, fingerprint string) bool {
	return r.applyDelete(testReportContext(ctx, r.stores, node), node, sid, group, key, fingerprint) == nil
}
func testApplyDeleteBySID(r *Registry, ctx context.Context, node, sid string) {
	r.applyDeleteBySID(testReportContext(ctx, r.stores, node), node, sid)
}
func testApplyNodeFullSnapshot(r *Registry, ctx context.Context, node string, expected []clusterstate.NodeSandboxRef, seen map[string]struct{}) {
	r.applyNodeFullSnapshot(testReportContext(ctx, r.stores, node), node, expected, seen)
}
func testApplyBuildUpsert(r *Registry, ctx context.Context, node string, event *routesync.BuildEvent) error {
	return r.applyBuildUpsert(testReportContext(ctx, r.stores, node), node, event)
}
func testApplyBuildDelete(r *Registry, ctx context.Context, node, build, template string) error {
	return r.applyBuildDelete(testReportContext(ctx, r.stores, node), node, build, template)
}
func testApplyNodeBuildFullSnapshot(r *Registry, ctx context.Context, node string, expected []clusterstate.NodeBuildRef, seen map[string]struct{}) error {
	return r.applyNodeBuildFullSnapshot(testReportContext(ctx, r.stores, node), node, expected, seen)
}
