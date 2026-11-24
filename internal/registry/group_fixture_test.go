package registry

import (
	"bytes"
	"context"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// Publish the same bounded snapshot as a registered Node. This fixture uses
// the production repositories; it provides neither a Placer nor an auth bypass.
func publishTestGroups(t *testing.T, r *Registry, node string, names ...string) {
	t.Helper()
	groups := make([]nodegroup.Support, 0, len(names))
	for _, name := range names {
		groups = append(groups, nodegroup.Support{Group: name, Version: 1,
			Entries:        []nodegroup.Entry{{AuthRef: testAPIFingerprint, AuthMaterial: nodegroup.AuthMaterial{APISecret: testAPISecret}}},
			DefaultContext: &nodegroup.DefaultContext{TemplateID: testTemplateRef},
		})
	}
	publishTestSupport(t, r, node, groups...)
}

func publishTestSupport(t *testing.T, r *Registry, node string, groups ...nodegroup.Support) {
	t.Helper()
	seedTestNode(t, r.stores, node)
	profile, err := r.stores.nodes.ReadNodeProfile(t.Context(), node)
	if err != nil {
		t.Fatal(err)
	}
	id := "fixture-" + newID()
	if err := r.stores.groups.BeginGroupSupport(t.Context(), profile.Registration, id, 1, len(groups)); err != nil {
		t.Fatal(err)
	}
	if err := r.stores.groups.WriteGroupSupportBatch(t.Context(), profile.Registration, id, 0, groups); err != nil {
		t.Fatal(err)
	}
	if _, err := r.stores.groups.FinishGroupSupport(t.Context(), profile.Registration, id); err != nil {
		t.Fatal(err)
	}
}

type testGroupReads struct {
	metadata.GroupSupportRepository
	calls  int
	before func()
}

func (g *testGroupReads) ListGroupSupport(ctx context.Context, name string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.GroupSupportSelection], error) {
	g.calls++
	if g.before != nil {
		g.before()
	}
	return g.GroupSupportRepository.ListGroupSupport(ctx, name, cursor, limit)
}
func countTestGroupReads(r *Registry) *testGroupReads {
	g := &testGroupReads{GroupSupportRepository: r.stores.groups}
	r.stores.groups = g
	return g
}

func waitTestGroupSupport(t *testing.T, r *Registry, node, group string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		selection, err := r.stores.groups.ReadGroupSupport(t.Context(), group, node)
		if err != nil {
			t.Fatal(err)
		}
		if selection.Support.Exists {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Node did not complete Group publication")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func addTestBuildRef(t *testing.T, s *Stores, ctx context.Context, node string, ref clusterstate.NodeBuildRef) error {
	t.Helper()
	seedTestNode(t, s, node)
	return s.AddNodeBuildRef(ctx, node, ref)
}

func addTestSandboxRef(t *testing.T, s *Stores, ctx context.Context, node string, ref clusterstate.NodeSandboxRef) error {
	t.Helper()
	seedTestNode(t, s, node)
	return s.AddNodeSandboxRef(ctx, node, ref)
}

type testCommandWriter struct {
	bytes.Buffer
	err error
}

type testGroupCommit struct {
	metadata.RouteRepository
	before func(metadata.SandboxRecord)
}

func (s *testGroupCommit) CommitGroupIntent(ctx context.Context, expected metadata.Version, record metadata.SandboxRecord, ref metadata.NodeSandboxRef, admission metadata.GroupAdmission) (metadata.Version, error) {
	if s.before != nil {
		s.before(record)
	}
	return s.RouteRepository.CommitGroupIntent(ctx, expected, record, ref, admission)
}
func (w *testCommandWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	return w.Buffer.Write(p)
}

// Exercise the real framed Node channel with the registration used by the
// command. The handler can inject ACK loss or rejection without bypassing the
// connection and original-registration checks in sendAndWait.
func attachTestNode(t *testing.T, r *Registry, handler *fakeConn) {
	t.Helper()
	seedTestNode(t, r.stores, handler.nodeID)
	profile, err := r.stores.nodes.ReadNodeProfile(t.Context(), handler.nodeID)
	if err != nil {
		t.Fatal(err)
	}
	w := &testCommandWriter{err: handler.err}
	channel := &nodeChannel{nodeID: handler.nodeID, registration: profile.Registration, w: w}
	channel.flush = func() {
		msg, err := routesync.ReadMsg(&w.Buffer)
		if err != nil {
			t.Error(err)
			return
		}
		if err := handler.send(msg.Cmd); err != nil {
			t.Error(err)
		}
	}
	r.addNode(channel)
}
