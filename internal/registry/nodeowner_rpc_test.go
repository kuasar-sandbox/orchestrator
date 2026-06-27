package registry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

type rpcNodeOwner struct {
	ops      []string
	admit    bool
	released []string
}

func (o *rpcNodeOwner) PutManifestKey(ctx context.Context, nodeID, fingerprint, manifestKey string, expiresUnix int64) error {
	o.ops = append(o.ops, "put:"+nodeID+":"+fingerprint+":"+manifestKey)
	return nil
}

func (o *rpcNodeOwner) DropManifestKey(ctx context.Context, nodeID, fingerprint string) error {
	o.ops = append(o.ops, "drop:"+nodeID+":"+fingerprint)
	return nil
}

func (o *rpcNodeOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	o.ops = append(o.ops, "admit:"+nodeID+":"+buildID)
	return o.admit
}

func (o *rpcNodeOwner) ReleaseBuild(ctx context.Context, buildID string) {
	o.released = append(o.released, buildID)
}

func (o *rpcNodeOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return &NodeRecord{NodeID: nodeID, DataEndpoint: "10.0.0.1:8443"}, true, nil
}

func (o *rpcNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid string) error {
	o.ops = append(o.ops, "delete:"+nodeID+":"+sid)
	return nil
}

func TestHTTPNodeOwner(t *testing.T) {
	ctx := context.Background()
	owner := &rpcNodeOwner{admit: true}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeNodeOwner(w, r, owner)
	}))
	defer srv.Close()

	client := NewHTTPNodeOwner(srv.URL, srv.Client())
	if err := client.PutManifestKey(ctx, "n1", "fp", "mk", 123); err != nil {
		t.Fatal(err)
	}
	if err := client.DropManifestKey(ctx, "n1", "fp"); err != nil {
		t.Fatal(err)
	}
	if !client.AdmitBuild(ctx, "n1", "b1", &routesync.BuildResources{CPU: 1000}) {
		t.Fatal("admit build returned false")
	}
	client.ReleaseBuild(ctx, "b1")
	node, found, err := client.Runtime(ctx, "n1")
	if err != nil || !found || node.DataEndpoint == "" {
		t.Fatalf("runtime node=%+v found=%v err=%v", node, found, err)
	}
	if err := client.DeleteSandbox(ctx, "n1", "sb1"); err != nil {
		t.Fatal(err)
	}
	wantOps := []string{"put:n1:fp:mk", "drop:n1:fp", "admit:n1:b1", "delete:n1:sb1"}
	if len(owner.ops) != len(wantOps) {
		t.Fatalf("ops=%v want %v", owner.ops, wantOps)
	}
	for i := range wantOps {
		if owner.ops[i] != wantOps[i] {
			t.Fatalf("ops=%v want %v", owner.ops, wantOps)
		}
	}
	if len(owner.released) != 1 || owner.released[0] != "b1" {
		t.Fatalf("released=%v", owner.released)
	}
}
