package registry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type rpcNodeOwner struct {
	ops          []string
	admit        bool
	released     []string
	ackErr       error
	runtimeErr   error
	connectedErr error
}

func (o *rpcNodeOwner) Connected(ctx context.Context, nodeID string) error {
	o.ops = append(o.ops, "connected:"+nodeID)
	return o.connectedErr
}

func (o *rpcNodeOwner) PutKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	o.ops = append(o.ops, "put:"+nodeID+":"+pair.APISecretFingerprint)
	return nil
}

func (o *rpcNodeOwner) DropKeyPair(ctx context.Context, nodeID, apiSecretFingerprint string) error {
	o.ops = append(o.ops, "drop:"+nodeID+":"+apiSecretFingerprint)
	return nil
}

func (o *rpcNodeOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	o.ops = append(o.ops, "admit:"+nodeID+":"+buildID)
	return o.admit
}

func (o *rpcNodeOwner) ReleaseBuild(ctx context.Context, nodeID, buildID string) {
	o.released = append(o.released, nodeID+":"+buildID)
}

func (o *rpcNodeOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	if o.runtimeErr != nil {
		return nil, false, o.runtimeErr
	}
	return &NodeRecord{NodeID: nodeID, APIEndpoint: "10.0.0.1:7443", DataEndpoint: "10.0.0.1:8443"}, true, nil
}

func (o *rpcNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid, apiSecretFingerprint string) error {
	o.ops = append(o.ops, "delete:"+nodeID+":"+sid+":"+apiSecretFingerprint)
	return nil
}

func (o *rpcNodeOwner) SendCommand(ctx context.Context, nodeID string, cmd *routesync.Command) error {
	o.ops = append(o.ops, "send:"+nodeID+":"+cmd.Kind)
	return nil
}

func (o *rpcNodeOwner) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	o.ops = append(o.ops, "wait:"+nodeID+":"+cmd.Kind)
	if o.ackErr != nil {
		return nil, o.ackErr
	}
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}, nil
}

func TestHTTPNodeOwner(t *testing.T) {
	ctx := context.Background()
	owner := &rpcNodeOwner{admit: true}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeNodeOwner(w, r, owner)
	}))
	defer srv.Close()

	client := NewHTTPNodeOwner(srv.URL, srv.Client())
	apiFP := strings.Repeat("a", 64)
	pair := clusterstate.NodeKeyPair{
		APISecretFingerprint: apiFP, APISecretType: clusterstate.SecretRef, APISecretRef: "vault://tenant/api",
		ManifestKeyFingerprint: strings.Repeat("b", 64), ManifestKeyType: clusterstate.SecretRef, ManifestKeyRef: "vault://tenant/manifest",
		ExpiresUnix: 123,
	}
	if err := client.Connected(ctx, "n1"); err != nil {
		t.Fatal(err)
	}
	if err := client.PutKeyPair(ctx, "n1", pair); err != nil {
		t.Fatal(err)
	}
	if err := client.DropKeyPair(ctx, "n1", apiFP); err != nil {
		t.Fatal(err)
	}
	node, found, err := client.Runtime(ctx, "n1")
	if err != nil || !found || node.APIEndpoint != "10.0.0.1:7443" || node.DataEndpoint != "10.0.0.1:8443" {
		t.Fatalf("runtime node=%+v found=%v err=%v", node, found, err)
	}
	if err := client.DeleteSandbox(ctx, "n1", "sb1", strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if err := client.SendCommand(ctx, "n1", &routesync.Command{CmdID: "c1", Kind: routesync.CmdDelete, SID: "sb2"}); err != nil {
		t.Fatal(err)
	}
	ack, err := client.SendCommandAndWait(ctx, "n1", &routesync.Command{CmdID: "c2", Kind: routesync.CmdCreate}, time.Second)
	if err != nil || ack == nil || ack.Status != routesync.AckAccepted {
		t.Fatalf("send wait ack=%+v err=%v", ack, err)
	}
	wantOps := []string{
		"connected:n1", "put:n1:" + apiFP, "drop:n1:" + apiFP,
		"delete:n1:sb1:" + strings.Repeat("a", 64), "send:n1:delete", "wait:n1:create",
	}
	if len(owner.ops) != len(wantOps) {
		t.Fatalf("ops=%v want %v", owner.ops, wantOps)
	}
	for i := range wantOps {
		if owner.ops[i] != wantOps[i] {
			t.Fatalf("ops=%v want %v", owner.ops, wantOps)
		}
	}
}

func TestHTTPNodeOwnerPreservesCommandWaitDeadline(t *testing.T) {
	ctx := context.Background()
	owner := &rpcNodeOwner{ackErr: context.DeadlineExceeded}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeNodeOwner(w, r, owner)
	}))
	defer srv.Close()

	client := NewHTTPNodeOwner(srv.URL, srv.Client())
	_, err := client.SendCommandAndWait(ctx, "n1", &routesync.Command{CmdID: "c1", Kind: routesync.CmdBuildRegister}, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SendCommandAndWait err=%v, want context deadline", err)
	}
}

func TestHTTPNodeOwnerPreservesNodeGone(t *testing.T) {
	owner := &rpcNodeOwner{runtimeErr: ErrNodeGone, connectedErr: ErrNodeGone}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeNodeOwner(w, r, owner)
	}))
	defer srv.Close()

	client := NewHTTPNodeOwner(srv.URL, srv.Client())
	if err := client.Connected(context.Background(), "n1"); !errors.Is(err, ErrNodeGone) {
		t.Fatalf("Connected err=%v, want ErrNodeGone", err)
	}
	if _, _, err := client.Runtime(context.Background(), "n1"); !errors.Is(err, ErrNodeGone) {
		t.Fatalf("Runtime err=%v, want ErrNodeGone", err)
	}
}
