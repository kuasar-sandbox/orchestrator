package orch

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func TestClusterKeyLeaseDurableExactAckAndDrop(t *testing.T) {
	o := testOrch(t)
	node := keyLeaseTestNode(o)
	ctx := context.Background()
	authKey := strings.Repeat("a", 64)
	manifestKey := strings.Repeat("b", 64)
	authFP, _ := store.AuthKeyHash(authKey)
	manifestFP, _ := store.ManifestKeyHash(manifestKey)
	lease := routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: "/g", KeyRevision: 1,
		AuthKey:     routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: authKey, Fingerprint: authFP},
		ManifestKey: routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: manifestKey, Fingerprint: manifestFP},
		ExpiresUnix: time.Now().Add(time.Hour).Unix(),
	}
	ack := node.HandleCommand(ctx, &routesync.Command{
		CmdID: "put-1", Kind: routesync.CmdKeyPut, NodeEpoch: 7, SessionSeq: 1,
		AuthKeyFingerprint: authFP, ManifestKeyFingerprint: manifestFP, KeyLease: &lease,
	})
	if ack.Status != routesync.AckAccepted || ack.KeyLeaseRef == nil ||
		ack.KeyLeaseRef.AuthKeyFingerprint != authFP || ack.KeyLeaseRef.ManifestKeyFingerprint != manifestFP {
		t.Fatalf("key_put ack = %+v", ack)
	}
	if _, found, err := o.st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestFP); err != nil || !found {
		t.Fatalf("durable key lease found=%t err=%v", found, err)
	}

	drop := &routesync.Command{
		CmdID: "drop-1", Kind: routesync.CmdKeyDrop, NodeEpoch: 7, SessionSeq: 1,
		AuthKeyFingerprint: authFP, ManifestKeyFingerprint: manifestFP, KeyLeaseRef: ack.KeyLeaseRef,
	}
	dropAck := node.HandleCommand(ctx, drop)
	if dropAck.Status != routesync.AckAccepted || dropAck.KeyLeaseRef == nil || *dropAck.KeyLeaseRef != *ack.KeyLeaseRef {
		t.Fatalf("key_drop ack = %+v", dropAck)
	}
	// Exact drop is idempotent and still echoes the requested identity.
	if replay := node.HandleCommand(ctx, drop); replay.Status != routesync.AckAccepted || replay.KeyLeaseRef == nil || *replay.KeyLeaseRef != *ack.KeyLeaseRef {
		t.Fatalf("replayed key_drop ack = %+v", replay)
	}
}

func TestClusterKeyLeaseRejectsExpiredInput(t *testing.T) {
	o := testOrch(t)
	node := keyLeaseTestNode(o)
	authKey := strings.Repeat("a", 64)
	manifestKey := strings.Repeat("b", 64)
	authFP, _ := store.AuthKeyHash(authKey)
	manifestFP, _ := store.ManifestKeyHash(manifestKey)
	lease := routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: "/g", KeyRevision: 1,
		AuthKey:     routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: authKey, Fingerprint: authFP},
		ManifestKey: routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: manifestKey, Fingerprint: manifestFP},
		ExpiresUnix: time.Now().Unix() - 1,
	}
	ack := node.HandleCommand(context.Background(), &routesync.Command{
		CmdID: "expired", Kind: routesync.CmdKeyPut, NodeEpoch: 7, SessionSeq: 1,
		AuthKeyFingerprint: authFP, ManifestKeyFingerprint: manifestFP, KeyLease: &lease,
	})
	if ack.Status != routesync.AckRejected {
		t.Fatalf("expired key lease ack = %+v", ack)
	}
}

func TestClusterKeyLeaseRejectsMalformedRegistryAuth(t *testing.T) {
	o := testOrch(t)
	node := keyLeaseTestNode(o)
	authKey := strings.Repeat("a", 64)
	manifestKey := strings.Repeat("b", 64)
	authFP, _ := store.AuthKeyHash(authKey)
	manifestFP, _ := store.ManifestKeyHash(manifestKey)
	lease := routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: "/g", KeyRevision: 1,
		AuthKey:     routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: authKey, Fingerprint: authFP},
		ManifestKey: routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: manifestKey, Fingerprint: manifestFP},
		RegistryAuth: routesync.NodeRegistryAuthV1{
			Type: routesync.KeyMaterialInline, Value: `{"not_auths":true}`,
		},
		ExpiresUnix: time.Now().Add(time.Hour).Unix(),
	}
	ack := node.HandleCommand(context.Background(), &routesync.Command{
		CmdID: "bad-auth", Kind: routesync.CmdKeyPut, NodeEpoch: 7, SessionSeq: 1, KeyLease: &lease,
	})
	if ack.Status != routesync.AckRejected {
		t.Fatalf("malformed registry auth ack = %+v", ack)
	}
	if _, found, err := o.st.KeyLeaseByFingerprints(context.Background(), "/g", authFP, manifestFP); err != nil || found {
		t.Fatalf("malformed registry auth lease found=%t err=%v", found, err)
	}
}

func TestClusterKeyLeaseRejectsStaleRegistryAuthAfterCurrentAck(t *testing.T) {
	o := testOrch(t)
	node := keyLeaseTestNode(o)
	ctx := context.Background()
	authKey := strings.Repeat("a", 64)
	manifestKey := strings.Repeat("b", 64)
	authFP, _ := store.AuthKeyHash(authKey)
	manifestFP, _ := store.ManifestKeyHash(manifestKey)
	old := routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: "/g", KeyRevision: 1,
		AuthKey:     routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: authKey, Fingerprint: authFP},
		ManifestKey: routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: manifestKey, Fingerprint: manifestFP},
		RegistryAuth: routesync.NodeRegistryAuthV1{
			Type: routesync.KeyMaterialInline, Value: `{"auths":{"*":{"token":"old"}}}`,
		},
		ExpiresUnix: time.Now().Add(2 * time.Hour).Unix(),
	}
	current := old
	current.KeyRevision = 2
	current.RegistryAuth.Value = `{"auths":{"*":{"token":"current"}}}`
	current.ExpiresUnix = time.Now().Add(time.Hour).Unix()
	currentAck := node.HandleCommand(ctx, &routesync.Command{
		CmdID: "current", Kind: routesync.CmdKeyPut, NodeEpoch: 7, SessionSeq: 1,
		AuthKeyFingerprint: authFP, ManifestKeyFingerprint: manifestFP, KeyLease: &current,
	})
	if currentAck.Status != routesync.AckAccepted || currentAck.KeyLeaseRef == nil {
		t.Fatalf("current key_put ack = %+v", currentAck)
	}
	if staleAck := node.HandleCommand(ctx, &routesync.Command{
		CmdID: "stale", Kind: routesync.CmdKeyPut, NodeEpoch: 7, SessionSeq: 1,
		AuthKeyFingerprint: authFP, ManifestKeyFingerprint: manifestFP, KeyLease: &old,
	}); staleAck.Status != routesync.AckRejected {
		t.Fatalf("stale key_put ack = %+v", staleAck)
	}

	stored, found, err := o.st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestFP)
	if err != nil || !found || stored.KeyRevision != current.KeyRevision || stored.RegistryAuth != current.RegistryAuth.Value {
		t.Fatalf("stored current lease = %+v, found=%t err=%v", stored, found, err)
	}
	oldRef, err := old.Ref()
	if err != nil {
		t.Fatal(err)
	}
	staleDrop := node.HandleCommand(ctx, &routesync.Command{
		CmdID: "stale-drop", Kind: routesync.CmdKeyDrop, NodeEpoch: 7, SessionSeq: 1,
		AuthKeyFingerprint: authFP, ManifestKeyFingerprint: manifestFP, KeyLeaseRef: &oldRef,
	})
	if staleDrop.Status != routesync.AckAccepted {
		t.Fatalf("stale key_drop ack = %+v", staleDrop)
	}
	if _, found, err := o.st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestFP); err != nil || !found {
		t.Fatalf("stale key_drop removed current lease: found=%t err=%v", found, err)
	}
}

func TestResolveAllowedAcceptsIdenticalCredentialsAcrossGroups(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	authKey := strings.Repeat("a", 64)
	manifestKey := strings.Repeat("b", 64)
	registryAuth := `{"auths":{"*":{"token":"shared"}}}`
	for _, group := range []string{"/tenant/a", "/tenant/b"} {
		if _, err := o.st.PutKeyLease(ctx, store.KeyLease{
			Group: group, AuthKey: authKey, ManifestKey: manifestKey,
			RegistryAuth: registryAuth, ExpiresUnix: time.Now().Add(time.Hour).Unix(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	rawAuth, _ := hex.DecodeString(authKey)
	apiKey, err := apikey.Mint(rawAuth)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := o.resolveAllowed(ctx, apiKey)
	if err != nil || resolved.AuthKey != authKey || resolved.ManifestKey != manifestKey ||
		resolved.RegistryAuth != registryAuth {
		t.Fatalf("resolved shared credentials = %+v, %v", resolved, err)
	}
}

func TestResolveAllowedRejectsDifferentCredentialsAcrossGroups(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	authKey := strings.Repeat("a", 64)
	for index, manifestKey := range []string{strings.Repeat("b", 64), strings.Repeat("c", 64)} {
		if _, err := o.st.PutKeyLease(ctx, store.KeyLease{
			Group: "/tenant/" + string(rune('a'+index)), AuthKey: authKey, ManifestKey: manifestKey,
			ExpiresUnix: time.Now().Add(time.Hour).Unix(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	rawAuth, _ := hex.DecodeString(authKey)
	apiKey, err := apikey.Mint(rawAuth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.resolveAllowed(ctx, apiKey); err == nil {
		t.Fatal("different node credentials were treated as one lease")
	}
}

func keyLeaseTestNode(o *Orchestrator) *FinalClusterNode {
	ready := make(chan struct{})
	close(ready)
	return &FinalClusterNode{
		core: o, store: o.st, executionReady: ready,
		session: &ClusterSession{current: nodeexec.LocalSessionIdentity{
			NodeID: "node-1", NodeEpoch: 7, SessionSeq: 1, DataEndpoint: "node-1:8443",
		}},
	}
}
