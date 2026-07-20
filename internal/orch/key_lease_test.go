package orch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func TestClusterKeyLeaseDurableExactAckAndDrop(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	authKey := strings.Repeat("a", 64)
	manifestKey := strings.Repeat("b", 64)
	authFP, _ := store.AuthKeyHash(authKey)
	manifestFP, _ := store.ManifestKeyHash(manifestKey)
	lease := routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: "/g",
		AuthKey:     routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: authKey, Fingerprint: authFP},
		ManifestKey: routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: manifestKey, Fingerprint: manifestFP},
		ExpiresUnix: time.Now().Add(time.Hour).Unix(),
	}
	ack := o.HandleCommand(ctx, &routesync.Command{CmdID: "put-1", Kind: routesync.CmdKeyPut, KeyLease: &lease})
	if ack.Status != routesync.AckAccepted || ack.KeyLeaseRef == nil ||
		ack.KeyLeaseRef.AuthKeyFingerprint != authFP || ack.KeyLeaseRef.ManifestKeyFingerprint != manifestFP {
		t.Fatalf("key_put ack = %+v", ack)
	}
	if _, found, err := o.st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestFP); err != nil || !found {
		t.Fatalf("durable key lease found=%t err=%v", found, err)
	}

	drop := &routesync.Command{CmdID: "drop-1", Kind: routesync.CmdKeyDrop, KeyLeaseRef: ack.KeyLeaseRef}
	dropAck := o.HandleCommand(ctx, drop)
	if dropAck.Status != routesync.AckAccepted || dropAck.KeyLeaseRef == nil || *dropAck.KeyLeaseRef != *ack.KeyLeaseRef {
		t.Fatalf("key_drop ack = %+v", dropAck)
	}
	// Exact drop is idempotent and still echoes the requested identity.
	if replay := o.HandleCommand(ctx, drop); replay.Status != routesync.AckAccepted || replay.KeyLeaseRef == nil || *replay.KeyLeaseRef != *ack.KeyLeaseRef {
		t.Fatalf("replayed key_drop ack = %+v", replay)
	}
}

func TestClusterKeyLeaseRejectsExpiredInput(t *testing.T) {
	o := testOrch(t)
	authKey := strings.Repeat("a", 64)
	manifestKey := strings.Repeat("b", 64)
	authFP, _ := store.AuthKeyHash(authKey)
	manifestFP, _ := store.ManifestKeyHash(manifestKey)
	lease := routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: "/g",
		AuthKey:     routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: authKey, Fingerprint: authFP},
		ManifestKey: routesync.NodeKeyMaterialV1{Type: routesync.KeyMaterialInline, Value: manifestKey, Fingerprint: manifestFP},
		ExpiresUnix: time.Now().Unix() - 1,
	}
	ack := o.HandleCommand(context.Background(), &routesync.Command{CmdID: "expired", Kind: routesync.CmdKeyPut, KeyLease: &lease})
	if ack.Status != routesync.AckRejected {
		t.Fatalf("expired key lease ack = %+v", ack)
	}
}
