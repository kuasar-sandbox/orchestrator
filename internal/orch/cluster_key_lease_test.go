package orch

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestDormantExecutorRejectsUninstalledFinalKeyLease(t *testing.T) {
	o := testOrch(t)
	lease := &routesync.NodeKeyLeaseV1{Version: routesync.NodeKeyLeaseVersionV1, Group: "/g", ExpiresUnix: 1}
	ack := o.HandleCommand(context.Background(), &routesync.Command{
		CmdID: "put-final", Kind: routesync.CmdKeyPut, KeyLease: lease,
	})
	if ack.Status != routesync.AckRejected {
		t.Fatalf("uninstalled final key lease ACK = %+v", ack)
	}
	ref := &routesync.NodeKeyLeaseRefV1{Version: routesync.NodeKeyLeaseVersionV1, Group: "/g"}
	ack = o.HandleCommand(context.Background(), &routesync.Command{
		CmdID: "drop-final", Kind: routesync.CmdKeyDrop, KeyLeaseRef: ref,
	})
	if ack.Status != routesync.AckRejected {
		t.Fatalf("ignored final key drop ACK = %+v", ack)
	}
}
