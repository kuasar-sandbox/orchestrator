package orch

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestFinalClusterNodeFencesEveryCommandByCurrentSessionTuple(t *testing.T) {
	session := &ClusterSession{}
	session.current = nodeexec.LocalSessionIdentity{
		NodeID: "node-1", NodeEpoch: 7, SessionSeq: 12, DataEndpoint: "10.0.0.1:8443",
	}
	ready := make(chan struct{})
	close(ready)
	node := &FinalClusterNode{session: session, executionReady: ready}

	for _, kind := range []string{
		routesync.CmdFinalizeWorkflow,
		routesync.CmdRebindExecution,
		routesync.CmdAckRecoveryEvent,
		routesync.CmdCollectRecovery,
	} {
		command := &routesync.Command{CmdID: kind, Kind: kind, NodeEpoch: 7, SessionSeq: 11}
		ack := node.HandleCommand(context.Background(), command)
		if ack.Status != routesync.AckRejected || ack.Outcome != routesync.DispatchSessionMoved {
			t.Fatalf("stale %s command = %+v", kind, ack)
		}
	}
}

func TestFinalClusterNodeRejectsBuildRecoveryEventAck(t *testing.T) {
	session := &ClusterSession{}
	session.current = nodeexec.LocalSessionIdentity{
		NodeID: "node-1", NodeEpoch: 7, SessionSeq: 12, DataEndpoint: "10.0.0.1:8443",
	}
	ready := make(chan struct{})
	close(ready)
	node := &FinalClusterNode{session: session, executionReady: ready}
	command := &routesync.Command{
		CmdID: "build-event-ack", Kind: routesync.CmdAckRecoveryEvent,
		BuildID: "build-1", NodeEpoch: 7, SessionSeq: 12,
		EventAck: &routesync.EventAck{ObjectKind: "build", ObjectID: "build-1", EventSeq: 1},
	}
	ack := node.HandleCommand(context.Background(), command)
	if ack.Status != routesync.AckRejected || ack.Outcome != routesync.DispatchConflict {
		t.Fatalf("Build recovery event ACK = %+v", ack)
	}
}
