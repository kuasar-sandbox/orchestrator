package session

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

type holderDispatchRPCStub struct {
	holderID string
	command  DispatchCommand
	reply    DispatchReply
	err      error
	calls    int
}

type directoryFenceAuthority struct {
	fenced map[string]uint64
}

func (a directoryFenceAuthority) AllowDirectoryEntry(DirectoryEntry) bool { return true }

func (a directoryFenceAuthority) NodeEpochPermanentlyFenced(_ context.Context, nodeID string, nodeEpoch uint64) (bool, error) {
	return a.fenced[nodeID] >= nodeEpoch, nil
}

func (s *holderDispatchRPCStub) AdmitAndDispatchAt(_ context.Context, holderID string, command DispatchCommand) (DispatchReply, error) {
	s.calls++
	s.holderID = holderID
	s.command = command
	return s.reply, s.err
}

func TestDirectoryDispatcherRoutesCurrentTuple(t *testing.T) {
	directory := newTestDirectory()
	applyDirectoryUp(t, directory, "node-1", "registry-b", 7, 12)
	rpc := &holderDispatchRPCStub{reply: DispatchReply{Outcome: cluster.DispatchAcceptedAdmitted}}
	dispatcher, err := NewDirectoryDispatcher(directory, rpc)
	if err != nil {
		t.Fatal(err)
	}
	command := DispatchCommand{NodeID: "node-1", NodeEpoch: 7}
	reply, err := dispatcher.AdmitAndDispatch(context.Background(), command)
	if err != nil || reply.Outcome != cluster.DispatchAcceptedAdmitted {
		t.Fatalf("dispatch = %+v, %v", reply, err)
	}
	if rpc.calls != 1 || rpc.holderID != "registry-b" || rpc.command.SessionSeq != 12 {
		t.Fatalf("RPC = holder %q, command %+v, calls %d", rpc.holderID, rpc.command, rpc.calls)
	}
}

func TestDirectoryDispatcherClassifiesOnlySystemProvenNoSideEffect(t *testing.T) {
	directory := NewDirectory(directoryFenceAuthority{fenced: map[string]uint64{"node-1": 7, "node-2": 7}})
	applyDirectoryUp(t, directory, "node-1", "registry-b", 8, 1)
	rpc := &holderDispatchRPCStub{}
	dispatcher, err := NewDirectoryDispatcher(directory, rpc)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := dispatcher.AdmitAndDispatch(context.Background(), DispatchCommand{NodeID: "node-1", NodeEpoch: 7})
	if err != nil || reply.Outcome != cluster.DispatchDefinitiveReject || rpc.calls != 0 {
		t.Fatalf("newer epoch dispatch = %+v, %v, calls=%d", reply, err, rpc.calls)
	}
	reply, err = dispatcher.AdmitAndDispatch(context.Background(), DispatchCommand{NodeID: "missing", NodeEpoch: 7})
	if err != nil || reply.Outcome != cluster.DispatchSessionMoved || rpc.calls != 0 {
		t.Fatalf("missing session dispatch = %+v, %v, calls=%d", reply, err, rpc.calls)
	}
	applyDirectoryUp(t, directory, "node-2", "registry-b", 8, 1)
	directory.Apply(DirectoryDelta{Entry: DirectoryEntry{
		NodeID: "node-2", EnrollmentID: "enrollment-node-2",
		Tuple: Tuple{NodeEpoch: 8, SessionSeq: 1}, HolderMemberID: "registry-b",
	}, Up: false})
	reply, err = dispatcher.AdmitAndDispatch(context.Background(), DispatchCommand{NodeID: "node-2", NodeEpoch: 7})
	if err != nil || reply.Outcome != cluster.DispatchDefinitiveReject || rpc.calls != 0 {
		t.Fatalf("down newer epoch dispatch = %+v, %v, calls=%d", reply, err, rpc.calls)
	}
}

func TestDirectoryDispatcherDoesNotTreatNewerHintAsFenceProof(t *testing.T) {
	directory := newTestDirectory()
	applyDirectoryUp(t, directory, "node-1", "registry-b", 8, 1)
	dispatcher, err := NewDirectoryDispatcher(directory, &holderDispatchRPCStub{})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := dispatcher.AdmitAndDispatch(context.Background(), DispatchCommand{NodeID: "node-1", NodeEpoch: 7})
	if err != nil || reply.Outcome != cluster.DispatchSessionMoved {
		t.Fatalf("unproven newer hint dispatch = %+v, %v", reply, err)
	}
}

func TestDirectoryDispatcherPreservesAmbiguousTransportFailure(t *testing.T) {
	directory := newTestDirectory()
	applyDirectoryUp(t, directory, "node-1", "registry-b", 7, 12)
	rpc := &holderDispatchRPCStub{err: errors.New("ACK lost")}
	dispatcher, err := NewDirectoryDispatcher(directory, rpc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.AdmitAndDispatch(context.Background(), DispatchCommand{NodeID: "node-1", NodeEpoch: 7}); err == nil {
		t.Fatal("ambiguous transport failure was converted into a definitive result")
	}
	rpc.err = ErrSessionUnavailable
	reply, err := dispatcher.AdmitAndDispatch(context.Background(), DispatchCommand{NodeID: "node-1", NodeEpoch: 7})
	if err != nil || reply.Outcome != cluster.DispatchSessionMoved {
		t.Fatalf("pre-send stale session = %+v, %v", reply, err)
	}
}
