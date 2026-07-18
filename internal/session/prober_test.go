package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/placement"
)

type probeBatchCall struct {
	holderID string
	requests []placement.PlacementProbeRequest
}

type probeRPCStub struct {
	mu      sync.Mutex
	calls   []probeBatchCall
	started chan string
	release <-chan struct{}
	err     error
}

func (s *probeRPCStub) ProbePlacementBatch(_ context.Context, holderID string, _ ServeIdentity, requests []placement.PlacementProbeRequest) ([]placement.PlacementProbeResponse, error) {
	s.mu.Lock()
	s.calls = append(s.calls, probeBatchCall{holderID: holderID, requests: append([]placement.PlacementProbeRequest(nil), requests...)})
	s.mu.Unlock()
	if s.started != nil {
		s.started <- holderID
	}
	if s.release != nil {
		<-s.release
	}
	if s.err != nil {
		return nil, s.err
	}
	responses := make([]placement.PlacementProbeResponse, len(requests))
	for index, request := range requests {
		responses[index] = placement.PlacementProbeResponse{
			Class: placement.ProbeImmediate, NodeID: request.NodeID,
			NodeEpoch: request.ExpectedNodeEpoch, SessionSeq: request.ExpectedSessionSeq,
			LoadModelVersion: request.LoadModelVersion,
		}
	}
	return responses, nil
}

func (s *probeRPCStub) snapshotCalls() []probeBatchCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]probeBatchCall(nil), s.calls...)
}

func TestDirectoryProberBatchesCandidatesOnSameHolder(t *testing.T) {
	directory := NewDirectory()
	applyDirectoryUp(t, directory, "n1", "r1", 7, 10)
	applyDirectoryUp(t, directory, "n2", "r1", 8, 20)
	rpc := &probeRPCStub{}
	prober, err := NewDirectoryProber(directory, rpc)
	if err != nil {
		t.Fatal(err)
	}
	results := prober.ProbePair(context.Background(), testServeIdentity(), []placement.PlacementProbeRequest{
		probeRequest("n1"), probeRequest("n2"),
	})
	calls := rpc.snapshotCalls()
	if len(calls) != 1 || calls[0].holderID != "r1" || len(calls[0].requests) != 2 {
		t.Fatalf("Probe calls = %+v", calls)
	}
	if results[0].Response.NodeEpoch != 7 || results[1].Response.NodeEpoch != 8 {
		t.Fatalf("Probe results = %+v", results)
	}
}

func TestDirectoryProberCallsDifferentHoldersInParallel(t *testing.T) {
	directory := NewDirectory()
	applyDirectoryUp(t, directory, "n1", "r1", 7, 10)
	applyDirectoryUp(t, directory, "n2", "r2", 8, 20)
	started := make(chan string, 2)
	release := make(chan struct{})
	rpc := &probeRPCStub{started: started, release: release}
	prober, err := NewDirectoryProber(directory, rpc)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan []ProbeResult, 1)
	go func() {
		done <- prober.ProbePair(context.Background(), testServeIdentity(), []placement.PlacementProbeRequest{
			probeRequest("n1"), probeRequest("n2"),
		})
	}()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case holderID := <-started:
			seen[holderID] = true
		case <-time.After(time.Second):
			t.Fatal("both Holder probes did not start before either was released")
		}
	}
	close(release)
	results := <-done
	if len(results) != 2 || results[0].Response.Class != placement.ProbeImmediate || results[1].Response.Class != placement.ProbeImmediate {
		t.Fatalf("Probe results = %+v", results)
	}
}

func TestDirectoryProberFailsClosedForMissingOrBadHolder(t *testing.T) {
	directory := NewDirectory()
	applyDirectoryUp(t, directory, "n1", "r1", 7, 10)
	rpc := &probeRPCStub{err: errors.New("permit expired")}
	prober, err := NewDirectoryProber(directory, rpc)
	if err != nil {
		t.Fatal(err)
	}
	results := prober.ProbePair(context.Background(), testServeIdentity(), []placement.PlacementProbeRequest{
		probeRequest("n1"), probeRequest("missing"),
	})
	if results[0].Response.Class != placement.ProbeStale || results[1].Response.Class != placement.ProbeReject {
		t.Fatalf("Probe results = %+v", results)
	}
}

func applyDirectoryUp(t *testing.T, directory *Directory, nodeID, holderID string, epoch, seq uint64) {
	t.Helper()
	if !directory.Apply(DirectoryDelta{Up: true, Entry: DirectoryEntry{
		NodeID: nodeID, HolderMemberID: holderID, Tuple: Tuple{NodeEpoch: epoch, SessionSeq: seq},
	}}) {
		t.Fatal("directory update was ignored")
	}
}

func probeRequest(nodeID string) placement.PlacementProbeRequest {
	return placement.PlacementProbeRequest{
		Kind: placement.ObjectSandbox, NodeID: nodeID, LoadModelVersion: placement.LoadModelVersion,
		Sandbox: &placement.SandboxDemand{SlotUnits: 1},
	}
}

func testServeIdentity() ServeIdentity {
	return testServeIdentityAt(1)
}

func testServeIdentityAt(epoch uint64) ServeIdentity {
	return ServeIdentity{
		ClusterID: "c1", StorageGeneration: "g1", SystemEpoch: epoch,
		ManifestDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}
