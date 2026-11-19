package nodectl

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

// Exercise the actual server dispatch and resource framing without a kernel
// socket. The UDS/lease/process suites separately cover authentication/restart.
func TestServerCoordinatedCacheWireAndCompletion(t *testing.T) {
	s := makeState(1<<30, 0)
	c, _ := newReclaimTestCoordinator(s)
	r := admitReclaimTest(t, c, "", "wire", 32<<20)
	a := NewAdmissionController(AdmissionPolicy{Rate: 100, Burst: 100})
	a.SetWiring(s, nil, func(*PendingAdmit) (*Message, error) { return nil, nil })
	server := &Server{State: s, Admission: a, Allocator: allocatorCacheTest(), Logf: t.Logf}
	server.cacheOnce.Do(func() { server.cache = c })
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close(); serverConn.Close() })
	s.mu.Lock()
	s.bySID[r.spec.SandboxID].Conn = serverConn
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { defer close(done); server.serveConn(context.Background(), serverConn, r.spec.PeerPID) }()
	roundTrip := func(req *Message) *Message {
		t.Helper()
		req.Token = r.token
		if err := clientConn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := WriteMessage(clientConn, req); err != nil {
			t.Fatal(err)
		}
		resp, err := ReadMessage(clientConn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Type == TypeError {
			t.Fatal(resp.Msg)
		}
		return resp
	}
	first := roundTrip(&Message{Type: resource.TypeCacheReclaimUpdate, CacheReclaim: &resource.CacheReclaimState{RunID: r.run, Current: 32 << 20, Intent: reclaimIntent(1, 1, 4<<20)}})
	if first.CacheReclaimTask == nil || first.CacheReservation != 32<<20 {
		t.Fatalf("wire permit: %+v", first)
	}
	heartbeat := roundTrip(&Message{Type: TypeHeartbeat})
	if heartbeat.CacheReclaimTask == nil || *heartbeat.CacheReclaimTask != *first.CacheReclaimTask {
		t.Fatal("heartbeat lost immutable task")
	}
	completion := &resource.CacheReclaimState{RunID: r.run, Current: 28 << 20, LastSeq: first.CacheReclaimTask.Seq,
		Done: &resource.CacheReclaimDone{Task: *first.CacheReclaimTask, Current: 28 << 20}}
	acked := roundTrip(&Message{Type: resource.TypeCacheReclaimUpdate, CacheReclaim: completion})
	if acked.CacheReclaimAck != first.CacheReclaimTask.Seq || acked.CacheReservation != 28<<20 || acked.CacheReclaimTask != nil {
		t.Fatalf("completion not settled: %+v", acked)
	}
	grant := roundTrip(&Message{Type: TypeRequestCacheBudget, CacheCurrent: 28 << 20, CacheRequested: 4 << 20, CacheMaximum: r.spec.CacheMax})
	if grant.CacheGranted != 4<<20 || grant.CacheReservation != 32<<20 {
		t.Fatal("confirmed bytes were not reusable")
	}
	replay := roundTrip(&Message{Type: resource.TypeCacheReclaimUpdate, CacheReclaim: completion})
	if replay.CacheReservation != 32<<20 || s.reservedMemory != 128<<20 {
		t.Fatal("completion replay altered later allocation or Guest memory")
	}
	status := roundTrip(&Message{Type: TypeAdminStatus})
	if status.CacheAllocated != 32<<20 || status.Allocated.MemoryBytes != 128<<20 || status.CacheReclaimStatus == nil {
		t.Fatalf("independent status missing: %+v", status)
	}
	clientConn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server did not finish")
	}
}

func TestServerCacheCoordinationRequiresRecoverableLease(t *testing.T) {
	s := makeState(1<<30, 0)
	a := NewAdmissionController(AdmissionPolicy{})
	server := &Server{State: s, Admission: a, Logf: t.Logf}
	runID := "0123456789abcdef0123456789abcdef"
	_, err := server.buildAdmitOK(nil, 123, &Message{SandboxID: "not-recoverable", ClientFeatures: []string{resource.FeatureCacheBudgetV1, resource.FeatureCacheReclaimV1, resource.CacheReclaimRunFeature(runID)}, CacheReclaim: &resource.CacheReclaimState{RunID: runID}}, nil)
	if err == nil || s.ResourceSnapshot().ReservationCount != 0 {
		t.Fatal("coordination accepted without lifecycle recovery")
	}
}
