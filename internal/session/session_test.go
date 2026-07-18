package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
)

type testEndpoint struct {
	fenced   int
	commands []DispatchCommand
	reply    DispatchReply
	err      error
}

func (e *testEndpoint) FenceStaleSession() { e.fenced++ }

func (e *testEndpoint) AdmitAndDispatch(_ context.Context, command DispatchCommand) (DispatchReply, error) {
	e.commands = append(e.commands, command)
	if e.err != nil {
		return DispatchReply{}, e.err
	}
	if e.reply.Outcome == "" {
		return DispatchReply{Outcome: cluster.DispatchAcceptedAdmitted}, nil
	}
	return e.reply, nil
}

type testGate bool

func (g testGate) AllowSessionWork(ServeIdentity) bool { return bool(g) }

type testPublisher struct{ deltas []DirectoryDelta }

func (p *testPublisher) PublishSessionDelta(delta DirectoryDelta) {
	p.deltas = append(p.deltas, delta)
}

type testEnrollmentAuthority struct {
	mu      sync.Mutex
	active  map[string]NodeEnrollment
	retired map[string]IdentityRetirement
}

type serializedEnrollmentAuthority struct {
	mu                   sync.Mutex
	enrollment           NodeEnrollment
	registrationEntered  chan struct{}
	continueRegistration chan struct{}
	retired              bool
}

func (a *serializedEnrollmentAuthority) RunSessionRegistration(
	ctx context.Context,
	enrollment NodeEnrollment,
	install func() error,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.retired || enrollment != a.enrollment {
		return errors.New("identity is not enrolled")
	}
	close(a.registrationEntered)
	select {
	case <-a.continueRegistration:
	case <-ctx.Done():
		return ctx.Err()
	}
	return install()
}

func (a *serializedEnrollmentAuthority) RunIdentityRetirement(
	_ context.Context,
	retirement IdentityRetirement,
	remove func() (bool, error),
) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if retirement.NodeID != a.enrollment.NodeID || retirement.EnrollmentID != a.enrollment.EnrollmentID ||
		retirement.LastNodeEpoch < a.enrollment.NodeEpoch {
		return false, errors.New("identity retirement is not committed")
	}
	a.retired = true
	return remove()
}

func newTestEnrollmentAuthority(registrations ...Registration) *testEnrollmentAuthority {
	a := &testEnrollmentAuthority{
		active: make(map[string]NodeEnrollment), retired: make(map[string]IdentityRetirement),
	}
	for _, registration := range registrations {
		a.enroll(registration)
	}
	return a
}

func (a *testEnrollmentAuthority) enroll(registration Registration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active[registration.NodeID] = NodeEnrollment{
		NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
		NodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
	}
}

func (a *testEnrollmentAuthority) retire(retirement IdentityRetirement) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.retired[retirement.NodeID] = retirement
	delete(a.active, retirement.NodeID)
}

func (a *testEnrollmentAuthority) RunSessionRegistration(_ context.Context, enrollment NodeEnrollment, install func() error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	current, found := a.active[enrollment.NodeID]
	if !found {
		return errors.New("identity is not enrolled")
	}
	if enrollment.EnrollmentID != current.EnrollmentID {
		return ErrEnrollmentChanged
	}
	if enrollment.NodeEpoch != current.NodeEpoch {
		return ErrStaleSession
	}
	if enrollment.DataEndpoint != current.DataEndpoint {
		return ErrEndpointChanged
	}
	return install()
}

func (a *testEnrollmentAuthority) RunIdentityRetirement(
	_ context.Context,
	retirement IdentityRetirement,
	remove func() (bool, error),
) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	current, found := a.retired[retirement.NodeID]
	if !found || current != retirement {
		return false, errors.New("identity retirement is not committed")
	}
	return remove()
}

func TestHolderAcceptsOnlyIncreasingTupleAndFencesOldStream(t *testing.T) {
	publisher := &testPublisher{}
	first := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	authority := newTestEnrollmentAuthority(first)
	holder, err := NewHolder("registry-a", 2, nil, testGate(true), publisher, authority)
	if err != nil {
		t.Fatal(err)
	}
	oldEndpoint := &testEndpoint{}
	oldLease, err := holder.Register(context.Background(), first, oldEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer oldLease.Close()

	newEndpoint := &testEndpoint{}
	newLease, err := holder.Register(context.Background(), testRegistration("node-1", 7, 11, "10.0.0.1:8443"), newEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer newLease.Close()
	if oldEndpoint.fenced != 1 {
		t.Fatalf("old endpoint fenced %d times", oldEndpoint.fenced)
	}
	if _, err := holder.Register(context.Background(), testRegistration("node-1", 7, 11, "10.0.0.1:8443"), &testEndpoint{}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("equal tuple error = %v", err)
	}
	if _, err := holder.Register(context.Background(), testRegistration("node-1", 6, 99, "10.0.0.1:8443"), &testEndpoint{}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("lower epoch error = %v", err)
	}
	if len(publisher.deltas) != 2 || !publisher.deltas[0].Up || !publisher.deltas[1].Up {
		t.Fatalf("published deltas = %+v", publisher.deltas)
	}
}

func TestHolderAcceptsBuildOnlyRegistration(t *testing.T) {
	registration := testRegistration("build-node", 1, 1, "10.0.0.2:8443")
	registration.SandboxSlots = 0
	authority := newTestEnrollmentAuthority(registration)
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := holder.Register(context.Background(), registration, &testEndpoint{})
	if err != nil {
		t.Fatalf("build-only registration: %v", err)
	}
	lease.Close()

	registration.BuildSlots = 0
	if _, err := holder.Register(context.Background(), registration, &testEndpoint{}); err == nil {
		t.Fatal("registration without sandbox or build capacity was accepted")
	}
}

func TestHolderRejectsEndpointChangeWithinNodeEpoch(t *testing.T) {
	first := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	authority := newTestEnrollmentAuthority(first)
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Register(context.Background(), first, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Register(context.Background(), testRegistration("node-1", 7, 11, "10.0.0.2:8443"), &testEndpoint{}); !errors.Is(err, ErrEndpointChanged) {
		t.Fatalf("changed endpoint error = %v", err)
	}
	newEpoch := testRegistration("node-1", 8, 1, "10.0.0.2:8443")
	authority.enroll(newEpoch)
	if _, err := holder.Register(context.Background(), newEpoch, &testEndpoint{}); err != nil {
		t.Fatalf("new epoch endpoint change: %v", err)
	}
}

func TestHolderUsesSharedEnrollmentFenceAcrossHolderChanges(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	authority := newTestEnrollmentAuthority(registration)
	first, err := NewHolder("registry-a", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHolder("registry-b", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Register(context.Background(), registration, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	changed := testRegistration("node-1", 7, 11, "10.0.0.2:8443")
	if _, err := second.Register(context.Background(), changed, &testEndpoint{}); !errors.Is(err, ErrEndpointChanged) {
		t.Fatalf("cross-Holder endpoint change error = %v", err)
	}
}

func TestHolderDropsHighWatermarkOnlyAfterCommittedIdentityRetirement(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	authority := newTestEnrollmentAuthority(registration)
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	retirement := IdentityRetirement{
		NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID, LastNodeEpoch: registration.NodeEpoch,
	}
	if _, err := holder.RetireIdentity(context.Background(), retirement); err == nil {
		t.Fatal("uncommitted retirement removed Holder fencing state")
	}
	authority.retire(retirement)
	removed, err := holder.RetireIdentity(context.Background(), retirement)
	if err != nil || !removed || holder.Active() != 0 || holder.TrackedIdentities() != 0 || endpoint.fenced != 1 {
		t.Fatalf("retirement removed=%v active=%d tracked=%d fenced=%d err=%v",
			removed, holder.Active(), holder.TrackedIdentities(), endpoint.fenced, err)
	}
	if _, err := holder.Register(context.Background(), registration, &testEndpoint{}); err == nil {
		t.Fatal("retired identity registered again")
	}
}

func TestHolderSerializesRegistrationInstallationWithRetirement(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	authority := &serializedEnrollmentAuthority{
		enrollment: NodeEnrollment{
			NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
			NodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
		},
		registrationEntered:  make(chan struct{}),
		continueRegistration: make(chan struct{}),
	}
	publisher := &testPublisher{}
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), publisher, authority)
	if err != nil {
		t.Fatal(err)
	}
	registerResult := make(chan error, 1)
	go func() {
		_, err := holder.Register(context.Background(), registration, &testEndpoint{})
		registerResult <- err
	}()
	<-authority.registrationEntered

	retireResult := make(chan error, 1)
	go func() {
		removed, err := holder.RetireIdentity(context.Background(), IdentityRetirement{
			NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
			LastNodeEpoch: registration.NodeEpoch,
		})
		if err == nil && !removed {
			err = errors.New("retirement did not remove installed registration")
		}
		retireResult <- err
	}()
	select {
	case err := <-retireResult:
		t.Fatalf("retirement bypassed in-flight installation: %v", err)
	default:
	}
	close(authority.continueRegistration)
	if err := <-registerResult; err != nil {
		t.Fatalf("registration: %v", err)
	}
	if err := <-retireResult; err != nil {
		t.Fatalf("retirement: %v", err)
	}
	if holder.Active() != 0 || holder.TrackedIdentities() != 0 {
		t.Fatalf("retired identity remained installed: active=%d tracked=%d", holder.Active(), holder.TrackedIdentities())
	}
	if len(publisher.deltas) != 2 || !publisher.deltas[0].Up || publisher.deltas[1].Up {
		t.Fatalf("registration/retirement publication order = %+v", publisher.deltas)
	}
	if _, err := holder.Register(context.Background(), registration, &testEndpoint{}); err == nil {
		t.Fatal("registration installed after retirement completed")
	}
}

func TestHolderProbeUsesLocalObservationAndPermit(t *testing.T) {
	now := time.Unix(10, 0)
	clock := func() time.Time { return now }
	authority := newTestEnrollmentAuthority(testRegistration("node-1", 7, 10, "10.0.0.1:8443"))
	holder, err := NewHolder("registry-a", 1, clock, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	if _, err := holder.Register(context.Background(), registration, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(registration)
	if err := holder.UpdateSnapshot(registration.Tuple, snapshot); err != nil {
		t.Fatal(err)
	}
	now = now.Add(900 * time.Millisecond)
	response, err := holder.Probe(context.Background(), ProbeCall{
		ServeIdentity: ServeIdentity{ClusterID: "c1", StorageGeneration: "g1", SystemEpoch: 3},
		Request: placement.PlacementProbeRequest{
			Kind: placement.ObjectSandbox, NodeID: registration.NodeID,
			ExpectedNodeEpoch: registration.NodeEpoch, ExpectedSessionSeq: registration.SessionSeq,
			LoadModelVersion: placement.LoadModelVersion,
			Sandbox:          &placement.SandboxDemand{SlotUnits: 1},
		},
	})
	if err != nil || response.Class != placement.ProbeImmediate || response.SampleAge != 900*time.Millisecond {
		t.Fatalf("probe = %+v, %v", response, err)
	}
	now = now.Add(101 * time.Millisecond)
	response, err = holder.Probe(context.Background(), ProbeCall{
		ServeIdentity: ServeIdentity{ClusterID: "c1", StorageGeneration: "g1", SystemEpoch: 3},
		Request: placement.PlacementProbeRequest{
			Kind: placement.ObjectSandbox, NodeID: registration.NodeID,
			ExpectedNodeEpoch: registration.NodeEpoch, ExpectedSessionSeq: registration.SessionSeq,
			LoadModelVersion: placement.LoadModelVersion,
			Sandbox:          &placement.SandboxDemand{SlotUnits: 1},
		},
	})
	if err != nil || response.Class != placement.ProbeStale {
		t.Fatalf("stale probe = %+v, %v", response, err)
	}

	denied, err := NewHolder("registry-b", 1, clock, testGate(false), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := denied.Register(context.Background(), registration, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	if err := denied.UpdateSnapshot(registration.Tuple, snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := denied.Probe(context.Background(), ProbeCall{ServeIdentity: testServeIdentity()}); !errors.Is(err, ErrPermitUnavailable) {
		t.Fatalf("expired permit error = %v", err)
	}
}

func TestHolderLimitAndStaleSnapshot(t *testing.T) {
	r1 := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	r2 := testRegistration("node-2", 1, 1, "10.0.0.2:8443")
	authority := newTestEnrollmentAuthority(r1, r2)
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Register(context.Background(), r1, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Register(context.Background(), r2, &testEndpoint{}); !errors.Is(err, ErrHolderLimit) {
		t.Fatalf("limit error = %v", err)
	}
	snapshot := testSnapshot(r1)
	if err := holder.UpdateSnapshot(r1.Tuple, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := holder.UpdateSnapshot(r1.Tuple, snapshot); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("duplicate sample error = %v", err)
	}
}

func TestHolderDispatchUsesCurrentTupleAndCommittedTarget(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	authority := newTestEnrollmentAuthority(registration)
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	command := testDispatchCommand(t, registration)
	reply, err := holder.AdmitAndDispatch(context.Background(), command)
	if err != nil || reply.Outcome != cluster.DispatchAcceptedAdmitted {
		t.Fatalf("dispatch = %+v, %v", reply, err)
	}
	if len(endpoint.commands) != 1 || endpoint.commands[0].SessionSeq != registration.SessionSeq {
		t.Fatalf("endpoint commands = %+v", endpoint.commands)
	}

	newRegistration := testRegistration("node-1", 8, 1, "10.0.0.2:8443")
	authority.enroll(newRegistration)
	if _, err := holder.Register(context.Background(), newRegistration, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.AdmitAndDispatch(context.Background(), command); !errors.Is(err, ErrSessionUnavailable) {
		t.Fatalf("stale target error = %v", err)
	}
	if len(endpoint.commands) != 1 {
		t.Fatal("stale dispatch reached the node stream")
	}
}

func TestHolderDispatchFailsClosedWithoutPermit(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	authority := newTestEnrollmentAuthority(registration)
	holder, err := NewHolder("registry-a", 1, nil, testGate(false), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.AdmitAndDispatch(context.Background(), testDispatchCommand(t, registration)); !errors.Is(err, ErrPermitUnavailable) {
		t.Fatalf("expired permit error = %v", err)
	}
	if len(endpoint.commands) != 0 {
		t.Fatal("dispatch reached endpoint with an expired Permit")
	}
}

func TestDirectoryHighestTupleDownAndConflictAreFailClosed(t *testing.T) {
	directory := NewDirectory()
	e1 := DirectoryEntry{NodeID: "node-1", Tuple: Tuple{NodeEpoch: 7, SessionSeq: 10}, HolderMemberID: "registry-a"}
	if !directory.Apply(DirectoryDelta{Entry: e1, Up: true}) {
		t.Fatal("initial up was ignored")
	}
	if !directory.Apply(DirectoryDelta{Entry: e1, Up: false}) {
		t.Fatal("equal down was ignored")
	}
	if directory.Apply(DirectoryDelta{Entry: e1, Up: true}) {
		t.Fatal("equal up resurrected a down tuple")
	}
	if _, ok := directory.Lookup(e1.NodeID); ok {
		t.Fatal("down entry remained available")
	}

	e2 := e1
	e2.SessionSeq++
	if !directory.Apply(DirectoryDelta{Entry: e2, Up: true}) {
		t.Fatal("higher tuple did not replace down entry")
	}
	conflict := e2
	conflict.HolderMemberID = "registry-b"
	if !directory.Apply(DirectoryDelta{Entry: conflict, Up: true}) {
		t.Fatal("equal tuple split-holder conflict was ignored")
	}
	if _, ok := directory.Lookup(e1.NodeID); ok {
		t.Fatal("conflicting tuple remained available")
	}

	e3 := conflict
	e3.SessionSeq++
	if !directory.Apply(DirectoryDelta{Entry: e3, Up: true}) {
		t.Fatal("higher tuple did not resolve conflict")
	}
	if got, ok := directory.Lookup(e1.NodeID); !ok || got != e3 {
		t.Fatalf("lookup = %+v, %v", got, ok)
	}
}

func TestDirectoryDigestAndFullMergeAreDeterministic(t *testing.T) {
	a := DirectoryRecord{Entry: DirectoryEntry{NodeID: "a", Tuple: Tuple{NodeEpoch: 1, SessionSeq: 2}, HolderMemberID: "r1"}, Available: true}
	b := DirectoryRecord{Entry: DirectoryEntry{NodeID: "b", Tuple: Tuple{NodeEpoch: 3, SessionSeq: 4}, HolderMemberID: "r2"}, Conflict: true}
	if DirectoryDigest([]DirectoryRecord{a, b}) != DirectoryDigest([]DirectoryRecord{b, a}) {
		t.Fatal("directory digest depends on input order")
	}
	directory := NewDirectory()
	if changed := directory.MergeFull([]DirectoryRecord{b, a}); changed != 3 {
		t.Fatalf("merge changed %d records", changed)
	}
	if _, ok := directory.Lookup("a"); !ok {
		t.Fatal("available record was not merged")
	}
	if _, ok := directory.Lookup("b"); ok {
		t.Fatal("conflicting record became available")
	}
}

func TestDirectoryConflictCanonicalizesAcrossArrivalOrder(t *testing.T) {
	a := DirectoryEntry{NodeID: "node-1", Tuple: Tuple{NodeEpoch: 3, SessionSeq: 7}, HolderMemberID: "registry-a"}
	b := a
	b.HolderMemberID = "registry-b"
	left := NewDirectory()
	right := NewDirectory()
	left.Apply(DirectoryDelta{Entry: a, Up: true})
	left.Apply(DirectoryDelta{Entry: b, Up: true})
	right.Apply(DirectoryDelta{Entry: b, Up: true})
	right.Apply(DirectoryDelta{Entry: a, Up: true})

	leftSnapshot := left.Snapshot()
	rightSnapshot := right.Snapshot()
	if len(leftSnapshot) != 1 || len(rightSnapshot) != 1 ||
		leftSnapshot[0].Entry.HolderMemberID != "registry-a" ||
		rightSnapshot[0].Entry.HolderMemberID != "registry-a" ||
		!leftSnapshot[0].Conflict || !rightSnapshot[0].Conflict {
		t.Fatalf("canonical conflicts: left=%+v right=%+v", leftSnapshot, rightSnapshot)
	}
	if DirectoryDigest(leftSnapshot) != DirectoryDigest(rightSnapshot) {
		t.Fatal("split-holder conflicts retained arrival-order-dependent digests")
	}
}

func TestDirectoryFullMergeSkipsInvalidConflictRecords(t *testing.T) {
	directory := NewDirectory()
	healthy := DirectoryEntry{
		NodeID: "node-1", Tuple: Tuple{NodeEpoch: 3, SessionSeq: 7}, HolderMemberID: "registry-a",
	}
	directory.Apply(DirectoryDelta{Entry: healthy, Up: true})
	invalid := DirectoryRecord{
		Entry:    DirectoryEntry{NodeID: healthy.NodeID, Tuple: healthy.Tuple},
		Conflict: true,
	}
	zero := DirectoryRecord{Conflict: true}
	if changed := directory.MergeFull([]DirectoryRecord{invalid, zero}); changed != 0 {
		t.Fatalf("invalid merge changed %d records", changed)
	}
	if got, ok := directory.Lookup(healthy.NodeID); !ok || got != healthy {
		t.Fatalf("healthy entry after malformed merge = %+v, %v", got, ok)
	}
	if len(directory.Snapshot()) != 1 {
		t.Fatalf("malformed merge poisoned directory: %+v", directory.Snapshot())
	}
}

func TestWeightedRendezvousIsDeterministicAndSkipsUnavailable(t *testing.T) {
	members := []Member{
		{MemberID: "registry-a", Weight: 1, Available: true},
		{MemberID: "registry-b", Weight: 2, Available: true},
		{MemberID: "registry-c", Weight: 100, Available: false},
	}
	first, err := SelectReconnectTarget("node-1", members)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		got, err := SelectReconnectTarget("node-1", members)
		if err != nil || got != first {
			t.Fatalf("selection %q, %v; want %q", got, err, first)
		}
	}
	if first == "registry-c" {
		t.Fatal("selected unavailable member")
	}
	if _, err := SelectReconnectTarget("node-1", []Member{{MemberID: "registry-c", Weight: 1}}); err == nil {
		t.Fatal("selection succeeded without an available member")
	}
}

func testRegistration(nodeID string, epoch, seq uint64, endpoint string) Registration {
	return Registration{
		NodeID: nodeID, EnrollmentID: "enrollment-" + nodeID,
		Tuple: Tuple{NodeEpoch: epoch, SessionSeq: seq}, DataEndpoint: endpoint,
		RuntimeDigest: "runtime-v1", LoadModelVersion: placement.LoadModelVersion,
		SandboxSlots: 10, BuildSlots: 4, BuildCPU: 4000, BuildMemory: 8 << 30, BuildStorage: 100 << 30,
		FailureDomain: "zone-a",
	}
}

func testSnapshot(registration Registration) placement.PlacementLoadSnapshot {
	return placement.PlacementLoadSnapshot{
		NodeID: registration.NodeID, NodeEpoch: registration.NodeEpoch, SessionSeq: registration.SessionSeq,
		DataEndpoint: registration.DataEndpoint, SampleSeq: 1, LoadModelVersion: registration.LoadModelVersion,
		RuntimeDigest: registration.RuntimeDigest, WaterZone: "green",
		SandboxSlotCapacity: registration.SandboxSlots, SandboxQueueLimit: 10, SandboxRateTokenAvailable: true,
		BuildSlotCapacity: registration.BuildSlots, BuildCPUCapacity: registration.BuildCPU,
		BuildMemoryCapacity: registration.BuildMemory, BuildStorageCapacity: registration.BuildStorage,
		BuildQueueLimit: 10, BuildRateTokenAvailable: true,
	}
}

func testDispatchCommand(t *testing.T, registration Registration) DispatchCommand {
	t.Helper()
	demand, err := placement.NormalizeSandboxDemand(placement.SandboxDemand{SlotUnits: 1})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := cluster.NewDispatchIntent(demand, []byte("dispatch-spec"), "v1")
	if err != nil {
		t.Fatal(err)
	}
	var demandDigest, specDigest [sha256.Size]byte
	decoded, _ := hex.DecodeString(intent.DemandDigest)
	copy(demandDigest[:], decoded)
	decoded, _ = hex.DecodeString(intent.DispatchSpecDigest)
	copy(specDigest[:], decoded)
	opaque, err := cluster.EncodeExecutionBinding(cluster.ExecutionBinding{
		StorageGeneration: "g1", Kind: cluster.ExecutionKindSandbox, ObjectID: "s1",
		Group: "/g", RouteKey: "rk", NodeID: registration.NodeID, NodeEpoch: registration.NodeEpoch,
		DemandDigest: demandDigest, DispatchSpecDigest: specDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := cluster.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	return DispatchCommand{
		ServeIdentity: ServeIdentity{ClusterID: "c1", StorageGeneration: "g1", SystemEpoch: 1},
		Kind:          cluster.ExecutionKindSandbox, Group: "/g", RouteKey: "rk", ObjectID: "s1",
		NodeID: registration.NodeID, NodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
		Intent: intent, Binding: cluster.ExecutionBindingIntent{
			NodeID: registration.NodeID, NodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
			StorageGeneration: "g1", OpaqueBinding: opaque, BindingDigest: digest,
		},
	}
}
