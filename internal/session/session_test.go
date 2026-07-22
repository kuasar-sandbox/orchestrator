package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type testEndpoint struct {
	fenced   int
	commands []DispatchCommand
	reply    DispatchReply
	err      error
	keyAck   *routesync.NodeKeyLeaseRefV1
	wire     []*routesync.Command
	sendErr  error
	sent     bool
	unsent   bool
}

func (e *testEndpoint) SendNodeCommand(_ context.Context, command *routesync.Command) (routesync.CmdAck, bool, error) {
	e.wire = append(e.wire, command)
	if e.sendErr != nil {
		return routesync.CmdAck{}, e.sent, e.sendErr
	}
	if e.unsent {
		return routesync.CmdAck{}, false, nil
	}
	if command == nil {
		return routesync.CmdAck{}, false, errors.New("missing command")
	}
	var ref *routesync.NodeKeyLeaseRefV1
	if e.keyAck != nil {
		copyRef := *e.keyAck
		ref = &copyRef
	} else if command.KeyLease != nil {
		copyRef := keyLeaseRef(*command.KeyLease)
		ref = &copyRef
	} else if command.KeyLeaseRef != nil {
		copyRef := *command.KeyLeaseRef
		ref = &copyRef
	}
	return routesync.CmdAck{CmdID: command.CmdID, Status: routesync.AckAccepted, KeyLeaseRef: ref}, true, nil
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

type switchGate struct {
	mu      sync.RWMutex
	allowed bool
	checks  chan struct{}
}

func (g *switchGate) AllowSessionWork(ServeIdentity) bool {
	g.mu.RLock()
	allowed := g.allowed
	g.mu.RUnlock()
	select {
	case g.checks <- struct{}{}:
	default:
	}
	return allowed
}

func (g *switchGate) set(allowed bool) {
	g.mu.Lock()
	g.allowed = allowed
	g.mu.Unlock()
}

type allowDirectoryEntries struct{}

func (allowDirectoryEntries) AllowDirectoryEntry(DirectoryEntry) bool { return true }

func newTestDirectory() *Directory { return NewDirectory(allowDirectoryEntries{}) }

type directoryEnrollmentAuthority struct {
	mu     sync.RWMutex
	active map[string]string
}

func (a *directoryEnrollmentAuthority) AllowDirectoryEntry(entry DirectoryEntry) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.active[entry.NodeID] == entry.EnrollmentID
}

func (a *directoryEnrollmentAuthority) retire(nodeID string) {
	a.mu.Lock()
	delete(a.active, nodeID)
	a.mu.Unlock()
}

type testPublisher struct{ deltas []DirectoryDelta }

func (p *testPublisher) PublishSessionDelta(delta DirectoryDelta) {
	p.deltas = append(p.deltas, delta)
}

type testEnrollmentAuthority struct {
	mu      sync.Mutex
	active  map[string]Registration
	retired map[string]IdentityRetirement
}

type serializedEnrollmentAuthority struct {
	mu                   sync.Mutex
	registration         Registration
	registrationEntered  chan struct{}
	continueRegistration chan struct{}
	retired              bool
}

func (a *serializedEnrollmentAuthority) RunSessionRegistration(
	ctx context.Context,
	registration Registration,
	install func() error,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.retired || registration.NodeID != a.registration.NodeID ||
		registration.EnrollmentID != a.registration.EnrollmentID ||
		registration.NodeEpoch != a.registration.NodeEpoch ||
		!registrationStableWithinEpoch(registration, a.registration) {
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
	if retirement.NodeID != a.registration.NodeID || retirement.EnrollmentID != a.registration.EnrollmentID ||
		retirement.LastNodeEpoch < a.registration.NodeEpoch {
		return false, errors.New("identity retirement is not committed")
	}
	a.retired = true
	return remove()
}

func newTestEnrollmentAuthority(registrations ...Registration) *testEnrollmentAuthority {
	a := &testEnrollmentAuthority{
		active: make(map[string]Registration), retired: make(map[string]IdentityRetirement),
	}
	for _, registration := range registrations {
		a.enroll(registration)
	}
	return a
}

func (a *testEnrollmentAuthority) enroll(registration Registration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active[registration.NodeID] = registration
}

func (a *testEnrollmentAuthority) retire(retirement IdentityRetirement) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.retired[retirement.NodeID] = retirement
	delete(a.active, retirement.NodeID)
}

func (a *testEnrollmentAuthority) RunSessionRegistration(_ context.Context, registration Registration, install func() error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	current, found := a.active[registration.NodeID]
	if !found {
		return errors.New("identity is not enrolled")
	}
	if registration.EnrollmentID != current.EnrollmentID {
		return ErrEnrollmentChanged
	}
	if registration.NodeEpoch != current.NodeEpoch {
		return ErrStaleSession
	}
	if registration.DataEndpoint != current.DataEndpoint {
		return ErrEndpointChanged
	}
	if !registrationStableWithinEpoch(registration, current) {
		return ErrRegistrationChanged
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

func TestHolderRejectsStableRegistrationChangesWithinNodeEpoch(t *testing.T) {
	first := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	authority := newTestEnrollmentAuthority(first)
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Register(context.Background(), first, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Registration){
		"runtime": func(r *Registration) { r.RuntimeDigest = "runtime-v2" },
		"sandbox": func(r *Registration) { r.SandboxSlots++ },
		"build":   func(r *Registration) { r.BuildMemory++ },
		"domain":  func(r *Registration) { r.FailureDomain = "zone-b" },
	} {
		t.Run(name, func(t *testing.T) {
			next := first
			next.SessionSeq++
			mutate(&next)
			if _, err := holder.Register(context.Background(), next, &testEndpoint{}); !errors.Is(err, ErrRegistrationChanged) {
				t.Fatalf("changed registration error = %v", err)
			}
		})
	}
}

func TestHolderRegistrationDoesNotExposeMutableMaps(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	registration.Labels = map[string]string{"pool": "default"}
	registration.Capabilities = map[string]bool{"sandbox": true}
	authority := newTestEnrollmentAuthority(registration)
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Register(context.Background(), registration, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	registration.Labels["pool"] = "mutated-input"
	registration.Capabilities["sandbox"] = false
	stored, ok := holder.Registration(registration.NodeID)
	if !ok || stored.Labels["pool"] != "default" || !stored.Capabilities["sandbox"] {
		t.Fatalf("stored registration aliased input maps: %+v", stored)
	}
	stored.Labels["pool"] = "mutated-output"
	stored.Capabilities["sandbox"] = false
	again, ok := holder.Registration(registration.NodeID)
	if !ok || again.Labels["pool"] != "default" || !again.Capabilities["sandbox"] {
		t.Fatalf("stored registration was mutated through returned maps: %+v", again)
	}
}

func TestRegistrationRequiresSandboxCapacity(t *testing.T) {
	registration := testRegistration("builder-1", 1, 1, "10.0.0.2:8443")
	registration.SandboxSlots = 0
	if err := registration.Validate(); err == nil {
		t.Fatal("registration without sandbox capacity was accepted")
	}
}

func TestRegistrationRequiresSupportedLoadModel(t *testing.T) {
	registration := testRegistration("node-1", 1, 1, "10.0.0.1:8443")
	registration.LoadModelVersion++
	if err := registration.Validate(); err == nil {
		t.Fatal("registration with an unsupported load model was accepted")
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
	changed = registration
	changed.SessionSeq++
	changed.RuntimeDigest = "runtime-v2"
	if _, err := second.Register(context.Background(), changed, &testEndpoint{}); !errors.Is(err, ErrRegistrationChanged) {
		t.Fatalf("cross-Holder stable registration change error = %v", err)
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
		registration:         registration,
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
		ServeIdentity: testServeIdentityAt(3),
		Request: placement.PlacementProbeRequest{
			Kind: placement.ObjectSandbox, NodeID: registration.NodeID,
			ExpectedNodeEpoch: registration.NodeEpoch, ExpectedSessionSeq: registration.SessionSeq,
			LoadModelVersion: placement.LoadModelVersion, CatalogDigest: registrationCatalogDigest(registration),
			Sandbox: &placement.SandboxDemand{SlotUnits: 1},
		},
	})
	if err != nil || response.Class != placement.ProbeImmediate || response.SampleAge != 900*time.Millisecond {
		t.Fatalf("probe = %+v, %v", response, err)
	}
	now = now.Add(101 * time.Millisecond)
	response, err = holder.Probe(context.Background(), ProbeCall{
		ServeIdentity: testServeIdentityAt(3),
		Request: placement.PlacementProbeRequest{
			Kind: placement.ObjectSandbox, NodeID: registration.NodeID,
			ExpectedNodeEpoch: registration.NodeEpoch, ExpectedSessionSeq: registration.SessionSeq,
			LoadModelVersion: placement.LoadModelVersion, CatalogDigest: registrationCatalogDigest(registration),
			Sandbox: &placement.SandboxDemand{SlotUnits: 1},
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
	if _, err := holder.Probe(context.Background(), ProbeCall{
		ServeIdentity: ServeIdentity{ClusterID: "cluster-1"},
		Request: placement.PlacementProbeRequest{
			Kind: placement.ObjectSandbox, NodeID: registration.NodeID,
			ExpectedNodeEpoch: registration.NodeEpoch, ExpectedSessionSeq: registration.SessionSeq,
			LoadModelVersion: placement.LoadModelVersion, CatalogDigest: registrationCatalogDigest(registration),
			Sandbox: &placement.SandboxDemand{SlotUnits: 1},
		},
	}); err == nil {
		t.Fatal("Probe accepted an incomplete serving identity")
	}
}

func TestHolderProbeRejectsCandidateFromChangedStaticCatalog(t *testing.T) {
	registration := testRegistration("node-1", 8, 1, "10.0.0.1:8443")
	registration.Labels = map[string]string{"pool": "current"}
	registration.Capabilities = map[string]bool{"kvm": true}
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Register(context.Background(), registration, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	if err := holder.UpdateSnapshot(registration.Tuple, testSnapshot(registration)); err != nil {
		t.Fatal(err)
	}
	oldCatalog := registration
	oldCatalog.Labels = map[string]string{"pool": "old"}
	response, err := holder.Probe(context.Background(), ProbeCall{
		ServeIdentity: testServeIdentity(),
		Request: placement.PlacementProbeRequest{
			Kind: placement.ObjectSandbox, NodeID: registration.NodeID,
			ExpectedNodeEpoch: registration.NodeEpoch, ExpectedSessionSeq: registration.SessionSeq,
			LoadModelVersion: placement.LoadModelVersion, CatalogDigest: registrationCatalogDigest(oldCatalog),
			Sandbox: &placement.SandboxDemand{SlotUnits: 1},
		},
	})
	if err != nil || response.Class != placement.ProbeReject {
		t.Fatalf("changed Catalog Probe = %+v, %v", response, err)
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
	if _, err := holder.AdmitAndDispatch(context.Background(), command); !errors.Is(err, ErrKeyLeaseUnavailable) {
		t.Fatalf("dispatch without key lease error = %v", err)
	}
	lease := testKeyLease()
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
	); err != nil || !sent {
		t.Fatalf("key lease install sent=%v err=%v", sent, err)
	}
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

func TestHolderDispatchRequiresTheExactAcknowledgedKeyLease(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	old := testKeyLease()
	oldRef, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, old,
	)
	if err != nil || !sent {
		t.Fatalf("old lease install sent=%v err=%v", sent, err)
	}
	current := old
	current.KeyRevision++
	current.RegistryAuth.Type = routesync.KeyMaterialInline
	current.RegistryAuth.Value = "rotated-registry-auth"
	currentRef, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, current,
	)
	if err != nil || !sent {
		t.Fatalf("current lease install sent=%v err=%v", sent, err)
	}

	command := testDispatchCommand(t, registration)
	command.KeyLeaseRef = oldRef
	if _, err := holder.AdmitAndDispatch(context.Background(), command); !errors.Is(err, ErrKeyLeaseUnavailable) ||
		!errors.Is(err, ErrDispatchNotSent) {
		t.Fatalf("dispatch with superseded exact lease = %v", err)
	}
	command.KeyLeaseRef = currentRef
	if _, err := holder.AdmitAndDispatch(context.Background(), command); err != nil {
		t.Fatalf("dispatch with current exact lease: %v", err)
	}
	if len(endpoint.commands) != 1 {
		t.Fatalf("node received %d dispatches, want one", len(endpoint.commands))
	}
}

type blockingDispatchEndpoint struct {
	dispatchStarted chan struct{}
	releaseDispatch chan struct{}
	fenced          chan struct{}
	fenceOnce       sync.Once
}

func (e *blockingDispatchEndpoint) FenceStaleSession() {
	e.fenceOnce.Do(func() { close(e.fenced) })
}

func (e *blockingDispatchEndpoint) AdmitAndDispatch(context.Context, DispatchCommand) (DispatchReply, error) {
	close(e.dispatchStarted)
	<-e.releaseDispatch
	return DispatchReply{Outcome: cluster.DispatchAcceptedAdmitted}, nil
}

func (e *blockingDispatchEndpoint) SendNodeCommand(_ context.Context, command *routesync.Command) (routesync.CmdAck, bool, error) {
	ref := keyLeaseRef(*command.KeyLease)
	return routesync.CmdAck{Status: routesync.AckAccepted, KeyLeaseRef: &ref}, true, nil
}

func TestHolderSerializesDispatchWithSessionReplacement(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &blockingDispatchEndpoint{
		dispatchStarted: make(chan struct{}), releaseDispatch: make(chan struct{}), fenced: make(chan struct{}),
	}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, testKeyLease(),
	); err != nil || !sent {
		t.Fatalf("install key lease sent=%v err=%v", sent, err)
	}
	dispatchDone := make(chan error, 1)
	go func() {
		_, err := holder.AdmitAndDispatch(context.Background(), testDispatchCommand(t, registration))
		dispatchDone <- err
	}()
	<-endpoint.dispatchStarted

	replacement := registration
	replacement.SessionSeq++
	registerDone := make(chan error, 1)
	go func() {
		_, err := holder.Register(context.Background(), replacement, &testEndpoint{})
		registerDone <- err
	}()
	select {
	case err := <-registerDone:
		t.Fatalf("session replacement completed during dispatch: %v", err)
	case <-endpoint.fenced:
		t.Fatal("old session was fenced during dispatch")
	case <-time.After(20 * time.Millisecond):
	}
	close(endpoint.releaseDispatch)
	if err := <-dispatchDone; err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if err := <-registerDone; err != nil {
		t.Fatalf("replacement: %v", err)
	}
	select {
	case <-endpoint.fenced:
	case <-time.After(time.Second):
		t.Fatal("replaced session was not fenced")
	}
}

func TestBlockedSessionReplacementDoesNotBlockOtherNodeProbe(t *testing.T) {
	first := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	second := testRegistration("node-2", 8, 20, "10.0.0.2:8443")
	holder, err := NewHolder("registry-a", 2, nil, testGate(true), nil, newTestEnrollmentAuthority(first, second))
	if err != nil {
		t.Fatal(err)
	}
	blocking := &blockingDispatchEndpoint{
		dispatchStarted: make(chan struct{}), releaseDispatch: make(chan struct{}), fenced: make(chan struct{}),
	}
	if _, err := holder.Register(context.Background(), first, blocking); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Register(context.Background(), second, &testEndpoint{}); err != nil {
		t.Fatal(err)
	}
	if err := holder.UpdateSnapshot(second.Tuple, testSnapshot(second)); err != nil {
		t.Fatal(err)
	}
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), first.NodeID, first.NodeEpoch, first.DataEndpoint, testKeyLease(),
	); err != nil || !sent {
		t.Fatalf("install key lease sent=%v err=%v", sent, err)
	}
	dispatchDone := make(chan error, 1)
	go func() {
		_, err := holder.AdmitAndDispatch(context.Background(), testDispatchCommand(t, first))
		dispatchDone <- err
	}()
	<-blocking.dispatchStarted

	replacement := first
	replacement.SessionSeq++
	registerDone := make(chan error, 1)
	go func() {
		_, err := holder.Register(context.Background(), replacement, &testEndpoint{})
		registerDone <- err
	}()
	// Give replacement time to reach node-1's command fence. It must not hold
	// the Holder-wide lock while that command remains in flight.
	time.Sleep(20 * time.Millisecond)
	type probeResult struct {
		response placement.PlacementProbeResponse
		err      error
	}
	probeDone := make(chan probeResult, 1)
	go func() {
		response, err := holder.Probe(context.Background(), ProbeCall{
			ServeIdentity: testServeIdentity(),
			Request: placement.PlacementProbeRequest{
				Kind: placement.ObjectSandbox, NodeID: second.NodeID,
				ExpectedNodeEpoch: second.NodeEpoch, ExpectedSessionSeq: second.SessionSeq,
				LoadModelVersion: placement.LoadModelVersion, CatalogDigest: registrationCatalogDigest(second),
				Sandbox: &placement.SandboxDemand{SlotUnits: 1},
			},
		})
		probeDone <- probeResult{response: response, err: err}
	}()
	select {
	case result := <-probeDone:
		if result.err != nil || result.response.Class != placement.ProbeImmediate {
			t.Fatalf("unrelated probe = %+v, %v", result.response, result.err)
		}
	case <-time.After(200 * time.Millisecond):
		close(blocking.releaseDispatch)
		<-dispatchDone
		<-registerDone
		t.Fatal("node-1 replacement blocked node-2 probe")
	}
	close(blocking.releaseDispatch)
	if err := <-dispatchDone; err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if err := <-registerDone; err != nil {
		t.Fatalf("replacement: %v", err)
	}
}

func TestHolderRequiresExactKeyLeaseAcknowledgement(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	authority := newTestEnrollmentAuthority(registration)
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	wrong := keyLeaseRef(testKeyLease())
	wrong.ManifestKeyFingerprint = strings.Repeat("c", 24)
	endpoint := &testEndpoint{keyAck: &wrong}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	lease := testKeyLease()
	ref, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
	)
	if err == nil || !sent || holder.HasKeyLease(registration.NodeID, registration.NodeEpoch, ref) {
		t.Fatalf("mismatched ACK ref=%+v sent=%v err=%v", ref, sent, err)
	}
	endpoint.keyAck = nil
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
	); err != nil || !sent || !holder.HasKeyLease(registration.NodeID, registration.NodeEpoch, ref) {
		t.Fatalf("exact ACK sent=%v err=%v", sent, err)
	}
	if sent, err := holder.DropKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, ref,
	); err != nil || !sent || holder.HasKeyLease(registration.NodeID, registration.NodeEpoch, ref) {
		t.Fatalf("key drop sent=%v err=%v", sent, err)
	}
}

func TestKeyLeaseRevisionFencesRegistryAuthRotation(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}

	old := testKeyLease()
	old.RegistryAuth = routesync.NodeRegistryAuthV1{Type: routesync.KeyMaterialInline, Value: "old-auth"}
	current := old
	current.KeyRevision++
	current.RegistryAuth.Value = "current-auth"
	currentRef, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, current,
	)
	if err != nil || !sent {
		t.Fatalf("current key lease sent=%v err=%v", sent, err)
	}
	old.ExpiresUnix = current.ExpiresUnix + 60
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, old,
	); sent || !errors.Is(err, ErrKeyLeaseSuperseded) || !errors.Is(err, ErrDispatchNotSent) {
		t.Fatalf("stale registry auth sent=%v err=%v", sent, err)
	}
	if !holder.HasKeyLease(registration.NodeID, registration.NodeEpoch, currentRef) || len(endpoint.wire) != 1 {
		t.Fatalf("stale rotation changed current lease: current=%v wire=%d",
			holder.HasKeyLease(registration.NodeID, registration.NodeEpoch, currentRef), len(endpoint.wire))
	}

	conflict := current
	conflict.RegistryAuth.Value = "conflicting-auth"
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, conflict,
	); sent || !errors.Is(err, ErrKeyLeaseConflict) || !errors.Is(err, ErrDispatchNotSent) {
		t.Fatalf("equal-revision registry auth conflict sent=%v err=%v", sent, err)
	}
}

func TestKeyLeaseRevisionFencesWholeGroup(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}

	first := testKeyLease()
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, first,
	); err != nil || !sent {
		t.Fatalf("first group lease sent=%v err=%v", sent, err)
	}
	conflict := first
	conflict.AuthKey = testKeyMaterial(strings.Repeat("d", 64))
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, conflict,
	); sent || !errors.Is(err, ErrKeyLeaseConflict) || !errors.Is(err, ErrDispatchNotSent) {
		t.Fatalf("equal-revision group conflict sent=%v err=%v", sent, err)
	}
	if len(endpoint.wire) != 1 {
		t.Fatalf("conflicting group revision reached node: wire=%d", len(endpoint.wire))
	}
}

func TestAmbiguousKeyDropInvalidatesLocalAcknowledgement(t *testing.T) {
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
	lease := testKeyLease()
	ref, _, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoint.sendErr, endpoint.sent = errors.New("drop ACK lost"), true
	if sent, err := holder.DropKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, ref,
	); err == nil || !sent {
		t.Fatalf("ambiguous drop sent=%v err=%v", sent, err)
	}
	if holder.HasKeyLease(registration.NodeID, registration.NodeEpoch, ref) {
		t.Fatal("ambiguous key drop left the local lease ACK usable")
	}
}

func TestUnsentKeyDropReturnsFailure(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	lease := testKeyLease()
	ref, _, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoint.unsent = true
	if sent, err := holder.DropKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, ref,
	); sent || !errors.Is(err, ErrSessionUnavailable) {
		t.Fatalf("unsent drop sent=%v err=%v", sent, err)
	}
}

type orderedKeyEndpoint struct {
	putStarted chan struct{}
	releasePut chan struct{}
	calls      chan string
}

func (e *orderedKeyEndpoint) FenceStaleSession() {}

func (e *orderedKeyEndpoint) AdmitAndDispatch(context.Context, DispatchCommand) (DispatchReply, error) {
	return DispatchReply{Outcome: cluster.DispatchAcceptedAdmitted}, nil
}

func (e *orderedKeyEndpoint) SendNodeCommand(_ context.Context, command *routesync.Command) (routesync.CmdAck, bool, error) {
	e.calls <- command.Kind
	if command.Kind == routesync.CmdKeyPut {
		close(e.putStarted)
		<-e.releasePut
	}
	var ref routesync.NodeKeyLeaseRefV1
	if command.KeyLease != nil {
		ref = keyLeaseRef(*command.KeyLease)
	} else {
		ref = *command.KeyLeaseRef
	}
	return routesync.CmdAck{Status: routesync.AckAccepted, KeyLeaseRef: &ref}, true, nil
}

func TestKeyLeasePutAndDropAreSerializedPerReference(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &orderedKeyEndpoint{
		putStarted: make(chan struct{}), releasePut: make(chan struct{}), calls: make(chan string, 2),
	}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	lease := testKeyLease()
	ref := keyLeaseRef(lease)
	installDone := make(chan error, 1)
	go func() {
		_, _, err := holder.InstallKeyLease(
			context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
		)
		installDone <- err
	}()
	<-endpoint.putStarted
	if kind := <-endpoint.calls; kind != routesync.CmdKeyPut {
		t.Fatalf("first command = %s", kind)
	}
	dropDone := make(chan error, 1)
	go func() {
		_, err := holder.DropKeyLease(
			context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, ref,
		)
		dropDone <- err
	}()
	select {
	case kind := <-endpoint.calls:
		t.Fatalf("overlapping command reached node before put ACK: %s", kind)
	case <-time.After(20 * time.Millisecond):
	}
	close(endpoint.releasePut)
	if err := <-installDone; !errors.Is(err, ErrKeyLeaseSuperseded) {
		t.Fatalf("superseded install error = %v", err)
	}
	if kind := <-endpoint.calls; kind != routesync.CmdKeyDrop {
		t.Fatalf("second command = %s", kind)
	}
	if err := <-dropDone; err != nil {
		t.Fatalf("drop: %v", err)
	}
	if holder.HasKeyLease(registration.NodeID, registration.NodeEpoch, ref) {
		t.Fatal("serialized drop left the installed lease usable")
	}
}

func TestNewerKeyRefreshSupersedesWaitingDrop(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	lease := testKeyLease()
	ref, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
	)
	if err != nil || !sent {
		t.Fatalf("initial install sent=%v err=%v", sent, err)
	}

	operation := holder.keyLeaseOperation(registration.NodeID, ref.Group)
	operation.Lock()
	dropDone := make(chan struct {
		sent bool
		err  error
	}, 1)
	go func() {
		sent, err := holder.DropKeyLease(
			context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, ref,
		)
		dropDone <- struct {
			sent bool
			err  error
		}{sent: sent, err: err}
	}()
	waitForKeyLeaseSequence(t, holder, registration.NodeID, ref, 2)

	lease.ExpiresUnix++
	refreshDone := make(chan error, 1)
	go func() {
		_, _, err := holder.InstallKeyLease(
			context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
		)
		refreshDone <- err
	}()
	waitForKeyLeaseSequence(t, holder, registration.NodeID, ref, 3)
	operation.Unlock()

	drop := <-dropDone
	if drop.sent || !errors.Is(drop.err, ErrKeyLeaseSuperseded) {
		t.Fatalf("stale drop sent=%v err=%v", drop.sent, drop.err)
	}
	if err := <-refreshDone; err != nil {
		t.Fatalf("newer refresh: %v", err)
	}
	if !holder.HasKeyLease(registration.NodeID, registration.NodeEpoch, ref) {
		t.Fatal("newer refresh was overwritten by stale drop")
	}
	if len(endpoint.wire) != 2 || endpoint.wire[1].Kind != routesync.CmdKeyPut || endpoint.wire[1].KeyLease.ExpiresUnix != lease.ExpiresUnix {
		t.Fatalf("wire commands = %+v", endpoint.wire)
	}
}

func TestKeyLeaseRefreshCannotShortenAcknowledgedExpiry(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	lease := testKeyLease()
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
	); err != nil || !sent {
		t.Fatalf("initial install sent=%v err=%v", sent, err)
	}
	lease.ExpiresUnix--
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
	); sent || !errors.Is(err, ErrKeyLeaseSuperseded) || !errors.Is(err, ErrDispatchNotSent) {
		t.Fatalf("shortening refresh sent=%v err=%v", sent, err)
	}
	if len(endpoint.wire) != 1 {
		t.Fatalf("shortening refresh reached node: %+v", endpoint.wire)
	}
}

func TestKeyLeaseRefreshCannotSupersedeInFlightLongerExpiry(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	holder, err := NewHolder("registry-a", 1, nil, testGate(true), nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	lease := testKeyLease()
	ref := keyLeaseRef(lease)
	operation := holder.keyLeaseOperation(registration.NodeID, ref.Group)
	operation.Lock()

	longer := lease
	longer.ExpiresUnix += 60
	longerDone := make(chan error, 1)
	go func() {
		_, _, err := holder.InstallKeyLease(
			context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch,
			registration.DataEndpoint, longer,
		)
		longerDone <- err
	}()
	waitForKeyLeaseSequence(t, holder, registration.NodeID, ref, 1)

	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch,
		registration.DataEndpoint, lease,
	); sent || !errors.Is(err, ErrKeyLeaseSuperseded) || !errors.Is(err, ErrDispatchNotSent) {
		t.Fatalf("shorter overlapping refresh sent=%v err=%v", sent, err)
	}
	operation.Unlock()
	if err := <-longerDone; err != nil {
		t.Fatalf("longer refresh: %v", err)
	}
	if len(endpoint.wire) != 1 || endpoint.wire[0].KeyLease.ExpiresUnix != longer.ExpiresUnix {
		t.Fatalf("wire commands = %+v", endpoint.wire)
	}
}

func waitForKeyLeaseSequence(t *testing.T, holder *Holder, nodeID string, ref routesync.NodeKeyLeaseRefV1, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		holder.mu.RLock()
		held := holder.active[nodeID]
		if held != nil {
			held.leaseMu.RLock()
			sequence := held.keyLeaseSeq[ref.Group]
			held.leaseMu.RUnlock()
			holder.mu.RUnlock()
			if sequence >= want {
				return
			}
		} else {
			holder.mu.RUnlock()
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("key lease sequence did not reach %d", want)
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
	if _, err := holder.AdmitAndDispatch(context.Background(), testDispatchCommand(t, registration)); !errors.Is(err, ErrPermitUnavailable) || !errors.Is(err, ErrDispatchNotSent) {
		t.Fatalf("expired permit error = %v", err)
	}
	if len(endpoint.commands) != 0 {
		t.Fatal("dispatch reached endpoint with an expired Permit")
	}
}

func TestHolderRechecksPermitAfterWaitingForCommandFence(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	gate := &switchGate{allowed: true, checks: make(chan struct{}, 8)}
	holder, err := NewHolder("registry-a", 1, nil, gate, nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, testKeyLease(),
	); err != nil || !sent {
		t.Fatalf("install key lease sent=%v err=%v", sent, err)
	}
	for len(gate.checks) > 0 {
		<-gate.checks
	}

	holder.mu.RLock()
	held := holder.active[registration.NodeID]
	holder.mu.RUnlock()
	held.commandMu.Lock()
	command := testDispatchCommand(t, registration)
	done := make(chan error, 1)
	go func() {
		_, err := holder.AdmitAndDispatch(context.Background(), command)
		done <- err
	}()
	select {
	case <-gate.checks:
	case <-time.After(time.Second):
		held.commandMu.Unlock()
		t.Fatal("dispatch did not perform its initial Permit check")
	}
	gate.set(false)
	held.commandMu.Unlock()
	if err := <-done; !errors.Is(err, ErrPermitUnavailable) || !errors.Is(err, ErrDispatchNotSent) {
		t.Fatalf("expired Permit error = %v", err)
	}
	if len(endpoint.commands) != 0 {
		t.Fatal("dispatch reached endpoint after Permit expired while waiting")
	}
}

func TestHolderClassifiesCancellationWhileWaitingAsNotSent(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	gate := &switchGate{allowed: true, checks: make(chan struct{}, 8)}
	holder, err := NewHolder("registry-a", 1, nil, gate, nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	if _, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch,
		registration.DataEndpoint, testKeyLease(),
	); err != nil || !sent {
		t.Fatalf("install key lease sent=%v err=%v", sent, err)
	}
	for len(gate.checks) > 0 {
		<-gate.checks
	}
	holder.mu.RLock()
	held := holder.active[registration.NodeID]
	holder.mu.RUnlock()
	held.commandMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := holder.AdmitAndDispatch(ctx, testDispatchCommand(t, registration))
		done <- err
	}()
	select {
	case <-gate.checks:
	case <-time.After(time.Second):
		held.commandMu.Unlock()
		t.Fatal("dispatch did not perform its initial Permit check")
	}
	cancel()
	held.commandMu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, ErrDispatchNotSent) {
		t.Fatalf("canceled dispatch error = %v", err)
	}
	if len(endpoint.commands) != 0 {
		t.Fatal("canceled dispatch reached the endpoint")
	}
}

func TestKeyPutRechecksPermitAfterWaitingForCommandFence(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	gate := &switchGate{allowed: true, checks: make(chan struct{}, 8)}
	holder, err := NewHolder("registry-a", 1, nil, gate, nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	holder.mu.RLock()
	held := holder.active[registration.NodeID]
	holder.mu.RUnlock()
	held.commandMu.Lock()
	done := make(chan struct {
		sent bool
		err  error
	}, 1)
	go func() {
		_, sent, err := holder.InstallKeyLease(
			context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch,
			registration.DataEndpoint, testKeyLease(),
		)
		done <- struct {
			sent bool
			err  error
		}{sent: sent, err: err}
	}()
	select {
	case <-gate.checks:
	case <-time.After(time.Second):
		held.commandMu.Unlock()
		t.Fatal("key put did not perform its initial Permit check")
	}
	gate.set(false)
	held.commandMu.Unlock()
	result := <-done
	if result.sent || !errors.Is(result.err, ErrPermitUnavailable) || !errors.Is(result.err, ErrDispatchNotSent) {
		t.Fatalf("expired Permit key put sent=%v err=%v", result.sent, result.err)
	}
	if len(endpoint.wire) != 0 {
		t.Fatal("key put reached endpoint after Permit expired while waiting")
	}
}

func TestKeyPutRechecksExpiryAfterWaitingForCommandFence(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	now := time.Unix(1_000, 0)
	holder, err := NewHolder(
		"registry-a", 1, func() time.Time { return now }, testGate(true), nil,
		newTestEnrollmentAuthority(registration),
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	holder.mu.RLock()
	held := holder.active[registration.NodeID]
	holder.mu.RUnlock()
	held.commandMu.Lock()
	lease := testKeyLease()
	lease.ExpiresUnix = now.Unix() + 1
	ref := keyLeaseRef(lease)
	done := make(chan struct {
		sent bool
		err  error
	}, 1)
	go func() {
		_, sent, err := holder.InstallKeyLease(
			context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch,
			registration.DataEndpoint, lease,
		)
		done <- struct {
			sent bool
			err  error
		}{sent: sent, err: err}
	}()
	waitForKeyLeaseSequence(t, holder, registration.NodeID, ref, 1)
	now = now.Add(2 * time.Second)
	held.commandMu.Unlock()
	result := <-done
	if result.sent || !errors.Is(result.err, ErrKeyLeaseUnavailable) || !errors.Is(result.err, ErrDispatchNotSent) {
		t.Fatalf("expired key put sent=%v err=%v", result.sent, result.err)
	}
	if len(endpoint.wire) != 0 {
		t.Fatal("expired key put reached the endpoint")
	}
}

func TestKeyDropRechecksPermitAfterWaitingForCommandFence(t *testing.T) {
	registration := testRegistration("node-1", 7, 10, "10.0.0.1:8443")
	gate := &switchGate{allowed: true, checks: make(chan struct{}, 8)}
	holder, err := NewHolder("registry-a", 1, nil, gate, nil, newTestEnrollmentAuthority(registration))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &testEndpoint{}
	if _, err := holder.Register(context.Background(), registration, endpoint); err != nil {
		t.Fatal(err)
	}
	lease := testKeyLease()
	ref, sent, err := holder.InstallKeyLease(
		context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch, registration.DataEndpoint, lease,
	)
	if err != nil || !sent {
		t.Fatalf("initial key put sent=%v err=%v", sent, err)
	}
	for len(gate.checks) > 0 {
		<-gate.checks
	}
	holder.mu.RLock()
	held := holder.active[registration.NodeID]
	holder.mu.RUnlock()
	held.commandMu.Lock()
	done := make(chan struct {
		sent bool
		err  error
	}, 1)
	go func() {
		sent, err := holder.DropKeyLease(
			context.Background(), testServeIdentity(), registration.NodeID, registration.NodeEpoch,
			registration.DataEndpoint, ref,
		)
		done <- struct {
			sent bool
			err  error
		}{sent: sent, err: err}
	}()
	select {
	case <-gate.checks:
	case <-time.After(time.Second):
		held.commandMu.Unlock()
		t.Fatal("key drop did not perform its initial Permit check")
	}
	gate.set(false)
	held.commandMu.Unlock()
	result := <-done
	if result.sent || !errors.Is(result.err, ErrPermitUnavailable) || !errors.Is(result.err, ErrDispatchNotSent) {
		t.Fatalf("expired Permit key drop sent=%v err=%v", result.sent, result.err)
	}
	if len(endpoint.wire) != 1 {
		t.Fatal("key drop reached endpoint after Permit expired while waiting")
	}
}

func TestDirectoryHighestTupleDownAndConflictAreFailClosed(t *testing.T) {
	directory := newTestDirectory()
	e1 := DirectoryEntry{NodeID: "node-1", EnrollmentID: "enrollment-node-1", Tuple: Tuple{NodeEpoch: 7, SessionSeq: 10}, HolderMemberID: "registry-a"}
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

func TestDirectoryRetirementCollectsHintAndRejectsStaleDelta(t *testing.T) {
	authority := &directoryEnrollmentAuthority{active: map[string]string{"node-1": "enrollment-node-1"}}
	directory := NewDirectory(authority)
	entry := DirectoryEntry{
		NodeID: "node-1", EnrollmentID: "enrollment-node-1",
		Tuple: Tuple{NodeEpoch: 7, SessionSeq: 10}, HolderMemberID: "registry-a",
	}
	if !directory.Apply(DirectoryDelta{Entry: entry, Up: true}) {
		t.Fatal("active enrollment was not installed")
	}
	authority.retire(entry.NodeID)
	if !directory.Apply(DirectoryDelta{Entry: entry, Retired: true}) {
		t.Fatal("retirement did not collect the Directory hint")
	}
	if directory.Apply(DirectoryDelta{Entry: entry, Up: true}) {
		t.Fatal("stale delta resurrected a retired enrollment")
	}
	if records := directory.Snapshot(); len(records) != 0 {
		t.Fatalf("retired Directory records = %+v", records)
	}
}

func TestDirectoryFullMergePreservesAvailabilityAndConflict(t *testing.T) {
	a := DirectoryRecord{Entry: DirectoryEntry{NodeID: "a", EnrollmentID: "enrollment-a", Tuple: Tuple{NodeEpoch: 1, SessionSeq: 2}, HolderMemberID: "r1"}, Available: true}
	b := DirectoryRecord{Entry: DirectoryEntry{NodeID: "b", EnrollmentID: "enrollment-b", Tuple: Tuple{NodeEpoch: 3, SessionSeq: 4}, HolderMemberID: "r2"}, Conflict: true}
	directory := newTestDirectory()
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
	a := DirectoryEntry{NodeID: "node-1", EnrollmentID: "enrollment-node-1", Tuple: Tuple{NodeEpoch: 3, SessionSeq: 7}, HolderMemberID: "registry-a"}
	b := a
	b.HolderMemberID = "registry-b"
	left := newTestDirectory()
	right := newTestDirectory()
	left.Apply(DirectoryDelta{Entry: a, Up: true})
	left.Apply(DirectoryDelta{Entry: b, Up: true})
	right.Apply(DirectoryDelta{Entry: b, Up: true})
	right.Apply(DirectoryDelta{Entry: a, Up: true})

	leftSnapshot := left.Snapshot()
	rightSnapshot := right.Snapshot()
	if len(leftSnapshot) != 1 || len(rightSnapshot) != 1 ||
		leftSnapshot[0] != rightSnapshot[0] ||
		leftSnapshot[0].Entry.HolderMemberID != "registry-a" ||
		rightSnapshot[0].Entry.HolderMemberID != "registry-a" ||
		!leftSnapshot[0].Conflict || !rightSnapshot[0].Conflict {
		t.Fatalf("canonical conflicts: left=%+v right=%+v", leftSnapshot, rightSnapshot)
	}
}

func TestDirectoryFullMergeSkipsInvalidConflictRecords(t *testing.T) {
	directory := newTestDirectory()
	healthy := DirectoryEntry{
		NodeID: "node-1", EnrollmentID: "enrollment-node-1", Tuple: Tuple{NodeEpoch: 3, SessionSeq: 7}, HolderMemberID: "registry-a",
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

func TestDirectoryFullMergeRejectsUnauthorizedConflict(t *testing.T) {
	authority := &directoryEnrollmentAuthority{active: map[string]string{"node-1": "enrollment-good"}}
	directory := NewDirectory(authority)
	healthy := DirectoryEntry{
		NodeID: "node-1", EnrollmentID: "enrollment-good", Tuple: Tuple{NodeEpoch: 3, SessionSeq: 7}, HolderMemberID: "registry-a",
	}
	if !directory.Apply(DirectoryDelta{Entry: healthy, Up: true}) {
		t.Fatal("healthy entry was not installed")
	}
	poison := DirectoryRecord{Entry: healthy, Conflict: true}
	poison.Entry.EnrollmentID = "enrollment-unauthorized"
	poison.Entry.HolderMemberID = "registry-b"
	if changed := directory.MergeFull([]DirectoryRecord{poison}); changed != 0 {
		t.Fatalf("unauthorized conflict changed %d records", changed)
	}
	if got, ok := directory.Lookup(healthy.NodeID); !ok || got != healthy {
		t.Fatalf("healthy entry after unauthorized conflict = %+v, %v", got, ok)
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

func TestRegistrationRejectsNoncanonicalDataEndpoint(t *testing.T) {
	registration := testRegistration("node-1", 7, 1, "node-without-port")
	if err := registration.Validate(); err == nil {
		t.Fatal("node registration accepted a noncanonical data endpoint")
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
	lease := testKeyLease()
	request, err := cluster.NewNodeRequestEnvelopeV1(http.MethodPost, "/sandboxes", "", nil, []byte(`{"metadata":null,"templateID":"e2b-img-`+strings.Repeat("c", 64)+`","timeout":0}`))
	if err != nil {
		t.Fatal(err)
	}
	spec, err := cluster.MarshalSandboxDispatchSpec(cluster.SandboxDispatchSpecV1{
		Version: cluster.DispatchSpecVersionV1, TemplateRef: "e2b-img-" + strings.Repeat("c", 64),
		AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
		AccessToken: "access", Request: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := cluster.NewDispatchIntent(demand, spec, "v1")
	if err != nil {
		t.Fatal(err)
	}
	var demandDigest, specDigest [sha256.Size]byte
	decoded, _ := hex.DecodeString(intent.DemandDigest)
	copy(demandDigest[:], decoded)
	decoded, _ = hex.DecodeString(intent.DispatchSpecDigest)
	copy(specDigest[:], decoded)
	opaque, err := cluster.EncodeExecutionBinding(cluster.ExecutionBinding{
		RegistryGeneration: "g1", Kind: cluster.ExecutionKindSandbox, ObjectID: "s1",
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
		ServeIdentity: testServeIdentity(),
		Kind:          cluster.ExecutionKindSandbox, Group: "/g", RouteKey: "rk", ObjectID: "s1",
		NodeID: registration.NodeID, NodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
		Intent: intent, Binding: cluster.ExecutionBindingIntent{
			NodeID: registration.NodeID, NodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
			RegistryGeneration: "g1", OpaqueBinding: opaque, BindingDigest: digest,
		},
		KeyLeaseRef: keyLeaseRef(lease),
	}
}

func testKeyLease() routesync.NodeKeyLeaseV1 {
	auth := testKeyMaterial(strings.Repeat("a", 64))
	manifest := testKeyMaterial(strings.Repeat("b", 64))
	return routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: "/g", KeyRevision: 1,
		AuthKey: auth, ManifestKey: manifest,
		ExpiresUnix: time.Now().Add(time.Hour).Unix(),
	}
}

func testKeyMaterial(value string) routesync.NodeKeyMaterialV1 {
	raw, _ := hex.DecodeString(value)
	digest := sha256.Sum256(raw)
	return routesync.NodeKeyMaterialV1{
		Type: routesync.KeyMaterialInline, Value: value, Fingerprint: hex.EncodeToString(digest[:12]),
	}
}
