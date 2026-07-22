package controlplane

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type localSystemConsensus struct {
	*memoryConsensus
	state raftstore.SystemState
}

type registrationConsensus struct {
	*memoryConsensus
	state   raftstore.SystemState
	applies int
}

type directoryPermitConsensus struct {
	*localSystemConsensus
}

func (c *directoryPermitConsensus) AuthorizeServe(
	identity raftstore.PermitIdentity,
	_ raftstore.PermitOperation,
) error {
	if identity != c.identity {
		return raftstore.ErrPermitMismatch
	}
	return raftstore.ErrPermitDenied
}

func (c *registrationConsensus) ReadSystemStrong(context.Context) (raftstore.SystemState, error) {
	return c.state, nil
}

func (c *registrationConsensus) ApplySystem(context.Context, raftstore.SystemCommand) (raftstore.SystemApplyResult, error) {
	c.applies++
	return raftstore.SystemApplyResult{Applied: true}, nil
}

func (c *localSystemConsensus) ReadSystemLocal() (raftstore.SystemState, error) {
	return c.state, nil
}

func TestRaftStoreFencesRetiredNodeSessionAtPermitCommit(t *testing.T) {
	registryLayout := testControlRegistryLayout()
	runtime, err := newMemoryConsensus(registryLayout)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	registration := session.Registration{
		NodeID: "node-1", EnrollmentID: "enrollment-1",
		Tuple: session.Tuple{NodeEpoch: 7, SessionSeq: 3}, DataEndpoint: "10.0.0.1:8443",
		LoadModelVersion: 1, SandboxSlots: 64,
	}
	consensus := &localSystemConsensus{memoryConsensus: runtime, state: raftstore.SystemState{
		Initialized: true, ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
		SystemEpoch: 1, ActiveRegistryLayoutDigest: digest, LastApplied: 1,
		NodeEnrollments: map[string]raftstore.NodeEnrollmentRecord{"node-1": {
			NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
			MaxNodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
			EnrollmentIndex: 1, LastAppliedIndex: 1,
		}},
	}}
	store, err := NewRaftStore(consensus, registryLayout, digest)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := store.RefreshPermit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AuthorizeNodeSession(identity, registration); err != nil {
		t.Fatalf("active enrollment rejected: %v", err)
	}
	record := consensus.state.NodeEnrollments[registration.NodeID]
	record.Retired = true
	record.LastAppliedIndex = 2
	consensus.state.NodeEnrollments[registration.NodeID] = record
	consensus.state.LastApplied = 2
	if err := store.AuthorizeNodeSession(identity, registration); !errors.Is(err, session.ErrStaleSession) {
		t.Fatalf("retired enrollment error = %v", err)
	}
}

func TestNodeEpochFenceRequiresStrictlyNewerCommittedEpoch(t *testing.T) {
	registryLayout := testControlRegistryLayout()
	runtime, err := newMemoryConsensus(registryLayout)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	state := raftstore.SystemState{
		Initialized: true, ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
		SystemEpoch: 1, ActiveRegistryLayoutDigest: digest, LastApplied: 1,
		NodeEnrollments: map[string]raftstore.NodeEnrollmentRecord{"node-1": {
			NodeID: "node-1", EnrollmentID: "enrollment-1", MaxNodeEpoch: 7,
			EnrollmentIndex: 1, LastAppliedIndex: 1,
		}},
	}
	consensus := &registrationConsensus{memoryConsensus: runtime, state: state}
	store, err := NewRaftStore(consensus, registryLayout, digest)
	if err != nil {
		t.Fatal(err)
	}
	if fenced, err := store.NodeEpochPermanentlyFenced(context.Background(), "node-1", 7); err != nil || fenced {
		t.Fatalf("current NodeEpoch fenced=%v err=%v", fenced, err)
	}
	enrollment := consensus.state.NodeEnrollments["node-1"]
	enrollment.Retired = true
	consensus.state.NodeEnrollments["node-1"] = enrollment
	if fenced, err := store.NodeEpochPermanentlyFenced(context.Background(), "node-1", 7); err != nil || fenced {
		t.Fatalf("same-epoch enrollment retirement fenced execution=%v err=%v", fenced, err)
	}
	enrollment.MaxNodeEpoch = 8
	consensus.state.NodeEnrollments["node-1"] = enrollment
	if fenced, err := store.NodeEpochPermanentlyFenced(context.Background(), "node-1", 7); err != nil || !fenced {
		t.Fatalf("older NodeEpoch fenced=%v err=%v", fenced, err)
	}
}

func TestDirectoryRetainsCurrentSessionAcrossClosedServeGates(t *testing.T) {
	registryLayout := testControlRegistryLayout()
	runtime, err := newMemoryConsensus(registryLayout)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	entry := session.DirectoryEntry{
		NodeID: "node-1", EnrollmentID: "enrollment-1",
		Tuple: session.Tuple{NodeEpoch: 7, SessionSeq: 3}, HolderMemberID: "registry-a",
	}
	state := raftstore.SystemState{
		Initialized: true, ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
		SystemEpoch: 1, ActiveRegistryLayoutDigest: digest, LastApplied: 1,
		NodeEnrollments: map[string]raftstore.NodeEnrollmentRecord{"node-1": {
			NodeID: entry.NodeID, EnrollmentID: entry.EnrollmentID, MaxNodeEpoch: entry.NodeEpoch,
			EnrollmentIndex: 1, LastAppliedIndex: 1,
		}},
	}
	consensus := &directoryPermitConsensus{
		localSystemConsensus: &localSystemConsensus{memoryConsensus: runtime, state: state},
	}
	store, err := NewRaftStore(consensus, registryLayout, digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RefreshPermit(context.Background()); err != nil {
		t.Fatal(err)
	}
	directory := session.NewDirectory(store)
	if !directory.Apply(session.DirectoryDelta{Entry: entry, Up: true}) {
		t.Fatal("current Directory entry was rejected while serving operations were closed")
	}
	if got, found := directory.Lookup(entry.NodeID); !found || got != entry {
		t.Fatalf("current Directory entry = %+v, found=%v", got, found)
	}
	enrollment := consensus.state.NodeEnrollments[entry.NodeID]
	enrollment.Retired = true
	consensus.state.NodeEnrollments[entry.NodeID] = enrollment
	if _, found := directory.Lookup(entry.NodeID); found {
		t.Fatal("retired enrollment remained available in the Directory")
	}
}

func TestSessionReconnectSkipsUnchangedSystemCatalogMutation(t *testing.T) {
	registryLayout := testControlRegistryLayout()
	runtime, err := newMemoryConsensus(registryLayout)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	registration := session.Registration{
		NodeID: "node-1", EnrollmentID: "enrollment-1",
		Tuple: session.Tuple{NodeEpoch: 7, SessionSeq: 12}, DataEndpoint: "10.0.0.1:8443",
		RuntimeDigest: "runtime-1", Labels: map[string]string{"zone": "z1"},
		Capabilities: map[string]bool{"sandbox": true}, FailureDomain: "z1",
		LoadModelVersion: 1, SandboxSlots: 64, BuildSlots: 2, BuildCPU: 2000,
	}
	consensus := &registrationConsensus{memoryConsensus: runtime, state: raftstore.SystemState{
		Initialized: true, ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
		SystemEpoch: 1, ActiveRegistryLayoutDigest: digest, LastApplied: 10,
		NodeEnrollments: map[string]raftstore.NodeEnrollmentRecord{"node-1": {
			NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
			MaxNodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
			EnrollmentIndex: 2, LastAppliedIndex: 10,
			Catalog: &raftstore.NodeCatalogRecord{
				RuntimeDigest: registration.RuntimeDigest, Labels: registration.Labels,
				Capabilities: registration.Capabilities, FailureDomain: registration.FailureDomain,
				LoadModelVersion: registration.LoadModelVersion, SandboxSlots: registration.SandboxSlots,
				BuildSlots: registration.BuildSlots, BuildCPU: registration.BuildCPU,
				CatalogVersion: 1, LastRegistrationIndex: 10,
			},
		}},
	}}
	store, err := NewRaftStore(consensus, registryLayout, digest)
	if err != nil {
		t.Fatal(err)
	}
	installed := 0
	if err := store.RunSessionRegistration(context.Background(), registration, func() error {
		installed++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if consensus.applies != 0 || installed != 1 {
		t.Fatalf("unchanged reconnect applied=%d installed=%d", consensus.applies, installed)
	}
	registration.Draining = true
	if err := store.RunSessionRegistration(context.Background(), registration, func() error {
		installed++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if consensus.applies != 1 || installed != 2 {
		t.Fatalf("changed catalog applied=%d installed=%d", consensus.applies, installed)
	}
}
