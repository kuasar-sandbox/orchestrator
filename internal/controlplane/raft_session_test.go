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
