package raftstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

type testRemoteSystemClient struct {
	state SystemState
}

func (c *testRemoteSystemClient) ReadSystemStrong(context.Context) (SystemState, error) {
	return cloneSystemState(c.state), nil
}

func (c *testRemoteSystemClient) ApplySystem(_ context.Context, command SystemCommand) (SystemApplyResult, error) {
	var result SystemApplyResult
	c.state, result = ApplySystemCommand(c.state, c.state.LastApplied+1, command)
	return result, nil
}

func (c *testRemoteSystemClient) RefreshPermit(ctx context.Context) (PermitGrant, error) {
	result, err := c.ApplySystem(ctx, SystemCommand{Type: SystemRefreshPermit})
	if err != nil || result.PermitGrant == nil {
		return PermitGrant{}, errors.Join(err, errors.New("missing test Permit"))
	}
	return *result.PermitGrant, nil
}

func (c *testRemoteSystemClient) CloseRegistryGeneration(context.Context, RegistryLayout) (SystemState, error) {
	return SystemState{}, errors.New("unused")
}

func (c *testRemoteSystemClient) ConfirmPredecessorPermitDrain(context.Context, string) (SystemState, error) {
	return SystemState{}, errors.New("unused")
}

func (c *testRemoteSystemClient) BeginRecovery(context.Context) (SystemState, error) {
	return SystemState{}, errors.New("unused")
}

func (c *testRemoteSystemClient) ConfirmRecoveryPermitDrain(context.Context) (SystemState, error) {
	return SystemState{}, errors.New("unused")
}

func (c *testRemoteSystemClient) AdvanceRecovery(context.Context, RecoveryPhase, RecoveryPhase) (SystemState, error) {
	return SystemState{}, errors.New("unused")
}

func (c *testRemoteSystemClient) SetServingGates(context.Context, GateUpdate) (SystemState, error) {
	return SystemState{}, errors.New("unused")
}

func TestDataOnlyRegistryUsesSoleRemoteSystemGroupAndLocalBoundedPermit(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-1")
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	state, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	if result.Conflict || !result.Applied {
		t.Fatalf("bootstrap = %+v", result)
	}
	state, result = ApplySystemCommand(state, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	if result.Conflict || !result.Applied {
		t.Fatalf("activate = %+v", result)
	}
	client := &testRemoteSystemClient{state: state}
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, systemClient: client,
		permitCache: NewPermitCache(time.Now),
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: DataRaftShardID(0), ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	grant, err := runtime.RefreshPermit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.AuthorizeServe(grant.PermitIdentity, PermitRegistryRead); err != nil {
		t.Fatalf("locally cached remote Permit = %v", err)
	}
	local, err := runtime.ReadSystemLocal()
	if err != nil || local.LastApplied < grant.CommitIndex || local.Identity() != grant.PermitIdentity {
		t.Fatalf("cached remote System state = %+v, %v", local, err)
	}
	apply, err := runtime.ApplySystem(context.Background(), SystemCommand{
		Type: SystemEnrollNode,
		Enrollment: &NodeEnrollmentCommand{
			NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 1, DataEndpoint: "10.0.0.1:8443",
		},
	})
	if err != nil || apply.Conflict || !apply.Applied {
		t.Fatalf("remote System mutation = %+v, %v", apply, err)
	}
	if _, found := client.state.NodeEnrollments["node-1"]; !found {
		t.Fatal("remote System Group did not commit node enrollment")
	}
}

func TestRemoteSystemStateFromAnotherGenerationFailsClosed(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-1")
	digest, _ := registryLayout.Digest()
	other := testRegistryLayout(1, "generation-2")
	otherDigest, _ := other.Digest()
	state, _ := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &other, Digest: otherDigest,
	})
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest,
		systemClient: &testRemoteSystemClient{state: state}, permitCache: NewPermitCache(time.Now),
	}
	if _, err := runtime.ReadSystemStrong(context.Background()); err == nil {
		t.Fatal("remote System state from another generation was accepted")
	}
	if _, err := runtime.RefreshPermit(context.Background()); err == nil {
		t.Fatal("remote Permit from another generation was accepted")
	}
	if err := runtime.permitCache.Authorize(state.Identity(), PermitRegistryRead); !errors.Is(err, ErrPermitMissing) {
		t.Fatalf("foreign Permit poisoned local cache: %v", err)
	}
}
