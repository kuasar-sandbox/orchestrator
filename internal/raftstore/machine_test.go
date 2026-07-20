package raftstore

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestDataStateMachineAppliesConflictAndRestoresDeterministicSnapshot(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	identity := routeShardIdentity(t, registryLayout, "/g", "rk")
	replicas := replicaIDsForPlacement(registryLayout.DataShards[identity.ShardID])
	bootstrap, _ := NewDataShardBootstrap(registryLayout, identity.ShardID)
	machine := &DataStateMachine{raftShardID: DataRaftShardID(identity.ShardID), replicaID: replicas[0]}
	updateDataMachine(t, machine, 1, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap, ReplicaIDs: replicas,
	}, true)
	starting := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	updateDataMachine(t, machine, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	}, true)
	conflict := updateDataMachine(t, machine, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 99}, Route: &starting,
	}, false)
	if !conflict.Conflict || machine.state.LastApplied != 3 ||
		machine.state.Routes[routeMapKey("/g", "rk")].Revision.LogIndex != 2 {
		t.Fatalf("committed conflict = %+v, state=%+v", conflict, machine.state)
	}

	var first, second bytes.Buffer
	done := make(chan struct{})
	if err := machine.SaveSnapshot(&first, nil, done); err != nil {
		t.Fatal(err)
	}
	if err := machine.SaveSnapshot(&second, nil, done); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("unchanged data state produced non-deterministic snapshots")
	}

	restored := &DataStateMachine{raftShardID: machine.raftShardID, replicaID: replicas[1]}
	if err := restored.RecoverFromSnapshot(bytes.NewReader(first.Bytes()), nil, done); err != nil {
		t.Fatal(err)
	}
	lookup, err := restored.Lookup(DataLookup{Route: &routeapi.ReadRouteRequest{
		RequestIdentity: routeIdentity(identity), Group: "/g", RouteKey: "rk",
	}})
	if err != nil {
		t.Fatal(err)
	}
	result := lookup.(DataLookupResult)
	if result.Route == nil || result.Route.Outcome != routeapi.ReadNeedLeader {
		t.Fatalf("restored STARTING lookup = %+v", result)
	}

	corrupt := append([]byte(nil), first.Bytes()...)
	corrupt[len(corrupt)-1] ^= 1
	if err := restored.RecoverFromSnapshot(bytes.NewReader(corrupt), nil, done); err == nil {
		t.Fatal("corrupt snapshot restored")
	}
}

func TestSystemStateMachineReturnsQuorumPermitAndSnapshotsState(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	machine := &SystemStateMachine{raftShardID: SystemRaftShardID, replicaID: 1}
	updateSystemMachine(t, machine, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	}, true)
	updateSystemMachine(t, machine, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	}, true)
	permit := updateSystemMachine(t, machine, 3, SystemCommand{Type: SystemRefreshPermit}, true)
	if permit.PermitGrant == nil || permit.PermitGrant.CommitIndex != 3 || !permit.PermitGrant.WriteGate {
		t.Fatalf("permit result = %+v", permit)
	}
	conflict := updateSystemMachine(t, machine, 4, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Write: true},
	}, false)
	if !conflict.Conflict || machine.state.LastApplied != 4 {
		t.Fatalf("System conflict = %+v, applied=%d", conflict, machine.state.LastApplied)
	}

	var snapshot bytes.Buffer
	done := make(chan struct{})
	if err := machine.SaveSnapshot(&snapshot, nil, done); err != nil {
		t.Fatal(err)
	}
	restored := &SystemStateMachine{raftShardID: SystemRaftShardID, replicaID: 2}
	if err := restored.RecoverFromSnapshot(bytes.NewReader(snapshot.Bytes()), nil, done); err != nil {
		t.Fatal(err)
	}
	lookup, err := restored.Lookup(SystemStateLookup{})
	if err != nil {
		t.Fatal(err)
	}
	state := lookup.(SystemState)
	if state.ActiveRegistryLayoutDigest != digest || state.LastApplied != 4 || !state.ServeGate {
		t.Fatalf("restored System state = %+v", state)
	}
}

func TestStateMachineRejectsWrongShardAndUnknownCommandFields(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	identity := routeShardIdentity(t, registryLayout, "/g", "rk")
	replicas := replicaIDsForPlacement(registryLayout.DataShards[identity.ShardID])
	bootstrap, _ := NewDataShardBootstrap(registryLayout, identity.ShardID)
	command, err := EncodeDataCommand(DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap, ReplicaIDs: replicas,
	})
	if err != nil {
		t.Fatal(err)
	}
	wrong := &DataStateMachine{raftShardID: DataRaftShardID((identity.ShardID + 1) % registryLayout.VirtualShardCount), replicaID: replicas[0]}
	if _, err := wrong.Update(sm.Entry{Index: 1, Cmd: command}); err == nil {
		t.Fatal("command committed to another logical shard was accepted")
	}
	unknown := append(command[:len(command)-1], []byte(`,"unknown":true}`)...)
	correct := &DataStateMachine{raftShardID: DataRaftShardID(identity.ShardID), replicaID: replicas[0]}
	if _, err := correct.Update(sm.Entry{Index: 1, Cmd: unknown}); err == nil {
		t.Fatal("committed command with unknown fields was accepted")
	}
}

func TestJoiningReplicaReplaysBootstrapDeterministically(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	digest, _ := registryLayout.Digest()
	systemCommand, err := EncodeSystemCommand(SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	initialSystem := &SystemStateMachine{raftShardID: SystemRaftShardID, replicaID: 1}
	joiningSystem := &SystemStateMachine{raftShardID: SystemRaftShardID, replicaID: 99}
	initialResult, err := initialSystem.Update(sm.Entry{Index: 1, Cmd: systemCommand})
	if err != nil {
		t.Fatal(err)
	}
	joiningResult, err := joiningSystem.Update(sm.Entry{Index: 1, Cmd: systemCommand})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(initialResult.Data, joiningResult.Data) || initialSystem.state != joiningSystem.state {
		t.Fatal("System replicas applied the same committed history differently")
	}

	identity := registryLayoutShardIdentity(t, registryLayout, 0)
	bootstrap, _ := NewDataShardBootstrap(registryLayout, 0)
	dataCommand, err := EncodeDataCommand(DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
	})
	if err != nil {
		t.Fatal(err)
	}
	initialData := &DataStateMachine{raftShardID: DataRaftShardID(0), replicaID: 1}
	joiningData := &DataStateMachine{raftShardID: DataRaftShardID(0), replicaID: 99}
	initialResult, err = initialData.Update(sm.Entry{Index: 1, Cmd: dataCommand})
	if err != nil {
		t.Fatal(err)
	}
	joiningResult, err = joiningData.Update(sm.Entry{Index: 1, Cmd: dataCommand})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(initialResult.Data, joiningResult.Data) ||
		!reflect.DeepEqual(initialData.state, joiningData.state) {
		t.Fatal("data replicas applied the same committed history differently")
	}
}

func updateDataMachine(
	t *testing.T,
	machine *DataStateMachine,
	index uint64,
	command DataCommand,
	wantApplied bool,
) DataApplyResult {
	t.Helper()
	raw, err := EncodeDataCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := machine.Update(sm.Entry{Index: index, Cmd: raw})
	if err != nil {
		t.Fatal(err)
	}
	if (result.Value == 1) != wantApplied {
		t.Fatalf("data command %s result = %+v", command.Type, result)
	}
	var decoded DataApplyResult
	if err := json.Unmarshal(result.Data, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func updateSystemMachine(
	t *testing.T,
	machine *SystemStateMachine,
	index uint64,
	command SystemCommand,
	wantApplied bool,
) SystemApplyResult {
	t.Helper()
	raw, err := EncodeSystemCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := machine.Update(sm.Entry{Index: index, Cmd: raw})
	if err != nil {
		t.Fatal(err)
	}
	if (result.Value == 1) != wantApplied {
		t.Fatalf("System command %s result = %+v", command.Type, result)
	}
	var decoded SystemApplyResult
	if err := json.Unmarshal(result.Data, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}
