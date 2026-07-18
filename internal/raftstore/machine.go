package raftstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type SystemStateLookup struct{}
type DataStateLookup struct{}

type SystemStateMachine struct {
	raftShardID uint64
	replicaID   uint64
	state       SystemState
}

type DataStateMachine struct {
	raftShardID uint64
	replicaID   uint64
	state       DataState
}

var (
	_ sm.IStateMachine = (*SystemStateMachine)(nil)
	_ sm.IStateMachine = (*DataStateMachine)(nil)
)

func NewSystemStateMachine(raftShardID, replicaID uint64) sm.IStateMachine {
	return &SystemStateMachine{raftShardID: raftShardID, replicaID: replicaID}
}

func NewDataStateMachine(raftShardID, replicaID uint64) sm.IStateMachine {
	return &DataStateMachine{raftShardID: raftShardID, replicaID: replicaID}
}

func (m *SystemStateMachine) Update(entry sm.Entry) (sm.Result, error) {
	if m.raftShardID != SystemRaftShardID || m.replicaID == 0 {
		return sm.Result{}, errors.New("raftstore: invalid System Group state-machine identity")
	}
	command, err := DecodeSystemCommand(entry.Cmd)
	if err != nil {
		return sm.Result{}, fmt.Errorf("raftstore: decode committed System command: %w", err)
	}
	if command.Type == SystemBootstrap && !replicaPlacementContains(command.Manifest.SystemReplicas, m.replicaID) {
		return sm.Result{}, errors.New("raftstore: local replica is absent from System bootstrap manifest")
	}
	next, result := ApplySystemCommand(m.state, entry.Index, command)
	if result.Conflict && m.state.Initialized && entry.Index > m.state.LastApplied {
		next = m.state
		next.LastApplied = entry.Index
	}
	m.state = next
	return encodeApplyResult(result.Applied, result)
}

func (m *SystemStateMachine) Lookup(query any) (any, error) {
	switch query.(type) {
	case SystemStateLookup, *SystemStateLookup:
		return cloneSystemState(m.state), nil
	default:
		return nil, errors.New("raftstore: unsupported System Group lookup")
	}
}

func (m *SystemStateMachine) SaveSnapshot(
	writer io.Writer,
	_ sm.ISnapshotFileCollection,
	done <-chan struct{},
) error {
	if snapshotStopped(done) {
		return sm.ErrSnapshotStopped
	}
	raw, err := encodeSystemSnapshot(m.state)
	if err != nil {
		return err
	}
	if snapshotStopped(done) {
		return sm.ErrSnapshotStopped
	}
	_, err = io.Copy(writer, bytes.NewReader(raw))
	return err
}

func (m *SystemStateMachine) RecoverFromSnapshot(
	reader io.Reader,
	files []sm.SnapshotFile,
	done <-chan struct{},
) error {
	if m.raftShardID != SystemRaftShardID || m.replicaID == 0 {
		return errors.New("raftstore: invalid System Group state-machine identity")
	}
	if len(files) != 0 {
		return errors.New("raftstore: System snapshot contains unsupported external files")
	}
	if snapshotStopped(done) {
		return sm.ErrSnapshotStopped
	}
	raw, err := readSnapshot(reader)
	if err != nil {
		return err
	}
	state, err := decodeSystemSnapshot(raw)
	if err != nil {
		return err
	}
	if snapshotStopped(done) {
		return sm.ErrSnapshotStopped
	}
	m.state = state
	return nil
}

func (m *SystemStateMachine) Close() error { return nil }

func (m *DataStateMachine) Update(entry sm.Entry) (sm.Result, error) {
	logicalShardID, ok := LogicalShardID(m.raftShardID)
	if !ok || m.replicaID == 0 {
		return sm.Result{}, errors.New("raftstore: invalid data state-machine identity")
	}
	command, err := DecodeDataCommand(entry.Cmd)
	if err != nil {
		return sm.Result{}, fmt.Errorf("raftstore: decode committed data command: %w", err)
	}
	if command.Identity.ShardID != logicalShardID {
		return sm.Result{}, errors.New("raftstore: committed command targets another logical shard")
	}
	if command.Type == DataInitializeShard && !uint64SetContains(command.ReplicaIDs, m.replicaID) {
		return sm.Result{}, errors.New("raftstore: local replica is absent from data-shard bootstrap manifest")
	}
	result := ApplyDataCommand(&m.state, entry.Index, command)
	return encodeApplyResult(result.Applied, result)
}

func (m *DataStateMachine) Lookup(query any) (any, error) {
	switch value := query.(type) {
	case DataLookup:
		return LookupData(m.state, value)
	case *DataLookup:
		if value == nil {
			return nil, errors.New("raftstore: nil data lookup")
		}
		return LookupData(m.state, *value)
	case DataStateLookup, *DataStateLookup:
		return cloneDataStateForLookup(m.state), nil
	default:
		return nil, errors.New("raftstore: unsupported data-shard lookup")
	}
}

func (m *DataStateMachine) SaveSnapshot(
	writer io.Writer,
	_ sm.ISnapshotFileCollection,
	done <-chan struct{},
) error {
	if snapshotStopped(done) {
		return sm.ErrSnapshotStopped
	}
	raw, err := encodeDataSnapshot(m.state)
	if err != nil {
		return err
	}
	if snapshotStopped(done) {
		return sm.ErrSnapshotStopped
	}
	_, err = io.Copy(writer, bytes.NewReader(raw))
	return err
}

func (m *DataStateMachine) RecoverFromSnapshot(
	reader io.Reader,
	files []sm.SnapshotFile,
	done <-chan struct{},
) error {
	if len(files) != 0 {
		return errors.New("raftstore: data snapshot contains unsupported external files")
	}
	if snapshotStopped(done) {
		return sm.ErrSnapshotStopped
	}
	raw, err := readSnapshot(reader)
	if err != nil {
		return err
	}
	state, err := decodeDataSnapshot(raw)
	if err != nil {
		return err
	}
	logicalShardID, ok := LogicalShardID(m.raftShardID)
	if !ok || state.Initialized && state.ShardID != logicalShardID {
		return errors.New("raftstore: data snapshot belongs to another logical shard")
	}
	if snapshotStopped(done) {
		return sm.ErrSnapshotStopped
	}
	m.state = state
	return nil
}

func (m *DataStateMachine) Close() error { return nil }

func encodeApplyResult(applied bool, result any) (sm.Result, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return sm.Result{}, err
	}
	value := uint64(0)
	if applied {
		value = 1
	}
	return sm.Result{Value: value, Data: raw}, nil
}

func replicaPlacementContains(replicas []ReplicaPlacement, replicaID uint64) bool {
	for _, replica := range replicas {
		if replica.ReplicaID == replicaID {
			return true
		}
	}
	return false
}

func uint64SetContains(values []uint64, target uint64) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func snapshotStopped(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func cloneDataStateForLookup(state DataState) DataState {
	clone := state
	clone.ReplicaIDs = append([]uint64(nil), state.ReplicaIDs...)
	clone.PreparedReplicaIDs = append([]uint64(nil), state.PreparedReplicaIDs...)
	clone.ServingEpochs = append([]PermitIdentity(nil), state.ServingEpochs...)
	clone.Routes = make(map[string]clusterstate.RouteWorkflowRecord, len(state.Routes))
	for key, record := range state.Routes {
		clone.Routes[key] = cloneRouteRecord(record)
	}
	clone.Builds = make(map[string]clusterstate.BuildRecord, len(state.Builds))
	for key, record := range state.Builds {
		clone.Builds[key] = cloneBuildRecord(record)
	}
	clone.Fences = make(map[string]clusterstate.ExecutionFence, len(state.Fences))
	for key, fence := range state.Fences {
		clone.Fences[key] = fence
	}
	return clone
}
