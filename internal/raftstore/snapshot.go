package raftstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

const (
	snapshotFormatVersion = uint32(1)
	maxSnapshotBytes      = uint64(512 << 20)
	snapshotKindSystem    = byte(1)
	snapshotKindData      = byte(2)
)

const snapshotMagic = "kuasar-raft-snapshot-v1\x00"

type systemSnapshot struct {
	FormatVersion uint32      `json:"format_version"`
	State         SystemState `json:"state"`
}

type routeSnapshotRow struct {
	Key    string                           `json:"key"`
	Record clusterstate.RouteWorkflowRecord `json:"record"`
}

type buildSnapshotRow struct {
	Key    string                   `json:"key"`
	Record clusterstate.BuildRecord `json:"record"`
}

type fenceSnapshotRow struct {
	Key    string                      `json:"key"`
	Record clusterstate.ExecutionFence `json:"record"`
}

type dataSnapshot struct {
	FormatVersion      uint32             `json:"format_version"`
	Initialized        bool               `json:"initialized"`
	ClusterID          string             `json:"cluster_id"`
	RegistryGeneration string             `json:"registry_generation"`
	ShardID            uint32             `json:"shard_id"`
	SchemaVersion      uint32             `json:"schema_version"`
	ProtocolVersion    uint32             `json:"protocol_version"`
	HashVersion        string             `json:"hash_version"`
	RouteBucketCount   uint32             `json:"route_bucket_count"`
	BuildBucketCount   uint32             `json:"build_bucket_count"`
	VirtualShardCount  uint32             `json:"virtual_shard_count"`
	ReplicaIDs         []uint64           `json:"replica_ids"`
	PreparedReplicaIDs []uint64           `json:"prepared_replica_ids,omitempty"`
	ServingEpochs      []PermitIdentity   `json:"serving_epochs"`
	Routes             []routeSnapshotRow `json:"routes"`
	Builds             []buildSnapshotRow `json:"builds"`
	Fences             []fenceSnapshotRow `json:"fences"`
	LastApplied        uint64             `json:"last_applied"`
}

func encodeSystemSnapshot(state SystemState) ([]byte, error) {
	if err := state.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(systemSnapshot{FormatVersion: snapshotFormatVersion, State: cloneSystemState(state)})
	if err != nil {
		return nil, err
	}
	return wrapSnapshot(snapshotKindSystem, payload)
}

func decodeSystemSnapshot(raw []byte) (SystemState, error) {
	payload, err := unwrapSnapshot(raw, snapshotKindSystem)
	if err != nil {
		return SystemState{}, err
	}
	var snapshot systemSnapshot
	if err := unmarshalStrictBounded(payload, &snapshot, int(maxSnapshotBytes)); err != nil {
		return SystemState{}, err
	}
	if snapshot.FormatVersion != snapshotFormatVersion {
		return SystemState{}, errors.New("raftstore: unsupported System snapshot format")
	}
	if err := snapshot.State.Validate(); err != nil {
		return SystemState{}, err
	}
	return snapshot.State, nil
}

func encodeDataSnapshot(state DataState) ([]byte, error) {
	if err := state.Validate(); err != nil {
		return nil, err
	}
	snapshot := dataSnapshot{
		FormatVersion: snapshotFormatVersion, Initialized: state.Initialized, ClusterID: state.ClusterID,
		RegistryGeneration: state.RegistryGeneration, ShardID: state.ShardID,
		SchemaVersion: state.SchemaVersion, ProtocolVersion: state.ProtocolVersion, HashVersion: state.HashVersion,
		RouteBucketCount: state.RouteBucketCount, BuildBucketCount: state.BuildBucketCount,
		VirtualShardCount: state.VirtualShardCount, ReplicaIDs: append([]uint64(nil), state.ReplicaIDs...),
		PreparedReplicaIDs: append([]uint64(nil), state.PreparedReplicaIDs...),
		ServingEpochs:      append([]PermitIdentity(nil), state.ServingEpochs...), LastApplied: state.LastApplied,
	}
	for key, record := range state.Routes {
		snapshot.Routes = append(snapshot.Routes, routeSnapshotRow{Key: key, Record: cloneRouteRecord(record)})
	}
	for key, record := range state.Builds {
		snapshot.Builds = append(snapshot.Builds, buildSnapshotRow{Key: key, Record: cloneBuildRecord(record)})
	}
	for key, fence := range state.Fences {
		snapshot.Fences = append(snapshot.Fences, fenceSnapshotRow{Key: key, Record: fence})
	}
	sort.Slice(snapshot.Routes, func(i, j int) bool { return snapshot.Routes[i].Key < snapshot.Routes[j].Key })
	sort.Slice(snapshot.Builds, func(i, j int) bool { return snapshot.Builds[i].Key < snapshot.Builds[j].Key })
	sort.Slice(snapshot.Fences, func(i, j int) bool { return snapshot.Fences[i].Key < snapshot.Fences[j].Key })
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	return wrapSnapshot(snapshotKindData, payload)
}

func decodeDataSnapshot(raw []byte) (DataState, error) {
	payload, err := unwrapSnapshot(raw, snapshotKindData)
	if err != nil {
		return DataState{}, err
	}
	var snapshot dataSnapshot
	if err := unmarshalStrictBounded(payload, &snapshot, int(maxSnapshotBytes)); err != nil {
		return DataState{}, err
	}
	if snapshot.FormatVersion != snapshotFormatVersion {
		return DataState{}, errors.New("raftstore: unsupported data snapshot format")
	}
	state := DataState{
		Initialized: snapshot.Initialized, ClusterID: snapshot.ClusterID,
		RegistryGeneration: snapshot.RegistryGeneration, ShardID: snapshot.ShardID,
		SchemaVersion: snapshot.SchemaVersion, ProtocolVersion: snapshot.ProtocolVersion, HashVersion: snapshot.HashVersion,
		RouteBucketCount: snapshot.RouteBucketCount, BuildBucketCount: snapshot.BuildBucketCount,
		VirtualShardCount: snapshot.VirtualShardCount, ReplicaIDs: append([]uint64(nil), snapshot.ReplicaIDs...),
		PreparedReplicaIDs: append([]uint64(nil), snapshot.PreparedReplicaIDs...),
		ServingEpochs:      append([]PermitIdentity(nil), snapshot.ServingEpochs...),
		Routes:             make(map[string]clusterstate.RouteWorkflowRecord, len(snapshot.Routes)),
		Builds:             make(map[string]clusterstate.BuildRecord, len(snapshot.Builds)),
		Fences:             make(map[string]clusterstate.ExecutionFence, len(snapshot.Fences)), LastApplied: snapshot.LastApplied,
	}
	if !state.Initialized {
		if len(snapshot.Routes) != 0 || len(snapshot.Builds) != 0 || len(snapshot.Fences) != 0 {
			return DataState{}, errors.New("raftstore: uninitialized data snapshot contains rows")
		}
		state.Routes, state.Builds, state.Fences = nil, nil, nil
	}
	for _, row := range snapshot.Routes {
		if _, duplicate := state.Routes[row.Key]; duplicate {
			return DataState{}, errors.New("raftstore: duplicate Route snapshot key")
		}
		state.Routes[row.Key] = cloneRouteRecord(row.Record)
	}
	for _, row := range snapshot.Builds {
		if _, duplicate := state.Builds[row.Key]; duplicate {
			return DataState{}, errors.New("raftstore: duplicate Build snapshot key")
		}
		state.Builds[row.Key] = cloneBuildRecord(row.Record)
	}
	for _, row := range snapshot.Fences {
		if _, duplicate := state.Fences[row.Key]; duplicate {
			return DataState{}, errors.New("raftstore: duplicate fence snapshot key")
		}
		state.Fences[row.Key] = row.Record
	}
	if err := state.Validate(); err != nil {
		return DataState{}, err
	}
	return state, nil
}

func wrapSnapshot(kind byte, payload []byte) ([]byte, error) {
	if len(payload) == 0 || uint64(len(payload)) > maxSnapshotBytes {
		return nil, errors.New("raftstore: snapshot payload exceeds the configured bound")
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(payload)))
	raw := make([]byte, 0, len(snapshotMagic)+1+len(length)+len(payload)+sha256.Size)
	raw = append(raw, snapshotMagic...)
	raw = append(raw, kind)
	raw = append(raw, length[:]...)
	raw = append(raw, payload...)
	digest := sha256.Sum256(raw)
	raw = append(raw, digest[:]...)
	return raw, nil
}

func unwrapSnapshot(raw []byte, wantKind byte) ([]byte, error) {
	headerSize := len(snapshotMagic) + 1 + 8
	if len(raw) < headerSize+sha256.Size || !bytes.Equal(raw[:len(snapshotMagic)], []byte(snapshotMagic)) {
		return nil, errors.New("raftstore: invalid snapshot header")
	}
	if raw[len(snapshotMagic)] != wantKind {
		return nil, errors.New("raftstore: snapshot belongs to another state machine kind")
	}
	length := binary.BigEndian.Uint64(raw[len(snapshotMagic)+1 : headerSize])
	if length == 0 || length > maxSnapshotBytes || uint64(len(raw)) != uint64(headerSize)+length+sha256.Size {
		return nil, errors.New("raftstore: invalid snapshot length")
	}
	digestOffset := headerSize + int(length)
	want := sha256.Sum256(raw[:digestOffset])
	if !bytes.Equal(raw[digestOffset:], want[:]) {
		return nil, errors.New("raftstore: snapshot digest mismatch")
	}
	return append([]byte(nil), raw[headerSize:digestOffset]...), nil
}

func readSnapshot(reader io.Reader) ([]byte, error) {
	maximum := int64(len(snapshotMagic)+1+8+sha256.Size) + int64(maxSnapshotBytes)
	limited := &io.LimitedReader{R: reader, N: maximum + 1}
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum {
		return nil, fmt.Errorf("raftstore: snapshot exceeds %d bytes", maximum)
	}
	return raw, nil
}

func cloneSystemState(state SystemState) SystemState {
	clone := state
	if state.Transition != nil {
		transition := *state.Transition
		transition.Shards = append([]ShardTransition(nil), state.Transition.Shards...)
		clone.Transition = &transition
	}
	if state.Recovery != nil {
		recovery := *state.Recovery
		clone.Recovery = &recovery
	}
	if state.Closure != nil {
		closure := *state.Closure
		clone.Closure = &closure
	}
	return clone
}
