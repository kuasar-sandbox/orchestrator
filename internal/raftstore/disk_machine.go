package raftstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

var stateSnapshotMagic = [...]byte{
	'K', 'U', 'A', 'S', 'A', 'R', '-', 'P', 'E', 'B', 'B', 'L', 'E', '-', 'S', 'N', 'A', 'P', 4,
}

type diskStateMachine struct {
	engine    *PebbleStateEngine
	shardID   uint64
	replicaID uint64

	mu         sync.RWMutex
	activeSlot atomic.Uint32
	closed     bool
}

var _ sm.IOnDiskStateMachine = (*diskStateMachine)(nil)

func (m *diskStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	if err := snapshotStoppedOrClosed(stopc); err != nil {
		return 0, sm.ErrOpenStopped
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.validateIdentity(); err != nil {
		return 0, err
	}
	control, found, err := loadStateControl(m.engine.db, m.shardID, m.replicaID)
	if err != nil {
		return 0, err
	}
	if !found {
		m.activeSlot.Store(0)
		return 0, nil
	}
	prefix := stateSlotPrefix(m.shardID, m.replicaID, control.ActiveSlot)
	lastApplied, err := m.loadLastApplied(m.engine.db, prefix)
	if err != nil {
		return 0, err
	}
	m.activeSlot.Store(uint32(control.ActiveSlot))
	return lastApplied, nil
}

func (m *diskStateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, errors.New("raftstore: on-disk state machine is closed")
	}
	if m.shardID == SystemRaftShardID {
		return m.updateSystem(entries)
	}
	return m.updateData(entries)
}

func (m *diskStateMachine) updateSystem(entries []sm.Entry) ([]sm.Entry, error) {
	active := byte(m.activeSlot.Load())
	slot := active
	if slot == 0 {
		slot = 1
	}
	prefix := stateSlotPrefix(m.shardID, m.replicaID, slot)
	batch := m.engine.db.NewIndexedBatch()
	defer batch.Close()
	state := SystemState{}
	if active == 0 {
		if err := batch.DeleteRange(prefix, prefixUpperBound(prefix), nil); err != nil {
			return nil, err
		}
	} else {
		var found bool
		var err error
		state, found, err = loadSystemState(batch, prefix)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("raftstore: active System state is missing")
		}
	}
	for index := range entries {
		command, err := DecodeSystemCommand(entries[index].Cmd)
		if err != nil {
			return nil, fmt.Errorf("raftstore: decode committed System command: %w", err)
		}
		next, result := ApplySystemCommand(state, entries[index].Index, command)
		if result.Conflict && state.Initialized && entries[index].Index > state.LastApplied {
			next = state
			next.LastApplied = entries[index].Index
		}
		encoded, err := encodeApplyResult(result.Applied, result)
		if err != nil {
			return nil, err
		}
		entries[index].Result = encoded
		state = next
	}
	if state.Initialized {
		if err := setStateJSON(batch, stateRowKey(prefix, stateMetadataTable, ""), state); err != nil {
			return nil, err
		}
		if active == 0 {
			if err := batch.Set(stateControlKey(m.shardID, m.replicaID), encodeStateControl(stateControl{
				Version: stateEngineVersion, ActiveSlot: slot,
			}), nil); err != nil {
				return nil, err
			}
		}
	}
	if batch.Empty() {
		return entries, nil
	}
	if err := m.engine.commitNoSync(batch); err != nil {
		return nil, err
	}
	if active == 0 {
		m.activeSlot.Store(uint32(slot))
	}
	return entries, nil
}

func (m *diskStateMachine) updateData(entries []sm.Entry) ([]sm.Entry, error) {
	logicalShardID, ok := LogicalShardID(m.shardID)
	if !ok {
		return nil, errors.New("raftstore: invalid data state-machine identity")
	}
	active := byte(m.activeSlot.Load())
	slot := active
	if slot == 0 {
		slot = 1
	}
	prefix := stateSlotPrefix(m.shardID, m.replicaID, slot)
	batch := m.engine.db.NewIndexedBatch()
	defer batch.Close()
	state := DataState{
		Routes:         make(map[string]clusterstate.RouteWorkflowRecord),
		Builds:         make(map[string]clusterstate.BuildRecord),
		Fences:         make(map[string]clusterstate.ExecutionFence),
		UsedSandboxIDs: make(map[string]struct{}),
	}
	if active == 0 {
		if err := batch.DeleteRange(prefix, prefixUpperBound(prefix), nil); err != nil {
			return nil, err
		}
	} else {
		var found bool
		var err error
		state, found, err = loadDataStateMetadata(batch, prefix)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("raftstore: active data-shard metadata is missing")
		}
	}
	for index := range entries {
		command, err := DecodeDataCommand(entries[index].Cmd)
		if err != nil {
			return nil, fmt.Errorf("raftstore: decode committed data command: %w", err)
		}
		if command.Identity.ShardID != logicalShardID {
			return nil, errors.New("raftstore: committed command targets another logical shard")
		}
		if err := loadDataCommandRows(batch, prefix, &state, command); err != nil {
			return nil, err
		}
		previousChangefeedFloor := state.RouteChangefeedFloor
		result := ApplyDataCommand(&state, entries[index].Index, command)
		encoded, err := encodeApplyResult(result.Applied, result)
		if err != nil {
			return nil, err
		}
		entries[index].Result = encoded
		if result.Applied {
			if err := persistDataCommandRow(batch, prefix, state, command); err != nil {
				return nil, err
			}
			if command.Type == DataPutRoute {
				change := state.RouteChanges[len(state.RouteChanges)-1]
				if err := setStateJSON(batch, routeChangeRowKey(prefix, change.Revision), change); err != nil {
					return nil, err
				}
			}
		}
		if state.RouteChangefeedFloor > previousChangefeedFloor {
			if err := batch.DeleteRange(
				stateTablePrefix(prefix, stateRouteChangeTable),
				routeChangeRowKey(prefix, state.RouteChangefeedFloor+1), nil,
			); err != nil {
				return nil, err
			}
		}
		clearDataRows(&state)
	}
	if state.Initialized {
		metadata := metadataFromDataState(state)
		if err := setStateJSON(batch, stateRowKey(prefix, stateMetadataTable, ""), metadata); err != nil {
			return nil, err
		}
		if active == 0 {
			if err := batch.Set(stateControlKey(m.shardID, m.replicaID), encodeStateControl(stateControl{
				Version: stateEngineVersion, ActiveSlot: slot,
			}), nil); err != nil {
				return nil, err
			}
		}
	}
	if batch.Empty() {
		return entries, nil
	}
	if err := m.engine.commitNoSync(batch); err != nil {
		return nil, err
	}
	if active == 0 {
		m.activeSlot.Store(uint32(slot))
	}
	return entries, nil
}

func loadDataCommandRows(reader pebble.Reader, prefix []byte, state *DataState, command DataCommand) error {
	clearDataRows(state)
	switch command.Type {
	case DataPutRoute:
		key := routeMapKey(command.Route.Group, command.Route.RouteKey)
		var current clusterstate.RouteWorkflowRecord
		found, err := getStateJSON(reader, stateRowKey(prefix, stateRouteTable, key), &current)
		if err != nil {
			return err
		}
		if found {
			state.Routes[key] = current
			if current.State == clusterstate.WorkflowRouteTombstone && current.Tombstone != nil &&
				command.Route.State == clusterstate.WorkflowRouteStarting && command.Route.Starting != nil {
				loadFence := func(sandboxID string) error {
					fenceKey := fenceMapKey(current.Group, current.RouteKey, sandboxID)
					var fence clusterstate.ExecutionFence
					fenceFound, err := getStateJSON(reader, stateRowKey(prefix, stateFenceTable, fenceKey), &fence)
					if err != nil {
						return err
					}
					if fenceFound {
						state.Fences[fenceKey] = fence
					}
					return nil
				}
				if err := loadFence(command.Route.Starting.SandboxID); err != nil {
					return err
				}
				usedKey := fenceMapKey(current.Group, current.RouteKey, command.Route.Starting.SandboxID)
				if _, found, err := getStateValue(reader, stateRowKey(prefix, stateSandboxIDTable, usedKey)); err != nil {
					return err
				} else if found {
					state.UsedSandboxIDs[usedKey] = struct{}{}
				}
				if !current.Tombstone.FenceCompacted {
					sandboxID := current.Tombstone.SandboxID
					if current.Tombstone.PlacementFailure != nil {
						sandboxID = current.Tombstone.PlacementFailure.SandboxID
					}
					if err := loadFence(sandboxID); err != nil {
						return err
					}
				}
			}
		}
	case DataPutBuild:
		key := buildMapKey(command.Build.Group, command.Build.BuildID)
		var current clusterstate.BuildRecord
		found, err := getStateJSON(reader, stateRowKey(prefix, stateBuildTable, key), &current)
		if err != nil {
			return err
		}
		if found {
			state.Builds[key] = current
		}
	case DataPutFence:
		key := fenceMapKey(command.Fence.Group, command.Fence.RouteKey, command.Fence.SandboxID)
		var current clusterstate.ExecutionFence
		found, err := getStateJSON(reader, stateRowKey(prefix, stateFenceTable, key), &current)
		if err != nil {
			return err
		}
		if found {
			state.Fences[key] = current
		} else {
			routeKey := routeMapKey(command.Fence.Group, command.Fence.RouteKey)
			var route clusterstate.RouteWorkflowRecord
			routeFound, err := getStateJSON(reader, stateRowKey(prefix, stateRouteTable, routeKey), &route)
			if err != nil {
				return err
			}
			if routeFound {
				state.Routes[routeKey] = route
			}
		}
	case DataCompactFence:
		key := fenceMapKey(command.Compaction.Group, command.Compaction.RouteKey, command.Compaction.SandboxID)
		var fence clusterstate.ExecutionFence
		found, err := getStateJSON(reader, stateRowKey(prefix, stateFenceTable, key), &fence)
		if err != nil {
			return err
		}
		if found {
			state.Fences[key] = fence
		}
		routeKey := routeMapKey(command.Compaction.Group, command.Compaction.RouteKey)
		var route clusterstate.RouteWorkflowRecord
		routeFound, err := getStateJSON(reader, stateRowKey(prefix, stateRouteTable, routeKey), &route)
		if err != nil {
			return err
		}
		if routeFound {
			state.Routes[routeKey] = route
		}
	}
	return nil
}

func persistDataCommandRow(batch *pebble.Batch, prefix []byte, state DataState, command DataCommand) error {
	switch command.Type {
	case DataPutRoute:
		key := routeMapKey(command.Route.Group, command.Route.RouteKey)
		record := state.Routes[key]
		if err := setStateJSON(batch, stateRowKey(prefix, stateRouteTable, key), record); err != nil {
			return err
		}
		if err := persistRouteBucketIndex(batch, prefix, state, record); err != nil {
			return err
		}
		return persistPendingIndex(batch, prefix, "r"+key, routeNeedsCoordinator(record))
	case DataPutBuild:
		key := buildMapKey(command.Build.Group, command.Build.BuildID)
		if err := setStateJSON(batch, stateRowKey(prefix, stateBuildTable, key), state.Builds[key]); err != nil {
			return err
		}
		return persistPendingIndex(batch, prefix, "b"+key, buildNeedsCoordinator(state.Builds[key]))
	case DataPutFence:
		key := fenceMapKey(command.Fence.Group, command.Fence.RouteKey, command.Fence.SandboxID)
		if err := setStateJSON(batch, stateRowKey(prefix, stateFenceTable, key), state.Fences[key]); err != nil {
			return err
		}
		return batch.Set(stateRowKey(prefix, stateSandboxIDTable, key), []byte{1}, nil)
	case DataCompactFence:
		key := fenceMapKey(command.Compaction.Group, command.Compaction.RouteKey, command.Compaction.SandboxID)
		if err := batch.Delete(stateRowKey(prefix, stateFenceTable, key), nil); err != nil {
			return err
		}
		routeKey := routeMapKey(command.Compaction.Group, command.Compaction.RouteKey)
		if route, found := state.Routes[routeKey]; found && route.Tombstone != nil && route.Tombstone.FenceCompacted {
			return setStateJSON(batch, stateRowKey(prefix, stateRouteTable, routeKey), route)
		}
		return nil
	default:
		return nil
	}
}

func persistPendingIndex(batch *pebble.Batch, prefix []byte, qualifiedKey string, pending bool) error {
	key := stateRowKey(prefix, statePendingTable, qualifiedKey)
	if !pending {
		return batch.Delete(key, nil)
	}
	return batch.Set(key, []byte{1}, nil)
}

func persistRouteBucketIndex(
	batch *pebble.Batch,
	prefix []byte,
	state DataState,
	record clusterstate.RouteWorkflowRecord,
) error {
	bucket, shardID, err := clusterstate.RouteShardFor(
		record.Group, record.RouteKey, state.RouteBucketCount, state.VirtualShardCount,
	)
	if err != nil || shardID != state.ShardID {
		return errors.New("raftstore: Route bucket index belongs to another shard")
	}
	key := routeBucketIndexRowKey(prefix, record.Group, bucket, record.RouteKey)
	if _, listed := routeBucketEntry(record); !listed {
		return batch.Delete(key, nil)
	}
	return batch.Set(key, []byte{1}, nil)
}

func routeBucketIndexPrefix(prefix []byte, group string, bucket uint32) []byte {
	key := []byte(lengthKey(group))
	key = binary.BigEndian.AppendUint32(key, bucket)
	return stateRowKey(prefix, stateRouteBucketTable, string(key))
}

func routeBucketIndexRowKey(prefix []byte, group string, bucket uint32, routeKey string) []byte {
	key := routeBucketIndexPrefix(prefix, group, bucket)
	return append(key, routeKey...)
}

func clearDataRows(state *DataState) {
	clear(state.Routes)
	clear(state.Builds)
	clear(state.Fences)
	clear(state.UsedSandboxIDs)
	state.RouteChanges = nil
}

func (m *diskStateMachine) Lookup(query any) (any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, errors.New("raftstore: on-disk state machine is closed")
	}
	active := byte(m.activeSlot.Load())
	if active == 0 {
		return m.lookupEmpty(query)
	}
	snapshot := m.engine.db.NewSnapshot()
	defer snapshot.Close()
	prefix := stateSlotPrefix(m.shardID, m.replicaID, active)
	if m.shardID == SystemRaftShardID {
		state, found, err := loadSystemState(snapshot, prefix)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("raftstore: active System state is missing")
		}
		switch query.(type) {
		case SystemStateLookup, *SystemStateLookup:
			return cloneSystemState(state), nil
		default:
			return nil, errors.New("raftstore: unsupported System Group lookup")
		}
	}
	state, found, err := loadDataStateMetadata(snapshot, prefix)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("raftstore: active data-shard metadata is missing")
	}
	switch value := query.(type) {
	case DataLookup:
		return m.lookupData(snapshot, prefix, state, value)
	case *DataLookup:
		if value == nil {
			return nil, errors.New("raftstore: nil data lookup")
		}
		return m.lookupData(snapshot, prefix, state, *value)
	case DataStateLookup, *DataStateLookup:
		return cloneDataStateForLookup(state), nil
	case DataMutationLookup:
		return m.lookupDataMutation(snapshot, prefix, state, value)
	case *DataMutationLookup:
		if value == nil {
			return nil, errors.New("raftstore: nil data mutation lookup")
		}
		return m.lookupDataMutation(snapshot, prefix, state, *value)
	default:
		return nil, errors.New("raftstore: unsupported data-shard lookup")
	}
}

func (m *diskStateMachine) lookupEmpty(query any) (any, error) {
	if m.shardID == SystemRaftShardID {
		switch query.(type) {
		case SystemStateLookup, *SystemStateLookup:
			return SystemState{}, nil
		default:
			return nil, errors.New("raftstore: unsupported System Group lookup")
		}
	}
	state := DataState{}
	switch value := query.(type) {
	case DataLookup:
		return LookupData(state, value)
	case *DataLookup:
		if value == nil {
			return nil, errors.New("raftstore: nil data lookup")
		}
		return LookupData(state, *value)
	case DataStateLookup, *DataStateLookup:
		return state, nil
	case DataMutationLookup:
		return LookupDataMutation(state, value)
	case *DataMutationLookup:
		if value == nil {
			return nil, errors.New("raftstore: nil data mutation lookup")
		}
		return LookupDataMutation(state, *value)
	default:
		return nil, errors.New("raftstore: unsupported data-shard lookup")
	}
}

func (m *diskStateMachine) lookupDataMutation(
	reader pebble.Reader,
	prefix []byte,
	state DataState,
	query DataMutationLookup,
) (DataMutationStatus, error) {
	if err := query.Validate(); err != nil {
		return DataMutationStatus{}, err
	}
	command := query.Command
	switch command.Type {
	case DataPutRoute:
		key := routeMapKey(command.Route.Group, command.Route.RouteKey)
		var record clusterstate.RouteWorkflowRecord
		found, err := getStateJSON(reader, stateRowKey(prefix, stateRouteTable, key), &record)
		if err != nil {
			return DataMutationStatus{}, err
		}
		if found {
			state.Routes[key] = record
		}
	case DataPutBuild:
		key := buildMapKey(command.Build.Group, command.Build.BuildID)
		var record clusterstate.BuildRecord
		found, err := getStateJSON(reader, stateRowKey(prefix, stateBuildTable, key), &record)
		if err != nil {
			return DataMutationStatus{}, err
		}
		if found {
			state.Builds[key] = record
		}
	case DataPutFence:
		key := fenceMapKey(command.Fence.Group, command.Fence.RouteKey, command.Fence.SandboxID)
		var fence clusterstate.ExecutionFence
		found, err := getStateJSON(reader, stateRowKey(prefix, stateFenceTable, key), &fence)
		if err != nil {
			return DataMutationStatus{}, err
		}
		if found {
			state.Fences[key] = fence
		}
	}
	return LookupDataMutation(state, query)
}

func (m *diskStateMachine) lookupData(
	reader pebble.Reader,
	prefix []byte,
	state DataState,
	query DataLookup,
) (DataLookupResult, error) {
	if err := query.Validate(); err != nil {
		return DataLookupResult{}, err
	}
	switch {
	case query.Route != nil:
		key := routeMapKey(query.Route.Group, query.Route.RouteKey)
		var record clusterstate.RouteWorkflowRecord
		found, err := getStateJSON(reader, stateRowKey(prefix, stateRouteTable, key), &record)
		if err != nil {
			return DataLookupResult{}, err
		}
		if found {
			state.Routes[key] = record
		}
	case query.Build != nil:
		key := buildMapKey(query.Build.Group, query.Build.BuildID)
		var record clusterstate.BuildRecord
		found, err := getStateJSON(reader, stateRowKey(prefix, stateBuildTable, key), &record)
		if err != nil {
			return DataLookupResult{}, err
		}
		if found {
			state.Builds[key] = record
		}
	case query.Workflow != nil:
		if query.Workflow.RouteKey != "" {
			key := routeMapKey(query.Workflow.Group, query.Workflow.RouteKey)
			var record clusterstate.RouteWorkflowRecord
			found, err := getStateJSON(reader, stateRowKey(prefix, stateRouteTable, key), &record)
			if err != nil {
				return DataLookupResult{}, err
			}
			if found {
				state.Routes[key] = record
			}
		} else {
			key := buildMapKey(query.Workflow.Group, query.Workflow.BuildID)
			var record clusterstate.BuildRecord
			found, err := getStateJSON(reader, stateRowKey(prefix, stateBuildTable, key), &record)
			if err != nil {
				return DataLookupResult{}, err
			}
			if found {
				state.Builds[key] = record
			}
		}
	case query.RouteBucket != nil:
		bucket, err := lookupRouteBucketOnDisk(reader, prefix, state, *query.RouteBucket)
		if err != nil {
			return DataLookupResult{}, err
		}
		return DataLookupResult{RouteBucket: &bucket}, nil
	case query.Changefeed != nil:
		changefeed, err := lookupRouteChangefeedOnDisk(reader, prefix, state, *query.Changefeed)
		if err != nil {
			return DataLookupResult{}, err
		}
		return DataLookupResult{Changefeed: &changefeed}, nil
	case query.Fence != nil:
		key := fenceMapKey(query.Fence.Group, query.Fence.RouteKey, query.Fence.SandboxID)
		var fence clusterstate.ExecutionFence
		found, err := getStateJSON(reader, stateRowKey(prefix, stateFenceTable, key), &fence)
		if err != nil {
			return DataLookupResult{}, err
		}
		if found {
			state.Fences[key] = fence
		}
	case query.Pending != nil:
		pending, err := lookupPendingOnDisk(reader, prefix, state, *query.Pending)
		if err != nil {
			return DataLookupResult{}, err
		}
		return DataLookupResult{Pending: &pending}, nil
	}
	return LookupData(state, query)
}

func lookupRouteBucketOnDisk(
	reader pebble.Reader,
	prefix []byte,
	state DataState,
	query RouteBucketLookup,
) (RouteBucketResult, error) {
	result := RouteBucketResult{Group: query.Group, Bucket: query.Bucket}
	if !state.Initialized || !state.Accepts(query.Identity) {
		result.Reason = "Route shard identity is not available"
		return result, nil
	}
	if !routeBucketTargetsShard(state, query.Group, query.Bucket, query.Identity.ShardID) {
		result.Reason = "Route bucket targets another shard"
		return result, nil
	}
	result.Available = true
	result.SnapshotRevision = state.LastApplied
	result.Routes = make([]RouteBucketEntry, 0, int(query.Limit)+1)
	indexPrefix := routeBucketIndexPrefix(prefix, query.Group, query.Bucket)
	lowerBound := indexPrefix
	if query.AfterRouteKey != "" {
		lowerBound = append(append([]byte(nil), indexPrefix...), query.AfterRouteKey...)
	}
	iterator := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound, UpperBound: prefixUpperBound(indexPrefix),
	})
	defer iterator.Close()
	for valid := iterator.First(); valid; valid = iterator.Next() {
		if len(iterator.Value()) != 1 || iterator.Value()[0] != 1 {
			return RouteBucketResult{}, errors.New("raftstore: malformed Route bucket index row")
		}
		routeKey := string(iterator.Key()[len(indexPrefix):])
		if routeKey <= query.AfterRouteKey {
			continue
		}
		var record clusterstate.RouteWorkflowRecord
		mapKey := routeMapKey(query.Group, routeKey)
		found, err := getStateJSON(reader, stateRowKey(prefix, stateRouteTable, mapKey), &record)
		if err != nil {
			return RouteBucketResult{}, err
		}
		if !found || validateStoredRoute(state, mapKey, record) != nil || record.RouteKey != routeKey {
			return RouteBucketResult{}, errors.New("raftstore: Route bucket index differs from stored state")
		}
		bucket, _, err := clusterstate.RouteShardFor(
			record.Group, record.RouteKey, state.RouteBucketCount, state.VirtualShardCount,
		)
		entry, listed := routeBucketEntry(record)
		if err != nil || bucket != query.Bucket || !listed {
			return RouteBucketResult{}, errors.New("raftstore: Route bucket index differs from stored Route projection")
		}
		result.Routes = append(result.Routes, entry)
		if len(result.Routes) == int(query.Limit)+1 {
			break
		}
	}
	if err := iterator.Error(); err != nil {
		return RouteBucketResult{}, err
	}
	return finishRouteBucketPage(result, int(query.Limit))
}

func lookupRouteChangefeedOnDisk(
	reader pebble.Reader,
	prefix []byte,
	state DataState,
	query RouteChangefeedLookup,
) (RouteChangefeedResult, error) {
	result := RouteChangefeedResult{
		FloorRevision: state.RouteChangefeedFloor, HeadRevision: state.LastApplied,
		CursorRevision: query.AfterRevision, Changes: make([]RouteChange, 0),
	}
	if !state.Initialized || !state.Accepts(query.Identity) {
		result.Reason = "Route shard identity is not available"
		return result, nil
	}
	if !routeBucketTargetsShard(state, query.Group, query.Bucket, query.Identity.ShardID) {
		result.Reason = "Route bucket targets another shard"
		return result, nil
	}
	if query.AfterRevision > state.LastApplied {
		result.Reason = "Route changefeed position is ahead of local applied state"
		return result, nil
	}
	result.Available = true
	if query.AfterRevision < state.RouteChangefeedFloor {
		result.Reset = true
		result.CursorRevision = state.LastApplied
		return result, nil
	}
	if query.AfterRevision == ^uint64(0) {
		return result, nil
	}

	scanLimit := int(query.Limit) * 16
	if scanLimit < 256 {
		scanLimit = 256
	}
	if scanLimit > 16_384 {
		scanLimit = 16_384
	}
	tablePrefix := stateTablePrefix(prefix, stateRouteChangeTable)
	iterator := reader.NewIter(&pebble.IterOptions{
		LowerBound: routeChangeRowKey(prefix, query.AfterRevision+1),
		UpperBound: prefixUpperBound(tablePrefix),
	})
	defer iterator.Close()
	exhausted := true
	scanned := 0
	builder := newRouteChangefeedPageBuilder(query.Limit)
	for valid := iterator.First(); valid; valid = iterator.Next() {
		if scanned == scanLimit {
			exhausted = false
			break
		}
		var change RouteChange
		if err := decodeJSONValue(iterator.Value(), &change); err != nil {
			return RouteChangefeedResult{}, err
		}
		if err := validateRouteChange(state, change); err != nil {
			return RouteChangefeedResult{}, err
		}
		scanned++
		if change.Bucket != query.Bucket || change.Group != query.Group {
			result.CursorRevision = change.Revision
			continue
		}
		added, err := builder.add(change)
		if err != nil {
			return RouteChangefeedResult{}, err
		}
		if !added {
			exhausted = false
			break
		}
		result.CursorRevision = change.Revision
	}
	if err := iterator.Error(); err != nil {
		return RouteChangefeedResult{}, err
	}
	if exhausted {
		result.CursorRevision = state.LastApplied
	}
	result.Changes = builder.changes
	return result, nil
}

func routeChangeRowKey(prefix []byte, revision uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, revision)
	return stateRowKey(prefix, stateRouteChangeTable, string(key))
}

func lookupPendingOnDisk(
	reader pebble.Reader,
	prefix []byte,
	state DataState,
	query PendingLookup,
) (PendingLookupResult, error) {
	if !state.Accepts(query.Identity) {
		return PendingLookupResult{}, nil
	}
	afterKey, err := decodePendingCursor(query.AfterKey)
	if err != nil {
		return PendingLookupResult{}, err
	}
	builder := newPendingPageBuilder(query.Limit)
	tablePrefix := stateTablePrefix(prefix, statePendingTable)
	lowerBound := tablePrefix
	if afterKey != "" {
		lowerBound = stateRowKey(prefix, statePendingTable, afterKey)
	}
	iterator := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound, UpperBound: prefixUpperBound(tablePrefix),
	})
	defer iterator.Close()
	for valid := iterator.First(); valid; valid = iterator.Next() {
		qualified := string(iterator.Key()[len(tablePrefix):])
		if qualified <= afterKey {
			continue
		}
		if len(qualified) < 2 {
			return PendingLookupResult{}, errors.New("raftstore: malformed pending-workflow index key")
		}
		mapKey := qualified[1:]
		workflow := PendingWorkflow{Key: encodePendingCursor(qualified)}
		switch qualified[0] {
		case 'b':
			var record clusterstate.BuildRecord
			found, err := getStateJSON(reader, stateRowKey(prefix, stateBuildTable, mapKey), &record)
			if err != nil {
				return PendingLookupResult{}, err
			}
			if !found || validateStoredBuild(state, mapKey, record) != nil || !buildNeedsCoordinator(record) {
				return PendingLookupResult{}, errors.New("raftstore: pending Build index differs from stored state")
			}
			workflow.Build = &record
		case 'r':
			var record clusterstate.RouteWorkflowRecord
			found, err := getStateJSON(reader, stateRowKey(prefix, stateRouteTable, mapKey), &record)
			if err != nil {
				return PendingLookupResult{}, err
			}
			if !found || validateStoredRoute(state, mapKey, record) != nil || !routeNeedsCoordinator(record) {
				return PendingLookupResult{}, errors.New("raftstore: pending Route index differs from stored state")
			}
			workflow.Route = &record
		default:
			return PendingLookupResult{}, errors.New("raftstore: unknown pending-workflow index kind")
		}
		accepted, err := builder.add(workflow)
		if err != nil {
			return PendingLookupResult{}, err
		}
		if !accepted {
			break
		}
	}
	if err := iterator.Error(); err != nil {
		return PendingLookupResult{}, err
	}
	return builder.result(), nil
}

func pendingTableLowerBound(prefix, tablePrefix []byte, table, qualified byte, afterKey string) ([]byte, bool) {
	if afterKey == "" || qualified > afterKey[0] {
		return tablePrefix, true
	}
	if qualified < afterKey[0] {
		return nil, false
	}
	return stateRowKey(prefix, table, afterKey[1:]), true
}

func (m *diskStateMachine) Sync() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return errors.New("raftstore: on-disk state machine is closed")
	}
	return m.engine.sync()
}

type diskSnapshotContext struct {
	snapshot    *pebble.Snapshot
	prefix      []byte
	shardID     uint64
	lastApplied uint64
}

func (m *diskStateMachine) PrepareSnapshot() (any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, errors.New("raftstore: on-disk state machine is closed")
	}
	active := byte(m.activeSlot.Load())
	if active == 0 {
		return nil, errors.New("raftstore: cannot snapshot an empty state machine")
	}
	snapshot := m.engine.db.NewSnapshot()
	prefix := stateSlotPrefix(m.shardID, m.replicaID, active)
	lastApplied, err := m.loadLastApplied(snapshot, prefix)
	if err != nil {
		snapshot.Close()
		return nil, err
	}
	return &diskSnapshotContext{
		snapshot: snapshot, prefix: prefix, shardID: m.shardID, lastApplied: lastApplied,
	}, nil
}

func (m *diskStateMachine) SaveSnapshot(context any, writer io.Writer, done <-chan struct{}) error {
	snapshot, ok := context.(*diskSnapshotContext)
	if !ok || snapshot == nil || snapshot.snapshot == nil || snapshot.shardID != m.shardID {
		return errors.New("raftstore: invalid on-disk snapshot context")
	}
	defer snapshot.snapshot.Close()
	if err := snapshotStoppedOrClosed(done); err != nil {
		return err
	}
	if err := writeStateBytes(writer, stateSnapshotMagic[:]); err != nil {
		return err
	}
	if err := writeStateBytes(writer, encodeUint64(snapshot.shardID)); err != nil {
		return err
	}
	if err := writeStateBytes(writer, encodeUint64(snapshot.lastApplied)); err != nil {
		return err
	}
	iterator := snapshot.snapshot.NewIter(&pebble.IterOptions{
		LowerBound: snapshot.prefix, UpperBound: prefixUpperBound(snapshot.prefix),
	})
	defer iterator.Close()
	for valid := iterator.First(); valid; valid = iterator.Next() {
		if err := snapshotStoppedOrClosed(done); err != nil {
			return err
		}
		key := iterator.Key()[len(snapshot.prefix):]
		value := iterator.Value()
		if len(key) == 0 || len(key) > stateMaximumKeySize || len(value) > MaxRaftCommandBytes {
			return errors.New("raftstore: state snapshot record exceeds its bound")
		}
		if err := writeStateBytes(writer, encodeUint32(uint32(len(key)))); err != nil {
			return err
		}
		if err := writeStateBytes(writer, encodeUint32(uint32(len(value)))); err != nil {
			return err
		}
		if err := writeStateBytes(writer, key); err != nil {
			return err
		}
		if err := writeStateBytes(writer, value); err != nil {
			return err
		}
	}
	if err := iterator.Error(); err != nil {
		return err
	}
	return writeStateBytes(writer, make([]byte, 8))
}

func (m *diskStateMachine) RecoverFromSnapshot(reader io.Reader, done <-chan struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("raftstore: on-disk state machine is closed")
	}
	if err := snapshotStoppedOrClosed(done); err != nil {
		return err
	}
	magic := make([]byte, len(stateSnapshotMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return err
	}
	if !bytes.Equal(magic, stateSnapshotMagic[:]) {
		return errors.New("raftstore: invalid state snapshot magic")
	}
	header := make([]byte, 16)
	if _, err := io.ReadFull(reader, header); err != nil {
		return err
	}
	shardID := binary.BigEndian.Uint64(header[:8])
	lastApplied := binary.BigEndian.Uint64(header[8:])
	if shardID != m.shardID || lastApplied == 0 {
		return errors.New("raftstore: state snapshot belongs to another shard or has no applied index")
	}
	oldSlot := byte(m.activeSlot.Load())
	targetSlot := byte(1)
	if oldSlot == 1 {
		targetSlot = 2
	}
	targetPrefix := stateSlotPrefix(m.shardID, m.replicaID, targetSlot)
	if err := m.clearSnapshotSlot(targetPrefix); err != nil {
		return err
	}

	batch := m.engine.db.NewBatch()
	defer batch.Close()
	var (
		previousKey  []byte
		metadataSeen bool
		dataState    DataState
	)
	for {
		if err := snapshotStoppedOrClosed(done); err != nil {
			return err
		}
		lengths := make([]byte, 8)
		if _, err := io.ReadFull(reader, lengths); err != nil {
			return err
		}
		keyLength := binary.BigEndian.Uint32(lengths[:4])
		valueLength := binary.BigEndian.Uint32(lengths[4:])
		if keyLength == 0 {
			if valueLength != 0 {
				return errors.New("raftstore: malformed state snapshot terminator")
			}
			break
		}
		if keyLength > stateMaximumKeySize || valueLength > MaxRaftCommandBytes {
			return errors.New("raftstore: state snapshot record exceeds its bound")
		}
		key := make([]byte, keyLength)
		value := make([]byte, valueLength)
		if _, err := io.ReadFull(reader, key); err != nil {
			return err
		}
		if _, err := io.ReadFull(reader, value); err != nil {
			return err
		}
		if len(previousKey) != 0 && bytes.Compare(previousKey, key) >= 0 {
			return errors.New("raftstore: state snapshot keys are not strictly ordered")
		}
		previousKey = append(previousKey[:0], key...)
		if err := m.validateSnapshotRecord(key, value, lastApplied, &metadataSeen, &dataState); err != nil {
			return err
		}
		fullKey := append(append([]byte(nil), targetPrefix...), key...)
		if err := batch.Set(fullKey, value, nil); err != nil {
			return err
		}
		if len(batch.Repr()) >= stateRecoveryBatch {
			if err := m.engine.commitSync(batch); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	if !metadataSeen {
		return errors.New("raftstore: state snapshot has no metadata record")
	}
	trailing, err := io.ReadAll(io.LimitReader(reader, 1))
	if err != nil {
		return err
	}
	if len(trailing) != 0 {
		return errors.New("raftstore: state snapshot contains trailing bytes")
	}
	if !batch.Empty() {
		if err := m.engine.commitSync(batch); err != nil {
			return err
		}
		batch.Reset()
	}
	if err := batch.Set(stateControlKey(m.shardID, m.replicaID), encodeStateControl(stateControl{
		Version: stateEngineVersion, ActiveSlot: targetSlot,
	}), nil); err != nil {
		return err
	}
	if err := m.engine.commitSync(batch); err != nil {
		return err
	}
	m.activeSlot.Store(uint32(targetSlot))
	return nil
}

func (m *diskStateMachine) validateSnapshotRecord(
	key []byte,
	value []byte,
	lastApplied uint64,
	metadataSeen *bool,
	dataState *DataState,
) error {
	if len(key) == 0 {
		return errors.New("raftstore: empty state snapshot key")
	}
	table := key[0]
	mapKey := string(key[1:])
	if m.shardID == SystemRaftShardID {
		if table != stateMetadataTable || mapKey != "" || *metadataSeen {
			return errors.New("raftstore: System snapshot contains an unknown table")
		}
		var state SystemState
		if err := decodeJSONValue(value, &state); err != nil {
			return err
		}
		if err := state.Validate(); err != nil || state.LastApplied != lastApplied {
			return errors.New("raftstore: invalid System snapshot state")
		}
		*metadataSeen = true
		return nil
	}
	logicalShardID, ok := LogicalShardID(m.shardID)
	if !ok {
		return errors.New("raftstore: invalid data snapshot shard")
	}
	if table == stateMetadataTable {
		if mapKey != "" || *metadataSeen {
			return errors.New("raftstore: duplicate or malformed data snapshot metadata")
		}
		var metadata dataStateMetadata
		if err := decodeJSONValue(value, &metadata); err != nil {
			return err
		}
		state := metadata.dataState()
		if err := state.Validate(); err != nil || state.ShardID != logicalShardID || state.LastApplied != lastApplied {
			return errors.New("raftstore: invalid data snapshot metadata")
		}
		*dataState = state
		*metadataSeen = true
		return nil
	}
	if !*metadataSeen {
		return errors.New("raftstore: data snapshot row precedes metadata")
	}
	switch table {
	case stateRouteTable:
		var record clusterstate.RouteWorkflowRecord
		if err := decodeJSONValue(value, &record); err != nil {
			return err
		}
		return validateStoredRoute(*dataState, mapKey, record)
	case stateBuildTable:
		var record clusterstate.BuildRecord
		if err := decodeJSONValue(value, &record); err != nil {
			return err
		}
		return validateStoredBuild(*dataState, mapKey, record)
	case stateFenceTable:
		var fence clusterstate.ExecutionFence
		if err := decodeJSONValue(value, &fence); err != nil {
			return err
		}
		return validateStoredFence(*dataState, mapKey, fence)
	case stateRouteChangeTable:
		if len(mapKey) != 8 {
			return errors.New("raftstore: malformed Route change snapshot key")
		}
		var change RouteChange
		if err := decodeJSONValue(value, &change); err != nil {
			return err
		}
		if change.Revision != binary.BigEndian.Uint64([]byte(mapKey)) ||
			change.Revision <= dataState.RouteChangefeedFloor || change.Revision > lastApplied {
			return errors.New("raftstore: Route change snapshot key differs from its revision")
		}
		return validateRouteChange(*dataState, change)
	case statePendingTable:
		if len(mapKey) < 2 || mapKey[0] != 'b' && mapKey[0] != 'r' || len(value) != 1 || value[0] != 1 {
			return errors.New("raftstore: malformed pending-workflow snapshot row")
		}
		return nil
	case stateRouteBucketTable:
		group, bucket, routeKey, err := decodeRouteBucketIndexMapKey(mapKey)
		if err != nil || len(value) != 1 || value[0] != 1 ||
			!routeBucketTargetsShard(*dataState, group, bucket, logicalShardID) {
			return errors.New("raftstore: malformed Route bucket index snapshot row")
		}
		expectedBucket, shardID, hashErr := clusterstate.RouteShardFor(
			group, routeKey, dataState.RouteBucketCount, dataState.VirtualShardCount,
		)
		if hashErr != nil || expectedBucket != bucket || shardID != logicalShardID {
			return errors.New("raftstore: Route bucket index snapshot row belongs to another shard")
		}
		return nil
	case stateSandboxIDTable:
		if len(value) != 1 || value[0] != 1 || validateUsedSandboxID(*dataState, mapKey) != nil {
			return errors.New("raftstore: malformed used Sandbox ID snapshot row")
		}
		return nil
	default:
		return errors.New("raftstore: data snapshot contains an unknown table")
	}
}

func decodeRouteBucketIndexMapKey(key string) (string, uint32, string, error) {
	raw := []byte(key)
	if len(raw) < 8 {
		return "", 0, "", errors.New("raftstore: truncated Route bucket index key")
	}
	groupLength := binary.BigEndian.Uint32(raw[:4])
	raw = raw[4:]
	if groupLength == 0 || uint64(groupLength)+4 >= uint64(len(raw)) {
		return "", 0, "", errors.New("raftstore: malformed Route bucket index key")
	}
	group := string(raw[:groupLength])
	raw = raw[groupLength:]
	bucket := binary.BigEndian.Uint32(raw[:4])
	routeKey := string(raw[4:])
	return group, bucket, routeKey, nil
}

func (m *diskStateMachine) clearSnapshotSlot(prefix []byte) error {
	batch := m.engine.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(prefix, prefixUpperBound(prefix), nil); err != nil {
		return err
	}
	if err := m.engine.commitSync(batch); err != nil {
		return err
	}
	return nil
}

func (m *diskStateMachine) loadLastApplied(reader pebble.Reader, prefix []byte) (uint64, error) {
	if m.shardID == SystemRaftShardID {
		state, found, err := loadSystemState(reader, prefix)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, errors.New("raftstore: System state metadata is missing")
		}
		return state.LastApplied, nil
	}
	state, found, err := loadDataStateMetadata(reader, prefix)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, errors.New("raftstore: data state metadata is missing")
	}
	logicalShardID, ok := LogicalShardID(m.shardID)
	if !ok || state.ShardID != logicalShardID {
		return 0, errors.New("raftstore: data state metadata belongs to another shard")
	}
	return state.LastApplied, nil
}

func (m *diskStateMachine) validateIdentity() error {
	if m.engine == nil || m.engine.db == nil || m.replicaID == 0 || m.shardID == 0 {
		return errors.New("raftstore: incomplete on-disk state-machine identity")
	}
	if m.shardID != SystemRaftShardID {
		if _, ok := LogicalShardID(m.shardID); !ok {
			return errors.New("raftstore: invalid data state-machine shard")
		}
	}
	return nil
}

func (m *diskStateMachine) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}
