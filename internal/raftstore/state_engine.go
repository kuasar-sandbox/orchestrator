package raftstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const (
	stateEngineVersion    = uint32(2)
	stateControlTable     = byte(0)
	stateSlotTable        = byte(1)
	stateMetadataTable    = byte(0)
	stateRouteTable       = byte(1)
	stateBuildTable       = byte(2)
	stateFenceTable       = byte(3)
	stateRouteChangeTable = byte(4)
	stateRecoveryBatch    = 16 << 20
	stateMaximumKeySize   = MaxRaftCommandBytes
)

var stateKeyMagic = [...]byte{'K', 'U', 'A', 'S', 'A', 'R', 'S', 'M', 1}

type StateEngineTuning struct {
	BlockCacheBytes         uint64 `json:"block_cache_bytes"`
	MemTableBytes           uint64 `json:"memtable_bytes"`
	MemTableCount           uint64 `json:"memtable_count"`
	MaxConcurrentCompaction uint64 `json:"max_concurrent_compaction"`
	MaxOpenFiles            uint64 `json:"max_open_files"`
}

func DefaultStateEngineTuning() StateEngineTuning {
	return StateEngineTuning{
		BlockCacheBytes: 512 << 20, MemTableBytes: 32 << 20, MemTableCount: 4,
		MaxConcurrentCompaction: 4, MaxOpenFiles: 8192,
	}
}

func (t StateEngineTuning) Validate() error {
	maximumInt := uint64(^uint(0) >> 1)
	if t.BlockCacheBytes < 8<<20 || t.BlockCacheBytes > 64<<30 ||
		t.MemTableBytes < 4<<20 || t.MemTableBytes > 1<<30 ||
		t.MemTableCount < 2 || t.MemTableCount > 16 ||
		t.MaxConcurrentCompaction == 0 || t.MaxConcurrentCompaction > 32 ||
		t.MaxOpenFiles < 1000 || t.MaxOpenFiles > 1<<20 ||
		t.BlockCacheBytes > uint64(^uint64(0)>>1) || t.MemTableBytes > maximumInt ||
		t.MemTableCount > maximumInt || t.MaxConcurrentCompaction > maximumInt || t.MaxOpenFiles > maximumInt {
		return errors.New("raftstore: invalid bounded Pebble state-engine tuning")
	}
	return nil
}

type PebbleStateEngine struct {
	db *pebble.DB

	syncMu          sync.Mutex
	writeGeneration uint64
	syncGeneration  uint64
	closed          bool
}

func OpenPebbleStateEngine(path string, configured ...StateEngineTuning) (*PebbleStateEngine, error) {
	if path == "" || !filepath.IsAbs(path) {
		return nil, errors.New("raftstore: Pebble state-engine path must be absolute")
	}
	if len(configured) > 1 {
		return nil, errors.New("raftstore: multiple Pebble state-engine tuning values supplied")
	}
	tuning := DefaultStateEngineTuning()
	if len(configured) == 1 {
		tuning = configured[0]
	}
	if err := tuning.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	blockCache := pebble.NewCache(int64(tuning.BlockCacheBytes))
	database, err := pebble.Open(path, &pebble.Options{
		BytesPerSync: 1 << 20, WALBytesPerSync: 1 << 20,
		Cache: blockCache, MemTableSize: int(tuning.MemTableBytes),
		MemTableStopWritesThreshold: int(tuning.MemTableCount), MaxOpenFiles: int(tuning.MaxOpenFiles),
		MaxConcurrentCompactions: func() int { return int(tuning.MaxConcurrentCompaction) },
		FormatMajorVersion:       pebble.FormatMostCompatible,
		Levels:                   []pebble.LevelOptions{{FilterPolicy: bloom.FilterPolicy(10), FilterType: pebble.TableFilter}},
	})
	blockCache.Unref()
	if err != nil {
		return nil, fmt.Errorf("raftstore: open shared Pebble state engine: %w", err)
	}
	return &PebbleStateEngine{db: database}, nil
}

func (e *PebbleStateEngine) NewStateMachine(shardID, replicaID uint64) sm.IOnDiskStateMachine {
	return &diskStateMachine{engine: e, shardID: shardID, replicaID: replicaID}
}

func (e *PebbleStateEngine) commitNoSync(batch *pebble.Batch) error {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	if e.closed {
		return errors.New("raftstore: Pebble state engine is closed")
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	e.writeGeneration++
	return nil
}

func (e *PebbleStateEngine) commitSync(batch *pebble.Batch) error {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	if e.closed {
		return errors.New("raftstore: Pebble state engine is closed")
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	e.writeGeneration++
	e.syncGeneration = e.writeGeneration
	return nil
}

func (e *PebbleStateEngine) sync() error {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	if e.closed {
		return errors.New("raftstore: Pebble state engine is closed")
	}
	if e.syncGeneration == e.writeGeneration {
		return nil
	}
	if err := e.db.LogData([]byte("kuasar-state-engine-sync-v1"), pebble.Sync); err != nil {
		return err
	}
	e.syncGeneration = e.writeGeneration
	return nil
}

func (e *PebbleStateEngine) RemoveReplica(shardID, replicaID uint64) error {
	root := stateReplicaRoot(shardID, replicaID)
	batch := e.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(root, prefixUpperBound(root), nil); err != nil {
		return err
	}
	if err := e.commitSync(batch); err != nil {
		return err
	}
	return nil
}

func (e *PebbleStateEngine) Close() error {
	if e == nil || e.db == nil {
		return nil
	}
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	if e.closed {
		return nil
	}
	if e.syncGeneration != e.writeGeneration {
		if err := e.db.LogData([]byte("kuasar-state-engine-close-v1"), pebble.Sync); err != nil {
			return err
		}
		e.syncGeneration = e.writeGeneration
	}
	e.closed = true
	return e.db.Close()
}

type stateControl struct {
	Version    uint32
	ActiveSlot byte
}

func (c stateControl) Validate() error {
	if c.Version != stateEngineVersion || c.ActiveSlot != 1 && c.ActiveSlot != 2 {
		return errors.New("raftstore: invalid state-engine control record")
	}
	return nil
}

func encodeStateControl(control stateControl) []byte {
	raw := make([]byte, 5)
	binary.BigEndian.PutUint32(raw[:4], control.Version)
	raw[4] = control.ActiveSlot
	return raw
}

func decodeStateControl(raw []byte) (stateControl, error) {
	if len(raw) != 5 {
		return stateControl{}, errors.New("raftstore: malformed state-engine control record")
	}
	control := stateControl{Version: binary.BigEndian.Uint32(raw[:4]), ActiveSlot: raw[4]}
	return control, control.Validate()
}

type dataStateMetadata struct {
	Initialized          bool             `json:"initialized"`
	ClusterID            string           `json:"cluster_id"`
	RegistryGeneration   string           `json:"registry_generation"`
	ShardID              uint32           `json:"shard_id"`
	SchemaVersion        uint32           `json:"schema_version"`
	ProtocolVersion      uint32           `json:"protocol_version"`
	HashVersion          string           `json:"hash_version"`
	RouteBucketCount     uint32           `json:"route_bucket_count"`
	BuildBucketCount     uint32           `json:"build_bucket_count"`
	VirtualShardCount    uint32           `json:"virtual_shard_count"`
	ReplicaIDs           []uint64         `json:"replica_ids"`
	PreparedReplicaIDs   []uint64         `json:"prepared_replica_ids,omitempty"`
	ServingEpochs        []PermitIdentity `json:"serving_epochs"`
	RouteChangefeedFloor uint64           `json:"route_changefeed_floor"`
	LastApplied          uint64           `json:"last_applied"`
}

func metadataFromDataState(state DataState) dataStateMetadata {
	return dataStateMetadata{
		Initialized: state.Initialized, ClusterID: state.ClusterID,
		RegistryGeneration: state.RegistryGeneration, ShardID: state.ShardID,
		SchemaVersion: state.SchemaVersion, ProtocolVersion: state.ProtocolVersion,
		HashVersion: state.HashVersion, RouteBucketCount: state.RouteBucketCount,
		BuildBucketCount: state.BuildBucketCount, VirtualShardCount: state.VirtualShardCount,
		ReplicaIDs:           append([]uint64(nil), state.ReplicaIDs...),
		PreparedReplicaIDs:   append([]uint64(nil), state.PreparedReplicaIDs...),
		ServingEpochs:        append([]PermitIdentity(nil), state.ServingEpochs...),
		RouteChangefeedFloor: state.RouteChangefeedFloor, LastApplied: state.LastApplied,
	}
}

func (m dataStateMetadata) dataState() DataState {
	return DataState{
		Initialized: m.Initialized, ClusterID: m.ClusterID, RegistryGeneration: m.RegistryGeneration,
		ShardID: m.ShardID, SchemaVersion: m.SchemaVersion, ProtocolVersion: m.ProtocolVersion,
		HashVersion: m.HashVersion, RouteBucketCount: m.RouteBucketCount,
		BuildBucketCount: m.BuildBucketCount, VirtualShardCount: m.VirtualShardCount,
		ReplicaIDs:           append([]uint64(nil), m.ReplicaIDs...),
		PreparedReplicaIDs:   append([]uint64(nil), m.PreparedReplicaIDs...),
		ServingEpochs:        append([]PermitIdentity(nil), m.ServingEpochs...),
		RouteChangefeedFloor: m.RouteChangefeedFloor,
		Routes:               make(map[string]clusterstate.RouteWorkflowRecord),
		Builds:               make(map[string]clusterstate.BuildRecord),
		Fences:               make(map[string]clusterstate.ExecutionFence), LastApplied: m.LastApplied,
	}
}

func stateReplicaRoot(shardID, replicaID uint64) []byte {
	root := make([]byte, len(stateKeyMagic)+16)
	copy(root, stateKeyMagic[:])
	binary.BigEndian.PutUint64(root[len(stateKeyMagic):], shardID)
	binary.BigEndian.PutUint64(root[len(stateKeyMagic)+8:], replicaID)
	return root
}

func stateControlKey(shardID, replicaID uint64) []byte {
	return append(stateReplicaRoot(shardID, replicaID), stateControlTable)
}

func stateSlotPrefix(shardID, replicaID uint64, slot byte) []byte {
	prefix := stateReplicaRoot(shardID, replicaID)
	return append(prefix, stateSlotTable, slot)
}

func stateTablePrefix(slotPrefix []byte, table byte) []byte {
	return append(append([]byte(nil), slotPrefix...), table)
}

func stateRowKey(slotPrefix []byte, table byte, key string) []byte {
	result := make([]byte, 0, len(slotPrefix)+1+len(key))
	result = append(result, slotPrefix...)
	result = append(result, table)
	result = append(result, key...)
	return result
}

func prefixUpperBound(prefix []byte) []byte {
	upper := append([]byte(nil), prefix...)
	for index := len(upper) - 1; index >= 0; index-- {
		if upper[index] != 0xff {
			upper[index]++
			return upper[:index+1]
		}
	}
	panic("raftstore: state key prefix has no upper bound")
}

func getStateValue(reader pebble.Reader, key []byte) ([]byte, bool, error) {
	value, closer, err := reader.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return append([]byte(nil), value...), true, nil
}

func getStateJSON(reader pebble.Reader, key []byte, target any) (bool, error) {
	raw, found, err := getStateValue(reader, key)
	if err != nil || !found {
		return found, err
	}
	if err := unmarshalStrictBounded(raw, target, MaxRaftCommandBytes); err != nil {
		return false, err
	}
	return true, nil
}

func setStateJSON(batch *pebble.Batch, key []byte, value any) error {
	raw, err := marshalBounded(value, MaxRaftCommandBytes)
	if err != nil {
		return err
	}
	return batch.Set(key, raw, nil)
}

func loadStateControl(reader pebble.Reader, shardID, replicaID uint64) (stateControl, bool, error) {
	raw, found, err := getStateValue(reader, stateControlKey(shardID, replicaID))
	if err != nil || !found {
		return stateControl{}, found, err
	}
	control, err := decodeStateControl(raw)
	return control, err == nil, err
}

func loadSystemState(reader pebble.Reader, prefix []byte) (SystemState, bool, error) {
	var state SystemState
	found, err := getStateJSON(reader, stateRowKey(prefix, stateMetadataTable, ""), &state)
	if err != nil || !found {
		return SystemState{}, found, err
	}
	return state, true, state.Validate()
}

func loadDataStateMetadata(reader pebble.Reader, prefix []byte) (DataState, bool, error) {
	var metadata dataStateMetadata
	found, err := getStateJSON(reader, stateRowKey(prefix, stateMetadataTable, ""), &metadata)
	if err != nil || !found {
		return DataState{}, found, err
	}
	state := metadata.dataState()
	return state, true, state.Validate()
}

func snapshotStoppedOrClosed(done <-chan struct{}) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
		return nil
	}
}

func writeStateBytes(writer io.Writer, raw []byte) error {
	for len(raw) > 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}

func encodeUint32(value uint32) []byte {
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, value)
	return raw
}

func encodeUint64(value uint64) []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, value)
	return raw
}

func decodeJSONValue(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > MaxRaftCommandBytes {
		return errors.New("raftstore: state value exceeds its bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("raftstore: state value contains trailing JSON")
	}
	return nil
}
