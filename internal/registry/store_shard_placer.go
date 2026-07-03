package registry

import (
	"context"
	"errors"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
)

func (s *Stores) acquirePlacerImportSourceShard(ctx context.Context, sourceID, ownerID, runID string, ttl time.Duration) (clusterstate.PlacerImportSourceState, bool, error) {
	if sourceID == "" || ownerID == "" || runID == "" {
		return clusterstate.PlacerImportSourceState{}, false, errors.New("registry: source_id, owner_id and run_id are required")
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	sh, err := s.scaleImportSourceRecordSet(sourceID)
	if err != nil {
		return clusterstate.PlacerImportSourceState{}, false, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		curRec, found, err := sh.Get(ctx, clusterstate.PlacerLinkStateRecord)
		if err != nil {
			return clusterstate.PlacerImportSourceState{}, false, err
		}
		var cur clusterstate.PlacerImportSourceState
		if found {
			cur, err = clusterstate.DecodeShardValue[clusterstate.PlacerImportSourceState](curRec.Value)
			if err != nil {
				return clusterstate.PlacerImportSourceState{}, false, err
			}
		}
		now := time.Now()
		if found && scaleImportLeaseLiveForOther(cur, ownerID, runID, now) {
			return cur, false, nil
		}
		next := cur
		if !found || cur.OwnerID != ownerID || cur.RunID != runID {
			next.Term++
			if next.Term == 0 {
				next.Term = 1
			}
		}
		next.SourceID = sourceID
		next.OwnerID = ownerID
		next.RunID = runID
		next.ExpiresUnixMs = now.Add(ttl).UnixMilli()
		value, err := clusterstate.EncodeShardValue(next)
		if err != nil {
			return clusterstate.PlacerImportSourceState{}, false, err
		}
		expect := uint64(0)
		if found {
			expect = curRec.Meta.Rev
		}
		_, ok, err := sh.CAS(ctx, clusterstate.PlacerLinkStateRecord, expect, value)
		if err != nil {
			return clusterstate.PlacerImportSourceState{}, false, err
		}
		if ok {
			return next, true, nil
		}
	}
	return clusterstate.PlacerImportSourceState{}, false, shardkv.ErrConflict
}

func (s *Stores) checkPlacerImportSourceShard(ctx context.Context, sourceID, ownerID, runID string, term uint64) bool {
	if sourceID == "" || ownerID == "" || runID == "" || term == 0 {
		return false
	}
	sh, err := s.scaleImportSourceRecordSet(sourceID)
	if err != nil {
		return false
	}
	rec, found, err := sh.Get(ctx, clusterstate.PlacerLinkStateRecord)
	if err != nil || !found {
		return false
	}
	state, err := clusterstate.DecodeShardValue[clusterstate.PlacerImportSourceState](rec.Value)
	if err != nil {
		return false
	}
	return state.OwnerID == ownerID && state.RunID == runID && state.Term == term &&
		(state.ExpiresUnixMs <= 0 || state.ExpiresUnixMs > time.Now().UnixMilli())
}

func (s *Stores) checkpointPlacerImportSourceShard(ctx context.Context, sourceID, ownerID, runID string, term uint64, cursor string, complete bool, lastErr string) (clusterstate.PlacerImportSourceState, error) {
	if sourceID == "" || ownerID == "" || runID == "" || term == 0 {
		return clusterstate.PlacerImportSourceState{}, errors.New("registry: source_id, owner_id, run_id and term are required")
	}
	sh, err := s.scaleImportSourceRecordSet(sourceID)
	if err != nil {
		return clusterstate.PlacerImportSourceState{}, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		curRec, found, err := sh.Get(ctx, clusterstate.PlacerLinkStateRecord)
		if err != nil {
			return clusterstate.PlacerImportSourceState{}, err
		}
		if !found {
			return clusterstate.PlacerImportSourceState{}, errPlacerLinkStaleLease
		}
		cur, err := clusterstate.DecodeShardValue[clusterstate.PlacerImportSourceState](curRec.Value)
		if err != nil {
			return clusterstate.PlacerImportSourceState{}, err
		}
		if cur.OwnerID != ownerID || cur.RunID != runID || cur.Term != term ||
			(cur.ExpiresUnixMs > 0 && cur.ExpiresUnixMs <= time.Now().UnixMilli()) {
			return clusterstate.PlacerImportSourceState{}, errPlacerLinkStaleLease
		}
		next := cur
		next.SourceID = sourceID
		next.LastError = lastErr
		if complete {
			next.Cursor = ""
			next.Round++
		} else {
			next.Cursor = cursor
		}
		value, err := clusterstate.EncodeShardValue(next)
		if err != nil {
			return clusterstate.PlacerImportSourceState{}, err
		}
		_, ok, err := sh.CAS(ctx, clusterstate.PlacerLinkStateRecord, curRec.Meta.Rev, value)
		if err != nil {
			return clusterstate.PlacerImportSourceState{}, err
		}
		if ok {
			return next, nil
		}
	}
	return clusterstate.PlacerImportSourceState{}, shardkv.ErrConflict
}

func (s *Stores) scaleImportSourceShard(sourceID string) (*shardkv.Shard, error) {
	store := s.ShardStore()
	if store == nil {
		return nil, errors.New("registry: shard store is not initialized")
	}
	return store.Shard(shardkv.Namespace(clusterstate.NamespacePlacerLink), clusterstate.PlacerImportSourceShard(sourceID))
}

func (s *Stores) scaleImportSourceRecordSet(sourceID string) (*shardkv.RecordSet, error) {
	sh, err := s.scaleImportSourceShard(sourceID)
	if err != nil {
		return nil, err
	}
	return sh.RecordSet(clusterstate.RecordSetPlacerImport)
}

func scaleImportLeaseLiveForOther(cur clusterstate.PlacerImportSourceState, ownerID, runID string, now time.Time) bool {
	if cur.ExpiresUnixMs > 0 && cur.ExpiresUnixMs <= now.UnixMilli() {
		return false
	}
	return cur.OwnerID != "" && (cur.OwnerID != ownerID || cur.RunID != runID)
}
