package metastore

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func (s *Store) importKey(sourceID string) records.Key {
	return mainKey(s.key("import-source"), sourceID)
}
func leaseMatches(v metadata.PlacerImportSourceState, ownerID, runID string, term uint64, now int64) bool {
	return v.OwnerID == ownerID && v.RunID == runID && v.Term == term && (v.ExpiresUnixMs <= 0 || v.ExpiresUnixMs > now)
}
func (s *Store) Acquire(ctx context.Context, sourceID, ownerID, runID string, ttl time.Duration) (metadata.PlacerImportSourceState, bool, error) {
	if err := validateParts(sourceID, ownerID, runID); err != nil {
		return metadata.PlacerImportSourceState{}, false, err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	now := time.Now().UnixMilli()
	var out metadata.PlacerImportSourceState
	acquired := false
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		current, err := getTyped[metadata.PlacerImportSourceState](m, s.importKey(sourceID))
		if err != nil {
			return false, err
		}
		out = current.Value
		acquired = false
		if current.Exists && (out.ExpiresUnixMs <= 0 || out.ExpiresUnixMs > now) && out.OwnerID != "" && (out.OwnerID != ownerID || out.RunID != runID) {
			return false, nil
		}
		if !current.Exists || out.OwnerID != ownerID || out.RunID != runID {
			out.Term++
			if out.Term == 0 {
				out.Term = 1
			}
		}
		out.SourceID = sourceID
		out.OwnerID = ownerID
		out.RunID = runID
		out.ExpiresUnixMs = now + ttl.Milliseconds()
		acquired = true
		return true, putJSON(m.tr, s.importKey(sourceID), out)
	})
	if err != nil {
		return metadata.PlacerImportSourceState{}, false, err
	}
	return out, acquired, nil
}
func (s *Store) Check(ctx context.Context, sourceID, ownerID, runID string, term uint64) (bool, error) {
	if err := validateParts(sourceID, ownerID, runID); err != nil {
		return false, err
	}
	if term == 0 {
		return false, nil
	}
	matched := false
	err := s.read(ctx, func(tr *records.Tx) error {
		current, err := readTyped[metadata.PlacerImportSourceState](tr, s.importKey(sourceID))
		if err != nil {
			return err
		}
		matched = current.Exists && current.Value.SourceID == sourceID && leaseMatches(current.Value, ownerID, runID, term, time.Now().UnixMilli())
		return nil
	})
	return matched, err
}
func (s *Store) Checkpoint(ctx context.Context, sourceID, ownerID, runID string, term uint64, cursor string, complete bool, lastError string) (metadata.PlacerImportSourceState, error) {
	if err := validateParts(sourceID, ownerID, runID); err != nil {
		return metadata.PlacerImportSourceState{}, err
	}
	if term == 0 {
		return metadata.PlacerImportSourceState{}, metadata.ErrStaleLease
	}
	now := time.Now().UnixMilli()
	var out metadata.PlacerImportSourceState
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		current, err := getTyped[metadata.PlacerImportSourceState](m, s.importKey(sourceID))
		if err != nil {
			return false, err
		}
		if !current.Exists || current.Value.SourceID != sourceID || !leaseMatches(current.Value, ownerID, runID, term, now) {
			return false, metadata.ErrStaleLease
		}
		out = current.Value
		out.LastError = lastError
		if complete {
			out.Cursor = ""
			out.Round++
		} else {
			out.Cursor = cursor
		}
		return true, putJSON(m.tr, s.importKey(sourceID), out)
	})
	if err != nil {
		return metadata.PlacerImportSourceState{}, err
	}
	return out, nil
}
