package metastore

import (
	"context"
	"crypto/rand"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

type publicListCursorKey struct {
	Key []byte `json:"key"`
}

func (s *Store) publicListCursorKeyRecord() records.Key {
	return s.key("public-list-cursor-key", "v1")
}

func decodePublicListCursorKey(value publicListCursorKey) (key [32]byte, err error) {
	if len(value.Key) != len(key) {
		return key, invalidRecord("public list cursor encryption key")
	}
	copy(key[:], value.Key)
	return key, nil
}

func (s *Store) readPublicListCursorKey(ctx context.Context) (key [32]byte, exists bool, err error) {
	err = s.read(ctx, func(tr *records.Tx) error {
		stored, err := readTyped[publicListCursorKey](tr, s.publicListCursorKeyRecord())
		if err != nil {
			return err
		}
		exists = stored.Exists
		if exists {
			key, err = decodePublicListCursorKey(stored.Value)
			clear(stored.Value.Key)
		}
		return err
	})
	return
}

// GetOrCreatePublicListCursorKey performs one namespace-wide initialization.
// Existing keys are read-only, including corrupt keys (which fail closed).
// An unknown commit is never replayed or returned as a successful local key;
// the next independent call reads the durable outcome before considering a write.
func (s *Store) GetOrCreatePublicListCursorKey(ctx context.Context) ([32]byte, error) {
	key, exists, err := s.readPublicListCursorKey(ctx)
	if err != nil || exists {
		return key, err
	}
	var candidate [32]byte
	if _, err := rand.Read(candidate[:]); err != nil {
		return [32]byte{}, err
	}
	defer clear(candidate[:])
	_, err = s.mutate(ctx, func(m *mutation) (bool, error) {
		stored, err := getTyped[publicListCursorKey](m, s.publicListCursorKeyRecord())
		if err != nil {
			return false, err
		}
		if stored.Exists {
			key, err = decodePublicListCursorKey(stored.Value)
			clear(stored.Value.Key)
			return false, err
		}
		revisions, err := m.tr.Revisions([]records.Key{s.publicListCursorKeyRecord()})
		if err != nil {
			return false, err
		}
		if !revisions[0].IsZero() {
			// Absence after a prior initialization is lost key material, not
			// permission to silently invalidate every existing continuation.
			return false, invalidRecord("public list cursor encryption key was removed")
		}
		key = candidate
		return true, putJSON(m.tr, s.publicListCursorKeyRecord(), publicListCursorKey{Key: candidate[:]})
	})
	if errors.Is(err, metadata.ErrConflict) {
		// A concurrent initialization changed our pinned absent revision. Read
		// its winner without replaying a write against a new precondition.
		key, exists, err = s.readPublicListCursorKey(ctx)
		if err == nil && !exists {
			return [32]byte{}, metadata.ErrConflict
		}
	}
	if err != nil {
		clear(key[:])
		return [32]byte{}, err
	}
	return key, nil
}
