package metadata

import (
	"errors"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

var (
	ErrOwnershipConflict = errors.New("metadata identity is owned by another record")
	ErrConflict          = record.ErrConflict
	ErrUnknown           = storage.ErrUnknown
	ErrUnavailable       = storage.ErrUnavailable
	ErrCapacity          = storage.ErrCapacity
	ErrStaleLease        = errors.New("metadata: stale import source lease")
	ErrInvalidRecord     = storage.ErrInvalid
	ErrInvalidCursor     = errors.New("metadata: invalid cursor")
)

type Conflict = record.Conflict
type Unknown = storage.Unknown
type Unavailable = storage.Unavailable
type CapacityError = storage.CapacityError
