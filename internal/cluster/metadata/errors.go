package metadata

import "errors"

var (
	ErrOwnershipConflict = errors.New("metadata identity is owned by another record")
	ErrConflict          = errors.New("metadata: condition failed")
	ErrUnknown           = errors.New("metadata: commit outcome unknown")
	ErrUnavailable       = errors.New("metadata: storage unavailable")
	ErrCapacity          = errors.New("metadata: capacity exhausted")
	ErrStaleLease        = errors.New("metadata: stale import source lease")
	ErrInvalidRecord     = errors.New("metadata: invalid record")
)

type Conflict struct{ Expected, Actual Version }

func (e *Conflict) Error() string        { return ErrConflict.Error() }
func (e *Conflict) Is(target error) bool { return target == ErrConflict }

// Unknown deliberately does not unwrap the native error. A generic retry loop
// must not turn an uncertain commit into a repeated business mutation.
type Unknown struct{ Cause error }

func (e *Unknown) Error() string        { return ErrUnknown.Error() + ": " + e.Cause.Error() }
func (e *Unknown) Is(target error) bool { return target == ErrUnknown }

type Unavailable struct{ Cause error }

func (e *Unavailable) Error() string        { return ErrUnavailable.Error() + ": " + e.Cause.Error() }
func (e *Unavailable) Is(target error) bool { return target == ErrUnavailable }

type CapacityError struct{ Resource string }

func (e *CapacityError) Error() string        { return ErrCapacity.Error() + ": " + e.Resource }
func (e *CapacityError) Is(target error) bool { return target == ErrCapacity }
