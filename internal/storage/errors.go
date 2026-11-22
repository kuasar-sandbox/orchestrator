package storage

import "errors"

var (
	ErrRetry       = errors.New("storage: transaction definitely aborted; retry permitted")
	ErrUnknown     = errors.New("storage: commit outcome unknown")
	ErrUnavailable = errors.New("storage: unavailable")
	ErrCapacity    = errors.New("storage: capacity exhausted")
	ErrInvalid     = errors.New("storage: invalid argument or record")
)

// Unknown intentionally does not unwrap a driver error: a generic retry loop
// must not reinterpret an uncertain commit as a definitely aborted operation.
type Unknown struct{ Cause error }

func (e *Unknown) Error() string        { return ErrUnknown.Error() + ": " + e.Cause.Error() }
func (e *Unknown) Is(target error) bool { return target == ErrUnknown }

type Unavailable struct{ Cause error }

func (e *Unavailable) Error() string        { return ErrUnavailable.Error() + ": " + e.Cause.Error() }
func (e *Unavailable) Is(target error) bool { return target == ErrUnavailable }

type CapacityError struct{ Resource string }

func (e *CapacityError) Error() string        { return ErrCapacity.Error() + ": " + e.Resource }
func (e *CapacityError) Is(target error) bool { return target == ErrCapacity }
