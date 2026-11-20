//go:build fdb && cgo

package fdbdriver

import (
	"context"
	"errors"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/observebudget"
)

type reservationKey struct{}
type reservation struct {
	owner *databaseOwner
	slot  *observebudget.Reservation
}

func (s *Store) ReserveObservation(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if held, ok := ctx.Value(reservationKey{}).(*reservation); ok && held.owner == s.owner {
		return ctx, func() {}, nil
	}
	child, opRelease, err := s.operation(ctx)
	if err != nil {
		return nil, nil, err
	}
	release, err := s.owner.budget.TryAcquire()
	if err != nil {
		opRelease()
		return nil, nil, err
	}
	slot := observebudget.NewReservation(release)
	child, cancel := context.WithCancel(child)
	child = context.WithValue(child, reservationKey{}, &reservation{owner: s.owner, slot: slot})
	var once sync.Once
	var stop func() bool
	closeSlot := func() { once.Do(func() { cancel(); slot.Close(); opRelease() }) }
	stop = context.AfterFunc(child, closeSlot)
	return child, func() { closeSlot(); stop() }, nil
}
func (s *Store) observationSlot(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if held, ok := ctx.Value(reservationKey{}).(*reservation); ok && held.owner == s.owner {
		return held.slot.TryUse()
	}
	return s.owner.budget.TryAcquire()
}
func (s *Store) ObservationSlots() int     { return s.owner.budget.Used() }
func (s *Store) ActiveObservations() int64 { return s.activeObservations.Load() }

type observation struct {
	done   chan struct{}
	cancel context.CancelFunc
	mu     sync.Mutex
	err    error
}

func (o *observation) Done() <-chan struct{} { return o.done }
func (o *observation) Cancel()               { o.cancel(); <-o.done }
func (o *observation) Err() error            { o.mu.Lock(); defer o.mu.Unlock(); return o.err }
func observationError(err error) error {
	if err == nil {
		return nil
	}
	var e fdb.Error
	if errors.As(err, &e) {
		if e.Code == 1032 {
			return &metadata.CapacityError{Resource: "native database watches"}
		}
		return &metadata.Unavailable{Cause: err}
	}
	return err
}
func (s *Store) observeFutures(ctx context.Context, futures []fdb.FutureNil, releases []func(), opRelease func()) metadata.Invalidation {
	child, cancel := context.WithCancel(ctx)
	out := &observation{done: make(chan struct{}), cancel: cancel}
	s.activeObservations.Add(int64(len(futures)))
	go func() {
		results := make(chan error, len(futures))
		var workers sync.WaitGroup
		for i, future := range futures {
			workers.Add(1)
			go func(future fdb.FutureNil, release func()) {
				defer workers.Done()
				stop := context.AfterFunc(child, future.Cancel)
				err := future.Get()
				stop()
				future.Cancel()
				release()
				s.activeObservations.Add(-1)
				results <- err
			}(future, releases[i])
		}
		var err error
		if len(futures) > 0 {
			err = <-results
		}
		if child.Err() != nil {
			err = child.Err()
		}
		cancel()
		workers.Wait()
		out.mu.Lock()
		out.err = observationError(err)
		out.mu.Unlock()
		opRelease()
		close(out.done)
	}()
	return out
}
func cancelFutures(futures []fdb.FutureNil) {
	for _, future := range futures {
		future.Cancel()
	}
	for _, future := range futures {
		_ = future.Get()
	}
}
func retryObservation(tr fdb.Transaction, err error) bool {
	var e fdb.Error
	return errors.As(err, &e) && e.Code != 1032 && nativePredicate(int(fdb.ErrorPredicateRetryable), e.Code) && tr.OnError(e).Get() == nil
}
