// Package observe implements bounded admission and lifecycle for generic
// invalidations. Native watches and SQL notification/polling share this contract.
package observe

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/kuasar-sandbox/orchestrator/internal/observebudget"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

type Manager struct {
	Budget *observebudget.Budget
	life   *storage.Lifetime
	active atomic.Int64
}

func New(budget *observebudget.Budget) *Manager {
	return &Manager{Budget: budget, life: storage.NewLifetime()}
}
func (m *Manager) Close()        { m.life.Close() }
func (m *Manager) Active() int64 { return m.active.Load() }

type reservationKey struct{}
type reservation struct {
	budget *observebudget.Budget
	slot   *observebudget.Reservation
}

func (m *Manager) Reserve(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if held, ok := ctx.Value(reservationKey{}).(*reservation); ok && held.budget == m.Budget {
		return ctx, func() {}, nil
	}
	child, done, err := m.life.Enter(ctx)
	if err != nil {
		return nil, nil, err
	}
	release, err := m.Budget.TryAcquire()
	if err != nil {
		done()
		return nil, nil, err
	}
	slot := observebudget.NewReservation(release)
	child, cancel := context.WithCancel(child)
	child = context.WithValue(child, reservationKey{}, &reservation{m.Budget, slot})
	var once sync.Once
	closeSlot := func() { once.Do(func() { cancel(); slot.Close(); done() }) }
	stop := context.AfterFunc(child, closeSlot)
	return child, func() { closeSlot(); stop() }, nil
}

type observation struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error
}

func (o *observation) Done() <-chan struct{} { return o.done }
func (o *observation) Cancel()               { o.cancel(); <-o.done }
func (o *observation) Err() error {
	select {
	case <-o.done:
		return o.err
	default:
		return nil
	}
}
func (m *Manager) Run(ctx context.Context, count int, wait func(context.Context) error) (storage.Invalidation, error) {
	if count < 1 || count > 256 {
		return nil, &storage.CapacityError{Resource: "observation key count"}
	}
	child, done, err := m.life.Enter(ctx)
	if err != nil {
		return nil, err
	}
	releases := make([]func(), 0, count)
	for i := 0; i < count; i++ {
		var release func()
		if count == 1 {
			if held, ok := ctx.Value(reservationKey{}).(*reservation); ok && held.budget == m.Budget {
				release, err = held.slot.TryUse()
			} else {
				release, err = m.Budget.TryAcquire()
			}
		} else {
			release, err = m.Budget.TryAcquireCatalog()
		}
		if err != nil {
			for _, r := range releases {
				r()
			}
			done()
			return nil, err
		}
		releases = append(releases, release)
	}
	child, cancel := context.WithCancel(child)
	out := &observation{done: make(chan struct{}), cancel: cancel}
	m.active.Add(int64(count))
	go func() {
		err := wait(child)
		if child.Err() != nil {
			err = child.Err()
		}
		cancel()
		for _, release := range releases {
			release()
		}
		m.active.Add(-int64(count))
		done()
		out.err = err
		close(out.done)
	}()
	return out, nil
}
