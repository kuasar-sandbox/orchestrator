// Package observebudget bounds native observations without depending on FoundationDB.
package observebudget

import (
	"fmt"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

var ErrCapacity = storage.ErrCapacity

type CapacityError = storage.CapacityError

const DefaultObservationLimit = 8192
const DefaultReserveLeaderLimit = 7680

// Budget includes setup, active native futures and cancellation until completion.
// It has no waiting queue and therefore cannot turn overload into unbounded work.
type Budget struct {
	mu             sync.Mutex
	limit, used    int
	nonCatalog     int
	catalogReserve int
}

func NewBudget(limit int) *Budget {
	b := &Budget{limit: limit}
	if limit > 256 {
		b.catalogReserve = 256
	}
	return b
}
func (b *Budget) TryAcquire() (func(), error) {
	return b.tryAcquire(false)
}
func (b *Budget) TryAcquireCatalog() (func(), error) { return b.tryAcquire(true) }
func (b *Budget) tryAcquire(catalog bool) (func(), error) {
	b.mu.Lock()
	if b.used >= b.limit || (!catalog && b.nonCatalog >= b.limit-b.catalogReserve) {
		b.mu.Unlock()
		return nil, &CapacityError{Resource: "database observations"}
	}
	b.used++
	if !catalog {
		b.nonCatalog++
	}
	b.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.used--
			if !catalog {
				b.nonCatalog--
			}
			b.mu.Unlock()
		})
	}, nil
}
func (b *Budget) Used() int { b.mu.Lock(); defer b.mu.Unlock(); return b.used }
func (b *Budget) Configure(limit int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit < 1 || b.used != 0 {
		return fmt.Errorf("configure observation budget: limit=%d used=%d", limit, b.used)
	}
	b.limit = limit
	b.catalogReserve = 0
	if limit > 256 {
		b.catalogReserve = 256
	}
	return nil
}

// Reservation holds one budget slot across sequential read/observe re-arms.
// Close waits for the active native future to finish before returning the slot.
type Reservation struct {
	mu           sync.Mutex
	busy, closed bool
	done         chan struct{}
	release      func()
}

func NewReservation(release func()) *Reservation {
	return &Reservation{done: make(chan struct{}), release: release}
}
func (r *Reservation) TryUse() (func(), error) {
	r.mu.Lock()
	if r.closed || r.busy {
		r.mu.Unlock()
		return nil, &CapacityError{Resource: "observation reservation already active"}
	}
	r.busy = true
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.busy = false
			if r.closed {
				r.release()
				close(r.done)
			}
		})
	}, nil
}
func (r *Reservation) Close() {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		if !r.busy {
			r.release()
			close(r.done)
		}
	}
	r.mu.Unlock()
	<-r.done
}
