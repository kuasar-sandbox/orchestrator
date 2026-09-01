package proxyext

import "sync"

const defaultQueueCapacity = 64

// hub is a bounded, non-blocking fan-out. A slow subscriber is removed and its
// reset signal is closed; route synchronization never waits for a Watch callback.
type hub[T any] struct {
	mu       sync.Mutex
	nextID   uint64
	subs     map[uint64]*subscription[T]
	capacity int
}

type subscription[T any] struct {
	id     uint64
	events chan T
	reset  chan struct{}
}

func newHub[T any](capacity int) *hub[T] {
	if capacity < 1 {
		capacity = 1
	}
	return &hub[T]{subs: make(map[uint64]*subscription[T]), capacity: capacity}
}

func (h *hub[T]) subscribe() *subscription[T] {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	sub := &subscription[T]{id: h.nextID, events: make(chan T, h.capacity), reset: make(chan struct{})}
	h.subs[sub.id] = sub
	return sub
}

func (h *hub[T]) unsubscribe(sub *subscription[T]) {
	if sub == nil {
		return
	}
	h.mu.Lock()
	delete(h.subs, sub.id)
	h.mu.Unlock()
}

func (h *hub[T]) publish(event T) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, sub := range h.subs {
		select {
		case sub.events <- event:
		default:
			delete(h.subs, id)
			close(sub.reset)
		}
	}
}
