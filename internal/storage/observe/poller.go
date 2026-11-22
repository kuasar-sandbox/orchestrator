package observe

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

// Poller is shared by all handles for one SQL database/namespace. Notifications
// accelerate it; bounded periodic rereads cover missed notifications and external
// writers. Each sweep coalesces duplicate keys into batches, not one query per
// subscription. Conditions must use persistent, non-reused revision values.
type Poller struct {
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	wake    chan struct{}
	mu      sync.Mutex
	closed  bool
	watches map[*pollWatch][]storage.Condition
	read    func(context.Context, [][]byte) ([]storage.Value, error)
}
type pollWatch struct {
	done chan struct{}
	err  error
}

func NewPoller(read func(context.Context, [][]byte) ([]storage.Value, error)) *Poller {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Poller{ctx: ctx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1), watches: make(map[*pollWatch][]storage.Condition), read: read}
	go p.run()
	return p
}
func (p *Poller) Wake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}
func (p *Poller) finish(w *pollWatch, err error) {
	if _, ok := p.watches[w]; ok {
		delete(p.watches, w)
		w.err = err
		close(w.done)
	}
}
func (p *Poller) Invalidate(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for w := range p.watches {
		p.finish(w, err)
	}
}
func (p *Poller) Wait(ctx context.Context, conditions []storage.Condition) error {
	copyConditions := make([]storage.Condition, len(conditions))
	for i, c := range conditions {
		copyConditions[i] = storage.Condition{Key: bytes.Clone(c.Key), Value: storage.Clone(c.Value)}
	}
	w := &pollWatch{done: make(chan struct{})}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return context.Canceled
	}
	p.watches[w] = copyConditions
	p.mu.Unlock()
	p.Wake()
	select {
	case <-w.done:
		return w.err
	case <-ctx.Done():
		p.mu.Lock()
		p.finish(w, ctx.Err())
		p.mu.Unlock()
		return ctx.Err()
	}
}
func (p *Poller) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cancel()
	<-p.done
}
func (p *Poller) run() {
	defer close(p.done)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-p.ctx.Done():
			p.Invalidate(context.Canceled)
			return
		case <-p.wake:
		case <-tick.C:
		}
		p.sweep()
	}
}
func (p *Poller) sweep() {
	p.mu.Lock()
	snapshot := make(map[*pollWatch][]storage.Condition, len(p.watches))
	unique := make(map[string][]byte)
	for w, conditions := range p.watches {
		snapshot[w] = conditions
		for _, c := range conditions {
			unique[string(c.Key)] = c.Key
		}
	}
	p.mu.Unlock()
	keys := make([][]byte, 0, len(unique))
	for _, key := range unique {
		keys = append(keys, key)
	}
	values := make(map[string]storage.Value, len(keys))
	for start := 0; start < len(keys); start += 256 {
		part := keys[start:min(start+256, len(keys))]
		ctx, cancel := context.WithTimeout(p.ctx, 4*time.Second)
		rows, err := p.read(ctx, part)
		cancel()
		if err != nil {
			p.Invalidate(err)
			return
		}
		for i, key := range part {
			values[string(key)] = rows[i]
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for w, conditions := range snapshot {
		for _, c := range conditions {
			if !storage.Equal(c.Value, values[string(c.Key)]) {
				p.finish(w, nil)
				break
			}
		}
	}
}
