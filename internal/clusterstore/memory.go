package clusterstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	defaultRetention = 10000
	watchBuf         = 1024
)

type memoryStore struct {
	mu        sync.Mutex
	retention int64
	rev       int64
	leaseSeq  int64
	kv        map[string]KV
	leased    map[LeaseID]map[string]bool
	changelog []Event
	watchers  map[*memoryWatcher]struct{}
	closed    bool
}

type memoryWatcher struct {
	prefix string
	ch     chan Event
}

// OpenMemory returns a process-local clusterstore. It is intended for the size-1
// registry mode and tests; multi-member reliability is provided by the registry
// replication layer, not by this backend.
func OpenMemory(retention int) Store {
	if retention <= 0 {
		retention = defaultRetention
	}
	return &memoryStore{
		retention: int64(retention),
		kv:        map[string]KV{},
		leased:    map[LeaseID]map[string]bool{},
		watchers:  map[*memoryWatcher]struct{}{},
	}
}

func (s *memoryStore) Get(ctx context.Context, key string) (KV, bool, error) {
	if err := ctx.Err(); err != nil {
		return KV{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	kv, ok := s.kv[key]
	kv.Value = append([]byte(nil), kv.Value...)
	return kv, ok, nil
}

func (s *memoryStore) Put(ctx context.Context, key string, val []byte) (int64, error) {
	return s.PutLeased(ctx, key, val, 0)
}

func (s *memoryStore) PutLeased(ctx context.Context, key string, val []byte, lease LeaseID) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putLocked(key, val, lease), nil
}

func (s *memoryStore) Delete(ctx context.Context, key string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.kv[key]; !ok {
		return 0, nil
	}
	return s.deleteLocked(key), nil
}

func (s *memoryStore) CAS(ctx context.Context, key string, expectRev int64, val []byte) (int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, found := s.kv[key]
	if (!found && expectRev != 0) || (found && cur.ModRev != expectRev) {
		return 0, false, nil
	}
	return s.putLocked(key, val, 0), true, nil
}

func (s *memoryStore) Range(ctx context.Context, prefix string, fn func(KV) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	keys := make([]string, 0, len(s.kv))
	for key := range s.kv {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]KV, 0, len(keys))
	for _, key := range keys {
		kv := s.kv[key]
		kv.Value = append([]byte(nil), kv.Value...)
		out = append(out, kv)
	}
	s.mu.Unlock()
	for _, kv := range out {
		if err := fn(kv); err != nil {
			return err
		}
	}
	return nil
}

func (s *memoryStore) Watch(ctx context.Context, prefix string, fromRev int64) (<-chan Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.changelog) > 0 && fromRev > 0 && fromRev < s.changelog[0].Rev {
		return nil, ErrCompacted
	}
	w := &memoryWatcher{prefix: prefix, ch: make(chan Event, watchBuf)}
	if fromRev > 0 {
		for _, ev := range s.changelog {
			if ev.Rev > fromRev && strings.HasPrefix(ev.Key, prefix) {
				w.ch <- cloneEvent(ev)
			}
		}
	}
	s.watchers[w] = struct{}{}
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if _, ok := s.watchers[w]; ok {
			delete(s.watchers, w)
			close(w.ch)
		}
		s.mu.Unlock()
	}()
	return w.ch, nil
}

func (s *memoryStore) Rev(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rev, nil
}

func (s *memoryStore) Grant(ctx context.Context, ttlSec int64) (LeaseID, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaseSeq++
	id := LeaseID(s.leaseSeq)
	s.leased[id] = map[string]bool{}
	return id, nil
}

func (s *memoryStore) KeepAlive(ctx context.Context, lease LeaseID, ttlSec int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.leased[lease]; !ok {
		return fmt.Errorf("clusterstore: lease %d not found", lease)
	}
	return nil
}

func (s *memoryStore) Revoke(ctx context.Context, lease LeaseID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.leased[lease] {
		s.deleteLocked(key)
	}
	delete(s.leased, lease)
	return nil
}

func (s *memoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for w := range s.watchers {
		delete(s.watchers, w)
		close(w.ch)
	}
	return nil
}

func (s *memoryStore) putLocked(key string, val []byte, lease LeaseID) int64 {
	if _, found := s.kv[key]; found {
		for _, keys := range s.leased {
			delete(keys, key)
		}
	}
	s.rev++
	kv := KV{Key: key, Value: append([]byte(nil), val...), ModRev: s.rev}
	s.kv[key] = kv
	if lease != 0 {
		if s.leased[lease] == nil {
			s.leased[lease] = map[string]bool{}
		}
		s.leased[lease][key] = true
	}
	s.commit(Event{Type: EventPut, Key: key, Value: kv.Value, Rev: s.rev})
	return s.rev
}

func (s *memoryStore) deleteLocked(key string) int64 {
	delete(s.kv, key)
	for _, keys := range s.leased {
		delete(keys, key)
	}
	s.rev++
	s.commit(Event{Type: EventDelete, Key: key, Rev: s.rev})
	return s.rev
}

func (s *memoryStore) commit(ev Event) {
	ev = cloneEvent(ev)
	s.changelog = append(s.changelog, ev)
	if int64(len(s.changelog)) > s.retention {
		start := len(s.changelog) - int(s.retention)
		s.changelog = append([]Event(nil), s.changelog[start:]...)
	}
	for w := range s.watchers {
		if !strings.HasPrefix(ev.Key, w.prefix) {
			continue
		}
		select {
		case w.ch <- cloneEvent(ev):
		default:
			delete(s.watchers, w)
			close(w.ch)
		}
	}
}

func cloneEvent(ev Event) Event {
	ev.Value = append([]byte(nil), ev.Value...)
	return ev
}
