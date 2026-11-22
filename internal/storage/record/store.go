package record

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

var ErrConflict = errors.New("record: original condition changed")

type Conflict struct{ Expected, Actual Version }

func (e *Conflict) Error() string        { return ErrConflict.Error() }
func (e *Conflict) Is(target error) bool { return target == ErrConflict }

type Value struct {
	Exists  bool
	Version Version
	Payload []byte
}
type Item struct {
	Key   Key
	Value Value
}
type Page struct {
	Items []Item
	More  bool
}
type pending struct {
	key     Key
	payload []byte
	deleted bool
}
type Tx struct {
	read        storage.ReadTx
	write       storage.Tx
	pinned      map[string]Version
	pending     map[string]pending
	base        map[string]baseRecord
	stagedBytes int
	stageErr    error
}
type Store struct {
	engine   storage.Engine
	observer storage.Observer
}

func New(engine storage.Engine, observer storage.Observer) *Store {
	return &Store{engine: engine, observer: observer}
}
func (s *Store) Close() error { return s.engine.Close() }
func (s *Store) ReserveObservation(ctx context.Context) (context.Context, func(), error) {
	return s.observer.ReserveObservation(ctx)
}

func retry(ctx context.Context, attempt int) error {
	delay := time.Duration(min(attempt, 25)) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (s *Store) Read(ctx context.Context, fn func(*Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	for attempt := 1; ; attempt++ {
		err := s.engine.View(ctx, func(tx storage.ReadTx) error { return fn(&Tx{read: tx}) })
		if !errors.Is(err, storage.ErrRetry) {
			return err
		}
		if err = retry(ctx, attempt); err != nil {
			return err
		}
	}
}

// Update pins every observed logical revision across physical retries. Each
// touched key keeps a small durable revision cell even after deletion. One
// mutation assigns max(touched revisions)+1 to its writes, preserving atomic
// fences and per-key order without a global sequence or a hot global counter.
func (s *Store) Update(ctx context.Context, fn func(*Tx) (bool, error)) (Version, error) {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	pinned := make(map[string]Version)
	for attempt := 1; ; attempt++ {
		var committed Version
		err := s.engine.Update(ctx, func(native storage.Tx) error {
			tx := &Tx{read: native, write: native, pinned: pinned, pending: make(map[string]pending)}
			changed, err := fn(tx)
			if err != nil || !changed {
				return err
			}
			committed, err = tx.flush()
			return err
		})
		if err == nil {
			return committed, nil
		}
		if !errors.Is(err, storage.ErrRetry) {
			return Version{}, err
		}
		if err = retry(ctx, attempt); err != nil {
			return Version{}, err
		}
	}
}
func (t *Tx) pin(key Key, v Version) error {
	if t.pinned == nil {
		return nil
	}
	if old, ok := t.pinned[string(key)]; ok && old != v {
		return &Conflict{Expected: old, Actual: v}
	}
	t.pinned[string(key)] = v
	return nil
}
func (t *Tx) Get(key Key) (Value, error) {
	if t.write == nil {
		// A committed envelope already carries its revision. Ordinary reads
		// need no mutation bookkeeping or second counter value. Absence watches
		// explicitly read Revisions to retain the delete/recreate fence.
		phys, err := physical(key)
		if err != nil {
			return Value{}, err
		}
		raw, err := t.read.Get(phys)
		if err != nil || !raw.Exists {
			return Value{}, err
		}
		value, actual, err := t.decode(phys, raw.Bytes)
		if err == nil && !bytes.Equal(actual, key) {
			return Value{}, invalid("logical key collision")
		}
		return value, err
	}
	if err := t.loadBases([]Key{key}); err != nil {
		return Value{}, err
	}
	base := t.base[string(key)]
	if p, ok := t.pending[string(key)]; ok {
		return Value{Exists: !p.deleted, Version: base.version, Payload: bytes.Clone(p.payload)}, nil
	}
	if !base.raw.Exists {
		return Value{}, nil
	}
	value, _, err := t.decode(base.physical, base.raw.Bytes)
	return value, err
}

func (t *Tx) Expect(key Key, expected Version) (Value, error) {
	v, err := t.Get(key)
	if err != nil {
		return Value{}, err
	}
	if v.Version != expected {
		return Value{}, &Conflict{Expected: expected, Actual: v.Version}
	}
	return v, nil
}
func (t *Tx) Put(key Key, payload []byte) error {
	if t.write == nil {
		return fmt.Errorf("%w: read-only transaction", storage.ErrInvalid)
	}
	return t.stage(key, payload, false)
}

func (t *Tx) Delete(key Key) {
	if t.write == nil {
		panic("record: Delete in read-only transaction")
	}
	_ = t.stage(key, nil, true)
}

func (t *Tx) Revisions(keys []Key) ([]Version, error) {
	out := make([]Version, len(keys))
	var physicalKeys [][]byte
	var indexes []int
	for i, key := range keys {
		if base, ok := t.base[string(key)]; ok {
			out[i] = base.version
			continue
		}
		phys, err := physical(key)
		if err != nil {
			return nil, err
		}
		physicalKeys = append(physicalKeys, revisionKey(phys))
		indexes = append(indexes, i)
	}
	if len(physicalKeys) > 0 {
		rows, err := t.read.GetMany(physicalKeys)
		if err != nil {
			return nil, err
		}
		for j, index := range indexes {
			out[index], err = decodeCounter(rows[j], keys[index])
			if err != nil {
				return nil, err
			}
		}
	}
	for i, key := range keys {
		if err := t.pin(key, out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (t *Tx) scanCommitted(scope Key, after Key, limit storage.Limit) (Page, error) {
	prefix, err := physical(scope)
	if err != nil {
		return Page{}, err
	}
	bounds := storage.Range{Begin: prefix, End: storage.PrefixEnd(prefix)}
	if after != nil {
		if !bytes.HasPrefix(after, scope) {
			return Page{}, fmt.Errorf("%w: cursor scope", storage.ErrInvalid)
		}
		bounds.After, err = physical(after)
		if err != nil {
			return Page{}, err
		}
	}
	raw, err := t.read.Scan(bounds, limit)
	if err != nil {
		return Page{}, err
	}
	out := Page{More: raw.More}
	used := 0
	for _, row := range raw.Rows {
		size, err := payloadSize(row.Value)
		if err != nil {
			return Page{}, err
		}
		if len(out.Items) > 0 && used+size > limit.Bytes {
			out.More = true
			break
		}
		v, key, err := t.decode(row.Key, row.Value)
		if err != nil {
			return Page{}, err
		}
		if !bytes.HasPrefix(key, scope) {
			return Page{}, fmt.Errorf("%w: scope collision", storage.ErrInvalid)
		}
		if err = t.pin(key, v.Version); err != nil {
			return Page{}, err
		}
		out.Items = append(out.Items, Item{Key: key, Value: v})
		used += len(v.Payload)
	}
	return out, nil
}
func (t *Tx) flush() (Version, error) {
	if t.stageErr != nil {
		return Version{}, t.stageErr
	}
	names := make([]string, 0, len(t.pending))
	for name := range t.pending {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return Version{}, nil
	}
	keys := make([]Key, len(names))
	for i, name := range names {
		keys[i] = Key(name)
	}
	if err := t.loadBases(keys); err != nil {
		return Version{}, err
	}
	var latest Version
	for _, name := range names {
		if v := t.base[name].version; v.Compare(latest) > 0 {
			latest = v
		}
	}
	next, err := latest.next()
	if err != nil {
		return Version{}, err
	}
	for _, name := range names {
		p := t.pending[name]
		base := t.base[name]
		prefix := chunkPrefix(base.physical)
		for i := 0; i < base.chunks; i++ {
			if err = t.write.Delete(chunkKey(prefix, uint32(i))); err != nil {
				return Version{}, err
			}
		}
		if err = t.write.Put(revisionKey(base.physical), append(bytes.Clone(next[:]), p.key...)); err != nil {
			return Version{}, err
		}
		if p.deleted {
			if err = t.write.Delete(base.physical); err != nil {
				return Version{}, err
			}
		} else if err = t.encode(base.physical, p.key, p.payload, next); err != nil {
			return Version{}, err
		}
	}
	return next, nil
}

func (s *Store) Observe(ctx context.Context, keys []Key, expected []Version) (storage.Invalidation, error) {
	if len(keys) != len(expected) || len(keys) == 0 {
		return nil, fmt.Errorf("%w: observation conditions", storage.ErrInvalid)
	}
	conditions := make([]storage.Condition, len(keys))
	for i, key := range keys {
		phys, err := physical(key)
		if err != nil {
			return nil, err
		}
		conditions[i].Key = revisionKey(phys)
		if !expected[i].IsZero() {
			conditions[i].Value = storage.Value{Exists: true, Bytes: append(bytes.Clone(expected[i][:]), key...)}
		}
	}
	return s.observer.Observe(ctx, conditions)
}
