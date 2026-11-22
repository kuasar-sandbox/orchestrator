// Package metastore is the single implementation of cluster metadata repositories.
// Business invariants and indexes live here; storage drivers only handle bytes.
package metastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

const maxPageRows = 512
const maxPageBytes = 2 << 20

type Store struct {
	records  *records.Store
	observer storage.Observer
}

func New(engine storage.Engine, observer storage.Observer) *Store {
	return &Store{records: records.New(engine, observer), observer: observer}
}
func (s *Store) Close() error { return s.records.Close() }
func (s *Store) ReserveObservation(ctx context.Context) (context.Context, func(), error) {
	return s.records.ReserveObservation(ctx)
}
func (s *Store) ObservationSlots() int {
	if stats, ok := s.observer.(interface{ ObservationSlots() int }); ok {
		return stats.ObservationSlots()
	}
	return 0
}
func (s *Store) ActiveObservations() int64 {
	if stats, ok := s.observer.(interface{ ActiveObservations() int64 }); ok {
		return stats.ActiveObservations()
	}
	return 0
}
func (s *Store) read(ctx context.Context, fn func(*records.Tx) error) error {
	return s.records.Read(ctx, fn)
}

type mutation struct{ tr *records.Tx }

func (s *Store) mutate(ctx context.Context, fn func(*mutation) (bool, error)) (metadata.Version, error) {
	return s.records.Update(ctx, func(tr *records.Tx) (bool, error) { return fn(&mutation{tr: tr}) })
}

type record struct {
	exists  bool
	version metadata.Version
	payload []byte
}

func wrapRecord(v records.Value) record {
	return record{exists: v.Exists, version: v.Version, payload: v.Payload}
}
func (m *mutation) get(key records.Key) (record, error) {
	v, err := m.tr.Get(key)
	return wrapRecord(v), err
}
func (m *mutation) expect(key records.Key, expected metadata.Version) (record, error) {
	v, err := m.tr.Expect(key, expected)
	return wrapRecord(v), err
}
func invalidRecord(s string) error                     { return fmt.Errorf("%w: %s", metadata.ErrInvalidRecord, s) }
func (s *Store) key(parts ...string) records.Key       { return records.NewKey(parts...) }
func mainKey(scope records.Key, id string) records.Key { return scope.Child(id) }
func validateParts(parts ...string) error {
	total := 0
	for _, p := range parts {
		if p == "" || len(p) > 4096 || strings.IndexByte(p, 0) >= 0 {
			return invalidRecord("invalid key component")
		}
		total += len(p) + 4
	}
	if total > 8500 {
		return invalidRecord("key too long")
	}
	return nil
}
func putJSON[T any](tr *records.Tx, key records.Key, value T) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tr.Put(key, payload)
}
func clearRecord(tr *records.Tx, key records.Key) { tr.Delete(key) }
func typed[T any](r record) (metadata.Versioned[T], error) {
	out := metadata.Versioned[T]{Exists: r.exists, Version: r.version}
	if r.exists {
		if err := json.Unmarshal(r.payload, &out.Value); err != nil {
			return metadata.Versioned[T]{}, invalidRecord("record JSON")
		}
	}
	return out, nil
}
func getTyped[T any](m *mutation, key records.Key) (metadata.Versioned[T], error) {
	r, err := m.get(key)
	if err != nil {
		return metadata.Versioned[T]{}, err
	}
	return typed[T](r)
}
func readTyped[T any](tr *records.Tx, key records.Key) (metadata.Versioned[T], error) {
	r, err := tr.Get(key)
	if err != nil {
		return metadata.Versioned[T]{}, err
	}
	return typed[T](wrapRecord(r))
}
func pageLimit(rows int) storage.Limit { return storage.Limit{Rows: rows, Bytes: maxPageBytes} }
func encodeCursor(scope, key records.Key) metadata.Cursor {
	hash := sha256.Sum256(scope)
	raw := append(append([]byte(nil), hash[:16]...), key...)
	return metadata.Cursor(base64.RawURLEncoding.EncodeToString(raw))
}
func decodeCursor(scope records.Key, cursor metadata.Cursor) (records.Key, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(string(cursor))
	hash := sha256.Sum256(scope)
	if err != nil || len(raw) <= 16 || len(raw) > 9016 || !bytes.Equal(raw[:16], hash[:16]) || !bytes.HasPrefix(raw[16:], scope) {
		return nil, invalidRecord("cursor scope")
	}
	return records.Key(raw[16:]), nil
}
func listTyped[T any](ctx context.Context, s *Store, scope records.Key, cursor metadata.Cursor, limit int) (out metadata.Page[metadata.Versioned[T]], err error) {
	if limit < 1 || limit > maxPageRows {
		return out, invalidRecord("page limit must be 1..512")
	}
	last, err := decodeCursor(scope, cursor)
	if err != nil {
		return out, err
	}
	err = s.read(ctx, func(tr *records.Tx) error {
		out = metadata.Page[metadata.Versioned[T]]{}
		page, err := tr.Scan(scope, last, pageLimit(limit))
		if err != nil {
			return err
		}
		for _, item := range page.Items {
			v, err := typed[T](wrapRecord(item.Value))
			if err != nil {
				return err
			}
			out.Items = append(out.Items, v)
		}
		if page.More && len(page.Items) > 0 {
			out.Next = encodeCursor(scope, page.Items[len(page.Items)-1].Key)
		}
		return nil
	})
	return
}
