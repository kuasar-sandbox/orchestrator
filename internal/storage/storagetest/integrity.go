package storagetest

import (
	"bytes"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func runIntegrity(t *testing.T, factory Factory) {
	t.Run("deleted_record_observation_keeps_durable_revision", func(t *testing.T) {
		b := backend(t, factory(t))
		s := record.New(b, b)
		key := record.NewKey("observed", "object")
		first, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) { return true, tx.Put(key, []byte("same payload")) })
		check(t, err)
		_, err = s.Update(t.Context(), func(tx *record.Tx) (bool, error) { tx.Delete(key); return true, nil })
		check(t, err)
		var revisions []record.Version
		check(t, s.Read(t.Context(), func(tx *record.Tx) error {
			v, err := tx.Get(key)
			if err != nil {
				return err
			}
			if v.Exists || !v.Version.IsZero() {
				return errors.New("deleted record exposed a visible value")
			}
			revisions, err = tx.Revisions([]record.Key{key})
			return err
		}))
		if revisions[0].Compare(first) <= 0 {
			t.Fatal("deletion forgot durable revision")
		}
		watch, err := s.Observe(t.Context(), []record.Key{key}, revisions)
		check(t, err)
		_, err = s.Update(t.Context(), func(tx *record.Tx) (bool, error) { return true, tx.Put(key, []byte("same payload")) })
		check(t, err)
		await(t, watch)
		check(t, watch.Err())
		watch.Cancel()
	})
	t.Run("corrupt_payload_and_chunk_are_rejected", func(t *testing.T) {
		b := backend(t, factory(t))
		s := record.New(b, b)
		key := record.NewKey("integrity", "large")
		payload := bytes.Repeat([]byte("data"), 40_000)
		_, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) { return true, tx.Put(key, payload) })
		check(t, err)
		check(t, b.Update(t.Context(), func(tx storage.Tx) error {
			page, err := tx.Scan(storage.Range{Begin: []byte("c"), End: []byte("d")}, storage.Limit{Rows: 1, Bytes: 100_000})
			if err != nil {
				return err
			}
			if len(page.Rows) != 1 {
				return errors.New("missing test chunk")
			}
			row := page.Rows[0]
			row.Value[0] ^= 1
			return tx.Put(row.Key, row.Value)
		}))
		err = s.Read(t.Context(), func(tx *record.Tx) error { _, err := tx.Get(key); return err })
		if !errors.Is(err, storage.ErrInvalid) {
			t.Fatalf("corrupt chunk accepted: %v", err)
		}
		err = s.Read(t.Context(), func(tx *record.Tx) error { _, err := tx.GetMany([]record.Key{key}); return err })
		if !errors.Is(err, storage.ErrInvalid) {
			t.Fatalf("batch accepted corrupt chunk: %v", err)
		}
	})
}
