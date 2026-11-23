package storagetest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func runRecordBatch(t *testing.T, factory Factory) {
	t.Run("record_batch_order_absence_chunks_and_staged_writes", func(t *testing.T) {
		b := backend(t, factory(t))
		s := record.New(b, b)
		keys := make([]record.Key, 260)
		for i := range keys {
			keys[i] = record.NewKey("batch", fmt.Sprint(i))
		}
		large := bytes.Repeat([]byte("chunked"), 20_000)
		first, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			for _, key := range keys {
				if err := tx.Put(key, key); err != nil {
					return false, err
				}
			}
			return true, tx.Put(keys[258], large)
		})
		check(t, err)
		request := append([]record.Key{record.NewKey("batch", "missing")}, keys...)
		request = append(request, keys[258], keys[0])
		check(t, s.Read(t.Context(), func(tx *record.Tx) error {
			values, err := tx.GetMany(request)
			if err != nil {
				return err
			}
			if len(values) != len(request) || values[0].Exists {
				t.Fatal("batch lost absent slot or result ordering")
			}
			for i := 1; i < len(values); i++ {
				want := []byte(request[i])
				if bytes.Equal(request[i], keys[258]) {
					want = large
				}
				if !values[i].Exists || values[i].Version != first || !bytes.Equal(values[i].Payload, want) {
					t.Fatalf("batch lost envelope identity or chunked value at %d", i)
				}
			}
			values[1].Payload[0] ^= 1
			if !bytes.Equal(values[len(values)-1].Payload, keys[0]) {
				t.Fatal("duplicate result aliases caller-owned bytes")
			}
			return nil
		}))
		_, err = s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			tx.Delete(keys[0])
			if err := tx.Put(keys[258], []byte("replacement")); err != nil {
				return false, err
			}
			if err := tx.Put(request[0], nil); err != nil {
				return false, err
			}
			values, err := tx.GetMany(request)
			if err != nil {
				return false, err
			}
			if !values[0].Exists || len(values[0].Payload) != 0 || values[1].Exists || values[len(values)-1].Exists || string(values[259].Payload) != "replacement" || values[259].Version != first {
				t.Fatal("batch lost staged delete, empty insert, replacement or original fence")
			}
			return true, nil
		})
		check(t, err)
	})
}
