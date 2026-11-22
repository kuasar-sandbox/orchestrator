package storagetest

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

func runCapacity(t *testing.T, factory Factory) {
	t.Run("over_budget_transaction_cannot_partially_commit", func(t *testing.T) {
		b := backend(t, factory(t))
		payload := bytes.Repeat([]byte{7}, storage.MaxValueBytes)
		err := b.Update(t.Context(), func(tx storage.Tx) error {
			for i := 0; i < 100; i++ {
				// Even a callback that ignores a rejected mutation cannot commit
				// the earlier writes from an over-budget transaction.
				_ = tx.Put([]byte(fmt.Sprintf("capacity/%03d", i)), payload)
			}
			return nil
		})
		if !errors.Is(err, storage.ErrCapacity) {
			t.Fatalf("over-budget update returned %v", err)
		}
		if get(t, b, []byte("capacity/000")).Exists {
			t.Fatal("over-budget transaction partially committed")
		}
		put(t, b, []byte("usable-after-rejection"), []byte("yes"))
	})
	t.Run("physical_value_and_query_bounds", func(t *testing.T) {
		b := backend(t, factory(t))
		err := b.Update(t.Context(), func(tx storage.Tx) error {
			return tx.Put([]byte("oversized"), make([]byte, storage.MaxValueBytes+1))
		})
		if !errors.Is(err, storage.ErrCapacity) {
			t.Fatalf("oversized value returned %v", err)
		}
		check(t, b.View(t.Context(), func(tx storage.ReadTx) error {
			_, err := tx.Scan(storage.Range{}, storage.Limit{Rows: storage.MaxPageRows + 1, Bytes: 1024})
			if !errors.Is(err, storage.ErrInvalid) {
				return fmt.Errorf("invalid page bound returned %v", err)
			}
			return nil
		}))
	})
}
