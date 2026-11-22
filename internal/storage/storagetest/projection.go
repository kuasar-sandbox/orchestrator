package storagetest

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func runProjectionContracts(t *testing.T, factory Factory) {
	t.Run("stable_snapshot_latch_excludes_unrelated_delta_cursor", func(t *testing.T) {
		b := backend(t, factory(t))
		s := record.New(b, b)
		latch, cursor := record.NewKey("sync", "complete"), record.NewKey("sync", "applied")
		_, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			if err := tx.Put(latch, []byte("complete")); err != nil {
				return false, err
			}
			return true, tx.Put(cursor, []byte("100"))
		})
		check(t, err)
		abort := errors.New("retry after unrelated projection progress")
		attempts := 0
		wrapped := faultEngine{Engine: b, update: func(ctx context.Context, fn func(storage.Tx) error) error {
			attempts++
			if attempts > 1 {
				return b.Update(ctx, fn)
			}
			err := b.Update(ctx, func(tx storage.Tx) error {
				if err := fn(tx); err != nil {
					return err
				}
				return abort
			})
			if !errors.Is(err, abort) {
				return err
			}
			_, err = s.Update(ctx, func(tx *record.Tx) (bool, error) { return true, tx.Put(cursor, []byte("101")) })
			if err != nil {
				return err
			}
			return storage.ErrRetry
		}}
		candidate := record.New(wrapped, b)
		_, err = candidate.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			v, err := tx.Get(latch)
			if err != nil {
				return false, err
			}
			if !v.Exists || string(v.Payload) != "complete" {
				return false, record.ErrConflict
			}
			return true, tx.Put(record.NewKey("intent", "selected"), []byte("same candidate"))
		})
		check(t, err)
		if attempts != 2 {
			t.Fatalf("expected one physical retry, got %d", attempts)
		}
	})
	t.Run("withdrawing_support_preserves_instance_auth_reference", func(t *testing.T) {
		b := backend(t, factory(t))
		s := record.New(b, b)
		auth := record.NewKey("auth", "immutable")
		refs := record.NewKey("auth-references", "immutable")
		support, instance := refs.Child("support"), refs.Child("instance")
		_, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			if err := tx.Put(auth, []byte("verification material")); err != nil {
				return false, err
			}
			if err := tx.Put(support, nil); err != nil {
				return false, err
			}
			return true, tx.Put(instance, nil)
		})
		check(t, err)
		release := func(key record.Key) error {
			_, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
				tx.Delete(key)
				remaining, err := tx.Scan(refs, nil, storage.Limit{Rows: 1, Bytes: 1024})
				if err != nil {
					return false, err
				}
				if len(remaining.Items) == 0 {
					tx.Delete(auth)
				}
				return true, nil
			})
			return err
		}
		check(t, release(support))
		check(t, s.Read(t.Context(), func(tx *record.Tx) error {
			v, err := tx.Get(auth)
			if err == nil && !v.Exists {
				return errors.New("support removal deleted live instance auth")
			}
			return err
		}))
		check(t, release(instance))
		check(t, s.Read(t.Context(), func(tx *record.Tx) error {
			v, err := tx.Get(auth)
			if err == nil && v.Exists {
				return errors.New("unreferenced test auth was not reclaimed")
			}
			return err
		}))
	})
}
