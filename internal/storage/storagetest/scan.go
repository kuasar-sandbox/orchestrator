package storagetest

import (
	"bytes"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func runRecordScan(t *testing.T, factory Factory) {
	t.Run("record_scan_sees_staged_index_changes_and_cursor_scope", func(t *testing.T) {
		b := backend(t, factory(t))
		s := record.New(b, b)
		scope := record.NewKey("index")
		_, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			for _, id := range []string{"b", "d", "f"} {
				if err := tx.Put(scope.Child(id), []byte("before")); err != nil {
					return false, err
				}
			}
			return true, nil
		})
		check(t, err)
		assertScan := func(tx *record.Tx, limit storage.Limit) error {
			var after record.Key
			var keys []record.Key
			for i := 0; ; i++ {
				if i > 3 {
					return errors.New("record scan did not make progress")
				}
				page, err := tx.Scan(scope, after, limit)
				if err != nil {
					return err
				}
				for _, item := range page.Items {
					keys = append(keys, item.Key)
					after = item.Key
					if !bytes.Equal(item.Value.Payload, []byte("after")) {
						return errors.New("scan returned replaced payload")
					}
				}
				if !page.More {
					break
				}
			}
			if len(keys) != 3 || !bytes.Equal(keys[0], scope.Child("a")) || !bytes.Equal(keys[1], scope.Child("c")) || !bytes.Equal(keys[2], scope.Child("d")) {
				return errors.New("scan lost staged insert/delete/order")
			}
			_, err := tx.Scan(scope, record.NewKey("other", "cursor"), limit)
			if !errors.Is(err, storage.ErrInvalid) {
				return errors.New("scan accepted a cursor from another scope")
			}
			return nil
		}
		_, err = s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			for _, id := range []string{"a", "c", "d"} {
				if err := tx.Put(scope.Child(id), []byte("after")); err != nil {
					return false, err
				}
			}
			tx.Delete(scope.Child("b"))
			tx.Delete(scope.Child("f"))
			if err := tx.Put(record.NewKey("other", "a"), []byte("invisible")); err != nil {
				return false, err
			}
			if err := assertScan(tx, storage.Limit{Rows: 2, Bytes: 1024}); err != nil {
				return false, err
			}
			return true, assertScan(tx, storage.Limit{Rows: 2, Bytes: 1})
		})
		check(t, err)
		check(t, s.Read(t.Context(), func(tx *record.Tx) error {
			return assertScan(tx, storage.Limit{Rows: 2, Bytes: 1})
		}))
	})
}
