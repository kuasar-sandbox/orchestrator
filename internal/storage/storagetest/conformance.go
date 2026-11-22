// Package storagetest applies the same behavioral contract to each real backend.
package storagetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

type Backend interface {
	storage.Engine
	storage.Observer
}

// A factory returns an opener for one fresh namespace. Reopening shares durable
// data; separate subtests receive separate namespaces/databases.
type Factory func(*testing.T) func() Backend

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func backend(t *testing.T, open func() Backend) Backend {
	t.Helper()
	b := open()
	t.Cleanup(func() { check(t, b.Close()) })
	return b
}
func put(t *testing.T, b Backend, key, value []byte) {
	t.Helper()
	check(t, b.Update(t.Context(), func(tx storage.Tx) error { return tx.Put(key, value) }))
}
func get(t *testing.T, b Backend, key []byte) storage.Value {
	t.Helper()
	var out storage.Value
	check(t, b.View(t.Context(), func(tx storage.ReadTx) error { var err error; out, err = tx.Get(key); return err }))
	return out
}
func await(t *testing.T, o storage.Invalidation) {
	t.Helper()
	select {
	case <-o.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("invalidation did not complete")
	}
}

func Run(t *testing.T, factory Factory) {
	runCapacity(t, factory)
	t.Run("absence_empty_duplicates_rollback_and_reopen", func(t *testing.T) {
		open := factory(t)
		b := backend(t, open)
		check(t, b.Update(t.Context(), func(tx storage.Tx) error {
			if err := tx.Put([]byte("empty"), nil); err != nil {
				return err
			}
			if err := tx.Put([]byte("value"), []byte("data")); err != nil {
				return err
			}
			v, err := tx.GetMany([][]byte{[]byte("value"), []byte("missing"), []byte("empty"), []byte("value")})
			if err != nil {
				return err
			}
			if !v[0].Exists || !bytes.Equal(v[0].Bytes, []byte("data")) || v[1].Exists || !v[2].Exists || len(v[2].Bytes) != 0 || !storage.Equal(v[0], v[3]) {
				return errors.New("GetMany lost ordering, duplicates, empty or read-your-writes")
			}
			return nil
		}))
		abort := errors.New("abort")
		err := b.Update(t.Context(), func(tx storage.Tx) error {
			if err := tx.Delete([]byte("value")); err != nil {
				return err
			}
			return abort
		})
		if !errors.Is(err, abort) || !get(t, b, []byte("value")).Exists {
			t.Fatal("aborted mutation escaped")
		}
		check(t, b.Close())
		reopened := backend(t, open)
		if !get(t, reopened, []byte("empty")).Exists || !bytes.Equal(get(t, reopened, []byte("value")).Bytes, []byte("data")) {
			t.Fatal("reopen lost committed state")
		}
	})
	t.Run("binary_ranges_and_byte_bounded_progress", func(t *testing.T) {
		b := backend(t, factory(t))
		keys := [][]byte{{'a'}, {'a', 0}, {'a', 255}, {'a', 255, 0}, {'b'}, {255}, {255, 255}}
		for _, key := range keys {
			put(t, b, key, bytes.Repeat([]byte{7}, 100))
		}
		for _, prefix := range [][]byte{{'a'}, {'a', 255}, {255}} {
			var want [][]byte
			for _, key := range keys {
				if bytes.HasPrefix(key, prefix) {
					want = append(want, key)
				}
			}
			var got [][]byte
			var after []byte
			for pages := 0; ; pages++ {
				if pages > 10 {
					t.Fatal("page did not progress")
				}
				var page storage.Page
				check(t, b.View(t.Context(), func(tx storage.ReadTx) error {
					var err error
					page, err = tx.Scan(storage.Range{Begin: prefix, End: storage.PrefixEnd(prefix), After: after}, storage.Limit{Rows: 2, Bytes: 1})
					return err
				}))
				for _, row := range page.Rows {
					got = append(got, row.Key)
					after = row.Key
				}
				if !page.More {
					break
				}
				if len(page.Rows) == 0 {
					t.Fatal("empty continuation")
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("range %x: got %x want %x", prefix, got, want)
			}
		}
	})
	t.Run("serializable_missing_key_and_range_claim", func(t *testing.T) {
		b := backend(t, factory(t))
		var winners atomic.Int32
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for worker := 0; worker < 8; worker++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for attempt := 0; attempt < 100; attempt++ {
					won := false
					err := b.Update(t.Context(), func(tx storage.Tx) error {
						missing, err := tx.Get([]byte("claim"))
						if err != nil {
							return err
						}
						page, err := tx.Scan(storage.Range{Begin: []byte("slot/"), End: storage.PrefixEnd([]byte("slot/"))}, storage.Limit{Rows: 2, Bytes: 1024})
						if err != nil {
							return err
						}
						if missing.Exists || len(page.Rows) > 0 {
							return nil
						}
						won = true
						return tx.Put([]byte(fmt.Sprintf("slot/%d", id)), []byte("owned"))
					})
					if errors.Is(err, storage.ErrRetry) {
						time.Sleep(time.Millisecond)
						continue
					}
					if err != nil {
						errs <- err
					} else if won {
						winners.Add(1)
					}
					return
				}
				errs <- errors.New("retry exhausted")
			}(worker)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			check(t, err)
		}
		if winners.Load() != 1 {
			t.Fatalf("range phantom allowed %d winners", winners.Load())
		}
	})
	t.Run("arm_race_cross_handle_cancel_and_close", func(t *testing.T) {
		open := factory(t)
		b := backend(t, open)
		other := backend(t, open)
		key := []byte("watch")
		put(t, b, key, []byte("one"))
		old := get(t, b, key)
		put(t, other, key, []byte("two"))
		o, err := b.Observe(t.Context(), []storage.Condition{{Key: key, Value: old}})
		check(t, err)
		await(t, o)
		check(t, o.Err())
		o.Cancel()
		current := get(t, b, key)
		o, err = b.Observe(t.Context(), []storage.Condition{{Key: key, Value: current}})
		check(t, err)
		put(t, other, key, []byte("three"))
		await(t, o)
		check(t, o.Err())
		o.Cancel()
		current = get(t, b, key)
		reserved, release, err := b.ReserveObservation(t.Context())
		check(t, err)
		o, err = b.Observe(reserved, []storage.Condition{{Key: key, Value: current}})
		check(t, err)
		release()
		await(t, o)
		o, err = b.Observe(t.Context(), []storage.Condition{{Key: key, Value: current}})
		check(t, err)
		check(t, b.Close())
		await(t, o)
		if stats, ok := b.(interface {
			ObservationSlots() int
			ActiveObservations() int64
		}); ok && (stats.ObservationSlots() != 0 || stats.ActiveObservations() != 0) {
			t.Fatal("Close leaked observation resources")
		}
		if !get(t, other, key).Exists {
			t.Fatal("closing one handle closed its companion")
		}
	})
	RunRecords(t, factory)
}

type faultEngine struct {
	storage.Engine
	update func(context.Context, func(storage.Tx) error) error
}

func (f faultEngine) Update(ctx context.Context, fn func(storage.Tx) error) error {
	return f.update(ctx, fn)
}
func RunRecords(t *testing.T, factory Factory) {
	runRecordScan(t, factory)
	runExtension(t, factory)
	runProjectionContracts(t, factory)
	runIntegrity(t, factory)
	t.Run("record_delete_recreate_chunks_and_long_identity", func(t *testing.T) {
		open := factory(t)
		b := backend(t, open)
		s := record.New(b, b)
		long := bytes.Repeat([]byte("incompressible-identity-"), 180)
		key := record.NewKey("group", string(long), "route")
		payload := bytes.Repeat([]byte("payload"), 400_000)
		first, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			_, err := tx.Expect(key, record.Version{})
			if err != nil {
				return false, err
			}
			return true, tx.Put(key, payload)
		})
		check(t, err)
		check(t, s.Read(t.Context(), func(tx *record.Tx) error {
			v, err := tx.Get(key)
			if err == nil && (!v.Exists || !bytes.Equal(v.Payload, payload)) {
				return errors.New("chunk roundtrip")
			}
			return err
		}))
		_, err = s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			if _, err := tx.Expect(key, first); err != nil {
				return false, err
			}
			tx.Delete(key)
			return true, nil
		})
		check(t, err)
		check(t, b.Close())
		b = backend(t, open)
		s = record.New(b, b)
		fresh, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			if _, err := tx.Expect(key, record.Version{}); err != nil {
				return false, err
			}
			return true, tx.Put(key, []byte("replacement"))
		})
		check(t, err)
		if fresh.Compare(first) <= 0 {
			t.Fatal("delete/recreate reused or reversed revision")
		}
		_, err = s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			if _, err := tx.Expect(key, first); err != nil {
				return false, err
			}
			return true, tx.Put(key, []byte("late old reply"))
		})
		if !errors.Is(err, record.ErrConflict) {
			t.Fatalf("stale revision accepted: %v", err)
		}
	})
	t.Run("retry_keeps_original_business_precondition", func(t *testing.T) {
		b := backend(t, factory(t))
		plain := record.New(b, b)
		guard := record.NewKey("node", "registration")
		output := record.NewKey("intent")
		initial, err := plain.Update(t.Context(), func(tx *record.Tx) (bool, error) { return true, tx.Put(guard, []byte("old registration")) })
		check(t, err)
		var attempts int
		abort := errors.New("fault before commit")
		wrapped := faultEngine{Engine: b, update: func(ctx context.Context, fn func(storage.Tx) error) error {
			attempts++
			if attempts != 1 {
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
			_, err = plain.Update(ctx, func(tx *record.Tx) (bool, error) {
				if _, err := tx.Expect(guard, initial); err != nil {
					return false, err
				}
				return true, tx.Put(guard, []byte("new registration"))
			})
			if err != nil {
				return err
			}
			return storage.ErrRetry
		}}
		s := record.New(wrapped, b)
		_, err = s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			if _, err := tx.Get(guard); err != nil {
				return false, err
			}
			return true, tx.Put(output, []byte("must not attach to new registration"))
		})
		if !errors.Is(err, record.ErrConflict) || attempts != 2 {
			t.Fatalf("retry adopted new condition: attempts=%d err=%v", attempts, err)
		}
		check(t, plain.Read(t.Context(), func(tx *record.Tx) error {
			v, err := tx.Get(output)
			if v.Exists {
				return errors.New("stale intent committed")
			}
			return err
		}))
	})
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown_commit_effect_%v", committed), func(t *testing.T) {
			b := backend(t, factory(t))
			attempts := 0
			abort := errors.New("disconnect")
			wrapped := faultEngine{Engine: b, update: func(ctx context.Context, fn func(storage.Tx) error) error {
				attempts++
				err := b.Update(ctx, func(tx storage.Tx) error {
					if err := fn(tx); err != nil {
						return err
					}
					if !committed {
						return abort
					}
					return nil
				})
				if err != nil && !errors.Is(err, abort) {
					return err
				}
				return &storage.Unknown{Cause: abort}
			}}
			s := record.New(wrapped, b)
			key := record.NewKey("uncertain")
			version, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) { return true, tx.Put(key, []byte("value")) })
			if !errors.Is(err, storage.ErrUnknown) || attempts != 1 || !version.IsZero() {
				t.Fatalf("uncertain result replayed/published: %d %v %v", attempts, version, err)
			}
			plain := record.New(b, b)
			check(t, plain.Read(t.Context(), func(tx *record.Tx) error {
				v, err := tx.Get(key)
				if v.Exists != committed {
					return errors.New("unexpected committed effect")
				}
				return err
			}))
		})
	}
}
