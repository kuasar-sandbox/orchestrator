package storagetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

// supportRepository is an acceptance-only example of a future shared repository.
// Its schema and business predicates live entirely above the engine/record layer.
// It is deliberately not the production Group protocol, which is a separate stage.
type supportRepository struct{ store *record.Store }
type supportRow struct {
	Node, Group string
	Fence       record.Version
}

func registration(node string) record.Key  { return record.NewKey("extension", "registration", node) }
func byNode(node, group string) record.Key { return record.NewKey("extension", "by-node", node, group) }
func byGroup(group, node string) record.Key {
	return record.NewKey("extension", "by-group", group, node)
}

func (r supportRepository) publish(ctx context.Context, node, group string, fence record.Version) error {
	payload, err := json.Marshal(supportRow{node, group, fence})
	if err != nil {
		return err
	}
	_, err = r.store.Update(ctx, func(tx *record.Tx) (bool, error) {
		if _, err := tx.Expect(registration(node), fence); err != nil {
			return false, err
		}
		if err := tx.Put(byNode(node, group), payload); err != nil {
			return false, err
		}
		return true, tx.Put(byGroup(group, node), payload)
	})
	return err
}

func (r supportRepository) claim(ctx context.Context, node, group, operation string, fence record.Version) error {
	_, err := r.store.Update(ctx, func(tx *record.Tx) (bool, error) {
		if _, err := tx.Expect(registration(node), fence); err != nil {
			return false, err
		}
		support, err := tx.Get(byGroup(group, node))
		if err != nil {
			return false, err
		}
		if !support.Exists {
			return false, record.ErrConflict
		}
		var row supportRow
		if err := json.Unmarshal(support.Payload, &row); err != nil {
			return false, err
		}
		if row.Node != node || row.Group != group || row.Fence != fence {
			return false, record.ErrConflict
		}
		key := record.NewKey("extension", "claim", operation)
		if _, err := tx.Expect(key, record.Version{}); err != nil {
			return false, err
		}
		return true, tx.Put(key, []byte(node))
	})
	return err
}

func runExtension(t *testing.T, factory Factory) {
	t.Run("shared_repository_extension_without_driver_changes", func(t *testing.T) {
		b := backend(t, factory(t))
		s := record.New(b, b)
		r := supportRepository{store: s}
		fences := make(map[string]record.Version)
		for _, node := range []string{"node-a", "node-b"} {
			v, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
				return true, tx.Put(registration(node), []byte("registered"))
			})
			check(t, err)
			fences[node] = v
			for _, group := range []string{"group-a", "group-b", "group-c"} {
				check(t, r.publish(t.Context(), node, group, v))
			}
		}
		var winners atomic.Int32
		var workers sync.WaitGroup
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			workers.Add(1)
			go func(i int) {
				defer workers.Done()
				node := []string{"node-a", "node-b"}[i%2]
				err := r.claim(t.Context(), node, "group-a", "one-operation", fences[node])
				if err == nil {
					winners.Add(1)
				} else if !errors.Is(err, record.ErrConflict) {
					errs <- err
				}
			}(i)
		}
		workers.Wait()
		close(errs)
		for err := range errs {
			check(t, err)
		}
		if winners.Load() != 1 {
			t.Fatalf("concurrent selection produced %d claims", winners.Load())
		}
		newFence, err := s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
			return true, tx.Put(registration("node-a"), []byte("reconnected"))
		})
		check(t, err)
		if err = r.publish(t.Context(), "node-a", "group-new", fences["node-a"]); !errors.Is(err, record.ErrConflict) {
			t.Fatalf("old session published support: %v", err)
		}
		if err = r.claim(t.Context(), "node-a", "group-a", "after-reconnect", newFence); !errors.Is(err, record.ErrConflict) {
			t.Fatalf("old support was used in new session: %v", err)
		}
		check(t, r.publish(t.Context(), "node-a", "group-a", newFence))
		// Bounded cleanup atomically removes both index directions. Recover the
		// business identity from the known schema, not from a driver query type.
		removed := 0
		var after record.Key
		for {
			var count int
			var next record.Key
			_, err = s.Update(t.Context(), func(tx *record.Tx) (bool, error) {
				count = 0
				next = nil
				if _, err := tx.Expect(registration("node-a"), newFence); err != nil {
					return false, err
				}
				page, err := tx.Scan(record.NewKey("extension", "by-node", "node-a"), after, storage.Limit{Rows: 2, Bytes: 1024})
				if err != nil {
					return false, err
				}
				for _, item := range page.Items {
					var row supportRow
					if err := json.Unmarshal(item.Value.Payload, &row); err != nil {
						return false, err
					}
					if row.Fence != fences["node-a"] {
						continue // preserve a relation already replaced by the new session
					}
					tx.Delete(item.Key)
					tx.Delete(byGroup(row.Group, row.Node))
					count++
				}
				if page.More && len(page.Items) > 0 {
					next = page.Items[len(page.Items)-1].Key
				}
				return count > 0, nil
			})
			check(t, err)
			removed += count
			if next == nil {
				break
			}
			after = next
			if removed > 3 {
				t.Fatal("cleanup did not make bounded progress")
			}
		}
		if removed != 2 {
			t.Fatalf("cleaned %d support relations", removed)
		}
		check(t, s.Read(t.Context(), func(tx *record.Tx) error {
			for _, group := range []string{"group-a", "group-b", "group-c"} {
				page, err := tx.Scan(record.NewKey("extension", "by-group", group), nil, storage.Limit{Rows: 4, Bytes: 1024})
				if err != nil {
					return err
				}
				if group == "group-a" {
					if len(page.Items) != 2 || !bytes.Equal(page.Items[0].Key, byGroup(group, "node-a")) || !bytes.Equal(page.Items[1].Key, byGroup(group, "node-b")) {
						return errors.New("old cleanup deleted current registration support")
					}
				} else if len(page.Items) != 1 || !bytes.Equal(page.Items[0].Key, byGroup(group, "node-b")) {
					return fmt.Errorf("%s reverse index is inconsistent", group)
				}
			}
			v, err := tx.Get(record.NewKey("extension", "claim", "one-operation"))
			if err == nil && !v.Exists {
				return errors.New("support cleanup removed an accepted operation")
			}
			return err
		}))
	})
}
