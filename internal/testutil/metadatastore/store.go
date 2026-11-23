// Package metadatastore runs consumer tests against the production metadata
// repositories on a disposable SQLite database. Only connection-owner placement
// and admission-capacity fault injection remain test adapters.
package metadatastore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metastore"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/sqlengine"
)

type Owners struct {
	ID      string
	Members []string
	Count   int
}

func (o Owners) LocalID() string {
	if o.ID == "" {
		return "registry"
	}
	return o.ID
}
func (o Owners) Candidates(ctx context.Context, id string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(o.Members) == 0 {
		return []string{o.LocalID()}, nil
	}
	n := o.Count
	if n <= 0 || n > len(o.Members) {
		n = len(o.Members)
	}
	sum := sha256.Sum256([]byte(id))
	start := int(binary.BigEndian.Uint32(sum[:4])) % len(o.Members)
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, o.Members[(start+i)%len(o.Members)])
	}
	return out, nil
}

// Version constructs a complete deterministic stamp only for test expectations.
func Version(n uint64) metadata.Version {
	var v metadata.Version
	binary.BigEndian.PutUint64(v[:8], n)
	return v
}

type Store struct {
	*metastore.Store
	mu                  sync.Mutex
	limit, reservations int
}

func New(t testing.TB) *Store {
	t.Helper()
	engine, err := sqlengine.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "metadata.sqlite"), "consumer-test")
	if err != nil {
		t.Fatal(err)
	}
	native := metastore.New(engine, engine)
	t.Cleanup(func() {
		if err := native.Close(); err != nil {
			t.Errorf("close metadata fixture: %v", err)
		}
	})
	return &Store{Store: native, limit: 4096}
}

func (s *Store) Repositories() metadata.Repositories {
	repos := s.Store.Repositories()
	repos.Observations = s
	return repos
}
func (s *Store) SetObservationLimit(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limit = limit
}
func (s *Store) ObservationReservations() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reservations
}

type reservationKey struct{}

func (s *Store) ReserveObservation(ctx context.Context) (context.Context, func(), error) {
	if ctx.Value(reservationKey{}) == s {
		return ctx, func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	if s.reservations >= s.limit {
		s.mu.Unlock()
		return nil, nil, metadata.ErrCapacity
	}
	s.reservations++
	s.mu.Unlock()
	var once sync.Once
	undo := func() { once.Do(func() { s.mu.Lock(); s.reservations--; s.mu.Unlock() }) }
	reserved, release, err := s.Store.ReserveObservation(ctx)
	if err != nil {
		undo()
		return nil, nil, err
	}
	return context.WithValue(reserved, reservationKey{}, s), func() { release(); undo() }, nil
}
