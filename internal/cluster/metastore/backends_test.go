package metastore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/sqlengine"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/storagetest"
)

var backendCases = map[string]storagetest.Factory{
	"sqlite": func(t *testing.T) func() storagetest.Backend {
		path := filepath.Join(t.TempDir(), "metadata.sqlite")
		return func() storagetest.Backend {
			e, err := sqlengine.OpenSQLite(t.Context(), path, "metadata-conformance")
			check(t, err)
			return e
		}
	},
	"postgres": func(t *testing.T) func() storagetest.Backend {
		dsn := os.Getenv("STORAGE_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("set STORAGE_TEST_POSTGRES_DSN for real PostgreSQL repository conformance")
		}
		var id [16]byte
		_, err := rand.Read(id[:])
		check(t, err)
		namespace := "metadata-conformance-" + hex.EncodeToString(id[:])
		return func() storagetest.Backend {
			e, err := sqlengine.OpenPostgres(t.Context(), dsn, namespace)
			check(t, err)
			return e
		}
	},
}

type testStore struct {
	*Store
	backend           storagetest.Backend
	open              func() storagetest.Backend
	afterNativeCommit func(int) error
	beforeRetry       func() error
}
type testEngine struct {
	storage.Engine
	owner *testStore
}

func (e testEngine) Update(ctx context.Context, fn func(storage.Tx) error) error {
	if e.owner.beforeRetry != nil {
		abort := errors.New("test: definitely aborted physical transaction")
		err := e.Engine.Update(ctx, func(tx storage.Tx) error {
			if err := fn(tx); err != nil {
				return err
			}
			return abort
		})
		if !errors.Is(err, abort) {
			return err
		}
		intervene := e.owner.beforeRetry
		e.owner.beforeRetry = nil
		if err := intervene(); err != nil {
			return err
		}
		return storage.ErrRetry
	}
	if err := e.Engine.Update(ctx, fn); err != nil {
		return err
	}
	if e.owner.afterNativeCommit != nil {
		return e.owner.afterNativeCommit(1)
	}
	return nil
}
func newBackendStore(t *testing.T, open func() storagetest.Backend) *testStore {
	t.Helper()
	b := open()
	s := &testStore{backend: b, open: open}
	s.Store = New(testEngine{Engine: b, owner: s}, b)
	t.Cleanup(func() { check(t, s.Close()) })
	return s
}
func companion(t *testing.T, s *testStore) *testStore { return newBackendStore(t, s.open) }
func TestRepositoriesConformance(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			runRepositoryConformance(t, func(t *testing.T) *testStore { return newBackendStore(t, factory(t)) })
		})
	}
}
