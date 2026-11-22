package cluster_stub_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metastore"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/sqlengine"
)

type testMetadataEngine interface {
	storage.Engine
	storage.Observer
}

var nativeMetadataFactory func(testing.TB) testMetadataEngine

func newTestRegistryStores(t testing.TB) *registry.Stores {
	t.Helper()
	backend := os.Getenv("STORAGE_TEST_BACKEND")
	if backend == "" {
		if nativeMetadataFactory != nil {
			backend = "fdb"
		} else {
			backend = "sqlite"
		}
	}
	var engine testMetadataEngine
	var err error
	switch backend {
	case "sqlite":
		engine, err = sqlengine.OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "metadata.sqlite"), "cluster-http")
	case "postgres":
		dsn := os.Getenv("STORAGE_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Fatal("PostgreSQL HTTP integration requires STORAGE_TEST_POSTGRES_DSN")
		}
		engine, err = sqlengine.OpenPostgres(t.Context(), dsn, "cluster-http-"+uuid.NewString())
	case "fdb":
		if nativeMetadataFactory == nil {
			t.Fatal("FDB HTTP integration requires a native build")
		}
		engine = nativeMetadataFactory(t)
	default:
		t.Fatalf("unknown STORAGE_TEST_BACKEND %q", backend)
	}
	if err != nil {
		t.Fatal(err)
	}
	database := metastore.New(engine, engine)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	owners, err := cluster.NewConnectionOwners("registry", []cluster.MemberView{{Version: 1, Members: []string{"registry"}}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := registry.NewStores(database.Repositories(), owners)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
