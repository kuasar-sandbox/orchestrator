package sqlengine_test

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage/sqlengine"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/storagetest"
)

func TestSQLiteConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) func() storagetest.Backend {
		path := filepath.Join(t.TempDir(), "metadata.sqlite")
		return func() storagetest.Backend {
			e, err := sqlengine.OpenSQLite(t.Context(), path, "conformance")
			if err != nil {
				t.Fatal(err)
			}
			return e
		}
	})
}
func TestPostgresConformance(t *testing.T) {
	dsn := os.Getenv("STORAGE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set STORAGE_TEST_POSTGRES_DSN to run against a real isolated PostgreSQL server")
	}
	storagetest.Run(t, func(t *testing.T) func() storagetest.Backend {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			t.Fatal(err)
		}
		namespace := "conformance-" + hex.EncodeToString(id[:])
		return func() storagetest.Backend {
			e, err := sqlengine.OpenPostgres(t.Context(), dsn, namespace)
			if err != nil {
				t.Fatal(err)
			}
			return e
		}
	})
}
