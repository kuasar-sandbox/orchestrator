package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
)

func TestOpenAddsRuntimePrepareColumnToExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	created, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `ALTER TABLE builds DROP COLUMN runtime_prepare_json`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded, err := Open(path, box)
	if err != nil {
		t.Fatalf("upgrade legacy database: %v", err)
	}
	defer upgraded.Close()
	var count int
	if err := upgraded.db.QueryRowContext(context.Background(), `
		SELECT count(*) FROM pragma_table_info('builds') WHERE name='runtime_prepare_json'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("runtime_prepare_json columns = %d, want 1", count)
	}
}
