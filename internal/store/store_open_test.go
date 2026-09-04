package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
)

func TestOpenEscapesSQLiteURIPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node#state?100%.db")
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database was not created at the literal path %q: %v", path, err)
	}

	// An unescaped file: URI truncates this path at '#', creating the database
	// outside the test-owned directory instead.
	truncated := strings.SplitN(path, "#", 2)[0]
	if _, err := os.Stat(truncated); !os.IsNotExist(err) {
		t.Fatalf("SQLite URI fragment leaked a database at %q: %v", truncated, err)
	}
}
