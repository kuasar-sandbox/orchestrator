package sqlengine

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

// OpenSQLite uses WAL, FULL durability and BEGIN IMMEDIATE for writes. Read-only
// transactions retain ordinary WAL snapshots. SQLite is a single-Registry backend.
func OpenSQLite(ctx context.Context, path, namespace string) (*Engine, error) {
	if path == "" || path == ":memory:" {
		return nil, fmt.Errorf("%w: persistent SQLite path required", storage.ErrInvalid)
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	if canonical, err := filepath.EvalSymlinks(path); err == nil {
		path = canonical
	}
	// Metadata includes private key material. Create a new database privately
	// even when its parent is an existing, more permissive directory.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Set("_txlock", "immediate")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "busy_timeout(1000)")
	u.RawQuery = q.Encode()
	return open(ctx, "sqlite:"+path, "sqlite", u.String(), namespace, false)
}
