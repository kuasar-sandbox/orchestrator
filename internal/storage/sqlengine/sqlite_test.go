package sqlengine_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/sqlengine"
)

func TestSQLiteNewDatabaseThroughDirectoryAliasSharesAdmission(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "data")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(directory, alias); err != nil {
		t.Fatal(err)
	}
	first, err := sqlengine.OpenSQLite(t.Context(), filepath.Join(alias, "metadata.sqlite"), "alias-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := sqlengine.OpenSQLite(t.Context(), filepath.Join(directory, "metadata.sqlite"), "alias-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	_, release, err := first.ReserveObservation(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	if first.ObservationSlots() != 1 || second.ObservationSlots() != 1 {
		t.Fatalf("aliases split the database admission budget: first=%d second=%d", first.ObservationSlots(), second.ObservationSlots())
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	if second.ObservationSlots() != 0 {
		t.Fatal("closing one alias leaked its reservation")
	}
	if err = second.Update(t.Context(), func(tx storage.Tx) error { return tx.Put([]byte("still-open"), []byte("ok")) }); err != nil {
		t.Fatalf("closing one alias disabled the remaining handle: %v", err)
	}
}
