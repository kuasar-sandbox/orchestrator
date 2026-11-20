package observebudget

import (
	"errors"
	"testing"
)

func TestCatalogReservedBudget(t *testing.T) {
	b := NewBudget(260)
	var releases []func()
	for i := 0; i < 4; i++ {
		release, err := b.TryAcquire()
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	if _, err := b.TryAcquire(); !errors.Is(err, ErrCapacity) {
		t.Fatalf("non-catalog consumed reserve: %v", err)
	}
	catalog, err := b.TryAcquireCatalog()
	if err != nil {
		t.Fatal(err)
	}
	catalog()
	catalog()
	for _, release := range releases {
		release()
		release()
	}
	if b.Used() != 0 {
		t.Fatal("release not exactly once")
	}
}
