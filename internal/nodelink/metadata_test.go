package nodelink

import (
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/testutil/metadatastore"
	"testing"
)

func newTestRegistryStores(t testing.TB) *registry.Stores {
	t.Helper()
	store, err := registry.NewStores(metadatastore.New(t).Repositories(), metadatastore.Owners{ID: "registry"})
	if err != nil {
		t.Fatal(err)
	}
	return store
}
