//go:build fdb && cgo

package cluster_stub_test

import (
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/fdbdriver"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
)

// Native CI runs the same public HTTP and NodeLink scenarios as the pure Go
// suite, with an isolated prefix in the real, explicit test database.
func newTestRegistryStores(t testing.TB) *registry.Stores {
	t.Helper()
	clusterFile := os.Getenv("FDB_CLUSTER_FILE")
	if clusterFile == "" {
		t.Fatal("native cluster integration requires FDB_CLUSTER_FILE")
	}
	db, err := fdbdriver.Open(fdbdriver.Options{ClusterFile: clusterFile, Prefix: "test/cluster-http/" + strings.ReplaceAll(t.Name(), "/", "-") + "/" + uuid.NewString() + "/"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	owners, err := cluster.NewConnectionOwners("registry", []cluster.MemberView{{Version: 1, Members: []string{"registry"}}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	stores, err := registry.NewStores(db.Repositories(), owners)
	if err != nil {
		t.Fatal(err)
	}
	return stores
}
