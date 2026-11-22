//go:build fdb && cgo

package cluster_stub_test

import (
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/kuasar-sandbox/orchestrator/internal/fdbdriver"
)

// Native CI runs the same public HTTP and NodeLink scenarios as the pure Go
// suite, with an isolated prefix in the real, explicit test database.
func init() { nativeMetadataFactory = newNativeMetadataEngine }
func newNativeMetadataEngine(t testing.TB) testMetadataEngine {
	t.Helper()
	clusterFile := os.Getenv("FDB_CLUSTER_FILE")
	if clusterFile == "" {
		t.Fatal("native cluster integration requires FDB_CLUSTER_FILE")
	}
	db, err := fdbdriver.OpenEngine(fdbdriver.EngineOptions{ClusterFile: clusterFile, Prefix: "test/cluster-http/" + strings.ReplaceAll(t.Name(), "/", "-") + "/" + uuid.NewString() + "/"})
	if err != nil {
		t.Fatal(err)
	}
	return db
}
