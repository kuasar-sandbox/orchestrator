//go:build fdb && cgo

package registry

import (
	"os"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/fdbdriver"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/storagetest"
)

func init() {
	registryNativeBackends["fdb"] = func(t *testing.T) func() storagetest.Backend {
		file := os.Getenv("FDB_CLUSTER_FILE")
		if file == "" {
			t.Fatal("FDB_CLUSTER_FILE must point to an isolated test database")
		}
		prefix := "/registry-faults/" + newID() + "/"
		return func() storagetest.Backend {
			e, err := fdbdriver.OpenEngine(fdbdriver.EngineOptions{ClusterFile: file, Prefix: prefix})
			if err != nil {
				t.Fatal(err)
			}
			return e
		}
	}
}
