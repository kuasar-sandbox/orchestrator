//go:build fdb && cgo

package metastore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/fdbdriver"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/storagetest"
)

func init() {
	benchmarkBackends["fdb"] = func(b *testing.B) *Store {
		e, err := fdbdriver.OpenEngine(fdbdriver.EngineOptions{ClusterFile: os.Getenv("FDB_CLUSTER_FILE"), Prefix: fmt.Sprintf("/benchmark/%d/", time.Now().UnixNano())})
		if err != nil {
			b.Fatal(err)
		}
		return New(e, e)
	}
	backendCases["fdb"] = func(t *testing.T) func() storagetest.Backend {
		file := os.Getenv("FDB_CLUSTER_FILE")
		if file == "" {
			t.Fatal("FDB_CLUSTER_FILE must point to isolated test database")
		}
		var id [16]byte
		_, err := rand.Read(id[:])
		check(t, err)
		prefix := "/metadata-conformance/" + hex.EncodeToString(id[:]) + "/"
		return func() storagetest.Backend {
			e, err := fdbdriver.OpenEngine(fdbdriver.EngineOptions{ClusterFile: file, Prefix: prefix})
			check(t, err)
			return e
		}
	}
}
