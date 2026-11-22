//go:build fdb && cgo

package fdbdriver

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/storage/storagetest"
)

func TestEngineConformance(t *testing.T) {
	file := os.Getenv("FDB_CLUSTER_FILE")
	if file == "" {
		t.Fatal("FDB_CLUSTER_FILE must point to an isolated native test database")
	}
	storagetest.Run(t, func(t *testing.T) func() storagetest.Backend {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			t.Fatal(err)
		}
		prefix := "/storage-conformance/" + hex.EncodeToString(id[:]) + "/"
		return func() storagetest.Backend {
			e, err := OpenEngine(EngineOptions{ClusterFile: file, Prefix: prefix})
			if err != nil {
				t.Fatal(err)
			}
			return e
		}
	})
}
