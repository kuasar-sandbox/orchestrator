package store

import (
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
)

func BenchmarkNodeGroup(b *testing.B) {
	for _, operation := range []string{"Get", "Update", "DeleteRecreate", "Renew", "Snapshot128"} {
		b.Run(operation, func(b *testing.B) {
			must := func(err error) {
				b.Helper()
				if err != nil {
					b.Fatal(err)
				}
			}
			box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
			must(err)
			s, err := Open(filepath.Join(b.TempDir(), "groups.db"), box)
			must(err)
			b.Cleanup(func() {
				if err := s.Close(); err != nil {
					b.Error(err)
				}
			})
			pair := testKeyPair("1", "2")
			raw, err := hex.DecodeString(pair.APISecret)
			must(err)
			key, err := apikey.Mint(raw)
			must(err)
			put := &nodegroup.KeyPut{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, ExpiresUnix: time.Now().Unix() + 3600}
			current, _, err := s.GroupPut(b.Context(), key, "g", 0, nodegroup.Context{}, put)
			must(err)
			if operation == "Snapshot128" {
				for i := 1; i < 128; i++ {
					_, _, err := s.GroupPut(b.Context(), key, "g"+strings.Repeat("x", i), 0, nodegroup.Context{}, nil)
					must(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				switch operation {
				case "Get":
					_, err = s.GroupGet(b.Context(), key, "g")
				case "Update":
					current, _, err = s.GroupPut(b.Context(), key, "g", current.Version, nodegroup.Context{TargetPort: int(current.Version%2) + 80}, nil)
				case "DeleteRecreate":
					current, err = s.GroupDelete(b.Context(), key, "g", current.Version)
					must(err)
					current, _, err = s.GroupPut(b.Context(), key, "g", current.Version, nodegroup.Context{}, nil)
				case "Renew":
					put.ExpiresUnix++
					current, _, err = s.GroupPut(b.Context(), key, "g", current.Version, nodegroup.Context{}, put)
				case "Snapshot128":
					_, groups, readErr := s.GroupSnapshot(b.Context())
					err = readErr
					if err == nil && len(groups) != 128 {
						b.Fatalf("snapshot lost groups: %d", len(groups))
					}
				}
				must(err)
			}
		})
	}
}
