package placer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
)

// BenchmarkFileGroupImportPass exercises the production import loop with one
// file per Group. Registry lease/checkpoint replies are in memory and there are
// no target Nodes: this measures source reading and command preparation, not
// end-to-end import throughput or Node writes. Fixture creation is not timed.
func BenchmarkFileGroupImportPass(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 100_000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			dir := b.TempDir()
			for i := 0; i < count; i++ {
				record := clusterstate.SandboxGroupRecord{
					Group:     "/g/" + strconv.Itoa(i),
					APISecret: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret},
				}
				raw, err := json.Marshal(record)
				if err != nil {
					b.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, strconv.Itoa(i)+".json"), raw, 0600); err != nil {
					b.Fatal(err)
				}
			}
			source, err := NewFileGroupSource("benchmark", dir)
			if err != nil {
				b.Fatal(err)
			}
			transport := &benchmarkImportTransport{}
			svc := NewRemoteLinks([]RegistryLink{{Name: "registry", BaseURL: "http://registry.invalid", Client: &http.Client{Transport: transport}}}, clustercfg.PlacementConfig{}, nil)
			transport.lease, err = json.Marshal(registry.ImportSourceLeaseResponse{
				Acquired: true,
				Lease: registry.ImportSourceLease{
					SourceID: "benchmark", OwnerID: "local", RunID: svc.runID, Term: 1,
					ExpiresUnixMs: time.Now().Add(time.Hour).UnixMilli(),
				},
			})
			if err != nil {
				b.Fatal(err)
			}
			input := ImportSource{SourceID: "benchmark", Importer: source}
			emit := func(result ImportResult) { b.Fatalf("unexpected target result: %+v", result) }
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := svc.importSource(b.Context(), input, nil, false, emit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type benchmarkImportTransport struct{ lease []byte }

func (t *benchmarkImportTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	_ = req.Body.Close()
	switch req.URL.Path {
	case registry.PlacerLinkImportSourcePath:
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(t.lease)), Header: make(http.Header), Request: req}, nil
	case registry.PlacerLinkSourceCursorPath:
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody, Header: make(http.Header), Request: req}, nil
	default:
		return nil, fmt.Errorf("unexpected benchmark request: %s", req.URL.Path)
	}
}
