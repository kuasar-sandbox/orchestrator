package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

// This test uses the real companion CLI supplied by the integration source set.
// It covers HTTP -> Export -> sandbox-ctl -> storage, then the portable fast path.
func TestExportPublicationCLIAPI(t *testing.T) {
	binary := os.Getenv("KUASAR_TEST_SANDBOX_CTL")
	if binary == "" {
		t.Skip("KUASAR_TEST_SANDBOX_CTL is required for companion CLI integration")
	}
	t.Setenv("PATH", filepath.Dir(binary)+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MANIFEST_CONFIG", "")
	for _, kind := range []types.ResumeSourceKind{types.ResumeSourceSandbox, types.ResumeSourceSnapshot} {
		for _, template := range []bool{false, true} {
			for _, keep := range []bool{false, true} {
				for _, portable := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/template=%t/keep=%t/portable=%t", kind, template, keep, portable), func(t *testing.T) {
						ctx := context.Background()
						dir := t.TempDir()
						o := migrationOrchestrator(t, dir, []byte("runtime"))
						o.snapshotSandboxRef = nil
						output := filepath.Join(dir, "published")
						o.cfg.Checkpoint.Remote.RefLocationParent = "file://" + output
						mk := strings.Repeat("6", 64)
						_, apiKey := defaultTestCredentials(t, mk)
						sid := "report-source"
						suffix := ".sandbox"
						if kind == types.ResumeSourceSnapshot {
							suffix = ".snapshot"
						}
						local := makeLocalArtifact(t, dir, sid, suffix)
						sink := snapshot.NewFileSink(filepath.Dir(local), "report", nil, false, nil)
						cfg := &rtconfig.PortableSandboxConfig{
							Version: 1, Resources: rtconfig.PortableResourcesConfig{Capacity: rtconfig.CapacityConfig{CPU: 1, Memory: "1GiB"}, Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "1GiB"}},
							Boot:   rtconfig.PortableBootConfig{Kernel: "file://kernel@digest:" + mk, Runtime: "file://runtime@digest:" + mk, Root: rtconfig.PortableRootConfig{Base: "self"}},
							Launch: rtconfig.PortableLaunchConfig{Workdir: "/", Restart: "never"},
						}
						runtime, err := rtconfig.MarshalPortableSandboxConfig(cfg)
						if err != nil {
							t.Fatal(err)
						}
						body := bytes.Repeat([]byte{0x41}, 4096)
						e, err := sandboxfile.BuildSource(sparse.Dense(bytes.NewReader(body), uint64(len(body))), nil, runtime)
						if err != nil {
							t.Fatal(err)
						}
						eRef, ePath, err := sink.AbsorbSandbox(ctx, e)
						if err != nil {
							t.Fatal(err)
						}
						sourcePath := ePath
						if kind == types.ResumeSourceSnapshot {
							scfg, _ := snapshot.MarshalConfig(&snapshot.Config{Version: 1, SandboxRef: eRef})
							s, err := snapshotfile.BuildSource(sparse.Dense(bytes.NewReader(body), uint64(len(body))), []byte("{}"), []byte("{}"), scfg)
							if err != nil {
								t.Fatal(err)
							}
							_, sourcePath, err = sink.AbsorbSnapshot(ctx, s)
							if err != nil {
								t.Fatal(err)
							}
						}
						if err := os.Rename(sourcePath, local); err != nil {
							t.Fatal(err)
						}
						sb := migrationSandbox(t, dir, sid, mk, local)
						sb.ResumeSource.Kind = kind
						if err := o.st.Put(ctx, sb); err != nil {
							t.Fatal(err)
						}
						o.cache(sb)
						if portable {
							initial, err := o.ExportSandbox(ctx, apiKey, sid, true, true)
							if err != nil {
								t.Fatal(err)
							}
							sb.ResumeSource.Ref = initial.SandboxRef
							if kind == types.ResumeSourceSnapshot {
								sb.ResumeSource.Ref = initial.SnapshotRef
							}
							if err := o.st.Put(ctx, sb); err != nil {
								t.Fatal(err)
							}
							o.cache(sb)
						}
						request := httptest.NewRequest(http.MethodPost, "/sandboxes/"+sid+"/export", strings.NewReader(fmt.Sprintf(`{"toTemplate":%t,"keepSource":%t}`, template, keep)))
						request.Header.Set("X-API-KEY", apiKey)
						response := httptest.NewRecorder()
						api.New(o, "example.test", api.Resources{}, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler().ServeHTTP(response, request)
						if response.Code != http.StatusOK {
							t.Fatalf("HTTP %d %s", response.Code, response.Body.String())
						}
						var result types.ExportResult
						if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
							t.Fatal(err)
						}
						if err := result.Validate(); err != nil {
							t.Fatal(err)
						}
						if portable && len(result.RemovedRefs) != 0 {
							t.Fatalf("portable removals=%v", result.RemovedRefs)
						}
						if !portable && len(result.RemovedRefs) == 0 {
							t.Fatal("local roots not reported")
						}
						if strings.Contains(response.Body.String(), dir) {
							t.Fatal("private directories in result")
						}
						fields := map[string]json.RawMessage{}
						json.Unmarshal(response.Body.Bytes(), &fields)
						want := 3
						if kind == types.ResumeSourceSnapshot {
							want = 4
						}
						if len(fields) != want {
							t.Fatalf("shape=%s", response.Body.String())
						}
						storage, _ := artifact.NewProcessStorage(nil)
						location, err := reflocation.Resolve(o.cfg.Checkpoint.Remote.RefLocationParent, sid)
						if err != nil {
							t.Fatal(err)
						}
						locations := rtconfig.RefLocations{sid: location.Path}
						root := result.SandboxRef
						if kind == types.ResumeSourceSnapshot {
							root = result.SnapshotRef
						}
						info, err := storage.Inspect(ctx, root, locations)
						storage.Close()
						if err != nil {
							t.Fatal(err)
						}
						if kind == types.ResumeSourceSnapshot && result.SandboxRef != info.Snapshot.SandboxRef {
							t.Fatal("incoherent final S/E")
						}
						stored, err := o.st.Get(ctx, sid)
						if err != nil {
							t.Fatal(err)
						}
						if keep {
							if stored == nil || stored.ResumeSource != sb.ResumeSource {
								t.Fatal("keep-source changed source")
							}
						} else if stored != nil {
							t.Fatal("move did not finalize source")
						}
					})
				}
			}
		}
	}
}
