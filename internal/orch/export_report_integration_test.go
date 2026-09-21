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
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
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
						// The source basename uses checkpoint context; relative node
						// configuration must still use the conductor's original cwd.
						configPath := filepath.Join(dir, "manifest.yaml")
						if err := os.WriteFile(configPath, []byte("chunker: {mode: fixed, fixed: {size: 4KiB}}\ncrypto: {chunk: aes, manifest: aes}\n"), 0600); err != nil {
							t.Fatal(err)
						}
						cwd, err := os.Getwd()
						if err != nil {
							t.Fatal(err)
						}
						o.cfg.ManifestConfig, err = filepath.Rel(cwd, configPath)
						if err != nil {
							t.Fatal(err)
						}
						output := filepath.Join(dir, "published")
						o.cfg.Checkpoint.Remote.RefLocationParent = "file://" + output
						mk := strings.Repeat("6", 64)
						apiSecret, apiKey := defaultTestCredentials(t, mk)
						if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: mk}, "", 0, ""); err != nil {
							t.Fatal(err)
						}
						sid := "report-source"
						suffix := ".sandbox"
						if kind == types.ResumeSourceSnapshot {
							suffix = ".snapshot"
						}
						local := makeLocalArtifact(t, dir, sid, suffix)
						if err := os.Remove(local); err != nil {
							t.Fatal(err)
						}
						sink := snapshot.NewFileSink(filepath.Dir(local), sid, nil, false, nil)
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
						sourceRef, sourcePath := eRef, ePath
						if kind == types.ResumeSourceSnapshot {
							scfg, _ := snapshot.MarshalConfig(&snapshot.Config{Version: 1, SandboxRef: eRef})
							s, err := snapshotfile.BuildSource(sparse.Dense(bytes.NewReader(body), uint64(len(body))), []byte("{}"), []byte("{}"), scfg)
							if err != nil {
								t.Fatal(err)
							}
							sourceRef, sourcePath, err = sink.AbsorbSnapshot(ctx, s)
							if err != nil {
								t.Fatal(err)
							}
						}
						if kind == types.ResumeSourceSnapshot {
							err = sink.CommitSnapshot(ctx, sourceRef, sourcePath)
						} else {
							err = sink.CommitSandbox(ctx, sourceRef, sourcePath)
						}
						if err != nil {
							t.Fatal(err)
						}
						if err := sink.Close(); err != nil {
							t.Fatal(err)
						}
						sb := migrationSandbox(t, dir, sid, mk, local)
						sb.ResumeSource = types.ResumeSource{Kind: kind, Ref: sourceRef}
						if kind == types.ResumeSourceSnapshot {
							sb.ResumeSource.SandboxRef = eRef
						}
						if err := o.st.Put(ctx, sb); err != nil {
							t.Fatal(err)
						}
						o.cache(sb)
						if portable {
							initial, err := o.ExportSandbox(ctx, apiKey, sid, false, true)
							if err != nil {
								t.Fatal(err)
							}
							original, err := o.st.Get(ctx, sid)
							if err != nil || original.ResumeSource != sb.ResumeSource {
								t.Fatalf("local keep-source changed S0/E0: %+v %v", original, err)
							}
							if _, err := o.ImportSandbox(ctx, apiKey, initial.Result, "report-imported"); err != nil {
								t.Fatal(err)
							}
							sid = "report-imported"
							sb, err = o.st.Get(ctx, sid)
							if err != nil {
								t.Fatal(err)
							}
							// Every S/E carrier and any source store is unavailable. A
							// portable Export must use only the authenticated DB pair.
							if err := os.RemoveAll(output); err != nil {
								t.Fatal(err)
							}
							if err := os.RemoveAll(filepath.Dir(local)); err != nil {
								t.Fatal(err)
							}
							o.cfg.ManifestConfig = filepath.Join(dir, "missing-config.yaml")
							o.artifactPublisher = func(context.Context, *types.Sandbox, types.ResumeSource) (artifact.PublishReport, error) {
								t.Error("portable Export invoked artifact publisher")
								return artifact.PublishReport{}, fmt.Errorf("artifact access prohibited")
							}
						}
						stale := filepath.Join(sb.BaseDir, "checkpoint", "stale-local-checkpoint")
						if err := os.MkdirAll(filepath.Dir(stale), 0700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(stale, []byte("owned checkpoint"), 0600); err != nil {
							t.Fatal(err)
						}
						entered, release := make(chan struct{}), make(chan struct{})
						unblock := sync.OnceFunc(func() { close(release) })
						defer unblock()
						if !keep {
							o.removeSandboxBaseDir = func(path string) error {
								close(entered)
								<-release
								return os.RemoveAll(path)
							}
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
						if !keep {
							waitExportCleanupStep(t, entered)
							pending, err := o.st.Get(ctx, sid)
							if err != nil || pending == nil || pending.State != types.StateDeleting || pending.ResumeSource != sb.ResumeSource || o.lookup(sid) != nil {
								t.Fatalf("HTTP success lost deleting ownership: %+v, %v", pending, err)
							}
							if _, err := os.Stat(stale); err != nil {
								t.Fatalf("HTTP success bypassed owned cleanup: %v", err)
							}
							unblock()
							waitForExportDeletion(t, o, sid)
							for _, path := range []string{sb.RunDir, sb.BaseDir} {
								if _, err := os.Stat(path); !os.IsNotExist(err) {
									t.Fatalf("move retained %s: %v", path, err)
								}
							}
						}
						// Published artifacts must remain readable after source cleanup.
						if !portable {
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
						} else {
							wantE := sb.ResumeSource.Ref
							if kind == types.ResumeSourceSnapshot {
								wantE = sb.ResumeSource.SandboxRef
								if result.SnapshotRef != sb.ResumeSource.Ref {
									t.Fatal("offline S changed")
								}
							}
							if result.SandboxRef != wantE {
								t.Fatal("offline E changed")
							}
						}
						if !template {
							payload, err := migrationtoken.Open(migrationtoken.KeyMaterial{APISecret: apiSecret, ManifestKey: mk}, result.Result)
							if err != nil {
								t.Fatal(err)
							}
							if kind == types.ResumeSourceSnapshot {
								if payload.ResumeSourceRef != result.SnapshotRef || payload.ResumeSandboxRef != result.SandboxRef {
									t.Fatal("token/report pair mismatch")
								}
							} else if payload.ResumeSourceRef != result.SandboxRef || payload.ResumeSandboxRef != "" {
								t.Fatal("E-only token retained S")
							}
						}
						stored, err := o.st.Get(ctx, sid)
						if err != nil {
							t.Fatal(err)
						}
						if keep {
							if _, err := os.Stat(stale); err != nil {
								t.Fatalf("keep-source removed checkpoint: %v", err)
							}
							if stored == nil || stored.State != types.StatePaused || stored.ResumeSource != sb.ResumeSource {
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
