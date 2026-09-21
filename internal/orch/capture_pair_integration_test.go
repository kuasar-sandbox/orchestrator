package orch

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	uploadstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

// Native sinks -> ctl response -> actual sandbox-ctl JSON -> capture adapter ->
// paused SQLite/cache. Carrier files disappear before the response: neither CLI
// nor conductor may reparse the freshly produced S to discover E.
func TestCapturePairCLIToPausedDatabase(t *testing.T) {
	binary := os.Getenv("KUASAR_TEST_SANDBOX_CTL")
	if binary == "" {
		t.Skip("KUASAR_TEST_SANDBOX_CTL is required for companion CLI integration")
	}
	for _, mode := range []string{"local", "bundle"} {
		for _, kind := range []types.CaptureKind{types.CaptureSnapshot, types.CaptureSandbox} {
			for _, torn := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/torn=%t", mode, kind, torn), func(t *testing.T) {
					cfg := checkpointOrchestratorConfig(t, mode)
					short := shortOrchestratorTestDir(t)
					cfg.Paths.RunRoot = filepath.Join(short, "run")
					cfg.Paths.BaseRoot = filepath.Join(short, "base")
					o, sb, key, launcher, vs, _ := newCheckpointPauseFixture(t, cfg, "")
					o.executables = configresolve.ExecutablesForNodeCtl(filepath.Join(filepath.Dir(binary), "node-ctl"))
					// Capturing E from a running memory restore must discard its old S/E.
					sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("a", 64), SandboxRef: "manifest://" + strings.Repeat("b", 64)}
					if err := o.st.Put(context.Background(), sb); err != nil {
						t.Fatal(err)
					}
					o.cache(sb)
					if err := os.MkdirAll(sb.RunDir, 0700); err != nil {
						t.Fatal(err)
					}
					listener, err := net.Listen("unix", filepath.Join(sb.RunDir, "ctl.sock"))
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					done := make(chan error, 1)
					produced := make(chan types.ResumeSource, 1)
					go func() {
						c, err := listener.Accept()
						if err != nil {
							done <- err
							return
						}
						defer c.Close()
						var req ctl.Request
						if err = ctl.ReadMessage(c, &req); err != nil {
							done <- err
							return
						}
						if req.Mode != mode || req.Upload || req.ResumeAfter {
							done <- fmt.Errorf("capture request=%+v", req)
							return
						}
						response, err := capturePairResponse(context.Background(), req, kind)
						if err != nil {
							done <- err
							return
						}
						want := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: response.SandboxRef}
						if kind == types.CaptureSnapshot {
							want = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: response.SnapshotRef, SandboxRef: response.SandboxRef}
						}
						produced <- want
						if err = os.RemoveAll(req.OutDir); err != nil {
							done <- err
							return
						}
						if torn {
							response.SandboxRef = ""
						}
						done <- ctl.WriteMessage(c, &response)
					}()
					err = o.Pause(context.Background(), sb.ID, key, sandboxcfg.CaptureRequest{Kind: kind})
					if serverErr := <-done; serverErr != nil {
						t.Fatal(serverErr)
					}
					want := <-produced
					got, getErr := o.st.Get(context.Background(), sb.ID)
					if getErr != nil {
						t.Fatal(getErr)
					}
					if torn {
						if err == nil || got.State != types.StateRunning || got.ResumeSource != sb.ResumeSource || launcher.stops.Load() != 0 || vs.detaches.Load() != 0 {
							t.Fatalf("torn capture committed: state=%s pair=%+v err=%v", got.State, got.ResumeSource, err)
						}
					} else if err != nil || got.State != types.StatePaused || got.ResumeSource != want || o.lookup(sb.ID).ResumeSource != want {
						t.Fatalf("paused pair=%+v want=%+v state=%s err=%v", got.ResumeSource, want, got.State, err)
					}
				})
			}
		}
	}
}

func capturePairResponse(ctx context.Context, req ctl.Request, kind types.CaptureKind) (ctl.Response, error) {
	return capturePairResponseForOwner(ctx, req, kind, "producer", [32]byte{17}, 3)
}

func capturePairResponseForOwner(ctx context.Context, req ctl.Request, kind types.CaptureKind, sid string, key [32]byte, value byte) (ctl.Response, error) {
	var sink snapshot.ArtifactSink
	var upload *snapshot.IngestSink
	if req.Upload {
		cfg := &manifest.Config{Chunker: chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}}, Crypto: manifestcrypto.Config{Chunk: "aes", Manifest: "aes"}}
		writer, err := cfg.NewIngesterWithWriter(func() ([32]byte, error) { return key, nil }, nil, captureUploadWriter{})
		if err != nil {
			return ctl.Response{}, err
		}
		upload = snapshot.NewIngestSink(writer, nil)
		sink = upload
	} else if req.Mode == "bundle" {
		cfg := &manifest.Config{Chunker: chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}}, Crypto: manifestcrypto.Config{Chunk: "aes", Manifest: "aes"}}
		var err error
		sink, err = snapshot.NewBundleSink(ctx, req.OutDir, sid, cfg, func() ([32]byte, error) { return key, nil }, nil)
		if err != nil {
			return ctl.Response{}, err
		}
	} else {
		sink = snapshot.NewFileSink(req.OutDir, sid, nil, false, nil)
	}
	defer sink.Close()
	portable := &rtconfig.PortableSandboxConfig{Version: 1, Resources: rtconfig.PortableResourcesConfig{Capacity: rtconfig.CapacityConfig{CPU: 1, Memory: "512MiB"}, Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "512MiB"}}, Boot: rtconfig.PortableBootConfig{Kernel: "file://kernel@digest:" + strings.Repeat("1", 64), Runtime: "file://runtime@digest:" + strings.Repeat("2", 64), Root: rtconfig.PortableRootConfig{Base: "self"}}, Launch: rtconfig.PortableLaunchConfig{Workdir: "/", Restart: "never"}}
	cfg, err := rtconfig.MarshalPortableSandboxConfig(portable)
	if err != nil {
		return ctl.Response{}, err
	}
	e, err := sandboxfile.BuildSource(sparse.Dense(bytes.NewReader(bytes.Repeat([]byte{value}, 4096)), 4096), nil, cfg)
	if err != nil {
		return ctl.Response{}, err
	}
	eRef, ePath, err := sink.AbsorbSandbox(ctx, e)
	if err != nil {
		return ctl.Response{}, err
	}
	result := ctl.Response{Type: ctl.TypeExportDone}
	if kind == types.CaptureSnapshot {
		scfg, err := snapshot.MarshalConfig(&snapshot.Config{Version: 1, SandboxRef: eRef})
		if err != nil {
			return ctl.Response{}, err
		}
		source, err := snapshotfile.BuildSource(sparse.Dense(bytes.NewReader(bytes.Repeat([]byte{value + 1}, 4096)), 4096), []byte("{}"), []byte("{}"), scfg)
		if err != nil {
			return ctl.Response{}, err
		}
		sRef, sPath, err := sink.AbsorbSnapshot(ctx, source)
		if err != nil {
			return ctl.Response{}, err
		}
		if err := sink.CommitSnapshot(ctx, sRef, sPath); err != nil {
			return ctl.Response{}, err
		}
		result.SnapshotRef, result.SnapshotPath, err = snapshot.CommittedArtifactRef(sink, sRef, sPath)
		if err != nil {
			return ctl.Response{}, err
		}
		result.Type = ctl.TypeSnapshotDone
	} else if err := sink.CommitSandbox(ctx, eRef, ePath); err != nil {
		return ctl.Response{}, err
	}
	result.SandboxRef, result.SandboxPath, err = snapshot.CommittedArtifactRef(sink, eRef, ePath)
	if err != nil {
		return ctl.Response{}, err
	}
	if err := sink.Close(); err != nil {
		return ctl.Response{}, err
	}
	if upload != nil {
		result.SandboxManifestKey = snapshot.HexKey(upload.SandboxResult().ManifestKey)
		if kind == types.CaptureSnapshot {
			_, uploaded := upload.Results()
			result.SnapshotManifestKey = snapshot.HexKey(uploaded.ManifestKey)
		}
	}
	return result, nil
}

// An upload producer only needs StoreWriter. There is deliberately no readable
// artifact store behind these successful writes.
type captureUploadWriter struct{}

func (captureUploadWriter) AdmitWrite(context.Context) (uploadstore.WriteAdmission, error) {
	salt, err := uploadstore.SaltForGeneration("NONE")
	return uploadstore.WriteAdmission{Generation: "NONE", Salt: salt}, err
}
func (captureUploadWriter) Put(context.Context, uploadstore.WriteAdmission, uploadstore.Partition, uploadstore.ContentKey, []byte) (bool, error) {
	return true, nil
}

func TestUploadCaptureCLIToPausedDatabase(t *testing.T) {
	binary := os.Getenv("KUASAR_TEST_SANDBOX_CTL")
	if binary == "" {
		t.Skip("KUASAR_TEST_SANDBOX_CTL is required for companion CLI integration")
	}
	for _, kind := range []types.CaptureKind{types.CaptureSnapshot, types.CaptureSandbox} {
		for _, resume := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resume=%t", kind, resume), func(t *testing.T) {
				ctx := context.Background()
				cfg := checkpointOrchestratorConfig(t, "local")
				short := shortOrchestratorTestDir(t)
				cfg.Paths.RunRoot, cfg.Paths.BaseRoot = filepath.Join(short, "run"), filepath.Join(short, "base")
				o, sb, _, _, _, _ := newCheckpointPauseFixture(t, cfg, "")
				if err := os.MkdirAll(sb.RunDir, 0700); err != nil {
					t.Fatal(err)
				}
				listener, err := net.Listen("unix", filepath.Join(sb.RunDir, "ctl.sock"))
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				done := make(chan error, 1)
				produced := make(chan ctl.Response, 1)
				go func() {
					conn, err := listener.Accept()
					if err != nil {
						done <- err
						return
					}
					defer conn.Close()
					var req ctl.Request
					if err = ctl.ReadMessage(conn, &req); err != nil {
						done <- err
						return
					}
					if !req.Upload || req.ResumeAfter != resume {
						done <- fmt.Errorf("request=%+v", req)
						return
					}
					response, err := capturePairResponse(ctx, req, kind)
					if err != nil {
						done <- err
						return
					}
					produced <- response
					done <- ctl.WriteMessage(conn, &response)
				}()
				command, sourceKind := "snapshot", types.ResumeSourceSnapshot
				if kind == types.CaptureSandbox {
					command, sourceKind = "export", types.ResumeSourceSandbox
				}
				cmd := exec.CommandContext(ctx, binary, command, "--json", "--upload", "--path-id", sb.ID, "--run-root", filepath.Dir(sb.RunDir), fmt.Sprintf("--resume=%t", resume))
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				out, err := cmd.Output()
				if err != nil {
					t.Fatalf("capture: %v: %s", err, &stderr)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				response := <-produced
				source, err := decodeCaptureSource(out, sourceKind)
				if err != nil {
					t.Fatal(err)
				}
				want := types.ResumeSource{Kind: sourceKind, Ref: "manifest://" + response.SandboxManifestKey}
				if kind == types.CaptureSnapshot {
					want.Ref, want.SandboxRef = "manifest://"+response.SnapshotManifestKey, want.Ref
				}
				if source != want {
					t.Fatalf("source=%+v want=%+v", source, want)
				}
				changed, err := o.st.CommitRunningPaused(ctx, sb.ID, sb.RunID, source)
				if err != nil || !changed {
					t.Fatalf("pause=%t: %v", changed, err)
				}
				got, err := o.st.Get(ctx, sb.ID)
				if err != nil || got.State != types.StatePaused || got.ResumeSource != want {
					t.Fatalf("stored=%+v: %v", got, err)
				}
			})
		}
	}
}
