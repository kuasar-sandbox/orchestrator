package orch

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// Exercise both real CLIs around the durable SQLite commit. The producer uses
// real native sinks; no KVM, deployed unit, service or artifact store is needed.
func TestCheckpointCleanupCLICommitAndRetry(t *testing.T) {
	sandboxBinary, nodeBinary := os.Getenv("KUASAR_TEST_SANDBOX_CTL"), os.Getenv("KUASAR_TEST_NODE_CTL")
	if sandboxBinary == "" || nodeBinary == "" {
		t.Skip("KUASAR_TEST_SANDBOX_CTL and KUASAR_TEST_NODE_CTL are required")
	}
	if filepath.Dir(sandboxBinary) != filepath.Dir(nodeBinary) {
		t.Fatal("companion binaries must share their configured bin directory")
	}
	for _, mode := range []string{"local", "bundle"} {
		for _, kind := range []types.CaptureKind{types.CaptureSnapshot, types.CaptureSandbox} {
			for _, outcome := range []string{"commit", "sink-failure", "database-failure", "cleanup-failure"} {
				t.Run(fmt.Sprintf("%s/%s/%s", mode, kind, outcome), func(t *testing.T) {
					ctx := context.Background()
					f := newSandboxFinalizerFixture(t, "managed")
					root := shortOrchestratorTestDir(t)
					f.o.cfg.Paths.RunRoot, f.o.cfg.Paths.BaseRoot = filepath.Join(root, "run"), filepath.Join(root, "base")
					f.o.cfg.Checkpoint.Mode = mode
					f.o.cfg.ManifestConfig = filepath.Join(root, "manifest.yaml")
					if err := os.WriteFile(f.o.cfg.ManifestConfig, []byte("chunker: {mode: fixed, fixed: {size: 4KiB}}\ncrypto: {chunk: aes, manifest: aes, local: off}\n"), 0600); err != nil {
						t.Fatal(err)
					}
					f.sb.RunDir = nodepath.SandboxRunDir(f.o.cfg.Paths.RunRoot, f.sb.ID)
					f.sb.BaseDir = nodepath.SandboxBaseDir(f.o.cfg.Paths.BaseRoot, f.sb.ID)
					if err := os.MkdirAll(f.sb.RunDir, 0700); err != nil {
						t.Fatal(err)
					}
					f.o.executables = configresolve.ExecutablesForNodeCtl(nodeBinary)
					lifetime, stop := context.WithCancel(ctx)
					f.o.SetLifecycleContext(lifetime)
					drain := func() {
						stop()
						timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
						defer cancel()
						if err := f.o.DrainPauses(timeout); err != nil {
							t.Errorf("drain cleanup: %v", err)
						}
					}
					t.Cleanup(drain)
					var key [32]byte
					decoded, err := hex.DecodeString(f.sb.ManifestKey)
					if err != nil {
						t.Fatal(err)
					}
					copy(key[:], decoded)
					dir := filepath.Join(f.sb.BaseDir, "checkpoint")
					if err := os.MkdirAll(dir, 0700); err != nil {
						t.Fatal(err)
					}
					request := ctl.Request{Mode: mode, OutDir: dir}
					previous, err := capturePairResponseForOwner(ctx, request, types.CaptureSnapshot, f.sb.ID, key, 3)
					if err != nil {
						t.Fatal(err)
					}
					f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: previous.SnapshotRef, SandboxRef: previous.SandboxRef}
					if err := f.o.st.Put(ctx, f.sb); err != nil {
						t.Fatal(err)
					}
					f.o.cache(f.sb)
					junk := filepath.Join(dir, f.sb.ID+".snapshot.4294967295.partial")
					unknown := filepath.Join(dir, "user.tmp")
					for _, path := range []string{junk, unknown} {
						if err := os.WriteFile(path, []byte("protected until successful source commit"), 0600); err != nil {
							t.Fatal(err)
						}
					}
					if outcome == "database-failure" {
						installStoreTrigger(t, f.dbPath, `CREATE TRIGGER reject_checkpoint BEFORE UPDATE OF state ON sandboxes WHEN NEW.state='paused' BEGIN SELECT RAISE(ABORT, 'reject checkpoint commit'); END`)
					}
					if outcome == "sink-failure" {
						// The producer must refuse to replace an unexpected regular
						// alias. Data written by the failed attempt is not a commit.
						alias := "sandbox"
						if kind == types.CaptureSnapshot {
							alias = "snapshot"
						}
						path := filepath.Join(dir, f.sb.ID+"."+alias)
						if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
							t.Fatal(err)
						}
						if err := os.WriteFile(path, []byte("user-owned alias collision"), 0600); err != nil {
							t.Fatal(err)
						}
					}
					listener, err := net.Listen("unix", filepath.Join(f.sb.RunDir, "ctl.sock"))
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					type result struct {
						response ctl.Response
						damaged  string
						original []byte
						err      error
					}
					done := make(chan result, 1)
					go func() {
						var result result
						defer func() { done <- result }()
						conn, err := listener.Accept()
						if err != nil {
							result.err = err
							return
						}
						defer conn.Close()
						var req ctl.Request
						if err := ctl.ReadMessage(conn, &req); err != nil {
							result.err = err
							return
						}
						result.response, err = capturePairResponseForOwner(ctx, req, kind, f.sb.ID, key, 7)
						if err != nil {
							if outcome != "sink-failure" {
								result.err = err
								return
							}
							result.err = ctl.WriteMessage(conn, &ctl.Response{Type: ctl.TypeError, Msg: err.Error()})
							return
						}
						if outcome == "sink-failure" {
							result.err = fmt.Errorf("sink accepted unexpected alias")
							return
						}
						if outcome == "cleanup-failure" {
							raw := result.response.SnapshotRef
							if kind == types.CaptureSandbox {
								raw = result.response.SandboxRef
							}
							ref, err := manifest.ParseRef(raw)
							if err != nil {
								result.err = err
								return
							}
							result.damaged = filepath.Join(dir, filepath.Base(ref.Path))
							result.original, err = os.ReadFile(result.damaged)
							if err == nil {
								err = os.WriteFile(result.damaged, []byte("injected unreadable keep metadata"), 0600)
							}
							if err != nil {
								result.err = err
								return
							}
						}
						result.err = ctl.WriteMessage(conn, &result.response)
					}()
					pauseErr := f.o.Pause(ctx, f.sb.ID, f.apiKey, sandboxcfg.CaptureRequest{Kind: kind})
					produced := <-done
					if produced.err != nil {
						t.Fatal(produced.err)
					}
					row, err := f.o.st.Get(ctx, f.sb.ID)
					if err != nil {
						t.Fatal(err)
					}
					if outcome == "sink-failure" || outcome == "database-failure" {
						if pauseErr == nil || row.State != types.StateRunning || row.ResumeSource != f.sb.ResumeSource || row.RunDir != f.sb.RunDir || row.RunID != f.sb.RunID || row.VswitchPort != f.sb.VswitchPort {
							t.Fatalf("uncommitted source changed owner: %+v pause=%v", row, pauseErr)
						}
						for _, raw := range []string{previous.SnapshotRef, previous.SandboxRef} {
							ref, _ := manifest.ParseRef(raw)
							if _, err := os.Stat(filepath.Join(dir, filepath.Base(ref.Path))); err != nil {
								t.Fatal("old source was deleted before commit", err)
							}
						}
						if _, err := os.Stat(junk); err != nil {
							t.Fatal("cleanup ran before source commit", err)
						}
						return
					}
					want := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: produced.response.SandboxRef}
					if kind == types.CaptureSnapshot {
						want = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: produced.response.SnapshotRef, SandboxRef: produced.response.SandboxRef}
					}
					if pauseErr != nil || row.State != types.StatePaused || row.ResumeSource != want || row.RunID != "" || row.VswitchPort != "" {
						t.Fatalf("committed Pause lost pair/runner fence: %+v want=%+v err=%v", row, want, pauseErr)
					}
					if outcome == "cleanup-failure" {
						if row.RunDir != f.sb.RunDir {
							t.Fatal("failed keep plan lost last durable marker")
						}
						if _, err := os.Stat(junk); err != nil {
							t.Fatal("failed keep plan deleted candidate", err)
						}
						if _, err := f.o.Connect(ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{}); err == nil {
							t.Fatal("Resume passed failed checkpoint cleanup")
						}
						if _, ok := f.o.launches.Lookup(f.sb.ID); ok {
							t.Fatal("cleanup failure acquired new runner")
						}
						drain() // Stop and join the old owner before reconstructing it.
						if err := os.WriteFile(produced.damaged, produced.original, 0600); err != nil {
							t.Fatal(err)
						}
						restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
						restarted.lc, restarted.vs, restarted.executables = f.lc, f.vs, f.o.executables
						if err := restarted.ReconcileSandboxes(ctx); err != nil {
							t.Fatal(err)
						}
						f.o = restarted
					}
					row, err = f.o.st.Get(ctx, f.sb.ID)
					if err != nil || row.RunDir != "" || row.ResumeSource != want {
						t.Fatalf("cleanup did not converge: %+v %v", row, err)
					}
					if _, err := os.Stat(junk); !os.IsNotExist(err) {
						t.Fatal("failed-attempt partial remains", err)
					}
					if _, err := os.Stat(unknown); err != nil {
						t.Fatal("unknown file removed", err)
					}
					for _, raw := range []string{previous.SnapshotRef, previous.SandboxRef} {
						ref, _ := manifest.ParseRef(raw)
						if _, err := os.Stat(filepath.Join(dir, filepath.Base(ref.Path))); !os.IsNotExist(err) {
							t.Fatalf("unused previous source remains: %s: %v", raw, err)
						}
					}
					for _, raw := range []string{want.Ref, want.SandboxRef} {
						if raw == "" {
							continue
						}
						ref, _ := manifest.ParseRef(raw)
						if _, err := os.Stat(filepath.Join(dir, filepath.Base(ref.Path))); err != nil {
							t.Fatal("current source deleted", err)
						}
					}
					if kind == types.CaptureSandbox {
						if _, err := os.Lstat(filepath.Join(dir, f.sb.ID+".snapshot")); !os.IsNotExist(err) {
							t.Fatal("E-only retains obsolete Snapshot alias", err)
						}
					}
				})
			}
		}
	}
}
