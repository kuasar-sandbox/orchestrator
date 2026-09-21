package orch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

// Draining is a completion fence, including terminal observation. A successful
// Export alone guarantees deleting acceptance, not absence of the source row.
func waitForExportDeletion(t *testing.T, o *Orchestrator, sid string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := o.DrainSandboxDeletes(ctx); err != nil {
		t.Fatalf("drain move deletion: %v", err)
	}
	if sb, err := o.st.Get(ctx, sid); err != nil || sb != nil {
		t.Fatalf("finalized move source = %+v, %v", sb, err)
	}
}

func waitExportCleanupStep(t *testing.T, step <-chan struct{}) {
	t.Helper()
	select {
	case <-step:
	case <-time.After(3 * time.Second):
		t.Fatal("move cleanup did not reach the controlled step")
	}
}

func newExportDeleteFixture(t *testing.T, kind types.ResumeSourceKind, portable bool) (sandboxFinalizerFixture, context.CancelFunc) {
	t.Helper()
	fixture := newSandboxFinalizerFixture(t, "move-source")
	o, sb := fixture.o, fixture.sb
	o.cfg.Sandbox.Boot.Runtime = filepath.Join(filepath.Dir(fixture.dbPath), "runtime.erofs")
	if err := os.WriteFile(o.cfg.Sandbox.Boot.Runtime, []byte("runtime"), 0600); err != nil {
		t.Fatal(err)
	}
	checkpoint := filepath.Join(sb.BaseDir, "checkpoint")
	if err := os.MkdirAll(checkpoint, 0700); err != nil {
		t.Fatal(err)
	}
	// Portable sources may still own checkpoint files from an older capture.
	for _, suffix := range []string{".snapshot", ".sandbox", ".stale"} {
		if err := os.WriteFile(filepath.Join(checkpoint, sb.ID+suffix), []byte("checkpoint"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	sb.State = types.StatePaused
	sb.Metadata, sb.Env = map[string]string{}, map[string]string{}
	sb.ResumeSource = types.ResumeSource{Kind: kind, Ref: filepath.Join(checkpoint, sb.ID+"."+string(kind))}
	if kind == types.ResumeSourceSnapshot {
		sb.ResumeSource.SandboxRef = filepath.Join(checkpoint, sb.ID+".sandbox")
	}
	if portable {
		sb.ResumeSource.Ref = "manifest://" + strings.Repeat("b", 64)
		if kind == types.ResumeSourceSnapshot {
			sb.ResumeSource.SandboxRef = "manifest://" + strings.Repeat("c", 64)
		}
	}
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	lifecycle, cancel := context.WithCancel(context.Background())
	o.SetLifecycleContext(lifecycle)
	t.Cleanup(func() {
		cancel()
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := o.DrainSandboxDeletes(ctx); err != nil {
			t.Errorf("drain fixture deletion: %v", err)
		}
	})
	return fixture, cancel
}

func TestExportMoveAcceptsCommonDeletionForEveryArtifactAndResult(t *testing.T) {
	for _, kind := range []types.ResumeSourceKind{types.ResumeSourceSnapshot, types.ResumeSourceSandbox} {
		for _, portable := range []bool{false, true} {
			for _, template := range []bool{false, true} {
				for _, keepFirst := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/portable=%t/template=%t/keepFirst=%t", kind, portable, template, keepFirst), func(t *testing.T) {
						fixture, _ := newExportDeleteFixture(t, kind, portable)
						o, sb, ctx := fixture.o, fixture.sb, context.Background()
						published := testPublicationReport(kind, "manifest://"+strings.Repeat("d", 64))
						publishCalls := 0
						o.artifactPublisher = func(_ context.Context, _ *types.Sandbox, source types.ResumeSource) (artifact.PublishReport, error) {
							publishCalls++
							if portable || source != sb.ResumeSource {
								t.Errorf("unexpected publication source: %+v", source)
							}
							return published, nil
						}
						// A named publication directory and sibling node state are
						// outside this sandbox's cleanup ownership.
						shared := filepath.Join(filepath.Dir(sb.BaseDir), "published-output")
						if err := os.WriteFile(shared, []byte("shared artifact"), 0600); err != nil {
							t.Fatal(err)
						}
						recorder := &objectObserverRecorder{}
						o.SetExtensionObserver(recorder)
						events, unsubscribe := o.Subscribe()
						defer unsubscribe()
						entered, release := make(chan struct{}), make(chan struct{})
						unblock := sync.OnceFunc(func() { close(release) })
						defer unblock()
						o.removeSandboxBaseDir = func(path string) error {
							if path != sb.BaseDir {
								return fmt.Errorf("wrong cleanup owner %q", path)
							}
							close(entered)
							<-release
							return os.RemoveAll(path)
						}
						if keepFirst {
							kept, err := o.ExportSandbox(ctx, fixture.apiKey, sb.ID, template, true)
							if err != nil || kept.Validate() != nil {
								t.Fatalf("keep-source result = %+v, %v", kept, err)
							}
							stored, err := o.st.Get(ctx, sb.ID)
							if err != nil || !reflect.DeepEqual(stored, sb) || !reflect.DeepEqual(o.lookup(sb.ID), sb) {
								t.Fatalf("keep-source changed row/cache: %+v, %v", stored, err)
							}
							select {
							case event := <-events:
								t.Fatalf("keep-source changed route: %+v", event)
							default:
							}
							if _, err := os.Stat(filepath.Join(sb.BaseDir, "checkpoint", sb.ID+".stale")); err != nil {
								t.Fatal(err)
							}
						}
						// The existing async helper enforces that Export returns even
						// while physical BaseDir removal cannot complete.
						out := waitExportResult(t, startExport(o, ctx, fixture.apiKey, sb.ID, template, false))
						if out.err != nil {
							t.Fatal(out.err)
						}
						wantRef, wantE := published.SandboxRef, ""
						if kind == types.ResumeSourceSnapshot {
							wantRef, wantE = published.SnapshotRef, published.SandboxRef
						}
						if portable {
							wantRef, wantE = sb.ResumeSource.Ref, sb.ResumeSource.SandboxRef
						}
						if template {
							got, err := types.ParseTemplateID(out.result)
							wantKind := types.KindSbx
							if kind == types.ResumeSourceSnapshot {
								wantKind = types.KindSnp
							}
							if err != nil || got.Ref != wantRef || got.Kind != wantKind {
								t.Fatalf("template = %+v, %v", got, err)
							}
						} else {
							payload, err := migrationtoken.Open(migrationtoken.KeyMaterial{APISecret: sb.APISecret, ManifestKey: sb.ManifestKey}, out.result)
							if err != nil || payload.ResumeSourceKind != kind || payload.ResumeSourceRef != wantRef || payload.ResumeSandboxRef != wantE {
								t.Fatalf("token lost published S/E: %v", err)
							}
							assertMigrationCredentialsEqual(t, payloadCredentials(payload), sandboxCredentials(sb))
						}
						if o.lookup(sb.ID) != nil {
							t.Fatal("accepted move remained cached")
						}
						select {
						case event := <-events:
							if event.Kind != routesync.TypeDelete || event.SID != sb.ID {
								t.Fatalf("acceptance event = %+v", event)
							}
						default:
							t.Fatal("Export returned before withdrawing the live route")
						}
						if err := o.Range(ctx, func(entry routesync.RouteEntry) error {
							if entry.SandboxID == sb.ID {
								t.Error("deleting source remained in full route snapshot")
							}
							return nil
						}); err != nil {
							t.Fatal(err)
						}
						waitExportCleanupStep(t, entered)
						deleting := assertDeletingOwnershipState(t, fixture, false)
						if deleting.ResumeSource != sb.ResumeSource {
							t.Fatal("delete acceptance rewrote the source pair")
						}
						if kinds := recorder.sandboxKinds(); len(kinds) != 0 {
							t.Fatalf("observed deletion before cleanup: %v", kinds)
						}
						if _, err := os.Stat(filepath.Join(sb.BaseDir, "checkpoint", sb.ID+".stale")); err != nil {
							t.Fatalf("checkpoint removed outside the controlled finalizer: %v", err)
						}
						unblock()
						waitForExportDeletion(t, o, sb.ID)
						for _, path := range []string{sb.RunDir, sb.BaseDir} {
							if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
								t.Fatalf("finalizer retained %s: %v", path, err)
							}
						}
						if data, err := os.ReadFile(shared); err != nil || string(data) != "shared artifact" {
							t.Fatalf("move damaged shared output: %q, %v", data, err)
						}
						if kinds := recorder.sandboxKinds(); !reflect.DeepEqual(kinds, []string{"delete"}) {
							t.Fatalf("final observer events = %v", kinds)
						}
						select {
						case event := <-events:
							t.Fatalf("cleanup emitted another route event: %+v", event)
						default:
						}
						wantCalls := 0
						if !portable {
							wantCalls = 1
							if keepFirst {
								wantCalls++
							}
						}
						if publishCalls != wantCalls {
							t.Fatalf("publish calls = %d, want %d", publishCalls, wantCalls)
						}
					})
				}
			}
		}
	}
}

func TestExportMoveCleanupFailureKeepsDeletingAndRetries(t *testing.T) {
	for _, phase := range []string{"RunDir", "BaseDir", "hard-delete"} {
		t.Run(phase, func(t *testing.T) {
			fixture, _ := newExportDeleteFixture(t, types.ResumeSourceSnapshot, true)
			o, sb := fixture.o, fixture.sb
			recorder := &objectObserverRecorder{}
			o.SetExtensionObserver(recorder)
			var calls atomic.Int32
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			fault := errors.New("injected directory failure")
			remove := func(path string) error {
				switch calls.Add(1) {
				case 1:
					if phase != "hard-delete" {
						return fault
					}
				case 2:
					// A second pass proves that the first directory/SQL failure
					// returned to the worker and was retried without another API call.
					close(entered)
					<-release
				}
				return os.RemoveAll(path)
			}
			if phase == "RunDir" {
				o.removeSandboxRunDir = remove
			} else {
				o.removeSandboxBaseDir = remove
			}
			if phase == "hard-delete" {
				installStoreTrigger(t, fixture.dbPath, `CREATE TRIGGER fail_move_delete BEFORE DELETE ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced hard delete failure'); END`)
			}
			result, err := o.ExportSandbox(context.Background(), fixture.apiKey, sb.ID, false, false)
			if err != nil || result.Validate() != nil {
				t.Fatalf("accepted result = %+v, %v", result, err)
			}
			waitExportCleanupStep(t, entered)
			deleting := assertDeletingOwnershipState(t, fixture, false)
			if deleting.ResumeSource != sb.ResumeSource || o.lookup(sb.ID) != nil {
				t.Fatal("cleanup failure restored or rewrote the source")
			}
			if kinds := recorder.sandboxKinds(); len(kinds) != 0 {
				t.Fatalf("failed cleanup emitted observer event: %v", kinds)
			}
			if phase != "hard-delete" {
				if _, err := os.Stat(filepath.Join(sb.BaseDir, "checkpoint", sb.ID+".stale")); err != nil {
					t.Fatalf("failed cleanup lost checkpoint: %v", err)
				}
			} else {
				if _, err := os.Stat(sb.BaseDir); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("hard-delete attempted before BaseDir cleanup: %v", err)
				}
				installStoreTrigger(t, fixture.dbPath, `DROP TRIGGER fail_move_delete`)
			}
			unblock()
			waitForExportDeletion(t, o, sb.ID)
			if calls.Load() != 2 {
				t.Fatalf("cleanup calls = %d, want initial failure and one retry", calls.Load())
			}
			if kinds := recorder.sandboxKinds(); !reflect.DeepEqual(kinds, []string{"delete"}) {
				t.Fatalf("retry observer events = %v", kinds)
			}
		})
	}
}

func TestExportMoveReconcilesAcceptedDeletionAfterRestart(t *testing.T) {
	fixture, stop := newExportDeleteFixture(t, types.ResumeSourceSnapshot, true)
	attempted := make(chan struct{}, 1)
	fixture.o.removeSandboxBaseDir = func(string) error {
		select {
		case attempted <- struct{}{}:
		default:
		}
		return errors.New("persistent BaseDir failure")
	}
	result, err := fixture.o.ExportSandbox(context.Background(), fixture.apiKey, fixture.sb.ID, true, false)
	if err != nil || result.Validate() != nil {
		t.Fatalf("Export = %+v, %v", result, err)
	}
	waitExportCleanupStep(t, attempted)
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := fixture.o.DrainSandboxDeletes(ctx); err != nil {
		t.Fatal(err)
	}
	assertDeletingOwnershipState(t, fixture, false)
	// Reopen the same isolated database, without the old process's worker map.
	restarted := testOrchCfgAt(t, fixture.o.cfg, fixture.dbPath)
	restarted.lc = &sandboxFinalizerLauncher{unit: fixture.lc.unit, state: "inactive"}
	vs := &sandboxFinalizerVS{detached: true}
	restarted.vs = vs
	recorder := &objectObserverRecorder{}
	restarted.SetExtensionObserver(recorder)
	if err := restarted.ReconcileSandboxes(ctx); err != nil {
		t.Fatalf("restart reconciliation: %v", err)
	}
	waitForExportDeletion(t, restarted, fixture.sb.ID)
	for _, path := range []string{fixture.sb.RunDir, fixture.sb.BaseDir} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("restart retained %s: %v", path, err)
		}
	}
	if vs.calls != 0 {
		t.Fatalf("restart detached a released port again: %d", vs.calls)
	}
	if kinds := recorder.sandboxKinds(); !reflect.DeepEqual(kinds, []string{"delete"}) {
		t.Fatalf("restart observer events = %v", kinds)
	}
}

func TestExportMoveCancellationAfterAcceptanceKeepsResult(t *testing.T) {
	fixture, _ := newExportDeleteFixture(t, types.ResumeSourceSandbox, true)
	o, sb := fixture.o, fixture.sb
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	o.removeSandboxBaseDir = func(path string) error {
		close(entered)
		<-release
		return os.RemoveAll(path)
	}
	events, unsubscribe := o.Subscribe()
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The existing worker lock holds Export after durable acceptance and route
	// withdrawal, before it can finish starting the finalizer or return.
	o.deleteMu.Lock()
	unlock := sync.OnceFunc(o.deleteMu.Unlock)
	defer unlock()
	done := startExport(o, ctx, fixture.apiKey, sb.ID, false, false)
	select {
	case event := <-events:
		if event.Kind != routesync.TypeDelete || event.SID != sb.ID {
			t.Fatalf("acceptance event = %+v", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Export did not accept deletion")
	}
	cancel()
	unlock()
	out := waitExportResult(t, done)
	if out.err != nil || !strings.HasPrefix(out.result, "kmt1.") {
		t.Fatalf("post-acceptance cancellation lost result: %v", out.err)
	}
	waitExportCleanupStep(t, entered)
	assertDeletingOwnershipState(t, fixture, false)
	unblock()
	waitForExportDeletion(t, o, sb.ID)
}

func TestExportMoveRejectsChangedSourceIdentity(t *testing.T) {
	for _, change := range []string{"S", "E", "kind", "created", "template", "stable", "attempt"} {
		t.Run(change, func(t *testing.T) {
			fixture := newExportResumeFixture(t)
			o, sb := fixture.o, fixture.sb
			o.artifactPublisher = func(_ context.Context, _ *types.Sandbox, source types.ResumeSource) (artifact.PublishReport, error) {
				unlock := o.lifecycle.Lock(sb.ID)
				defer unlock()
				current := cloneSandbox(sb)
				switch change {
				case "attempt":
					o.exports.mu.Lock()
					old := o.exports.active[sb.ID]
					o.exports.mu.Unlock()
					o.exports.Finish(old)
					next, started := o.exports.Begin(sb.ID, source, false, func(error) {})
					if !started {
						return artifact.PublishReport{}, errors.New("could not install successor export attempt")
					}
					t.Cleanup(func() { o.exports.Finish(next) })
				case "S":
					current.ResumeSource.Ref = "manifest://" + strings.Repeat("b", 64)
				case "E":
					current.ResumeSource.SandboxRef = "manifest://" + strings.Repeat("c", 64)
				case "kind":
					current.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: source.SandboxRef}
				case "created":
					current.CreatedUnix++
				case "template":
					current.TemplateID = types.TemplateID{Profile: current.Profile, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("c", 64)}.String()
				case "stable":
					current.StableIDValue = "successor"
					current.ForwardAccessToken = ""
					materializeTestSandboxCredentials(t, current)
				}
				if change == "created" || change == "stable" {
					// Put preserves immutable identity on update; model a
					// replacement incarnation through delete/insert instead.
					if err := o.st.Delete(context.Background(), sb.ID); err != nil {
						return artifact.PublishReport{}, err
					}
				}
				if err := o.st.Put(context.Background(), current); err != nil {
					return artifact.PublishReport{}, err
				}
				o.cache(current)
				return testPublicationReport(source.Kind, "manifest://"+strings.Repeat("d", 64)), nil
			}
			result, err := o.ExportSandbox(fixture.ctx, fixture.apiKey, sb.ID, false, false)
			if !errors.Is(err, api.ErrExportPreempted) || result.Result != "" {
				t.Fatalf("changed %s accepted deletion: %+v, %v", change, result, err)
			}
			current, err := o.st.Get(fixture.ctx, sb.ID)
			if err != nil || current == nil || current.State != types.StatePaused || o.lookup(sb.ID) == nil {
				t.Fatalf("changed source lost: %+v, %v", current, err)
			}
			if _, err := os.Stat(fixture.localRef); err != nil {
				t.Fatalf("changed source lost checkpoint: %v", err)
			}
		})
	}
}

func TestExportMoveRejectsUnownedCleanupPathsBeforeAcceptance(t *testing.T) {
	for _, field := range []string{"RunDir", "BaseDir", "EnvdUDS"} {
		t.Run(field, func(t *testing.T) {
			fixture, _ := newExportDeleteFixture(t, types.ResumeSourceSnapshot, true)
			o, sb := fixture.o, fixture.sb
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "untouched")
			if err := os.WriteFile(sentinel, []byte("external"), 0600); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "RunDir":
				sb.RunDir = outside
			case "BaseDir":
				sb.BaseDir = outside
			case "EnvdUDS":
				sb.EnvdUDS = sentinel
			}
			if err := o.st.Put(context.Background(), sb); err != nil {
				t.Fatal(err)
			}
			result, err := o.ExportSandbox(context.Background(), fixture.apiKey, sb.ID, true, false)
			if err == nil || result.Result != "" {
				t.Fatalf("invalid %s accepted: %+v, %v", field, result, err)
			}
			current, err := o.st.Get(context.Background(), sb.ID)
			if err != nil || !reflect.DeepEqual(current, sb) {
				t.Fatalf("invalid ownership changed row: %+v, %v", current, err)
			}
			if _, stops := fixture.lc.stopSnapshot(); stops != 0 || fixture.vs.calls != 0 {
				t.Fatal("invalid ownership stopped runner or detached network")
			}
			if data, err := os.ReadFile(sentinel); err != nil || string(data) != "external" {
				t.Fatalf("invalid ownership touched external path: %q, %v", data, err)
			}
		})
	}
}

func TestExportMoveDeleteAcceptanceFailureHasNoCleanupEffects(t *testing.T) {
	for _, kind := range []types.ResumeSourceKind{types.ResumeSourceSnapshot, types.ResumeSourceSandbox} {
		for _, portable := range []bool{false, true} {
			for _, template := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/portable=%t/template=%t", kind, portable, template), func(t *testing.T) {
					fixture, _ := newExportDeleteFixture(t, kind, portable)
					o, sb := fixture.o, fixture.sb
					o.artifactPublisher = func(_ context.Context, _ *types.Sandbox, source types.ResumeSource) (artifact.PublishReport, error) {
						return testPublicationReport(source.Kind, "manifest://"+strings.Repeat("d", 64)), nil
					}
					recorder := &objectObserverRecorder{}
					o.SetExtensionObserver(recorder)
					events, unsubscribe := o.Subscribe()
					defer unsubscribe()
					installStoreTrigger(t, fixture.dbPath, `CREATE TRIGGER reject_move BEFORE UPDATE OF state ON sandboxes WHEN NEW.state='deleting' BEGIN SELECT RAISE(ABORT, 'reject delete acceptance'); END`)
					result, err := o.ExportSandbox(context.Background(), fixture.apiKey, sb.ID, template, false)
					if err == nil || !strings.Contains(err.Error(), "reject delete acceptance") || result.Result != "" {
						t.Fatalf("rejected acceptance = %+v, %v", result, err)
					}
					current, err := o.st.Get(context.Background(), sb.ID)
					if err != nil || !reflect.DeepEqual(current, sb) || !reflect.DeepEqual(o.lookup(sb.ID), sb) {
						t.Fatalf("failed acceptance changed source/cache: %+v, %v", current, err)
					}
					if _, stops := fixture.lc.stopSnapshot(); stops != 0 || fixture.vs.calls != 0 {
						t.Fatal("failed acceptance stopped runner or detached network")
					}
					if kinds := recorder.sandboxKinds(); len(kinds) != 0 {
						t.Fatalf("failed acceptance observed deletion: %v", kinds)
					}
					select {
					case event := <-events:
						t.Fatalf("failed acceptance withdrew route: %+v", event)
					default:
					}
					for _, path := range []string{filepath.Join(sb.RunDir, "owned"), filepath.Join(sb.BaseDir, "checkpoint", sb.ID+".stale")} {
						if _, err := os.Stat(path); err != nil {
							t.Fatalf("failed acceptance removed %s: %v", path, err)
						}
					}
					installStoreTrigger(t, fixture.dbPath, `DROP TRIGGER reject_move`)
					if result, err := o.ExportSandbox(context.Background(), fixture.apiKey, sb.ID, template, false); err != nil || result.Validate() != nil {
						t.Fatalf("retry rejected export: %+v, %v", result, err)
					}
					waitForExportDeletion(t, o, sb.ID)
				})
			}
		}
	}
}

func TestExportMoveSameIDConflictUntilCleanupCompletes(t *testing.T) {
	for _, admission := range []string{"import", "create", "cluster-create"} {
		t.Run(admission, func(t *testing.T) {
			fixture := newExportResumeFixture(t)
			o, sb, ctx := fixture.o, fixture.sb, fixture.ctx
			request := createRequestFixture(t, o, "6")
			request.Metadata[sandboxcfg.NsIdentity] = fmt.Sprintf(`{"id":%q}`, sb.ID)
			fingerprint, err := store.APISecretHash(sb.APISecret)
			if err != nil {
				t.Fatal(err)
			}
			command := clusterCreateCommand(fingerprint, sb.ID)
			o.artifactPublisher = func(_ context.Context, _ *types.Sandbox, source types.ResumeSource) (artifact.PublishReport, error) {
				return testPublicationReport(source.Kind, "manifest://"+strings.Repeat("d", 64)), nil
			}
			var allowCleanup atomic.Bool
			attempted := make(chan struct{}, 1)
			o.removeSandboxBaseDir = func(path string) error {
				if !allowCleanup.Load() {
					select {
					case attempted <- struct{}{}:
					default:
					}
					return errors.New("BaseDir unavailable")
				}
				return os.RemoveAll(path)
			}
			t.Cleanup(func() {
				allowCleanup.Store(true)
				drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := o.DrainSandboxDeletes(drainCtx); err != nil {
					t.Errorf("drain pending move: %v", err)
				}
			})
			result, err := o.ExportSandbox(ctx, fixture.apiKey, sb.ID, false, false)
			if err != nil {
				t.Fatal(err)
			}
			waitExportCleanupStep(t, attempted)
			before, err := o.st.Get(ctx, sb.ID)
			if err != nil || before == nil || before.State != types.StateDeleting {
				t.Fatalf("pending deletion = %+v, %v", before, err)
			}
			switch admission {
			case "import":
				if _, err := o.ImportSandbox(ctx, fixture.apiKey, result.Result, ""); !errors.Is(err, api.ErrAlreadyExists) {
					t.Fatalf("import while deleting = %v", err)
				}
			case "create":
				if _, err := o.Create(ctx, request); !errors.Is(err, api.ErrAlreadyExists) {
					t.Fatalf("create while deleting = %v", err)
				}
			case "cluster-create":
				ack := o.HandleCommand(ctx, command)
				if ack.Status != routesync.AckRejected || ack.HTTPStatus != 409 {
					t.Fatalf("cluster create while deleting = %+v", ack)
				}
			}
			after, err := o.st.Get(ctx, sb.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("conflicting admission changed deleting owner: %+v, %v", after, err)
			}
			if fixture.launcher.starts.Load() != 0 {
				t.Fatal("conflicting admission launched a successor")
			}
			allowCleanup.Store(true)
			waitForExportDeletion(t, o, sb.ID)
			switch admission {
			case "import":
				if id, err := o.ImportSandbox(ctx, fixture.apiKey, result.Result, ""); err != nil || id != sb.ID {
					t.Fatalf("import after cleanup = %q, %v", id, err)
				}
				imported, err := o.st.Get(ctx, sb.ID)
				if err != nil || imported == nil || imported.State != types.StatePaused || imported.ResumeSource.Ref != result.SnapshotRef || imported.ResumeSource.SandboxRef != result.SandboxRef {
					t.Fatalf("imported source = %+v, %v", imported, err)
				}
				assertMigrationCredentialsEqual(t, sandboxCredentials(imported), sandboxCredentials(sb))
			case "create":
				if created, err := o.Create(ctx, request); err != nil || created.ID != sb.ID {
					t.Fatalf("create after cleanup = %+v, %v", created, err)
				}
			case "cluster-create":
				ack := o.HandleCommand(ctx, command)
				if ack.Status != routesync.AckAccepted {
					t.Fatalf("cluster create after cleanup = %+v", ack)
				}
			}
			if admission != "import" {
				waitForSandbox(t, o, ctx, sb.ID, func(current *types.Sandbox) bool { return current.State == types.StateRunning }, "same-ID successor running")
			}
		})
	}
}

func TestExportMoveLatePublicationCannotDeleteResumedSource(t *testing.T) {
	fixture := newExportResumeFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	// A publisher may complete successfully even after Resume canceled its
	// context. The attempt fence must still reject its obsolete KMT result.
	fixture.o.artifactPublisher = func(_ context.Context, _ *types.Sandbox, source types.ResumeSource) (artifact.PublishReport, error) {
		close(entered)
		<-release
		return testPublicationReport(source.Kind, "manifest://"+strings.Repeat("d", 64)), nil
	}
	done := startExport(fixture.o, fixture.ctx, fixture.apiKey, fixture.sb.ID, false, false)
	waitExportCleanupStep(t, entered)
	if _, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{}); err != nil {
		t.Fatal(err)
	}
	assertLocalResumeWon(t, fixture)
	unblock()
	result := waitExportResult(t, done)
	if result.result != "" || !errors.Is(result.err, api.ErrExportPreempted) {
		t.Fatalf("late preempted publication = %v", result.err)
	}
	assertLocalResumeWon(t, fixture)
}

func TestExportMoveDeletesFullyCleanedPausedSource(t *testing.T) {
	fixture, _ := newExportDeleteFixture(t, types.ResumeSourceSandbox, true)
	sb := fixture.sb
	if err := os.RemoveAll(sb.RunDir); err != nil {
		t.Fatal(err)
	}
	sb.RunID, sb.RunDir, sb.VswitchPort, sb.FloatingIP = "", "", "", ""
	if err := fixture.o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	fixture.o.cache(sb)
	if result, err := fixture.o.ExportSandbox(context.Background(), fixture.apiKey, sb.ID, true, false); err != nil || result.Validate() != nil {
		t.Fatalf("clean paused Export = %+v, %v", result, err)
	}
	waitForExportDeletion(t, fixture.o, sb.ID)
	if _, err := os.Stat(sb.BaseDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("clean paused move retained BaseDir/checkpoint: %v", err)
	}
	if _, stops := fixture.lc.stopSnapshot(); stops != 0 || fixture.vs.calls != 0 {
		t.Fatal("clean paused move repeated runtime cleanup")
	}
}
