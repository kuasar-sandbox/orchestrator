package orch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type multiPoolLauncher struct {
	*reconcileLauncher
	listMu      sync.Mutex
	patterns    []string
	failPattern string
	listErr     error
}

func (l *multiPoolLauncher) List(ctx context.Context, pattern string) ([]launcher.Unit, error) {
	l.listMu.Lock()
	l.patterns = append(l.patterns, pattern)
	l.listMu.Unlock()
	if pattern == l.failPattern {
		return nil, l.listErr
	}
	return l.reconcileLauncher.List(ctx, pattern)
}

func multiPoolUnits() config.UnitsConfig {
	return config.UnitsConfig{
		RunnerPools:     []config.RunPoolConfig{{Unit: "first@.service"}, {Unit: "shared@.service", Size: 1}, {Unit: "shared@.service", Size: 2}, {Unit: "shared@.service", Size: 1}},
		BuilderPools:    []config.RunPoolConfig{{Unit: "first-build@.service"}, {Unit: "shared-build@.service", Size: 1}, {Unit: "shared-build@.service", Size: 1}},
		PoolWaitTimeout: "1s",
	}
}

func TestMultiPoolUnitInstallationAndEnumeration(t *testing.T) {
	cfg := &config.Config{Units: multiPoolUnits()}
	cfg.Units.Dir = t.TempDir()
	lc := &multiPoolLauncher{reconcileLauncher: &reconcileLauncher{}}
	o := New(cfg, nil, lc, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for n := 0; n < 2; n++ {
		if err := o.InstallUnits(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(cfg.Units.Dir)
	if err != nil || len(entries) != 6 || lc.reloads != 1 {
		t.Fatalf("installation files=%d reloads=%d err=%v", len(entries), lc.reloads, err)
	}
	if len(o.runnerPool.pools) != 4 || len(o.builderRunPool.pools) != 3 {
		t.Fatal("installation deduplicated pools")
	}
	run := "sr-00000000-0000-7000-8000-000000000378"
	unit := instanceUnit("shared@.service", run)
	lc.units = []launcher.Unit{{Name: unit, ActiveState: "active"}}
	units, err := o.listRunUnits(context.Background(), runKindSandbox)
	if err != nil || len(units) != 1 {
		t.Fatalf("enumeration %v %v", units, err)
	}
	if !reflect.DeepEqual(lc.patterns, []string{"first@*.service", "shared@*.service"}) {
		t.Fatalf("enumeration repeated templates: %v", lc.patterns)
	}
	if got, err := o.resolveRunUnit(context.Background(), runKindSandbox, run); err != nil || got != unit {
		t.Fatalf("actual unit %q %v", got, err)
	}
	if _, ok, err := o.runs.wait(context.Background(), run); ok || err == nil {
		t.Fatal("restored historical logical pool")
	}
	// Conflicting generated content is rejected before any file is written.
	cfg.Units.Dir = t.TempDir()
	cfg.Units.BuilderPools = []config.RunPoolConfig{{Unit: "shared@.service"}}
	if err := o.InstallUnits(context.Background()); err == nil {
		t.Fatal("runner/builder content collision accepted")
	}
	entries, _ = os.ReadDir(cfg.Units.Dir)
	if len(entries) != 0 {
		t.Fatal("collision partially wrote files")
	}
	no := false
	cfg.Units.Install = &no
	if err := o.InstallUnits(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, _ = os.ReadDir(cfg.Units.Dir)
	if len(entries) != 0 || lc.reloads != 1 {
		t.Fatal("install=false touched external units")
	}
}

func TestMultiPoolSandboxRecoveryAndIndexLifetime(t *testing.T) {
	for _, state := range []string{"running", "missing", "paused", "deleting", "starting", "resuming"} {
		t.Run(state, func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "multi-"+state)
			o, sb := f.o, f.sb
			o.cfg.Units = multiPoolUnits()
			id := sb.RunID
			actual := instanceUnit("shared@.service", id)
			orphanID := "sr-00000000-0000-7000-8000-000000000379"
			orphan := instanceUnit("first@.service", orphanID)
			lc := &multiPoolLauncher{reconcileLauncher: &reconcileLauncher{units: []launcher.Unit{{Name: orphan, ActiveState: "active"}}}}
			if state != "missing" {
				lc.units = append(lc.units, launcher.Unit{Name: actual, ActiveState: "active"})
			}
			o.lc = lc
			o.vs = &reconcileVS{}
			switch state {
			case "paused":
				sb.State = types.StatePaused
				sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("a", 64)}
			case "starting", "resuming":
				sb.State = types.StateStarting
				sb.LaunchMode = types.LaunchImage
				if state == "resuming" {
					sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("a", 64)}
					sb.LaunchMode = types.LaunchMemory
				}
			}
			if err := o.st.Put(context.Background(), sb); err != nil {
				t.Fatal(err)
			}
			if state == "deleting" {
				beginDeletingForTest(t, f)
			}
			if err := o.ReconcileSandboxes(context.Background()); err != nil {
				t.Fatal(err)
			}
			row, err := o.st.Get(context.Background(), sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			if state == "running" {
				if row.State != types.StateRunning || row.RunID != id || o.runs.unit(id) != actual {
					t.Fatalf("lost live ownership: %+v", row)
				}
				if containsString(lc.stopped, actual) {
					t.Fatal("stopped live adopted run")
				}
				if _, ok, err := o.runs.wait(context.Background(), id); ok || err == nil {
					t.Fatal("adopted run assigned twice")
				}
			} else {
				if state == "deleting" {
					if row != nil {
						t.Fatalf("deleting retained row: %+v", row)
					}
				} else {
					want := types.StateDead
					if state == "paused" {
						want = types.StatePaused
					}
					if state == "resuming" {
						want = types.StateStarting
					}
					if row == nil || row.State != want || row.RunID != "" {
						t.Fatalf("recovery row: %+v want %s", row, want)
					}
				}
				if o.runs.unit(id) != "" {
					t.Fatal("completed lifecycle retained index")
				}
				if state != "missing" && !containsString(lc.stopped, actual) {
					t.Fatal("did not stop actual template")
				}
			}
			if !containsString(lc.stopped, orphan) || o.runs.unit(orphanID) != "" {
				t.Fatal("orphan ownership retained")
			}
			seen := map[string]bool{}
			for _, unit := range lc.stopped {
				if seen[unit] {
					t.Fatalf("duplicate stop %s", unit)
				}
				seen[unit] = true
				if strings.HasPrefix(unit, "sandbox-runner@") {
					t.Fatal("guessed default template")
				}
			}
		})
	}
}

func TestMultiPoolMissingIndexDoesNotClearOwnershipOnEnumerationError(t *testing.T) {
	for _, state := range []string{"paused", "deleting", "starting", "running"} {
		t.Run(state, func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "list-error-"+state)
			o := f.o
			o.cfg.Units = multiPoolUnits()
			f.sb.State = types.State(state)
			if state == "starting" {
				f.sb.LaunchMode = types.LaunchImage
			}
			if state == "paused" {
				f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("a", 64)}
			}
			if err := o.st.Put(context.Background(), f.sb); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("systemd enumeration unavailable")
			lc := &multiPoolLauncher{reconcileLauncher: &reconcileLauncher{}, failPattern: "shared@*.service", listErr: failure}
			o.lc = lc
			// Direct cleanup is intentionally before the primary startup enumeration.
			if err := o.fenceSandboxRunner(context.Background(), f.sb.RunID); !errors.Is(err, failure) {
				t.Fatalf("lookup: %v", err)
			}
			if err := o.ReconcileSandboxes(context.Background()); !errors.Is(err, failure) {
				t.Fatalf("reconcile: %v", err)
			}
			row, err := o.st.Get(context.Background(), f.sb.ID)
			if err != nil || row == nil || row.RunID != f.sb.RunID || row.State != types.State(state) {
				t.Fatalf("cleared ownership: %+v %v", row, err)
			}
			if len(lc.stopped) != 0 {
				t.Fatal("stopped a guessed unit")
			}
		})
	}
}

func TestMultiPoolBuildRecoveryAndIndexLifetime(t *testing.T) {
	for _, state := range []string{"cancelled", "terminal-error", "terminal-delete", "accepted-live", "accepted-collected", "failed-collected", "pending-cleanup"} {
		t.Run(state, func(t *testing.T) {
			cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
			cfg.Units = multiPoolUnits()
			o := testOrchCfg(t, cfg)
			ctx := context.Background()
			id := "br-00000000-0000-7000-8000-000000000378"
			unit := instanceUnit("shared-build@.service", id)
			orphanID := "br-00000000-0000-7000-8000-000000000379"
			orphan := instanceUnit("first-build@.service", orphanID)
			lc := &multiPoolLauncher{reconcileLauncher: &reconcileLauncher{units: []launcher.Unit{{Name: orphan, ActiveState: "active"}}}}
			if !strings.HasSuffix(state, "collected") {
				lc.units = append(lc.units, launcher.Unit{Name: unit, ActiveState: "active"})
			}
			o.lc, o.vs = lc, &reconcileVS{}
			build := buildReconcileRow(t, id)
			if state == "terminal-error" || state == "terminal-delete" || state == "pending-cleanup" {
				build.Status = types.BuildError
				build.Reason = "retained failure"
				build.FinishedUnix = time.Now().Unix()
			}
			if err := o.st.PutBuild(ctx, build); err != nil {
				t.Fatal(err)
			}
			if state == "cancelled" || state == "terminal-delete" {
				if _, _, _, err := o.st.RequestBuildAction(ctx, build, state == "terminal-delete", true, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasPrefix(state, "accepted") {
				result := configsock.BuildResult{Target: types.BuildTarget{Kind: types.BuildTargetImage}, ImageRef: "manifest://" + strings.Repeat("c", 64)}
				if _, err := o.st.AcceptBuildResult(ctx, build.BuildID, id, result); err != nil {
					t.Fatal(err)
				}
			}
			for _, dir := range []string{nodepath.BuildRunDir(cfg.Paths.RunRoot, build.BuildID), nodepath.BuildBaseDir(cfg.Paths.BaseRoot, build.BuildID)} {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if state == "pending-cleanup" {
				failure := errors.New("retain directory owner")
				o.removeBuildBaseDir = func(string) error { return failure }
				if err := o.ReconcileBuilds(ctx); !errors.Is(err, failure) {
					t.Fatalf("cleanup: %v", err)
				}
				row, err := o.st.GetBuild(ctx, build.BuildID)
				if err != nil || row.RunID != id || !row.ExecutionClaimed || o.runs.unit(id) != unit {
					t.Fatalf("lost pending ownership: %+v %v", row, err)
				}
				o.removeBuildBaseDir = os.RemoveAll
			}
			if err := o.ReconcileBuilds(ctx); err != nil {
				t.Fatal(err)
			}
			row, err := o.st.GetBuild(ctx, build.BuildID)
			if err != nil {
				t.Fatal(err)
			}
			if state == "terminal-delete" {
				if row != nil {
					t.Fatalf("retained terminal deletion: %+v", row)
				}
			} else {
				want := types.BuildError
				if strings.HasPrefix(state, "accepted") {
					want = types.BuildReady
				}
				if row == nil || row.Status != want || row.RunID != "" || row.ExecutionClaimed || row.RuntimeVswitchPort != "" || row.ExecutionResult != nil {
					t.Fatalf("unreleased Build: %+v want %s", row, want)
				}
			}
			if o.runs.unit(id) != "" || o.runs.unit(orphanID) != "" {
				t.Fatal("completed Build/orphan leaked unit index")
			}
			if !containsString(lc.stopped, orphan) {
				t.Fatal("orphan builder not fenced")
			}
			if !strings.HasSuffix(state, "collected") && !containsString(lc.stopped, unit) {
				t.Fatal("actual builder not fenced")
			}
			for _, stopped := range lc.stopped {
				if strings.HasPrefix(stopped, "sandbox-builder@") {
					t.Fatal("guessed default builder")
				}
			}
		})
	}
}

func TestMultiPoolBuildEarlyCleanupLookupAndEnumerationFailure(t *testing.T) {
	cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
	cfg.Units = multiPoolUnits()
	o := testOrchCfg(t, cfg)
	ctx := context.Background()
	id := "br-00000000-0000-7000-8000-000000000378"
	unit := instanceUnit("shared-build@.service", id)
	b := buildReconcileRow(t, id)
	b.Status = types.BuildError
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := o.st.RequestBuildAction(ctx, b, true, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("enumeration failed")
	lc := &multiPoolLauncher{reconcileLauncher: &reconcileLauncher{units: []launcher.Unit{{Name: unit, ActiveState: "active"}}}, failPattern: "shared-build@*.service", listErr: failure}
	o.lc, o.vs = lc, &reconcileVS{}
	if err := o.stopBuilderRun(id); !errors.Is(err, failure) {
		t.Fatalf("pre-scan cleanup: %v", err)
	}
	if err := o.ReconcileBuilds(ctx); !errors.Is(err, failure) {
		t.Fatalf("reconcile: %v", err)
	}
	row, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || row == nil || row.RunID != id || !row.ExecutionClaimed {
		t.Fatalf("lost terminal pending deletion: %+v %v", row, err)
	}
	if len(lc.stopped) != 0 {
		t.Fatal("enumeration error guessed unit")
	}
	lc.failPattern = ""
	// The early direct cleanup path must find and fence the actual second template.
	if err := o.stopBuilderRun(id); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(lc.stopped, []string{unit}) || o.runs.unit(id) != unit {
		t.Fatalf("early cleanup %v index %q", lc.stopped, o.runs.unit(id))
	}
	if err := o.ReconcileBuilds(ctx); err != nil {
		t.Fatal(err)
	}
	row, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil || row != nil || o.runs.unit(id) != "" {
		t.Fatalf("cleanup did not retire owner: %+v %v", row, err)
	}
}
