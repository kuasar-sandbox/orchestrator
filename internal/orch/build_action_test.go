package orch

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type cancelAssignmentLauncher struct {
	*runPoolTestLauncher
	entered, release chan struct{}
}

func (l *cancelAssignmentLauncher) Resources(ctx context.Context, unit, kind string) (launcher.ResourceProperties, error) {
	close(l.entered)
	<-l.release
	return l.runPoolTestLauncher.Resources(ctx, unit, kind)
}

func TestBuildCancelBeforeRunBindingRetainsExactUnitFence(t *testing.T) {
	cfg := buildNetworkTestConfig()
	cfg.Paths.RunRoot, cfg.Paths.BaseRoot = t.TempDir(), t.TempDir()
	cfg.Units.Builder, cfg.Builder.TotalTimeoutSec = "sandbox-builder@.service", 120
	o := testOrchCfg(t, cfg)
	lc := &cancelAssignmentLauncher{newRunPoolTestLauncher(), make(chan struct{}), make(chan struct{})}
	o.lc, o.vs = lc, stubVS{}
	o.builderRunPool = newRunPool(runKindBuild, 0, time.Second, cfg.Paths.RunRoot, lc, o.builderUnit, o.log)
	ctx, stop := context.WithCancel(context.Background())
	released := false
	t.Cleanup(func() {
		if !released {
			close(lc.release)
		}
		stop()
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := o.DrainBuilds(drain); err != nil {
			t.Error(err)
		}
	})
	if err := o.builderRunPool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	key, _, _ := allowlistedBuildIdentity(t, o)
	b := registerTriggerTestBuild(t, o, key)
	if err := o.TriggerBuild(ctx, key, b.TemplateID, b.BuildID, api.TriggerSpec{FromImage: "registry.test/hang:v1"}, api.BuildAuth{}); err != nil {
		t.Fatal(err)
	}
	go o.BuildPool(ctx, time.Hour)
	var unit string
	select {
	case unit = <-lc.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no unit")
	}
	assignment := make(chan error, 1)
	go func() {
		_, _, err := o.WaitAssignment(ctx, runKindBuild, o.builderUnitToRunID(unit))
		assignment <- err
	}()
	select {
	case <-lc.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no assignment preparation")
	}
	o.pendMu.Lock()
	owner := o.pend[b.BuildID]
	o.pendMu.Unlock()
	row, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || row.RunID != "" || !row.ExecutionClaimed || owner == nil || owner.cancelExecution == nil {
		t.Fatalf("prebinding ownership = %+v, %v", row, err)
	}
	res, err := o.DeleteBuild(ctx, key, b.TemplateID, api.DeleteBuildOptions{Cancel: true})
	if err != nil || !res.Pending {
		t.Fatalf("delete before binding = %+v, %v", res, err)
	}
	select {
	case stopped := <-lc.stopped:
		if stopped != unit {
			t.Fatalf("wrong stop %q", stopped)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prebinding unit was not stopped")
	}
	usage, err := o.st.BuildUsage(ctx)
	if err != nil || usage.ExecutionBuilds != 1 {
		t.Fatalf("claim released before preparation joined: %+v, %v", usage, err)
	}
	close(lc.release)
	released = true
	select {
	case err := <-assignment:
		if err == nil {
			t.Fatal("cancelled assignment was published")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("assignment remained blocked")
	}
	select {
	case <-owner.done:
	case <-time.After(5 * time.Second):
		t.Fatal("prebinding cleanup remained blocked")
	}
	row, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil || row != nil {
		t.Fatalf("delete after exact-unit cleanup = %+v, %v", row, err)
	}
}

func TestBuildIntentWriteFailureAndPostTerminalDeleteCompensation(t *testing.T) {
	path := t.TempDir() + "/node.db"
	o := testOrchCfgAt(t, &config.Config{}, path)
	ctx := context.Background()
	key, _, _ := allowlistedBuildIdentity(t, o)
	b := registerTriggerTestBuild(t, o, key)
	b.Status, b.ExecutionClaimed = types.BuildBuilding, true
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TRIGGER fail_intent BEFORE UPDATE OF cancel_requested_unix ON builds BEGIN SELECT RAISE(FAIL,'intent fault'); END`); err != nil {
		t.Fatal(err)
	}
	res, err := o.DeleteBuild(ctx, key, b.TemplateID, api.DeleteBuildOptions{Cancel: true})
	if err == nil || res.Pending {
		t.Fatalf("failed intent acknowledged = %+v, %v", res, err)
	}
	row, _ := o.st.GetBuild(ctx, b.BuildID)
	if row.CancelRequestedUnix != 0 || row.DeleteRequestedUnix != 0 {
		t.Fatal("failed intent partially committed")
	}
	if _, err = db.Exec(`DROP TRIGGER fail_intent`); err != nil {
		t.Fatal(err)
	}
	if _, err = o.DeleteBuild(ctx, key, b.TemplateID, api.DeleteBuildOptions{Cancel: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TRIGGER fail_delete BEFORE DELETE ON builds BEGIN SELECT RAISE(FAIL,'delete fault'); END`); err != nil {
		t.Fatal(err)
	}
	o.completeBuild(ctx, b, nil, context.Canceled)
	row, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil || row == nil || row.Status != types.BuildError || row.ExecutionClaimed || row.DeleteRequestedUnix == 0 {
		t.Fatalf("terminal committed, delete pending = %+v, %v", row, err)
	}
	usage, err := o.st.BuildUsage(ctx)
	if err != nil || usage.ExecutionBuilds != 0 || usage.RegistrationBuilds != 0 {
		t.Fatalf("pending delete budget = %+v, %v", usage, err)
	}
	if _, err = db.Exec(`DROP TRIGGER fail_delete`); err != nil {
		t.Fatal(err)
	}
	// A fresh controller's startup uses this same bounded sweep, without TTL.
	if err := o.reapRequestedBuilds(ctx); err != nil {
		t.Fatal(err)
	}
	row, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil || row != nil {
		t.Fatalf("delete compensation = %+v, %v", row, err)
	}
}

func TestBuildActionsExactIdentityOwnershipAndAdmission(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "delete"}[remove], func(t *testing.T) {
			o := testOrch(t)
			ctx := context.Background()
			key, _, _ := allowlistedBuildIdentity(t, o)
			b := registerTriggerTestBuild(t, o, key)
			registerTriggerTestBuild(t, o, key)
			if _, err := o.RegisterBuild(ctx, key, api.RegisterSpec{Profile: types.ProfileE2B, Resources: b.Resources}); !errors.Is(err, api.ErrBuildAdmission) {
				t.Fatalf("full registration = %v", err)
			}
			call := func(key, tid, bid string) (api.BuildActionResult, error) {
				if remove {
					return o.DeleteBuild(ctx, key, tid, api.DeleteBuildOptions{})
				}
				return o.CancelBuild(ctx, key, tid, bid)
			}
			for _, tid := range []string{"e2b-img-bWFuaWZlc3Q6Ly94", "trigger-test", "trigger-test-alias", "registry.example/a:v1", "transient-", "transient-a/b"} {
				if _, err := call(key, tid, b.BuildID); !errors.Is(err, api.ErrBadRequest) {
					t.Fatalf("%q: %v", tid, err)
				}
			}
			if _, err := call("wrong-owner", b.TemplateID, b.BuildID); !errors.Is(err, api.ErrNotFound) {
				t.Fatalf("non-owner: %v", err)
			}
			if _, err := call(key, "transient-abcdef0123456789", b.BuildID); !errors.Is(err, api.ErrNotFound) {
				t.Fatalf("cluster transient missing: %v", err)
			}
			if !remove {
				if _, err := call(key, b.TemplateID, "other-build"); !errors.Is(err, api.ErrNotFound) {
					t.Fatalf("mismatched pair: %v", err)
				}
			}
			res, err := call(key, b.TemplateID, b.BuildID)
			if err != nil || res.Pending {
				t.Fatalf("action at full capacity = %+v, %v", res, err)
			}
			if _, err := o.RegisterBuild(ctx, key, api.RegisterSpec{Profile: types.ProfileE2B, Resources: b.Resources}); err != nil {
				t.Fatalf("immediate new registration: %v", err)
			}
			row, err := o.st.GetBuild(ctx, b.BuildID)
			if err != nil || (remove && row != nil) || (!remove && (row == nil || row.Status != types.BuildError || row.Reason != store.BuildCancelledReason)) {
				t.Fatalf("action row = %+v, %v", row, err)
			}
		})
	}
}

func TestCancellationKeepsClaimInAdminMetricsAndHeartbeat(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	key, _, _ := allowlistedBuildIdentity(t, o)
	b := registerTriggerTestBuild(t, o, key)
	if err := o.TriggerBuild(ctx, key, b.TemplateID, b.BuildID, api.TriggerSpec{FromImage: "registry.test/image:v1"}, api.BuildAuth{}); err != nil {
		t.Fatal(err)
	}
	limit, _ := configresolve.BuilderExecutionLimit(o.cfg.Builder)
	if won, err := o.st.ClaimBuildExecution(ctx, b.BuildID, limit, time.Now()); err != nil || !won {
		t.Fatalf("claim = %v, %v", won, err)
	}
	mx := metrics.New()
	o.SetMetrics(mx)
	res, err := o.CancelBuild(ctx, key, b.TemplateID, b.BuildID)
	if err != nil || !res.Pending {
		t.Fatalf("cancel = %+v, %v", res, err)
	}
	status, err := o.BuilderAdmissionStatus(ctx)
	if err != nil || status.Registration.UsedBuilds != 0 || status.Execution.UsedBuilds != 1 || status.Execution.Used != b.Resources || len(status.Builds) != 1 || !status.Builds[0].ExecutionClaimed || !status.Builds[0].CancelRequested || status.Builds[0].TemplateID != b.TemplateID {
		t.Fatalf("admin = %+v, %v", status, err)
	}
	hb := o.Heartbeat()
	if hb.BuildRegistrationUsage.Builds != 0 || hb.BuildExecutionUsage.Builds != 1 || hb.BuildExecutionUsage.Resources.Types() != b.Resources {
		t.Fatalf("heartbeat = %+v", hb)
	}
	w := httptest.NewRecorder()
	mx.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{"builder_registration_used_builds 0", "builder_execution_used_builds 1", "builder_execution_used_cpu_milli 2000", "builder_execution_waiting_builds 0"} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("missing %q in metrics", want)
		}
	}
	select {
	case <-o.buildWake:
	default:
		t.Fatal("no immediate BuildPool wake")
	}
	select {
	case <-o.HeartbeatChanges():
	default:
		t.Fatal("no immediate node usage update")
	}
}

func TestBuildCancelDuringAttachStopsUnitBeforeOwnerRollback(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "cancel-delete"}[remove], func(t *testing.T) {
			cfg := buildNetworkTestConfig()
			cfg.Paths.RunRoot, cfg.Paths.BaseRoot = t.TempDir(), t.TempDir()
			cfg.Units.Builder, cfg.Builder.TotalTimeoutSec = "sandbox-builder@.service", 120
			o := testOrchCfg(t, cfg)
			lc := newRunPoolTestLauncher()
			o.lc = lc
			o.builderRunPool = newRunPool(runKindBuild, 0, time.Second, cfg.Paths.RunRoot, lc, o.builderUnit, o.log)
			ctx, stop := context.WithCancel(context.Background())
			vs := &blockedKillAttachVS{entered: make(chan struct{}), gate: make(chan struct{}), detached: make(chan string, 4)}
			o.vs = vs
			gateClosed := false
			t.Cleanup(func() {
				if !gateClosed {
					close(vs.gate)
				}
				stop()
				drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := o.DrainBuilds(drain); err != nil {
					t.Error(err)
				}
			})
			if err := o.builderRunPool.Start(ctx); err != nil {
				t.Fatal(err)
			}
			key, _, _ := allowlistedBuildIdentity(t, o)
			b := registerTriggerTestBuild(t, o, key)
			if err := o.TriggerBuild(ctx, key, b.TemplateID, b.BuildID, api.TriggerSpec{FromImage: "registry.test/hang:v1"}, api.BuildAuth{}); err != nil {
				t.Fatal(err)
			}
			go o.BuildPool(ctx, time.Hour)
			var unit string
			select {
			case unit = <-lc.started:
			case <-time.After(5 * time.Second):
				t.Fatal("unit did not start")
			}
			if id, ok, err := o.WaitAssignment(ctx, runKindBuild, o.builderUnitToRunID(unit)); err != nil || !ok || id != b.BuildID {
				t.Fatalf("assignment = %q, %v, %v", id, ok, err)
			}
			select {
			case <-vs.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("attach did not enter")
			}
			o.pendMu.Lock()
			owner := o.pend[b.BuildID]
			o.pendMu.Unlock()
			if owner == nil || owner.cancelExecution == nil || owner.templateID != b.TemplateID {
				t.Fatal("claimed Build has no cancellable owner")
			}
			if _, err := o.DeleteBuild(ctx, key, b.TemplateID, api.DeleteBuildOptions{}); !errors.Is(err, api.ErrBuildOwned) {
				t.Fatalf("ordinary DELETE = %v", err)
			}
			select {
			case <-owner.executionCtx.Done():
				t.Fatal("ordinary DELETE interrupted execution")
			default:
			}
			requestCtx, disconnect := context.WithCancel(context.Background())
			res, err := o.CancelBuild(requestCtx, key, b.TemplateID, b.BuildID)
			disconnect()
			if err != nil || !res.Pending {
				t.Fatalf("cancel = %+v, %v", res, err)
			}
			if remove {
				res, err = o.DeleteBuild(ctx, key, b.TemplateID, api.DeleteBuildOptions{Cancel: true})
				if err != nil || !res.Pending {
					t.Fatalf("upgrade = %+v, %v", res, err)
				}
				res, err = o.DeleteBuild(ctx, key, b.TemplateID, api.DeleteBuildOptions{})
				if err != nil || !res.Pending {
					t.Fatalf("must not downgrade = %+v, %v", res, err)
				}
			}
			select {
			case stopped := <-lc.stopped:
				if stopped != unit {
					t.Fatalf("stopped %q, want %q", stopped, unit)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cancel did not actively stop the unit while attach was blocked")
			}
			select {
			case port := <-vs.detached:
				t.Fatalf("premature Detach %s", port)
			default:
			}
			for _, path := range []string{nodepath.BuildRunDir(cfg.Paths.RunRoot, b.BuildID), nodepath.BuildBaseDir(cfg.Paths.BaseRoot, b.BuildID)} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("directory removed before attach owner exited: %v", err)
				}
			}
			usage, err := o.st.BuildUsage(ctx)
			if err != nil || usage.RegistrationBuilds != 0 || usage.ExecutionBuilds != 1 {
				t.Fatalf("during attach = %+v, %v", usage, err)
			}
			close(vs.gate)
			gateClosed = true
			select {
			case <-owner.done:
			case <-time.After(5 * time.Second):
				t.Fatal("owner did not finish cleanup")
			}
			select {
			case port := <-vs.detached:
				if port != "late-kill-port" {
					t.Fatalf("wrong port %q", port)
				}
			default:
				t.Fatal("late uncommitted port was not rolled back")
			}
			row, err := o.st.GetBuild(ctx, b.BuildID)
			if err != nil || (remove && row != nil) || (!remove && (row == nil || row.Status != types.BuildError || row.ExecutionClaimed || row.Reason != store.BuildCancelledReason)) {
				t.Fatalf("final row = %+v, %v", row, err)
			}
			for _, path := range []string{nodepath.BuildRunDir(cfg.Paths.RunRoot, b.BuildID), nodepath.BuildBaseDir(cfg.Paths.BaseRoot, b.BuildID)} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("directory remains: %s, %v", path, err)
				}
			}
		})
	}
}

func TestBuildDirectImageCancellationCommitOrder(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "result-first", true: "cancel-first"}[cancelFirst], func(t *testing.T) {
			o := testOrch(t)
			ctx := context.Background()
			key, _, _ := allowlistedBuildIdentity(t, o)
			b := registerTriggerTestBuild(t, o, key)
			b.Status, b.ExecutionClaimed = types.BuildBuilding, true
			b.FromTemplate = types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String()
			if err := o.st.PutBuild(ctx, b); err != nil {
				t.Fatal(err)
			}
			result, err := o.runBuildUnit(ctx, b)
			if err != nil || result == nil {
				t.Fatalf("fast result = %+v, %v", result, err)
			}
			if cancelFirst {
				if _, err := o.CancelBuild(ctx, key, b.TemplateID, b.BuildID); err != nil {
					t.Fatal(err)
				}
			}
			o.completeBuild(ctx, b, result, nil)
			if !cancelFirst {
				if res, err := o.CancelBuild(ctx, key, b.TemplateID, b.BuildID); err != nil || res.Pending {
					t.Fatalf("terminal cancel = %+v, %v", res, err)
				}
			}
			row, err := o.st.GetBuild(ctx, b.BuildID)
			want := types.BuildReady
			if cancelFirst {
				want = types.BuildError
			}
			if err != nil || row.Status != want || row.ExecutionClaimed {
				t.Fatalf("ordered terminal = %+v, %v", row, err)
			}
		})
	}
}
