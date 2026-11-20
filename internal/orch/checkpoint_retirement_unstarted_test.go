package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
)

func unstartedCaptureFixture(t *testing.T) (sandboxFinalizerFixture, store.RunPoolOwnership) {
	t.Helper()
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	f.sb.State = types.StatePausing
	f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	m := retiredTestOwnership(t, f)
	m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0
	if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.sb.BaseDir, "checkpoint"), 0700); err != nil {
		t.Fatal(err)
	}
	return f, m
}

func assertUnstartedCapturePending(t *testing.T, f sandboxFinalizerFixture, m store.RunPoolOwnership) {
	t.Helper()
	sb, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || sb.State != types.StatePausing || sb.RunID != m.RunID {
		t.Fatalf("lost pausing owner: %+v %v", sb, err)
	}
	current, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID)
	if err != nil || !found || current.RetirementOperationID != m.RetirementOperationID {
		t.Fatalf("lost capture intent: %+v %v", current, err)
	}
	if _, stops := f.lc.stopSnapshot(); stops != 0 {
		t.Fatalf("uncertain capture fenced producer: %d", stops)
	}
}

func assertUnstartedCaptureDead(t *testing.T, f sandboxFinalizerFixture, m store.RunPoolOwnership) {
	t.Helper()
	sb, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || sb.State != types.StateDead || sb.RunID != "" || sb.RunDir != "" ||
		sb.BaseDir != f.sb.BaseDir || sb.ResumeSource != f.sb.ResumeSource {
		t.Fatalf("unstarted capture did not preserve usable history: %+v %v", sb, err)
	}
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || found {
		t.Fatalf("retired intent remains: %t %v", found, err)
	}
	if _, err := os.Stat(filepath.Join(f.sb.BaseDir, "owned")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RunDir survived: %v", err)
	}
}

func TestUnstartedCaptureRecoveryFencesPreparationAndExactRun(t *testing.T) {
	for _, busy := range []string{"none", "writer", "source"} {
		t.Run(busy, func(t *testing.T) {
			f, m := unstartedCaptureFixture(t)
			var release func()
			switch busy {
			case "writer":
				lock, err := memorycheckpoint.LockPreparation(filepath.Join(f.sb.BaseDir, "checkpoint"), m.RetirementCheckpointID)
				if err != nil {
					t.Fatal(err)
				}
				release = func() { _ = lock.Close() }
			case "source":
				var err error
				_, release, err = f.o.trySourceOperation(context.Background(), f.sb)
				if err != nil {
					t.Fatal(err)
				}
			}
			if release != nil {
				if err := f.o.reconcileRetiredRun(context.Background(), m); !retirementIsPending(err) {
					t.Fatalf("busy capture was not deferred: %v", err)
				}
				assertUnstartedCapturePending(t, f, m)
				release()
			}
			if err := f.o.reconcileRetiredRun(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			assertUnstartedCaptureDead(t, f, m)
		})
	}
}

func TestUnstartedCaptureRecoveryWaitsForInventoryThenWorkerFinishes(t *testing.T) {
	f, m := unstartedCaptureFixture(t)
	f.o.cfg.ResourceListen = &config.ResourceListenConfig{Enabled: true}
	restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
	restarted.lc, restarted.vs = f.lc, f.vs
	ctx, cancel := context.WithCancel(context.Background())
	restarted.SetLifecycleContext(ctx)
	t.Cleanup(func() { cancel(); drainRunEnds(t, restarted) })
	f.o = restarted
	if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.reconcileRetiredRun(context.Background(), m); !errors.Is(err, errRetirementPending) {
		t.Fatalf("missing controller accepted: %v", err)
	}
	assertUnstartedCapturePending(t, f, m)
	state := nodectl.NewState(4<<30, 4000, 0, 0, nodectl.Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	admission := nodectl.NewAdmissionController(nodectl.AdmissionPolicy{Rate: 100, Burst: 100})
	if err := restarted.ConfigureMemoryPressure(context.Background(), state, admission, nodectl.DefaultPressurePolicy()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.reconcileRetiredRun(context.Background(), m); !errors.Is(err, errRetirementPending) {
		t.Fatalf("unready inventory accepted: %v", err)
	}
	assertUnstartedCapturePending(t, f, m)
	listenResourceStateForTest(t, state)
	drainRunEnds(t, restarted)
	assertUnstartedCaptureDead(t, f, m)
}

func TestUnstartedCaptureRecoveryRejectsChangedOrHolderIdentity(t *testing.T) {
	for _, field := range []string{"instance", "run", "holder"} {
		t.Run(field, func(t *testing.T) {
			f, m := unstartedCaptureFixture(t)
			switch field {
			case "instance":
				m.RetirementInstanceID = "replacement"
				if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
					t.Fatal(err)
				}
			case "run":
				f.sb.RunID = "sr-00000000-0000-7000-8000-000000000288"
			case "holder":
				m.Role, m.MemoryID, m.MemoryEpoch = store.RunRoleMemoryHolder, "memory", 1
				if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.o.st.Put(context.Background(), f.sb); err != nil {
				t.Fatal(err)
			}
			if err := f.o.reconcileRetiredRun(context.Background(), m); !errors.Is(err, errCaptureOutcomePending) {
				t.Fatalf("changed owner was accepted: %v", err)
			}
			if _, stops := f.lc.stopSnapshot(); stops != 0 {
				t.Fatalf("wrong identity fenced: %d", stops)
			}
		})
	}
}
