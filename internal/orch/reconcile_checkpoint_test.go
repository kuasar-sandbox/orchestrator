package orch

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

func TestReconcileAdoptionReleasesCheckpointsWithoutRetiringRun(t *testing.T) {
	f := newDiskCheckpointFixture(t, "adoption-release")
	f.o.cfg.Units = multiPoolUnits()
	actual := instanceUnit("shared@.service", f.sb.RunID)
	lc := &multiPoolLauncher{reconcileLauncher: &reconcileLauncher{
		units: []launcher.Unit{{Name: actual, ActiveState: "active"}},
	}}
	f.o.lc = lc
	peer := serveDiskCheckpointPeer(t, f.sb)
	refs := []ctl.DiffCheckpointRef{{DiffID: "root", ID: "obsolete-root"}, {DiffID: "data", ID: "obsolete-data"}}
	peer.setup(refs, false, false)
	lifecycle, cancel := context.WithCancel(context.Background())
	f.o.SetLifecycleContext(lifecycle)
	t.Cleanup(func() {
		cancel()
		drain, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := f.o.DrainPauses(drain); err != nil {
			t.Errorf("drain adopted sandbox work: %v", err)
		}
	})
	if err := f.o.ReconcileSandboxes(lifecycle); err != nil {
		t.Fatal(err)
	}
	// Draining observes completion of the real ctl List/Delete exchange. The
	// recovered exact runner remains live and assigned throughout that work.
	drain, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := f.o.DrainPauses(drain); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	lists, remaining := peer.lists, len(peer.refs)
	deletes := append([][]ctl.DiffCheckpointRef(nil), peer.deletes...)
	peer.mu.Unlock()
	if lists != 1 || remaining != 0 || len(deletes) != 1 || !reflect.DeepEqual(deletes[0], refs) {
		t.Fatalf("startup release = lists %d, deletes %+v, remaining %d", lists, deletes, remaining)
	}
	row, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || row == nil || row.State != types.StateRunning || row.RunID != f.sb.RunID || row.RunDir != f.sb.RunDir || row.BaseDir != f.sb.BaseDir || f.o.runs.unit(f.sb.RunID) != actual {
		t.Fatalf("checkpoint release retired adopted ownership: %+v, %v", row, err)
	}
	if containsString(lc.stopped, actual) {
		t.Fatal("checkpoint release stopped adopted runner")
	}
	if _, ok, err := f.o.runs.wait(context.Background(), f.sb.RunID); ok || err == nil {
		t.Fatal("checkpoint release made adopted runner assignable again")
	}
}

func TestReconcileInterruptedPauseNeverAdoptsLiveRunnerAsRunning(t *testing.T) {
	f := newDiskCheckpointFixture(t, "interrupted-pause")
	f.o.cfg.Units = multiPoolUnits()
	actual := instanceUnit("shared@.service", f.sb.RunID)
	lc := &multiPoolLauncher{reconcileLauncher: &reconcileLauncher{units: []launcher.Unit{{Name: actual, ActiveState: "active"}}}}
	f.o.lc = lc
	if ok, err := f.o.st.SetPausing(context.Background(), f.sb.ID, f.sb.RunID, true); err != nil || !ok {
		t.Fatalf("gate=%v %v", ok, err)
	}
	if err := f.o.ReconcileSandboxes(context.Background()); err != nil {
		t.Fatal(err)
	}
	row, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || row.State != types.StateDead || row.RunID != "" {
		t.Fatalf("uncertain capture was adopted: %+v %v", row, err)
	}
	if !containsString(lc.stopped, actual) {
		t.Fatal("interrupted capture runner was not fenced")
	}
}
