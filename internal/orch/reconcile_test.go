package orch

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type reconcileLauncher struct {
	units   []launcher.Unit
	stopped []string
	reset   []string
}

func (l *reconcileLauncher) Start(context.Context, string) error { return nil }
func (l *reconcileLauncher) Stop(_ context.Context, unit string) error {
	l.stopped = append(l.stopped, unit)
	return nil
}
func (l *reconcileLauncher) ResetFailed(_ context.Context, unit string) error {
	l.reset = append(l.reset, unit)
	return nil
}
func (l *reconcileLauncher) List(context.Context, string) ([]launcher.Unit, error) {
	return append([]launcher.Unit(nil), l.units...), nil
}
func (l *reconcileLauncher) Reload(context.Context) error { return nil }
func (l *reconcileLauncher) Close() error                 { return nil }

func TestReconcileCleansOrphanPoolRunners(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	cfg.Paths.BaseRoot = filepath.Join(t.TempDir(), "base")
	cfg.Units.Runner = "sandbox-runner@.service"
	cfg.Units.Builder = "sandbox-builder@.service"
	knownRun := "sr-00000000-0000-7000-8000-000000000001"
	knownUnit := "sandbox-runner@" + knownRun + ".service"
	orphanUnit := "sandbox-runner@sr-00000000-0000-7000-8000-000000000002.service"
	failedUnit := "sandbox-runner@sr-00000000-0000-7000-8000-000000000003.service"
	manifestKey := strings.Repeat("a", 64)
	sb := &types.Sandbox{
		ID: "sandbox-1", TemplateID: "bare-img-" + strings.Repeat("b", 64),
		Profile: types.ProfileBare, State: types.StateRunning, RunID: knownRun,
		RunDir:    filepath.Join(cfg.Paths.RunRoot, "sandbox-1"),
		BaseDir:   filepath.Join(cfg.Paths.BaseRoot, "sandbox-1"),
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey, CreatedUnix: 1,
	}
	if err := st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	lc := &reconcileLauncher{units: []launcher.Unit{
		{Name: knownUnit, ActiveState: "active"},
		{Name: orphanUnit, ActiveState: "active"},
		{Name: failedUnit, ActiveState: "failed"},
	}}
	o := New(cfg, st, lc, stubVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o.lookup(sb.ID) == nil {
		t.Fatal("known running sandbox was not adopted")
	}
	for _, unit := range []string{orphanUnit, failedUnit} {
		if !containsString(lc.stopped, unit) || !containsString(lc.reset, unit) {
			t.Fatalf("orphan %s cleanup: stopped=%v reset=%v", unit, lc.stopped, lc.reset)
		}
	}
	if containsString(lc.stopped, knownUnit) || containsString(lc.reset, knownUnit) {
		t.Fatalf("known unit was cleaned: stopped=%v reset=%v", lc.stopped, lc.reset)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
