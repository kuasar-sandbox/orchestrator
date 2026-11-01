package orch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

func TestPauseCheckpointReturnsCaptureIDAndRejectsFullQuota(t *testing.T) {
	o, sb, key, _, _, args := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	ctx := context.Background()
	id, err := o.Pause(ctx, sb.ID, key, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, Checkpoint: true})
	if err != nil || id == "" {
		t.Fatalf("checkpoint pause=%s %v", id, err)
	}
	_, captureID := splitNamedCheckpointArgs(t, readCheckpointArgs(t, args))
	if id != captureID.String() {
		t.Fatalf("returned %s, captured %s", id, captureID)
	}
	retained, err := o.st.Checkpoint(ctx, sb.ID)
	if err != nil || retained == nil || retained.ID != id {
		t.Fatalf("retained=%+v %v", retained, err)
	}
	if _, err := o.Pause(ctx, sb.ID, key, sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox, Checkpoint: true}); !errors.Is(err, api.ErrCheckpointLimit) {
		t.Fatalf("quota=%v", err)
	}
	if err := o.DeleteCheckpoint(ctx, key, sb.ID, id); err != nil {
		t.Fatal(err)
	}
	current, err := o.st.Get(ctx, sb.ID)
	if err != nil || current.State != types.StatePaused || current.ResumeSource != retained.Source {
		t.Fatal("consumption damaged current resume source")
	}
}

func TestRetainedCheckpointExportSurvivesConcurrentResume(t *testing.T) {
	for _, toTemplate := range []bool{false, true} {
		t.Run(map[bool]string{false: "migration", true: "template"}[toTemplate], func(t *testing.T) {
			f := newExportResumeFixture(t)
			id := retainExportFixture(t, f)
			publisher := newBlockingExportPublisher("manifest://" + strings.Repeat("d", 64))
			t.Cleanup(publisher.Release)
			f.o.artifactPublisher = publisher.Publish
			done := make(chan error, 1)
			go func() { _, err := f.o.ExportSandbox(f.ctx, f.apiKey, f.sb.ID, toTemplate, false, id); done <- err }()
			waitPublisherStarted(t, publisher)
			current, err := f.o.Connect(f.ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{})
			if err != nil || current.State != types.StateStarting {
				t.Fatalf("resume=%+v %v", current, err)
			}
			// A still-publishing checkpoint occupies quota; rejection must not wait for
			// the export's source-operation lock to be released.
			ctx, cancel := context.WithTimeout(f.ctx, time.Second)
			defer cancel()
			if _, err := f.o.Pause(ctx, f.sb.ID, f.apiKey, sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox, Checkpoint: true}); !errors.Is(err, api.ErrCheckpointLimit) {
				t.Fatalf("in-flight quota=%v", err)
			}
			select {
			case cause := <-publisher.canceled:
				t.Fatalf("Resume canceled checkpoint export: %v", cause)
			default:
			}
			publisher.Release()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if cp, err := f.o.st.Checkpoint(f.ctx, f.sb.ID); err != nil || cp != nil {
				t.Fatal("export did not consume retained checkpoint")
			}
			assertLocalResumeWon(t, f)
		})
	}
}

func retainExportFixture(t *testing.T, f exportResumeFixture) string {
	t.Helper()
	sb := cloneSandbox(f.sb)
	sb.State = types.StatePausing
	if err := f.o.st.Put(f.ctx, sb); err != nil {
		t.Fatal(err)
	}
	id := "retained-source"
	if ok, err := f.o.st.CommitPaused(f.ctx, sb.ID, sb.RunID, sb.ResumeSource, id); err != nil || !ok {
		t.Fatalf("retain=%v %v", ok, err)
	}
	return id
}

func TestRetainedCheckpointExportSelectsOldSourceInEveryLiveState(t *testing.T) {
	for _, state := range []types.State{types.StateRunning, types.StatePausing, types.StatePaused, types.StateStarting} {
		t.Run(string(state), func(t *testing.T) {
			f := newExportResumeFixture(t)
			id := retainExportFixture(t, f)
			current := cloneSandbox(f.sb)
			current.State = state
			current.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("b", 64)}
			if state == types.StateStarting {
				current.LaunchMode = types.LaunchCold
			}
			if err := f.o.st.Put(f.ctx, current); err != nil {
				t.Fatal(err)
			}
			fail := errors.New("publication unavailable")
			f.o.artifactPublisher = func(_ context.Context, _ *types.Sandbox, source types.ResumeSource) (artifact.PublishReport, error) {
				if source != f.sb.ResumeSource {
					t.Errorf("export selected current source instead of capture: %+v", source)
				}
				return artifact.PublishReport{}, fail
			}
			if _, err := f.o.ExportSandbox(f.ctx, f.apiKey, f.sb.ID, true, false, id); !errors.Is(err, fail) {
				t.Fatalf("failure=%v", err)
			}
			if cp, err := f.o.st.Checkpoint(f.ctx, f.sb.ID); err != nil || cp == nil {
				t.Fatal("failed export consumed checkpoint")
			}
			f.o.artifactPublisher = func(_ context.Context, _ *types.Sandbox, source types.ResumeSource) (artifact.PublishReport, error) {
				return testPublicationReport(source.Kind, "manifest://"+strings.Repeat("c", 64)), nil
			}
			if _, err := f.o.ExportSandbox(f.ctx, f.apiKey, f.sb.ID, true, false, id); err != nil {
				t.Fatal(err)
			}
			if cp, err := f.o.st.Checkpoint(f.ctx, f.sb.ID); err != nil || cp != nil {
				t.Fatal("successful export did not consume checkpoint")
			}
			got, err := f.o.st.Get(f.ctx, f.sb.ID)
			if err != nil || got.State != state || got.ResumeSource != current.ResumeSource {
				t.Fatal("checkpoint export changed parent lifecycle")
			}
		})
	}
}
