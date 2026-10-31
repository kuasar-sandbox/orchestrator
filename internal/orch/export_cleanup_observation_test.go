package orch

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

// Consume the ownership update separately from a subsequent move's deletion.
// Exact event validation preserves the existing no-premature-delete assertions.
func assertExportPausedCleanupPublished(t *testing.T, o *Orchestrator, want *types.Sandbox, events <-chan routesync.Event, recorder *objectObserverRecorder) {
	t.Helper()
	if events != nil {
		select {
		case event := <-events:
			if event.Kind != routesync.TypeUpsert || !reflect.DeepEqual(event.Route, o.routeEntry(want)) {
				t.Fatalf("paused cleanup route = %+v, want ownership of %+v", event, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("export did not publish paused cleanup ownership")
		}
	}
	if recorder != nil {
		if kinds := recorder.sandboxKinds(); !reflect.DeepEqual(kinds, []string{"upsert"}) {
			t.Fatalf("paused cleanup observer events = %v", kinds)
		}
		recorder.reset()
	}
}

func TestExportPausedCleanupPublishesOwnershipAndPressure(t *testing.T) {
	for _, outcome := range []string{"keep-source", "publication-failure", "partial-cleanup", "run-dir-only"} {
		t.Run(outcome, func(t *testing.T) {
			fixture, _ := newExportDeleteFixture(t, types.ResumeSourceSnapshot, false)
			o, sb := fixture.o, fixture.sb
			sb.PauseReason = types.PauseReasonResource
			sb.ResourceObligation = true
			sb.PressureVersion = 7
			sb.PressureSinceUnixNano = time.Now().Add(-time.Minute).UnixNano()
			if outcome == "run-dir-only" {
				sb.RunID, sb.VswitchPort, sb.FloatingIP, sb.InnerIP, sb.PortMAC = "", "", "", "", ""
			}
			if err := o.st.Put(context.Background(), sb); err != nil {
				t.Fatal(err)
			}
			o.cache(sb)
			pressure := attachPressure(t, o)
			if p := pressure.PressureSnapshot(); p.Pending != 1 || p.Cleanup != 1 {
				t.Fatalf("fixture did not establish paused cleanup pressure: %+v", p)
			}
			recorder := &objectObserverRecorder{}
			o.SetExtensionObserver(recorder)
			events, unsubscribe := o.Subscribe()
			defer unsubscribe()
			fault := errors.New("injected export failure")
			if outcome == "partial-cleanup" {
				o.removeSandboxRunDir = func(string) error { return fault }
			}
			publishCalls := 0
			o.artifactPublisher = func(_ context.Context, source *types.Sandbox, artifactSource types.ResumeSource) (artifact.PublishReport, error) {
				publishCalls++
				if pausedCleanupPending(source) {
					t.Error("publisher received residual runtime ownership")
				}
				if outcome == "publication-failure" {
					return artifact.PublishReport{}, fault
				}
				return testPublicationReport(artifactSource.Kind, "manifest://"+strings.Repeat("d", 64)), nil
			}
			result, err := o.ExportSandbox(context.Background(), fixture.apiKey, sb.ID, true, true, "")
			if outcome == "publication-failure" || outcome == "partial-cleanup" {
				if !errors.Is(err, fault) || result.Result != "" {
					t.Fatalf("failed export = %+v, %v", result, err)
				}
			} else if err != nil || result.Validate() != nil {
				t.Fatalf("keep-source export = %+v, %v", result, err)
			}
			wantCalls, wantCleanup := 1, 0
			want := preparedExportSource(sb)
			if outcome == "partial-cleanup" {
				wantCalls, wantCleanup = 0, 1
				want.RunDir, want.EnvdUDS, want.CiUDS = sb.RunDir, sb.EnvdUDS, sb.CiUDS
			}
			if publishCalls != wantCalls {
				t.Fatalf("publish calls = %d, want %d", publishCalls, wantCalls)
			}
			current, err := o.st.Get(context.Background(), sb.ID)
			if err != nil || !reflect.DeepEqual(current, want) || !reflect.DeepEqual(o.lookup(sb.ID), want) {
				t.Fatalf("paused source or ownership diverged: %+v, %v", current, err)
			}
			if p := pressure.PressureSnapshot(); p.Cleanup != wantCleanup || p.Pending != 1 || p.Paused != 1 || p.ReservedMemory != 0 {
				t.Fatalf("cleanup pressure did not follow durable ownership: %+v", p)
			}
			if outcome != "run-dir-only" {
				assertExportPausedCleanupPublished(t, o, want, events, recorder)
			}
			if kinds := recorder.sandboxKinds(); len(kinds) != 0 {
				t.Fatalf("export emitted an extra observer event: %v", kinds)
			}
			select {
			case event := <-events:
				t.Fatalf("export emitted an extra route event: %+v", event)
			default:
			}
			if _, err := os.Stat(sb.ResumeSource.Ref); err != nil {
				t.Fatalf("export removed retained local source: %v", err)
			}
		})
	}
}
