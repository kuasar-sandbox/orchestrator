package orch

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPauseExitRouteProjection(t *testing.T) {
	for _, state := range []types.State{types.StateStarting, types.StateRunning, types.StatePausing, types.StatePaused} {
		for _, resultRun := range []string{"", "current", "previous"} {
			t.Run(fmt.Sprintf("%s/result-%s", state, resultRun), func(t *testing.T) {
				sb := &types.Sandbox{ID: "route-facts", State: state, RunID: "current"}
				if resultRun != "" {
					sb.ExecutionResult = &types.SandboxExecutionResult{SID: sb.ID, RunID: resultRun}
				}
				got, err := routeEntryBase(sb)
				want := state
				if resultRun == sb.RunID && (state == types.StateStarting || state == types.StateRunning) {
					want = types.StateDead
				}
				if err != nil || got.State != string(want) {
					t.Fatalf("route=%s err=%v want=%s", got.State, err, want)
				}
			})
		}
	}
}

func TestResumeRetiresPreviousResultInAllViews(t *testing.T) {
	for _, kind := range []types.ResumeSourceKind{types.ResumeSourceSandbox, types.ResumeSourceSnapshot} {
		t.Run(string(kind), func(t *testing.T) {
			f := newResumeModeFixture(t, kind, true)
			code := 0
			f.sb.ExecutionResult = &types.SandboxExecutionResult{SID: f.sb.ID, RunID: "previous", Stage: types.SandboxResultRun, ExitCode: &code}
			if err := f.o.st.Put(f.ctx, f.sb); err != nil {
				t.Fatal(err)
			}
			accepted, err := f.o.Connect(f.ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			stored, err := f.o.st.Get(f.ctx, f.sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			for name, view := range map[string]*types.Sandbox{"accepted": accepted, "stored": stored, "cached": f.o.lookup(f.sb.ID)} {
				if view == nil || view.State != types.StateStarting || view.ExecutionResult != nil {
					t.Errorf("%s retained a previous-run result on the new launch", name)
				}
			}
			finishAcceptedAutoLaunch(t, f, accepted.LaunchMode)
			running := f.o.lookup(f.sb.ID)
			if running == nil || running.ExecutionResult != nil {
				t.Fatal("running projection retained previous-run result")
			}
		})
	}
}

func TestExportCompletedPauseWithRunnerResult(t *testing.T) {
	for _, kind := range []types.ResumeSourceKind{types.ResumeSourceSandbox, types.ResumeSourceSnapshot} {
		for _, keep := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/keep%t", kind, keep), func(t *testing.T) {
				dir := t.TempDir()
				o := migrationOrchestrator(t, dir, []byte("runtime"))
				ctx := context.Background()
				mk := strings.Repeat("7", 64)
				_, key := defaultTestCredentials(t, mk)
				sb := migrationSandbox(t, dir, "export-facts", mk, "manifest://"+strings.Repeat("c", 64))
				sb.ResumeSource.Kind = kind
				if kind == types.ResumeSourceSandbox {
					sb.ResumeSource.SandboxRef = ""
				}
				code := 17
				sb.ExecutionResult = &types.SandboxExecutionResult{SID: sb.ID, RunID: "previous", Stage: types.SandboxResultRun, ExitCode: &code}
				if err := o.st.Put(ctx, sb); err != nil {
					t.Fatal(err)
				}
				o.cache(sb)
				token, err := o.exportSandboxTokenForTest(ctx, key, sb.ID, false, keep)
				if err != nil || token == "" {
					t.Fatalf("export completed pause: %v", err)
				}
				if keep {
					got, err := o.st.Get(ctx, sb.ID)
					if err != nil || got == nil || got.State != types.StatePaused || got.ResumeSource != sb.ResumeSource || got.ExecutionResult == nil {
						t.Fatalf("keep-source changed independent facts: %v", err)
					}
				} else {
					waitForExportDeletion(t, o, sb.ID)
				}
			})
		}
	}
}
