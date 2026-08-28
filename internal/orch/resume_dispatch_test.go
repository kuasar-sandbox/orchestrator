package orch

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

// The final runner launch spec dispatches on the typed resume source: a
// Sandbox artifact (E) cold-boots via `run --from`, a snapshot restores memory
// via `run --restore`, and a fresh create gets neither.
func TestSandboxFinalLaunchSpecDispatchesResumeSource(t *testing.T) {
	tmpl := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("1", 64)}
	cfgSpec, err := sandboxcfg.ParseSpec(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		resumeKind string
		snapshot   string
		wantFlag   string
	}{
		{name: "cold create has no resume flag", resumeKind: "", snapshot: "", wantFlag: ""},
		{name: "snapshot resume uses --restore", resumeKind: types.ResumeSnapshot, snapshot: "/checkpoints/sid/sid.snapshot", wantFlag: "--restore"},
		{name: "sandbox artifact resume uses --from", resumeKind: types.ResumeSandbox, snapshot: "/checkpoints/sid/sid.sandbox", wantFlag: "--from"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := testOrchCfg(t, checkpointOrchestratorConfig(t, "local"))
			o.vs = &dispatchVS{}
			sb := &types.Sandbox{
				ID:          "resume-dispatch",
				Profile:     types.ProfileBare,
				TemplateID:  tmpl.String(),
				State:       types.StatePaused,
				RunDir:      t.TempDir(),
				SnapshotRef: tc.snapshot,
				ResumeKind:  tc.resumeKind,
				Metadata:    map[string]string{},
			}
			spec := o.sandboxFinalLaunchSpec(sb, tmpl, cfgSpec, nil)
			if spec == nil {
				t.Fatal("sandboxFinalLaunchSpec returned nil")
			}
			joined := " " + strings.Join(spec.Args, " ") + " "
			if tc.wantFlag == "" {
				if strings.Contains(joined, "--restore") || strings.Contains(joined, "--from") {
					t.Fatalf("cold create carries a resume flag: %v", spec.Args)
				}
				return
			}
			if !strings.Contains(joined, " "+tc.wantFlag+" "+tc.snapshot) {
				t.Fatalf("launch args = %v, want %s %s", spec.Args, tc.wantFlag, tc.snapshot)
			}
		})
	}
}

// dispatchVS satisfies vsClient with no-op networking for launch-spec tests.
type dispatchVS struct{}

func (v *dispatchVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	return &vswitch.Port{}, nil
}

func (v *dispatchVS) Detach(context.Context, string) error { return nil }

func (v *dispatchVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }
