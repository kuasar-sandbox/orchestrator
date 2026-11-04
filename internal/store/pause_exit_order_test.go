package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Analysis-only: the same durable facts must converge in both delivery orders.
func TestPauseExitFactOrders(t *testing.T) {
	for _, code := range []int{0, 17} {
		for _, first := range []string{"exit", "pause"} {
			for _, reopen := range []bool{false, true} {
				t.Run(fmt.Sprintf("code%d/%s_first/reopen%t", code, first, reopen), func(t *testing.T) {
					ctx := context.Background()
					box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
					if err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(t.TempDir(), "facts.db")
					st, err := Open(path, box)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = st.Close() }()
					sb := sandboxInsertFixture("independent-facts", 0)
					sb.State = types.StatePausing
					sb.RunDir = ""
					sb.EnvdUDS = ""
					sb.CiUDS = ""
					sb.VswitchPort = ""
					sb.FloatingIP = ""
					sb.InnerIP = ""
					sb.PortMAC = ""
					if err := st.InsertSandbox(ctx, sb); err != nil {
						t.Fatal(err)
					}
					source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("a", 64)}
					result := sandboxRunResult(sb.ID, sb.RunID, code)
					exit := func() {
						t.Helper()
						if ok, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result); err != nil || !ok {
							t.Fatalf("record exit: changed=%v err=%v", ok, err)
						}
					}
					pause := func() {
						t.Helper()
						if ok, err := st.CommitPaused(ctx, sb.ID, sb.RunID, source, ""); err != nil || !ok {
							t.Fatalf("commit pause: changed=%v err=%v", ok, err)
						}
					}
					if first == "exit" {
						exit()
					} else {
						pause()
					}
					if reopen {
						if err := st.Close(); err != nil {
							t.Fatal(err)
						}
						st, err = Open(path, box)
						if err != nil {
							t.Fatal(err)
						}
					}
					if first == "exit" {
						pause()
					} else {
						exit()
					}
					got, err := st.Get(ctx, sb.ID)
					if err != nil {
						t.Fatal(err)
					}
					if got.State != types.StatePaused || got.ResumeSource != source || got.ExecutionResult == nil || *got.ExecutionResult.ExitCode != code {
						t.Fatalf("facts did not converge: state=%s result=%v", got.State, got.ExecutionResult)
					}
					if ok, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result); err != nil || ok {
						t.Fatalf("duplicate exit: %v %v", ok, err)
					}
					if _, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, sandboxRunResult(sb.ID, sb.RunID, code+1)); !errors.Is(err, ErrSandboxResultConflict) {
						t.Fatalf("conflicting result: %v", err)
					}
					if ok, err := st.ClearPausedRunner(ctx, sb.ID, sb.RunID); err != nil || !ok {
						t.Fatalf("clear runner: %v %v", ok, err)
					}
					if ok, err := st.BeginResume(ctx, sb.ID, 0, types.LaunchCold, "/new/run", "/new/envd", "/new/ci"); err != nil || !ok {
						t.Fatalf("resume: %v %v", ok, err)
					}
					if _, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result); !errors.Is(err, ErrSandboxExecutionOwnership) {
						t.Fatalf("old exit affected successor: %v", err)
					}
					got, err = st.Get(ctx, sb.ID)
					if err != nil || got.State != types.StateStarting || got.ExecutionResult != nil {
						t.Fatalf("successor contaminated: %v", err)
					}
				})
			}
		}
	}
}
