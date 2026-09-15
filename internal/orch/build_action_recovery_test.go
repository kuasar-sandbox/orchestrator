package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestBuildActionRecoveryPrioritizesIntentAndAcceptedResult(t *testing.T) {
	for _, mode := range []string{"cancel", "delete"} {
		for _, phase := range []string{"pre-binding", "preparing", "exited", "result-first", "terminal"} {
			t.Run(mode+"/"+phase, func(t *testing.T) {
				ctx := context.Background()
				cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
				o := testOrchCfg(t, cfg)
				b := buildReconcileRow(t, "br-372-recovery")
				if phase == "pre-binding" {
					b.RunID = ""
				}
				// This source and preparation must never be decoded after durable intent.
				b.FromTemplate = "missing-source-that-must-not-be-opened"
				b.RuntimePrepareJSON = "corrupt preparation"
				if phase == "pre-binding" {
					b.RuntimeVswitchPort = ""
					b.RuntimeFloatingIP = ""
					b.RuntimePortMAC = ""
					b.RuntimePrepareJSON = ""
				}
				if phase == "terminal" {
					b.Status = types.BuildError
					b.ExecutionClaimed = false
					b.ExecutionClaimedUnix = 0
					b.RunID = ""
					b.RuntimeVswitchPort = ""
					b.RuntimeFloatingIP = ""
					b.RuntimePortMAC = ""
					b.RuntimePrepareJSON = ""
					b.FinishedUnix = time.Now().Unix()
				}
				if err := o.st.PutBuild(ctx, b); err != nil {
					t.Fatal(err)
				}
				result := configsock.BuildResult{Target: types.BuildTarget{Kind: types.BuildTargetImage}, ImageRef: "manifest://" + strings.Repeat("a", 64)}
				if phase == "result-first" {
					if ok, err := o.st.AcceptBuildResult(ctx, b.BuildID, b.RunID, result); err != nil || !ok {
						t.Fatalf("accept = %v, %v", ok, err)
					}
				}
				// Model the crash after durable intent but before in-memory notification.
				if phase == "terminal" {
					b.CancelRequestedUnix = time.Now().Unix()
					b.DeleteRequestedUnix = b.CancelRequestedUnix
					if err := o.st.PutBuild(ctx, b); err != nil {
						t.Fatal(err)
					}
				} else if _, _, pending, err := o.st.RequestBuildAction(ctx, b, mode == "delete", true, time.Now()); err != nil || !pending {
					t.Fatalf("intent = %v, %v", pending, err)
				}
				runDir := nodepath.BuildRunDir(cfg.Paths.RunRoot, b.BuildID)
				baseDir := nodepath.BuildBaseDir(cfg.Paths.BaseRoot, b.BuildID)
				if phase != "terminal" {
					for _, p := range []string{runDir, baseDir} {
						if err := os.MkdirAll(p, 0700); err != nil {
							t.Fatal(err)
						}
					}
				}
				unit := "sandbox-builder@br-372-recovery.service"
				lc := &reconcileLauncher{resourcesErr: errors.New("cancelled recovery must not adopt or read unit resources")}
				if phase != "exited" && phase != "terminal" {
					lc.units = []launcher.Unit{{Name: unit, ActiveState: "active"}}
				}
				vs := &reconcileVS{}
				restarted := New(cfg, o.st, lc, vs, o.log)
				if err := restarted.ReconcileBuilds(ctx); err != nil {
					t.Fatal(err)
				}
				row, err := o.st.GetBuild(ctx, b.BuildID)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "delete" || phase == "terminal" {
					if row != nil {
						t.Fatalf("pending deletion retained %+v", row)
					}
				} else {
					if row == nil || row.ExecutionClaimed || row.CancelRequestedUnix == 0 || row.RunID != "" {
						t.Fatalf("cancel recovery = %+v", row)
					}
					if phase == "result-first" {
						if row.Status != types.BuildReady || row.PersistID == "" {
							t.Fatalf("accepted result lost: %+v", row)
						}
					} else if row.Status != types.BuildError || row.Reason != store.BuildCancelledReason {
						t.Fatalf("cancel result = %+v", row)
					}
				}
				if len(lc.units) > 0 && (len(lc.stopped) == 0 || lc.stopped[0] != unit) {
					t.Fatalf("exact unit not stopped: %v", lc.stopped)
				}
				for _, p := range []string{runDir, baseDir} {
					if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("directory remains %s: %v", p, err)
					}
				}
				usage, err := o.st.BuildUsage(ctx)
				if err != nil || usage.RegistrationBuilds != 0 || usage.ExecutionBuilds != 0 {
					t.Fatalf("recovered usage = %+v, %v", usage, err)
				}
				if len(restarted.pend) != 0 {
					t.Fatal("cancelled execution was adopted")
				}
			})
		}
	}
}

func TestBuildActionRecoveryRetainsClaimAtEveryCleanupFailure(t *testing.T) {
	for _, fault := range []string{"stop", "detach", "run-dir", "base-dir"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
			o := testOrchCfg(t, cfg)
			b := buildReconcileRow(t, "br-372-fault")
			if err := o.st.PutBuild(ctx, b); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := o.st.RequestBuildAction(ctx, b, true, true, time.Now()); err != nil {
				t.Fatal(err)
			}
			unit := "sandbox-builder@br-372-fault.service"
			lc := &reconcileLauncher{units: []launcher.Unit{{Name: unit, ActiveState: "active"}}}
			vs := &reconcileVS{}
			first := New(cfg, o.st, lc, vs, o.log)
			sentinel := errors.New("injected " + fault + " failure")
			switch fault {
			case "stop":
				lc.stopErr = sentinel
			case "detach":
				vs.err = sentinel
			case "run-dir":
				first.removeBuildRunDir = func(string) error { return sentinel }
			case "base-dir":
				first.removeBuildBaseDir = func(string) error { return sentinel }
			}
			if err := first.ReconcileBuilds(ctx); !errors.Is(err, sentinel) {
				t.Fatalf("failure = %v", err)
			}
			row, err := o.st.GetBuild(ctx, b.BuildID)
			if err != nil || row == nil || !row.ExecutionClaimed || row.DeleteRequestedUnix == 0 {
				t.Fatalf("failed cleanup released ownership: %+v, %v", row, err)
			}
			usage, _ := o.st.BuildUsage(ctx)
			if usage.ExecutionBuilds != 1 || usage.RegistrationBuilds != 0 {
				t.Fatalf("fault usage = %+v", usage)
			}
			if fault == "stop" && len(vs.detached) > 0 {
				t.Fatal("detach ran before confirmed stop")
			}
			lc.stopErr = nil
			vs.err = nil
			restarted := New(cfg, o.st, lc, vs, o.log)
			if err := restarted.ReconcileBuilds(ctx); err != nil {
				t.Fatal(err)
			}
			row, err = o.st.GetBuild(ctx, b.BuildID)
			if err != nil || row != nil {
				t.Fatalf("restart compensation = %+v, %v", row, err)
			}
		})
	}
}

func TestBuildCancelResultOrderingAndReplayThroughCore(t *testing.T) {
	for _, first := range []string{"cancel", "result"} {
		t.Run(first, func(t *testing.T) {
			ctx := context.Background()
			o := testOrch(t)
			b := buildReconcileRow(t, "br-372-result")
			if err := o.st.PutBuild(ctx, b); err != nil {
				t.Fatal(err)
			}
			pend := &pendingBuild{build: b, templateID: b.TemplateID, result: make(chan configsock.BuildResult, 1)}
			o.pend[b.BuildID] = pend
			result := configsock.BuildResult{Target: types.BuildTarget{Kind: types.BuildTargetImage}, ImageRef: "manifest://" + strings.Repeat("a", 64)}
			if first == "result" {
				if err := o.PostBuildResult(ctx, b.RunID, b.BuildID, result); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := o.CancelBuildAdmin(ctx, b.BuildID); err != nil {
				t.Fatal(err)
			}
			closePendingBuildResultsAndTake(pend)
			err := o.PostBuildResult(ctx, b.RunID, b.BuildID, result)
			if first == "cancel" {
				if err == nil || !configsock.IsBuildReportRejection(err) {
					t.Fatalf("new result crossed intent: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("accepted replay rejected during cleanup: %v", err)
				}
				result.ImageRef = "manifest://" + strings.Repeat("b", 64)
				if err := o.PostBuildResult(ctx, b.RunID, b.BuildID, result); !errors.Is(err, store.ErrBuildResultConflict) {
					t.Fatalf("conflicting replay: %v", err)
				}
			}
			if _, ok, err := o.WaitAssignment(ctx, runKindBuild, b.RunID); err == nil && ok {
				t.Fatal("assignment replay advanced cancellation")
			}
			if err := o.PostBuildPhase(ctx, b.RunID, b.BuildID, "a", "phase-372", "starting"); err == nil {
				t.Fatal("new phase crossed intent")
			}
		})
	}
}
