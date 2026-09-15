package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestBuildActionStateMatrix(t *testing.T) {
	ctx := context.Background()
	for _, state := range []types.BuildState{types.BuildRegistered, types.BuildWaiting, types.BuildBuilding, types.BuildReady, types.BuildError} {
		for _, owned := range []bool{false, true} {
			if state == types.BuildBuilding && !owned {
				continue
			}
			for _, action := range []string{"cancel", "delete", "delete-cancel"} {
				t.Run(fmt.Sprintf("%s/owned=%v/%s", state, owned, action), func(t *testing.T) {
					st := testStore(t)
					b := admissionBuild("action", types.BuildResources{CPU: 1000, Memory: 1 << 30, Storage: 2 << 30})
					b.Status, b.ExecutionClaimed = state, owned
					if owned {
						b.ExecutionClaimedUnix = 123
						b.EnforcementStatus = "pending"
					}
					b.Reason = "original diagnosis"
					if err := st.PutBuild(ctx, b); err != nil {
						t.Fatal(err)
					}
					got, deleted, pending, err := st.RequestBuildAction(ctx, b, action != "cancel", action != "delete", time.Unix(200, 0))
					if owned && action == "delete" {
						if !errors.Is(err, ErrBuildDeleteConflict) {
							t.Fatalf("err=%v", err)
						}
						current, _ := st.GetBuild(ctx, b.BuildID)
						if current.CancelRequestedUnix != 0 || current.DeleteRequestedUnix != 0 {
							t.Fatal("409 changed intent")
						}
						return
					}
					if err != nil || got == nil || pending != owned || deleted != (!owned && action != "cancel") {
						t.Fatalf("got=%v deleted=%v pending=%v err=%v", got, deleted, pending, err)
					}
					current, err := st.GetBuild(ctx, b.BuildID)
					if err != nil {
						t.Fatal(err)
					}
					if deleted {
						if current != nil {
							t.Fatal("204 deletion retained row")
						}
						return
					}
					if owned {
						if !current.ExecutionClaimed || current.CancelRequestedUnix != 200 {
							t.Fatal("accepted intent lost claim or timestamp")
						}
					}
					if !owned && state != types.BuildReady && state != types.BuildError {
						if current.Status != types.BuildError || current.Reason != BuildCancelledReason {
							t.Fatal("unexecuted cancellation did not terminate")
						}
					}
					if !owned && (state == types.BuildReady || state == types.BuildError) {
						if current.Reason != b.Reason || current.Status != b.Status {
							t.Fatal("cancel changed accepted terminal result")
						}
					}
				})
			}
		}
	}
}

func TestBuildActionReleasesRealRegistrationAdmission(t *testing.T) {
	ctx := context.Background()
	for _, remove := range []bool{false, true} {
		for _, dimension := range []string{"count", "cpu", "memory", "storage"} {
			t.Run(fmt.Sprintf("delete=%v/%s", remove, dimension), func(t *testing.T) {
				st := testStore(t)
				resources := types.BuildResources{CPU: 1000, Memory: 1024, Storage: 2048}
				limit := types.BuildAdmissionLimit{}
				switch dimension {
				case "count":
					limit.MaxBuilds = 1
				case "cpu":
					limit.Resources.CPU = resources.CPU
				case "memory":
					limit.Resources.Memory = resources.Memory
				case "storage":
					limit.Resources.Storage = resources.Storage
				}
				b := admissionBuild("first", resources)
				second := admissionBuild("second", resources)
				if _, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, b, limit, "", nil); err != nil {
					t.Fatal(err)
				}
				if _, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, second, limit, "", nil); !errors.Is(err, ErrBuildRegistrationCapacity) {
					t.Fatalf("expected full admission: %v", err)
				}
				if _, _, pending, err := st.RequestBuildAction(ctx, b, remove, true, time.Now()); err != nil || pending {
					t.Fatalf("action pending=%v err=%v", pending, err)
				}
				if _, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, second, limit, "", nil); err != nil || !inserted {
					t.Fatalf("registration after action: %v %v", inserted, err)
				}
			})
		}
	}
}

func TestBuildActionIntentMonotonicAndExecutionIndependent(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	b := admissionBuild("running", types.BuildResources{CPU: 1000, Memory: 1024, Storage: 2048})
	b.Status = types.BuildBuilding
	b.ExecutionClaimed = true
	b.ExecutionClaimedUnix = 1
	b.RunID = "br-test"
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	for _, action := range []struct {
		remove, cancel bool
		now            int64
	}{{false, true, 20}, {false, true, 30}, {true, true, 40}, {true, false, 50}, {false, true, 60}} {
		got, deleted, pending, err := st.RequestBuildAction(ctx, b, action.remove, action.cancel, time.Unix(action.now, 0))
		if err != nil || deleted || !pending || got.CancelRequestedUnix != 20 {
			t.Fatalf("action=%+v result=%+v %v %v %v", action, got, deleted, pending, err)
		}
		if action.now >= 40 && got.DeleteRequestedUnix != 40 {
			t.Fatal("delete intent was downgraded")
		}
	}
	// Old copies cannot clear either timestamp.
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	current, _ := st.GetBuild(ctx, b.BuildID)
	if current.CancelRequestedUnix != 20 || current.DeleteRequestedUnix != 40 {
		t.Fatal("old copy cleared intent")
	}
	for _, state := range []types.BuildState{types.BuildBuilding, types.BuildReady, types.BuildError} {
		current.Status = state
		if state == types.BuildBuilding {
			current.FinishedUnix = 0
		}
		if err := st.PutBuild(ctx, current); err != nil {
			t.Fatal(err)
		}
		usage, err := st.BuildUsage(ctx)
		if err != nil || usage.RegistrationBuilds != 0 || usage.ExecutionBuilds != 1 || usage.Execution != b.Resources {
			t.Fatalf("state=%s usage=%+v err=%v", state, usage, err)
		}
	}
}

func TestWaitingDeleteAndClaimAtomicOrder(t *testing.T) {
	for i := 0; i < 20; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			ctx := context.Background()
			st := testStore(t)
			b := admissionBuild("waiting", types.BuildResources{CPU: 1000, Memory: 1024})
			b.Status = types.BuildWaiting
			if err := st.PutBuild(ctx, b); err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			var claimed, deleted bool
			var claimErr, deleteErr error
			go func() {
				defer wg.Done()
				<-start
				claimed, claimErr = st.ClaimBuildExecution(ctx, b.BuildID, types.BuildAdmissionLimit{MaxBuilds: 1}, time.Now())
			}()
			go func() {
				defer wg.Done()
				<-start
				_, deleted, _, deleteErr = st.RequestBuildAction(ctx, b, true, false, time.Now())
			}()
			close(start)
			wg.Wait()
			if claimErr != nil {
				t.Fatal(claimErr)
			}
			current, err := st.GetBuild(ctx, b.BuildID)
			if err != nil {
				t.Fatal(err)
			}
			if claimed {
				if deleted || !errors.Is(deleteErr, ErrBuildDeleteConflict) || current == nil || !current.ExecutionClaimed {
					t.Fatalf("claim won but delete=%v err=%v row=%v", deleted, deleteErr, current)
				}
			} else {
				if !deleted || deleteErr != nil || current != nil {
					t.Fatalf("delete won but deleted=%v err=%v row=%v", deleted, deleteErr, current)
				}
			}
		})
	}
}

func TestBuildActionFencesProgressAndPreservesResultReplay(t *testing.T) {
	for _, resultFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(resultFirst), func(t *testing.T) {
			ctx := context.Background()
			st := testStore(t)
			b := admissionBuild("result", types.BuildResources{CPU: 1000, Memory: 1024})
			b.Status = types.BuildBuilding
			b.ExecutionClaimed = true
			b.RunID = "br-result"
			if err := st.PutBuild(ctx, b); err != nil {
				t.Fatal(err)
			}
			result := types.BuildResult{Error: "original failure"}
			if resultFirst {
				if _, err := st.AcceptBuildResult(ctx, b.BuildID, b.RunID, result); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, pending, err := st.RequestBuildAction(ctx, b, true, true, time.Now()); err != nil || !pending {
				t.Fatalf("pending=%v err=%v", pending, err)
			}
			_, err := st.AcceptBuildResult(ctx, b.BuildID, b.RunID, result)
			if resultFirst && err != nil || !resultFirst && !errors.Is(err, ErrBuildExecutionOwnership) {
				t.Fatalf("result-first=%v replay=%v", resultFirst, err)
			}
			if _, err := st.AcceptBuildResult(ctx, b.BuildID, b.RunID, types.BuildResult{Error: "other"}); err == nil {
				t.Fatal("different result accepted")
			}
			if allowed, err := st.BuildingTaskIdentity(ctx, b.BuildID, b.RunID); err != nil || allowed {
				t.Fatal("cancelled bootstrap accepted")
			}
			if won, err := st.SetBuildRuntimePreparation(ctx, b.BuildID, b.RunID, "p1", "ip", "mac", "", "{}"); err != nil || won {
				t.Fatal("cancelled network commit accepted")
			}
			if err := st.SetBuildPhase(ctx, b.BuildID, "A", "sid", "starting"); !errors.Is(err, ErrBuildExecutionOwnership) {
				t.Fatalf("cancelled phase accepted: %v", err)
			}
		})
	}
}

func TestBuildDeleteOwnedMMDSAndRetryWithoutTTL(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	for _, id := range []string{"remove", "keep"} {
		b := admissionBuild(id, types.BuildResources{CPU: 1000, Memory: 1024})
		if _, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, b, types.BuildAdmissionLimit{}, "routes", MMDSRouteSecretValues{"key": []byte(id)}); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := st.GetBuild(ctx, "remove")
	b.Status = types.BuildBuilding
	b.ExecutionClaimed = true
	b.RunID = "br-remove"
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.RequestBuildAction(ctx, b, true, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`CREATE TRIGGER fail_requested_delete BEFORE DELETE ON builds WHEN OLD.build_id='remove' BEGIN SELECT RAISE(ABORT,'injected delete failure'); END`); err != nil {
		t.Fatal(err)
	}
	b.Status = types.BuildError
	if err := st.PutBuildTerminal(ctx, b, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DeleteRequestedBuild(ctx, b); err == nil {
		t.Fatal("injection did not fail deletion")
	}
	usage, err := st.BuildUsage(ctx)
	if err != nil || usage.ExecutionBuilds != 0 || usage.RegistrationBuilds != 1 {
		t.Fatalf("usage=%+v err=%v", usage, err)
	}
	pending, err := st.BuildsPendingDeletion(ctx, 128)
	if err != nil || len(pending) != 1 || pending[0].DeleteRequestedUnix == 0 {
		t.Fatal("missing deletion compensation")
	}
	if _, err := st.db.Exec(`DROP TRIGGER fail_requested_delete`); err != nil {
		t.Fatal(err)
	}
	if removed, err := st.DeleteRequestedBuild(ctx, pending[0]); err != nil || !removed {
		t.Fatalf("retry deleted=%v err=%v", removed, err)
	}
	var ids string
	if err := st.db.QueryRow(`SELECT group_concat(build_id) FROM build_mmds_route_secret_values`).Scan(&ids); err != nil || ids != "keep" {
		t.Fatalf("owned values=%q err=%v", ids, err)
	}
	// A late finalizer cannot delete a fresh registration of the same BuildID.
	replacement := admissionBuild("remove", types.BuildResources{CPU: 1000, Memory: 1024})
	replacement.TemplateID = "transient-new"
	if err := st.PutBuild(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if deleted, err := st.DeleteRequestedBuild(ctx, b); err != nil || deleted {
		t.Fatal("old identity affected replacement")
	}
	if err := st.PutBuildTerminal(ctx, b, nil); err == nil {
		t.Fatal("old terminal writer affected replacement")
	}
}

func TestBuildIntentAdditiveMigrationAllStates(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "builds.db")
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	states := []types.BuildState{types.BuildRegistered, types.BuildWaiting, types.BuildBuilding, types.BuildReady, types.BuildError}
	for _, state := range states {
		b := admissionBuild(string(state), types.BuildResources{CPU: 1000, Memory: 1024})
		b.Status = state
		b.ExecutionClaimed = state == types.BuildBuilding
		if err := st.PutBuild(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	for _, column := range []string{"cancel_requested_unix", "delete_requested_unix"} {
		if _, err := st.db.Exec(`ALTER TABLE builds DROP COLUMN ` + column); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, state := range states {
		b, err := st.GetBuild(ctx, string(state))
		if err != nil || b == nil || b.Status != state || b.CancelRequestedUnix != 0 || b.DeleteRequestedUnix != 0 {
			t.Fatalf("migration state=%s build=%+v err=%v", state, b, err)
		}
	}
}
