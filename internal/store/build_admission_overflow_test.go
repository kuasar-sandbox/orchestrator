package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestUnlimitedRegistrationPreservesRepresentableUsage(t *testing.T) {
	for dimension, resources := range map[string]func(int64) types.BuildResources{
		"cpu":     func(n int64) types.BuildResources { return types.BuildResources{CPU: n, Memory: 1} },
		"memory":  func(n int64) types.BuildResources { return types.BuildResources{CPU: 1, Memory: n} },
		"storage": func(n int64) types.BuildResources { return types.BuildResources{CPU: 1, Memory: 1, Storage: n} },
	} {
		for _, initial := range []int64{math.MaxInt64 - 1, math.MaxInt64} {
			t.Run(fmt.Sprintf("%s/%d", dimension, initial), func(t *testing.T) {
				st := testStore(t)
				ctx := context.Background()
				limit := types.BuildAdmissionLimit{}
				first := admissionBuild("first", resources(initial))
				if _, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, first, limit, "", nil); err != nil || !inserted {
					t.Fatalf("representable first registration: inserted=%v err=%v", inserted, err)
				}
				wantUsage := first.Resources
				wantCount := int64(1)
				if initial < math.MaxInt64 {
					second := admissionBuild("second", resources(1))
					if _, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, second, limit, "", nil); err != nil || !inserted {
						t.Fatalf("exact boundary registration: inserted=%v err=%v", inserted, err)
					}
					wantUsage, _ = wantUsage.Add(second.Resources)
					wantCount++
				}
				overflow := admissionBuild("overflow", resources(1))
				if _, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, overflow, limit, "", nil); inserted || !errors.Is(err, ErrBuildRegistrationCapacity) {
					t.Fatalf("overflow registration: inserted=%v err=%v", inserted, err)
				}
				if got, err := st.GetBuild(ctx, overflow.BuildID); err != nil || got != nil {
					t.Fatalf("rejected registration persisted: build=%+v err=%v", got, err)
				}
				if got, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, first, limit, "", nil); err != nil || inserted || got == nil || got.Resources != first.Resources {
					t.Fatalf("exact replay at arithmetic boundary: build=%+v inserted=%v err=%v", got, inserted, err)
				}
				usage, err := st.BuildUsage(ctx)
				if err != nil || usage.RegistrationBuilds != wantCount || usage.Registration != wantUsage || usage.ExecutionBuilds != 0 {
					t.Fatalf("usage after rejected overflow and replay: %+v, %v", usage, err)
				}
			})
		}
	}
}

func TestUnlimitedExecutionPreservesUsageUntilCancelledClaimIsReleased(t *testing.T) {
	for dimension, resources := range map[string]func(int64) types.BuildResources{
		"cpu":     func(n int64) types.BuildResources { return types.BuildResources{CPU: n, Memory: 1} },
		"memory":  func(n int64) types.BuildResources { return types.BuildResources{CPU: 1, Memory: n} },
		"storage": func(n int64) types.BuildResources { return types.BuildResources{CPU: 1, Memory: 1, Storage: n} },
	} {
		t.Run(dimension, func(t *testing.T) {
			st := testStore(t)
			ctx := context.Background()
			limit := types.BuildAdmissionLimit{}
			first := admissionBuild("first", resources(math.MaxInt64-1))
			first.Status = types.BuildWaiting
			if _, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, first, limit, "", nil); err != nil || !inserted {
				t.Fatalf("first registration: inserted=%v err=%v", inserted, err)
			}
			if won, err := st.ClaimBuildExecution(ctx, first.BuildID, limit, time.Now()); err != nil || !won {
				t.Fatalf("first claim: won=%v err=%v", won, err)
			}
			cancelled, deleted, pending, err := st.RequestBuildAction(ctx, first, false, true, time.Now())
			if err != nil || deleted || !pending || cancelled == nil || !cancelled.ExecutionClaimed {
				t.Fatalf("cancel must retain execution ownership: build=%+v deleted=%v pending=%v err=%v", cancelled, deleted, pending, err)
			}
			for _, id := range []string{"second", "third"} {
				b := admissionBuild(id, resources(1))
				b.Status = types.BuildWaiting
				if _, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, b, limit, "", nil); err != nil || !inserted {
					t.Fatalf("registration after cancel: inserted=%v err=%v", inserted, err)
				}
			}
			if won, err := st.ClaimBuildExecution(ctx, "second", limit, time.Now()); err != nil || !won {
				t.Fatalf("exact boundary claim: won=%v err=%v", won, err)
			}
			if won, err := st.ClaimBuildExecution(ctx, "third", limit, time.Now()); err != nil || won {
				t.Fatalf("overflow claim must wait: won=%v err=%v", won, err)
			}
			waiting, err := st.GetBuild(ctx, "third")
			if err != nil || waiting == nil || waiting.Status != types.BuildWaiting || waiting.ExecutionClaimed || waiting.RunID != "" || waiting.ExecutionClaimedUnix != 0 {
				t.Fatalf("rejected claim changed ownership: build=%+v err=%v", waiting, err)
			}
			wantExecution, _ := first.Resources.Add(resources(1))
			wantRegistration, _ := resources(1).Add(resources(1))
			usage, err := st.BuildUsage(ctx)
			if err != nil || usage.RegistrationBuilds != 2 || usage.Registration != wantRegistration || usage.ExecutionBuilds != 2 || usage.Execution != wantExecution {
				t.Fatalf("cancelled claim accounting: %+v, %v", usage, err)
			}
			// This fixture has no unit/runtime/phase ownership. Only its terminal
			// commit may now release the cancelled execution claim.
			cancelled.Status = types.BuildError
			if err := st.PutBuildTerminal(ctx, cancelled, nil); err != nil {
				t.Fatal(err)
			}
			if won, err := st.ClaimBuildExecution(ctx, "third", limit, time.Now()); err != nil || !won {
				t.Fatalf("claim after terminal release: won=%v err=%v", won, err)
			}
			usage, err = st.BuildUsage(ctx)
			if err != nil || usage.ExecutionBuilds != 2 || usage.Execution != wantRegistration {
				t.Fatalf("usage after terminal release: %+v, %v", usage, err)
			}
		})
	}
}

func TestUnlimitedRegistrationConcurrentArithmeticBoundary(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	const candidates = 16
	start := make(chan struct{})
	results := make(chan error, candidates)
	for i := range candidates {
		go func() {
			<-start
			_, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx,
				admissionBuild(fmt.Sprintf("concurrent-%d", i), types.BuildResources{CPU: math.MaxInt64/2 + 1, Memory: 1}),
				types.BuildAdmissionLimit{}, "", nil)
			results <- err
		}()
	}
	close(start)
	winners := 0
	for range candidates {
		switch err := <-results; {
		case err == nil:
			winners++
		case errors.Is(err, ErrBuildRegistrationCapacity):
		default:
			t.Errorf("unexpected concurrent registration error: %v", err)
		}
	}
	usage, err := st.BuildUsage(ctx)
	if err != nil || winners != 1 || usage.RegistrationBuilds != 1 || usage.Registration.CPU != math.MaxInt64/2+1 {
		t.Fatalf("concurrent arithmetic boundary: winners=%d usage=%+v err=%v", winners, usage, err)
	}
}

func TestUnlimitedExecutionConcurrentArithmeticBoundary(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	const candidates = 16
	// Cancellation has released registration usage; cleanup still owns the claim.
	owner := admissionBuild("cancelled-owner", types.BuildResources{CPU: math.MaxInt64 - 1, Memory: 1})
	owner.Status, owner.ExecutionClaimed, owner.CancelRequestedUnix = types.BuildBuilding, true, 1
	if err := st.PutBuild(ctx, owner); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan bool, candidates)
	errs := make(chan error, candidates)
	for i := range candidates {
		b := admissionBuild(fmt.Sprintf("concurrent-%d", i), types.BuildResources{CPU: 1, Memory: 1})
		b.Status = types.BuildWaiting
		if _, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, b, types.BuildAdmissionLimit{}, "", nil); err != nil || !inserted {
			t.Fatalf("register candidate: inserted=%v err=%v", inserted, err)
		}
	}
	for i := range candidates {
		go func() {
			<-start
			won, err := st.ClaimBuildExecution(ctx, fmt.Sprintf("concurrent-%d", i), types.BuildAdmissionLimit{}, time.Now())
			results <- won
			errs <- err
		}()
	}
	close(start)
	winners := 0
	for range candidates {
		if <-results {
			winners++
		}
		if err := <-errs; err != nil {
			t.Errorf("unexpected concurrent claim error: %v", err)
		}
	}
	usage, err := st.BuildUsage(ctx)
	if err != nil || winners != 1 || usage.RegistrationBuilds != candidates || usage.ExecutionBuilds != 2 || usage.Execution.CPU != math.MaxInt64 || usage.WaitingBuilds != candidates-1 {
		t.Fatalf("concurrent arithmetic boundary: winners=%d usage=%+v err=%v", winners, usage, err)
	}
}
