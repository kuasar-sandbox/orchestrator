package observe

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/observebudget"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

func TestSharedAdmissionReservationAndClose(t *testing.T) {
	budget := observebudget.NewBudget(4)
	first, second := New(budget), New(budget)
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
	var reserved []context.Context
	for range 4 {
		ctx, release, err := first.Reserve(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		reserved = append(reserved, ctx)
		t.Cleanup(release)
	}
	if _, _, err := second.Reserve(t.Context()); !errors.Is(err, storage.ErrCapacity) {
		t.Fatalf("another handle bypassed the shared budget: %v", err)
	}
	wait := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	if _, err := second.Run(t.Context(), 2, wait); !errors.Is(err, storage.ErrCapacity) || budget.Used() != 4 {
		t.Fatalf("failed multi-key admission leaked slots: used=%d err=%v", budget.Used(), err)
	}
	observation, err := second.Run(reserved[0], 1, wait)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Run(reserved[0], 1, wait); !errors.Is(err, storage.ErrCapacity) {
		t.Fatalf("one reservation admitted concurrent observations: %v", err)
	}
	first.Close()
	observation.Cancel()
	if budget.Used() != 0 || second.Active() != 0 {
		t.Fatalf("Close returned before reclaiming reservations: used=%d active=%d", budget.Used(), second.Active())
	}
	_, release, err := second.Reserve(t.Context())
	if err != nil {
		t.Fatalf("closing one owner disabled the other: %v", err)
	}
	release()
	if budget.Used() != 0 {
		t.Fatal("release did not return admission capacity")
	}
}
