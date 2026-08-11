package orch

import (
	"context"
	"errors"
	"testing"
)

func TestAcceptedOperationDrainClosesAdmissionAndWaits(t *testing.T) {
	var group acceptedOperationGroup
	finish, err := group.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	drainCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := group.Drain(drainCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain while active = %v, want context.Canceled", err)
	}
	if _, err := group.Begin(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Begin after Drain = %v, want context.Canceled", err)
	}

	finish()
	finish() // completion is idempotent
	if err := group.Drain(context.Background()); err != nil {
		t.Fatalf("Drain after completion: %v", err)
	}
}
