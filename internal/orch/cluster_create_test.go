package orch

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestClaimClusterCreateRejectsInflightAndStoredSandboxIDs(t *testing.T) {
	ctx := context.Background()
	o := testOrch(t)

	if err := o.claimClusterCreate(ctx, ""); err == nil {
		t.Fatal("empty sandbox id was accepted")
	}
	if err := o.claimClusterCreate(ctx, "inflight"); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := o.claimClusterCreate(ctx, "inflight"); err == nil {
		t.Fatal("concurrent duplicate claim was accepted")
	}
	o.releaseClusterCreate("inflight")
	if err := o.claimClusterCreate(ctx, "inflight"); err != nil {
		t.Fatalf("released claim was not reusable: %v", err)
	}
	o.releaseClusterCreate("inflight")

	if err := o.st.Put(ctx, &types.Sandbox{ID: "stored", State: types.StateRunning}); err != nil {
		t.Fatal(err)
	}
	if err := o.claimClusterCreate(ctx, "stored"); err == nil {
		t.Fatal("stored sandbox id was accepted")
	}
	if _, claimed := o.clusterCreates["stored"]; claimed {
		t.Fatal("failed stored-id claim leaked its in-flight marker")
	}
}
