package store

import (
	"context"
	"errors"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
)

func TestReconcileOrphanSandboxAdmissionsPreservesJournaledRecords(t *testing.T) {
	st := testStore(t)
	if err := st.ConfigureSandboxSlotAdmission(2, 2); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	orphan := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "sandbox-orphan", placement.BuildDemand{})
	if _, err := st.PrepareAdmission(orphan.ObjectID, orphan.DemandDigest, nodectl.SandboxAdmissionDemand{SlotUnits: 1}); err != nil {
		t.Fatal(err)
	}
	kept := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "sandbox-kept", placement.BuildDemand{})
	prepared, err := st.PrepareAdmission(kept.ObjectID, kept.DemandDigest, nodectl.SandboxAdmissionDemand{SlotUnits: 1})
	if err != nil {
		t.Fatal(err)
	}
	decision := nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: prepared.ReservationToken,
	}
	if _, err := st.RecordSandboxWorkflow(ctx, kept, decision, workflowSandbox(kept.ObjectID)); err != nil {
		t.Fatal(err)
	}

	if err := st.ReconcileOrphanSandboxAdmissions(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetAdmission(orphan.ObjectID, orphan.DemandDigest); !errors.Is(err, nodectl.ErrPreparedAdmissionMissing) {
		t.Fatalf("orphan Admission error = %v", err)
	}
	if got, err := st.GetAdmission(kept.ObjectID, kept.DemandDigest); err != nil || got.ReservationToken != prepared.ReservationToken {
		t.Fatalf("journaled Admission = %+v, %v", got, err)
	}
	usage, err := st.SandboxSlotAdmissionUsage(ctx)
	if err != nil || usage.AdmittedSlots != 1 || usage.SlotUsed != 1 {
		t.Fatalf("reconciled usage = %+v, %v", usage, err)
	}
}
