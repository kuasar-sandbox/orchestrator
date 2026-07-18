package nodeexec_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type sandboxAdmissionFake struct {
	prepared  map[string]nodectl.PreparedAdmissionResult
	promoted  []nodectl.PreparedAdmissionResult
	released  []string
	finalized []string
	prepares  int
	wake      chan struct{}
}

func (f *sandboxAdmissionFake) GetAdmission(id, _ string) (nodectl.PreparedAdmissionResult, error) {
	result, ok := f.prepared[id]
	if !ok {
		return nodectl.PreparedAdmissionResult{}, nodectl.ErrPreparedAdmissionMissing
	}
	return result, nil
}

func (f *sandboxAdmissionFake) PrepareAdmission(
	id, _ string,
	_ nodectl.SandboxAdmissionDemand,
) (nodectl.PreparedAdmissionResult, error) {
	f.prepares++
	result, ok := f.prepared[id]
	if !ok {
		return nodectl.PreparedAdmissionResult{}, errors.New("unexpected Sandbox Admission")
	}
	return result, nil
}

func (f *sandboxAdmissionFake) ClaimAdmission(id, _ string) (nodectl.PreparedAdmissionResult, error) {
	result := f.prepared[id]
	result.State = nodectl.PreparedClaimed
	f.prepared[id] = result
	return result, nil
}

func (f *sandboxAdmissionFake) ReleaseAdmission(id, _, reason string) (nodectl.PreparedAdmissionResult, error) {
	f.released = append(f.released, id+":"+reason)
	result := f.prepared[id]
	result.State = nodectl.PreparedReleased
	result.Reason = reason
	f.prepared[id] = result
	return result, nil
}

func (f *sandboxAdmissionFake) FinalizeAdmission(id, digest string) error {
	f.finalized = append(f.finalized, id+":"+digest)
	delete(f.prepared, id)
	return nil
}

func (f *sandboxAdmissionFake) PromoteQueued() ([]nodectl.PreparedAdmissionResult, error) {
	for _, result := range f.promoted {
		f.prepared[result.SandboxID] = result
	}
	return append([]nodectl.PreparedAdmissionResult(nil), f.promoted...), nil
}

func (f *sandboxAdmissionFake) Wake() <-chan struct{} { return f.wake }

func authorityStore(t *testing.T) *store.Store {
	t.Helper()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func authorityCommand(
	t *testing.T,
	kind clusterstate.ExecutionKind,
	objectID string,
) session.DispatchCommand {
	t.Helper()
	var (
		demand   []byte
		routeKey string
		err      error
	)
	if kind == clusterstate.ExecutionKindBuild {
		demand, err = placement.NormalizeBuildDemand(placement.BuildDemand{Slots: 1, Memory: 1 << 30})
	} else {
		demand, err = placement.NormalizeSandboxDemand(placement.SandboxDemand{
			SlotUnits: 1, StartupBudgetMemory: 1 << 30, FloorMemory: 512 << 20,
		})
		routeKey = "route-" + objectID
	}
	if err != nil {
		t.Fatal(err)
	}
	intent, err := clusterstate.NewDispatchIntent(demand, []byte(`{"id":"`+objectID+`"}`), "provider-v1/policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	demandDigest, _ := hex.DecodeString(intent.DemandDigest)
	specDigest, _ := hex.DecodeString(intent.DispatchSpecDigest)
	binding := clusterstate.ExecutionBinding{
		StorageGeneration: "generation-1", Kind: kind, ObjectID: objectID,
		Group: "group-1", RouteKey: routeKey, NodeID: "node-1", NodeEpoch: 7,
	}
	copy(binding.DemandDigest[:], demandDigest)
	copy(binding.DispatchSpecDigest[:], specDigest)
	opaque, err := clusterstate.EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	return session.DispatchCommand{
		Kind: kind, Group: "group-1", RouteKey: routeKey, ObjectID: objectID,
		NodeID: "node-1", NodeEpoch: 7, SessionSeq: 11, DataEndpoint: "10.0.0.1:8443",
		Intent: intent,
		Binding: clusterstate.ExecutionBindingIntent{
			NodeID: "node-1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
			StorageGeneration: "generation-1", OpaqueBinding: opaque, BindingDigest: digest,
		},
	}
}

func newAuthority(
	t *testing.T,
	st *store.Store,
	sandbox *sandboxAdmissionFake,
) *nodeexec.Authority {
	t.Helper()
	authority, err := nodeexec.NewAuthority(
		st,
		sandbox,
		func(context.Context) (nodeexec.LocalSessionIdentity, error) {
			return nodeexec.LocalSessionIdentity{
				NodeID: "node-1", NodeEpoch: 7, SessionSeq: 11, DataEndpoint: "10.0.0.1:8443",
			}, nil
		},
		func(context.Context) (nodeexec.BuildCapacity, string, error) {
			return nodeexec.BuildCapacity{Slots: 1, Memory: 2 << 30, QueueLimit: 4}, "", nil
		},
		func(record nodeexec.DispatchRecord) (*types.Build, error) {
			return &types.Build{
				BuildID: record.ObjectID, TemplateID: "transient-" + record.ObjectID,
				ManifestKey: strings.Repeat("4", 64), Profile: types.ProfileE2B,
				Kind: types.KindImg, CreatedUnix: time.Now().Unix(),
			}, nil
		},
		func(nodeexec.DispatchRecord) (nodectl.SandboxAdmissionDemand, error) {
			return nodectl.SandboxAdmissionDemand{
				CapacityMemoryBytes: 1 << 30, CapacityCPU: 2, FloorMemoryBytes: 512 << 20,
				FloorCPU: 1, StartupBudgetMemory: 1 << 30,
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

func TestAuthorityBuildDispatchIsDurableAndSessionFenced(t *testing.T) {
	st := authorityStore(t)
	sandbox := &sandboxAdmissionFake{prepared: map[string]nodectl.PreparedAdmissionResult{}, wake: make(chan struct{})}
	authority := newAuthority(t, st, sandbox)
	command := authorityCommand(t, clusterstate.ExecutionKindBuild, "build-1")

	reply, err := authority.AdmitAndDispatch(context.Background(), command)
	if err != nil || reply.Outcome != clusterstate.DispatchAcceptedAdmitted {
		t.Fatalf("dispatch = %+v, %v", reply, err)
	}
	retry, err := authority.AdmitAndDispatch(context.Background(), command)
	if err != nil || retry != reply {
		t.Fatalf("retry = %+v, %v; want %+v", retry, err, reply)
	}
	command.SessionSeq = 10
	stale, err := authority.AdmitAndDispatch(context.Background(), command)
	if err != nil || stale.Outcome != clusterstate.DispatchSessionMoved {
		t.Fatalf("stale tuple = %+v, %v", stale, err)
	}
	authority.FenceStaleSession()
	command.SessionSeq = 11
	fenced, err := authority.AdmitAndDispatch(context.Background(), command)
	if err != nil || fenced.Outcome != clusterstate.DispatchSessionMoved {
		t.Fatalf("fenced session = %+v, %v", fenced, err)
	}
}

func TestAuthoritySandboxQueuePromotionClaimFailureAndOriginalRetry(t *testing.T) {
	st := authorityStore(t)
	command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-1")
	digest := command.Intent.DemandDigest
	sandbox := &sandboxAdmissionFake{
		prepared: map[string]nodectl.PreparedAdmissionResult{
			"sandbox-1": {
				SandboxID: "sandbox-1", DemandDigest: digest, State: nodectl.PreparedQueued,
				ReservationToken: "token-1",
			},
		},
		wake: make(chan struct{}),
	}
	authority := newAuthority(t, st, sandbox)
	reply, err := authority.AdmitAndDispatch(context.Background(), command)
	if err != nil || reply.Outcome != clusterstate.DispatchAcceptedQueued {
		t.Fatalf("queued dispatch = %+v, %v", reply, err)
	}
	sandbox.promoted = []nodectl.PreparedAdmissionResult{{
		SandboxID: "sandbox-1", DemandDigest: digest, State: nodectl.PreparedAdmitted,
		ReservationToken: "token-1",
	}}
	if err := authority.PromoteSandboxQueue(context.Background()); err != nil {
		t.Fatal(err)
	}
	launchable, err := authority.Launchable(context.Background(), clusterstate.ExecutionKindSandbox, "", 10)
	if err != nil || len(launchable) != 1 || launchable[0].ObjectID != "sandbox-1" {
		t.Fatalf("launchable = %+v, %v", launchable, err)
	}
	claimed, err := authority.ClaimSandbox(context.Background(), launchable[0])
	if err != nil || claimed.AdmissionState != nodeexec.AdmissionLaunching || claimed.ReservationToken != "token-1" {
		t.Fatalf("claimed = %+v, %v", claimed, err)
	}
	retry, err := authority.AdmitAndDispatch(context.Background(), command)
	if err != nil || retry.Outcome != clusterstate.DispatchAcceptedQueued {
		t.Fatalf("retry changed original result = %+v, %v", retry, err)
	}
	if err := authority.FailSandbox(context.Background(), claimed, "launch_failed"); err != nil {
		t.Fatal(err)
	}
	if len(sandbox.released) != 1 || sandbox.released[0] != "sandbox-1:launch_failed" {
		t.Fatalf("released = %+v", sandbox.released)
	}
	pending, _, err := st.PendingExecutionEvents(context.Background(), "node-1", 7, routesync.EventCursor{}, 10, 1<<20)
	if err != nil || len(pending) != 1 || pending[0].State != "ERROR" || pending[0].Reason != "launch_failed" {
		t.Fatalf("pending failure = %+v, %v", pending, err)
	}
}

func TestAuthorityPersistsDefinitiveSandboxRejection(t *testing.T) {
	st := authorityStore(t)
	command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-rejected")
	sandbox := &sandboxAdmissionFake{
		prepared: map[string]nodectl.PreparedAdmissionResult{
			"sandbox-rejected": {
				SandboxID: "sandbox-rejected", DemandDigest: command.Intent.DemandDigest,
				State: nodectl.PreparedRejected, Reason: "exceeds_node_capacity",
			},
		},
		wake: make(chan struct{}),
	}
	authority := newAuthority(t, st, sandbox)
	reply, err := authority.AdmitAndDispatch(context.Background(), command)
	if err != nil || reply.Outcome != clusterstate.DispatchDefinitiveReject || reply.Reason != "exceeds_node_capacity" {
		t.Fatalf("rejection = %+v, %v", reply, err)
	}
	retry, err := authority.AdmitAndDispatch(context.Background(), command)
	if err != nil || retry != reply {
		t.Fatalf("rejection retry = %+v, %v", retry, err)
	}
	select {
	case <-authority.WorkWake():
		t.Fatal("definitive rejection became launchable")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestAuthorityReconcilesSandboxPromotionClaimAndReleaseCrashWindows(t *testing.T) {
	st := authorityStore(t)
	command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-reconcile")
	digest := command.Intent.DemandDigest
	sandbox := &sandboxAdmissionFake{
		prepared: map[string]nodectl.PreparedAdmissionResult{
			"sandbox-reconcile": {
				SandboxID: "sandbox-reconcile", DemandDigest: digest, State: nodectl.PreparedQueued,
				ReservationToken: "token-reconcile",
			},
		},
		wake: make(chan struct{}),
	}
	authority := newAuthority(t, st, sandbox)
	if reply, err := authority.AdmitAndDispatch(context.Background(), command); err != nil ||
		reply.Outcome != clusterstate.DispatchAcceptedQueued {
		t.Fatalf("queued = %+v, %v", reply, err)
	}

	// Crash after resource promotion but before the node journal transition.
	prepared := sandbox.prepared["sandbox-reconcile"]
	prepared.State = nodectl.PreparedAdmitted
	sandbox.prepared["sandbox-reconcile"] = prepared
	if err := authority.ReconcileSandboxAdmissions(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	record, err := st.GetNodeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox, "sandbox-reconcile")
	if err != nil || record.AdmissionState != nodeexec.AdmissionAdmitted || !record.ResourceClaimed {
		t.Fatalf("promotion repair = %+v, %v", record, err)
	}

	// Crash after resource Claim but before the journal records launch ownership.
	prepared.State = nodectl.PreparedClaimed
	sandbox.prepared["sandbox-reconcile"] = prepared
	if err := authority.ReconcileSandboxAdmissions(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	record, err = st.GetNodeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox, "sandbox-reconcile")
	if err != nil || record.AdmissionState != nodeexec.AdmissionLaunching {
		t.Fatalf("claim repair = %+v, %v", record, err)
	}
	launchable, err := authority.Launchable(context.Background(), clusterstate.ExecutionKindSandbox, "", 10)
	if err != nil || len(launchable) != 1 || launchable[0].AdmissionState != nodeexec.AdmissionLaunching {
		t.Fatalf("recovered launch = %+v, %v", launchable, err)
	}

	// Crash after terminal outbox commit but before controller release.
	record, err = st.FailPendingNodeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox,
		"sandbox-reconcile", digest, "launch_failed")
	if err != nil || !record.ResourceClaimed {
		t.Fatalf("terminal before release = %+v, %v", record, err)
	}
	if err := authority.FinalizeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox,
		record.ObjectID, record.DemandDigest, record.BindingDigest); !errors.Is(err, store.ErrNodeWorkflowState) {
		t.Fatalf("finalize before resource release error = %v", err)
	}
	if err := authority.ReconcileSandboxAdmissions(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	record, err = st.GetNodeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox, "sandbox-reconcile")
	if err != nil || record.ResourceClaimed {
		t.Fatalf("release repair = %+v, %v", record, err)
	}
	if len(sandbox.released) != 1 || sandbox.released[0] != "sandbox-reconcile:launch_failed" {
		t.Fatalf("controller release = %+v", sandbox.released)
	}
	if err := authority.FinalizeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox,
		record.ObjectID, record.DemandDigest, record.BindingDigest); err != nil {
		t.Fatal(err)
	}
	if len(sandbox.finalized) != 1 || !strings.HasPrefix(sandbox.finalized[0], "sandbox-reconcile:") {
		t.Fatalf("controller finalization = %+v", sandbox.finalized)
	}
	record, err = st.GetNodeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox, "sandbox-reconcile")
	if err != nil || record == nil || !record.WorkflowFinalized {
		t.Fatalf("journal finalization = %+v, %v", record, err)
	}
}

func TestAuthorityFailsClosedWhenActiveControllerStateIsMissing(t *testing.T) {
	st := authorityStore(t)
	command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-missing")
	sandbox := &sandboxAdmissionFake{
		prepared: map[string]nodectl.PreparedAdmissionResult{
			"sandbox-missing": {
				SandboxID: "sandbox-missing", DemandDigest: command.Intent.DemandDigest,
				State: nodectl.PreparedAdmitted, ReservationToken: "token-missing",
			},
		},
		wake: make(chan struct{}),
	}
	authority := newAuthority(t, st, sandbox)
	if _, err := authority.AdmitAndDispatch(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	delete(sandbox.prepared, "sandbox-missing")
	if err := authority.ReconcileSandboxAdmissions(context.Background(), 10); !errors.Is(err, nodectl.ErrPreparedAdmissionMissing) {
		t.Fatalf("missing controller state error = %v", err)
	}
	record, err := st.GetNodeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox, "sandbox-missing")
	if err != nil || record.AdmissionState != nodeexec.AdmissionAdmitted {
		t.Fatalf("missing state mutated journal = %+v, %v", record, err)
	}
}

func TestDispatchCommandFromWireUsesBusinessIDAndExactIntent(t *testing.T) {
	want := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-wire")
	wire := &routesync.Command{
		Kind: routesync.CmdSandboxAdmitDispatch, SID: want.ObjectID,
		NodeEpoch: want.NodeEpoch, SessionSeq: want.SessionSeq,
		StorageGeneration: want.Binding.StorageGeneration,
		Binding:           want.Binding.OpaqueBinding, BindingDigest: want.Binding.BindingDigest,
		Group: want.Group, RouteKey: want.RouteKey,
		NormalizedDemand: want.Intent.NormalizedDemand, DemandDigest: want.Intent.DemandDigest,
		DispatchSpec: want.Intent.DispatchSpec, DispatchSpecDigest: want.Intent.DispatchSpecDigest,
		ProviderPolicy: want.Intent.ProviderPolicyVersion,
	}
	got, err := nodeexec.DispatchCommandFromWire(wire, nodeexec.LocalSessionIdentity{
		NodeID: want.NodeID, NodeEpoch: want.NodeEpoch, SessionSeq: want.SessionSeq,
		DataEndpoint: want.DataEndpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != clusterstate.ExecutionKindSandbox || got.ObjectID != want.ObjectID || got.Group != want.Group ||
		got.RouteKey != want.RouteKey || got.NodeID != want.NodeID || got.NodeEpoch != want.NodeEpoch ||
		got.SessionSeq != want.SessionSeq || got.Binding != want.Binding ||
		!bytes.Equal(got.Intent.NormalizedDemand, want.Intent.NormalizedDemand) ||
		!bytes.Equal(got.Intent.DispatchSpec, want.Intent.DispatchSpec) {
		t.Fatalf("wire dispatch = %+v, want %+v", got, want)
	}
}

func TestAuthorityRejectsExistingLocalObjectBeforeAdmissionSideEffects(t *testing.T) {
	st := authorityStore(t)
	command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-existing")
	if err := st.Put(context.Background(), &types.Sandbox{
		ID: command.ObjectID, TemplateID: "e2b-img-" + strings.Repeat("5", 64),
		ManifestKey: strings.Repeat("6", 64), State: types.StateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	sandbox := &sandboxAdmissionFake{
		prepared: map[string]nodectl.PreparedAdmissionResult{
			command.ObjectID: {
				SandboxID: command.ObjectID, DemandDigest: command.Intent.DemandDigest,
				State: nodectl.PreparedAdmitted, ReservationToken: "must-not-be-created",
			},
		},
		wake: make(chan struct{}),
	}
	authority := newAuthority(t, st, sandbox)
	reply, err := authority.AdmitAndDispatch(context.Background(), command)
	if err != nil || reply.Outcome != clusterstate.DispatchConflict {
		t.Fatalf("occupied object reply = %+v, %v", reply, err)
	}
	if sandbox.prepares != 0 {
		t.Fatalf("Admission was called %d times for an occupied SID", sandbox.prepares)
	}
}
