package nodeexec_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"sync"
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
	prepared   map[string]nodectl.PreparedAdmissionResult
	promoted   []nodectl.PreparedAdmissionResult
	released   []string
	finalized  []string
	prepares   int
	prepareErr error
	promoteErr error
	wake       chan struct{}
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
	return result, f.prepareErr
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
	return append([]nodectl.PreparedAdmissionResult(nil), f.promoted...), f.promoteErr
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
		RegistryGeneration: "generation-1", Kind: kind, ObjectID: objectID,
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
			RegistryGeneration: "generation-1", OpaqueBinding: opaque, BindingDigest: digest,
		},
	}
}

func newAuthority(
	t *testing.T,
	journal nodeexec.WorkflowJournal,
	sandbox nodeexec.SandboxAdmissionController,
) *nodeexec.Authority {
	t.Helper()
	return newAuthorityAtSession(t, journal, sandbox, 11)
}

func newAuthorityAtSession(
	t *testing.T,
	journal nodeexec.WorkflowJournal,
	sandbox nodeexec.SandboxAdmissionController,
	sessionSeq uint64,
) *nodeexec.Authority {
	t.Helper()
	return newAuthorityWithIdentityAndCapacity(t, journal, sandbox, func(context.Context) (nodeexec.LocalSessionIdentity, error) {
		return nodeexec.LocalSessionIdentity{
			NodeID: "node-1", NodeEpoch: 7, SessionSeq: sessionSeq, DataEndpoint: "10.0.0.1:8443",
		}, nil
	}, func(context.Context) (nodeexec.BuildCapacity, string, error) {
		return nodeexec.BuildCapacity{Slots: 1, Memory: 2 << 30, QueueLimit: 4}, "", nil
	})
}

func newAuthorityWithCapacity(
	t *testing.T,
	journal nodeexec.WorkflowJournal,
	sandbox nodeexec.SandboxAdmissionController,
	capacity nodeexec.BuildCapacitySource,
) *nodeexec.Authority {
	t.Helper()
	return newAuthorityWithIdentityAndCapacity(t, journal, sandbox, func(context.Context) (nodeexec.LocalSessionIdentity, error) {
		return nodeexec.LocalSessionIdentity{
			NodeID: "node-1", NodeEpoch: 7, SessionSeq: 11, DataEndpoint: "10.0.0.1:8443",
		}, nil
	}, capacity)
}

func newAuthorityWithIdentityAndCapacity(
	t *testing.T,
	journal nodeexec.WorkflowJournal,
	sandbox nodeexec.SandboxAdmissionController,
	identity nodeexec.IdentitySource,
	capacity nodeexec.BuildCapacitySource,
) *nodeexec.Authority {
	t.Helper()
	authority, err := nodeexec.NewAuthority(
		journal,
		sandbox,
		identity,
		capacity,
		func(_ context.Context, record nodeexec.DispatchRecord) (*types.Build, error) {
			return &types.Build{
				BuildID: record.ObjectID, TemplateID: "transient-" + record.ObjectID,
				AuthKey: strings.Repeat("3", 64), ManifestKey: strings.Repeat("4", 64),
				CPUCount: 2, MemoryMB: 2048, Profile: types.ProfileE2B,
				Kind: types.KindImg, CreatedUnix: time.Now().Unix(),
			}, nil
		},
		func(_ context.Context, record nodeexec.DispatchRecord) (*types.Sandbox, error) {
			return &types.Sandbox{
				ID: record.ObjectID, TemplateID: "bare-img-" + strings.Repeat("5", 64),
				State: types.StateStarting, AuthKey: strings.Repeat("3", 64),
				ManifestKey: strings.Repeat("4", 64), CreatedUnix: time.Now().Unix(),
			}, nil
		},
		func(context.Context, nodeexec.DispatchRecord) (nodectl.SandboxAdmissionDemand, error) {
			return nodectl.SandboxAdmissionDemand{
				SlotUnits:           1,
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

type recordBarrierJournal struct {
	*store.Store
	mu      sync.Mutex
	arrived int
	ready   chan struct{}
}

type failingFinalizeJournal struct {
	*store.Store
	err error
}

func (j *failingFinalizeJournal) FinalizeNodeWorkflow(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID, bindingDigest string,
) error {
	if j.err != nil {
		return j.err
	}
	return j.Store.FinalizeNodeWorkflow(ctx, kind, objectID, bindingDigest)
}

func (j *recordBarrierJournal) RecordSandboxWorkflow(
	ctx context.Context,
	dispatch nodeexec.DispatchRecord,
	decision nodeexec.AdmissionDecision,
	sandbox *types.Sandbox,
) (*nodeexec.WorkflowRecord, error) {
	j.mu.Lock()
	j.arrived++
	if j.arrived == 2 {
		close(j.ready)
	}
	j.mu.Unlock()
	select {
	case <-j.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.Store.RecordSandboxWorkflow(ctx, dispatch, decision, sandbox)
}

type concurrentSandboxAdmission struct {
	*sandboxAdmissionFake
	mu sync.Mutex
}

func (f *concurrentSandboxAdmission) PrepareAdmission(
	id, digest string,
	demand nodectl.SandboxAdmissionDemand,
) (nodectl.PreparedAdmissionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sandboxAdmissionFake.PrepareAdmission(id, digest, demand)
}

func (f *concurrentSandboxAdmission) ReleaseAdmission(
	id, digest, reason string,
) (nodectl.PreparedAdmissionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sandboxAdmissionFake.ReleaseAdmission(id, digest, reason)
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
	policyChanged := command
	policyChanged.Intent.ProviderPolicyVersion = "provider-v2/policy-v2"
	conflict, err := authority.AdmitAndDispatch(context.Background(), policyChanged)
	if err != nil || conflict.Outcome != clusterstate.DispatchConflict {
		t.Fatalf("policy-version conflict = %+v, %v", conflict, err)
	}
	stored, err := st.GetNodeWorkflow(context.Background(), clusterstate.ExecutionKindBuild, command.ObjectID)
	if err != nil || stored.ProviderPolicyVersion != command.Intent.ProviderPolicyVersion {
		t.Fatalf("durable policy version = %+v, %v", stored, err)
	}
	newSessionCommand := command
	newSessionCommand.SessionSeq = 12
	newSessionAuthority := newAuthorityAtSession(t, st, sandbox, 12)
	newSessionRetry, err := newSessionAuthority.AdmitAndDispatch(context.Background(), newSessionCommand)
	if err != nil || newSessionRetry != reply {
		t.Fatalf("new Holder/session retry = %+v, %v; want %+v", newSessionRetry, err, reply)
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

func TestAuthorityFencesEverySessionOwnedWorkerEntry(t *testing.T) {
	st := authorityStore(t)
	sandbox := &sandboxAdmissionFake{prepared: map[string]nodectl.PreparedAdmissionResult{}, wake: make(chan struct{})}
	authority := newAuthority(t, st, sandbox)
	authority.FenceStaleSession()

	if err := authority.PromoteSandboxQueue(context.Background()); !errors.Is(err, nodeexec.ErrSessionFenced) {
		t.Fatalf("PromoteSandboxQueue error = %v", err)
	}
	if err := authority.ReconcileSandboxAdmissions(context.Background(), 1); !errors.Is(err, nodeexec.ErrSessionFenced) {
		t.Fatalf("ReconcileSandboxAdmissions error = %v", err)
	}
	if _, err := authority.PromoteBuildQueue(context.Background()); !errors.Is(err, nodeexec.ErrSessionFenced) {
		t.Fatalf("PromoteBuildQueue error = %v", err)
	}
	if _, err := authority.Launchable(context.Background(), clusterstate.ExecutionKindSandbox, "", 1); !errors.Is(err, nodeexec.ErrSessionFenced) {
		t.Fatalf("Launchable error = %v", err)
	}
	if _, err := authority.ClaimSandbox(context.Background(), nil); !errors.Is(err, nodeexec.ErrSessionFenced) {
		t.Fatalf("ClaimSandbox error = %v", err)
	}
	if _, err := authority.ClaimBuild(context.Background(), nil); !errors.Is(err, nodeexec.ErrSessionFenced) {
		t.Fatalf("ClaimBuild error = %v", err)
	}
	if err := authority.FailSandbox(context.Background(), nil, "fenced"); !errors.Is(err, nodeexec.ErrSessionFenced) {
		t.Fatalf("FailSandbox error = %v", err)
	}
	if err := authority.FinalizeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox, "", ""); !errors.Is(err, nodeexec.ErrSessionFenced) {
		t.Fatalf("FinalizeWorkflow error = %v", err)
	}
}

func TestConcurrentConflictingSandboxDispatchKeepsWinningReservation(t *testing.T) {
	st := authorityStore(t)
	command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-race")
	base := &sandboxAdmissionFake{
		prepared: map[string]nodectl.PreparedAdmissionResult{
			command.ObjectID: {
				SandboxID: command.ObjectID, DemandDigest: command.Intent.DemandDigest,
				State: nodectl.PreparedAdmitted, ReservationToken: "shared-token",
			},
		},
		wake: make(chan struct{}),
	}
	sandbox := &concurrentSandboxAdmission{sandboxAdmissionFake: base}
	journal := &recordBarrierJournal{Store: st, ready: make(chan struct{})}
	authority := newAuthority(t, journal, sandbox)
	conflicting := command
	conflicting.Intent.ProviderPolicyVersion = "provider-v2/policy-v2"

	var (
		replies [2]session.DispatchReply
		errs    [2]error
		wait    sync.WaitGroup
	)
	wait.Add(2)
	go func() {
		defer wait.Done()
		replies[0], errs[0] = authority.AdmitAndDispatch(context.Background(), command)
	}()
	go func() {
		defer wait.Done()
		replies[1], errs[1] = authority.AdmitAndDispatch(context.Background(), conflicting)
	}()
	wait.Wait()
	accepted, conflicts := 0, 0
	for index, reply := range replies {
		if errs[index] != nil {
			t.Fatalf("dispatch %d error = %v", index, errs[index])
		}
		switch reply.Outcome {
		case clusterstate.DispatchAcceptedAdmitted:
			accepted++
		case clusterstate.DispatchConflict:
			conflicts++
		default:
			t.Fatalf("dispatch %d reply = %+v", index, reply)
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("accepted=%d conflicts=%d replies=%+v", accepted, conflicts, replies)
	}
	if len(base.released) != 0 {
		t.Fatalf("loser released the winning reservation: %+v", base.released)
	}
	winner, err := st.GetNodeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox, command.ObjectID)
	if err != nil || winner == nil || winner.ReservationToken != "shared-token" {
		t.Fatalf("winner = %+v, %v", winner, err)
	}
}

func TestBuildSafetyFenceTerminatesExistingQueue(t *testing.T) {
	st := authorityStore(t)
	sandbox := &sandboxAdmissionFake{prepared: map[string]nodectl.PreparedAdmissionResult{}, wake: make(chan struct{})}
	var (
		capacityMu   sync.RWMutex
		safetyReason string
	)
	authority := newAuthorityWithCapacity(t, st, sandbox, func(context.Context) (nodeexec.BuildCapacity, string, error) {
		capacityMu.RLock()
		defer capacityMu.RUnlock()
		return nodeexec.BuildCapacity{Slots: 1, Memory: 2 << 30, QueueLimit: 4}, safetyReason, nil
	})
	first := authorityCommand(t, clusterstate.ExecutionKindBuild, "build-running")
	second := authorityCommand(t, clusterstate.ExecutionKindBuild, "build-queued")
	if reply, err := authority.AdmitAndDispatch(context.Background(), first); err != nil ||
		reply.Outcome != clusterstate.DispatchAcceptedAdmitted {
		t.Fatalf("first Build = %+v, %v", reply, err)
	}
	if reply, err := authority.AdmitAndDispatch(context.Background(), second); err != nil ||
		reply.Outcome != clusterstate.DispatchAcceptedQueued {
		t.Fatalf("queued Build = %+v, %v", reply, err)
	}
	capacityMu.Lock()
	safetyReason = "node_safety_state_rejects_build"
	capacityMu.Unlock()
	failed, err := authority.PromoteBuildQueue(context.Background())
	if err != nil || len(failed) != 1 || failed[0].ObjectID != second.ObjectID ||
		failed[0].AdmissionState != nodeexec.AdmissionTerminal {
		t.Fatalf("safety failure = %+v, %v", failed, err)
	}
	launchable, err := authority.Launchable(context.Background(), clusterstate.ExecutionKindBuild, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range launchable {
		if record.ObjectID == second.ObjectID {
			t.Fatal("safety-rejected queued Build became launchable")
		}
	}
	build, err := st.GetBuild(context.Background(), second.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if build == nil || build.Status != types.BuildError || build.Reason != safetyReason {
		t.Fatalf("queued Build local terminal state = %+v", build)
	}
	pending, _, err := st.PendingExecutionEvents(context.Background(), "node-1", 7, routesync.EventCursor{}, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range pending {
		if event.ObjectID == second.ObjectID {
			t.Fatalf("node-local Build lifecycle leaked into cluster outbox: %+v", event)
		}
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

func TestAuthorityJournalsPublishedAdmissionAndPromotionBeforeReturningFlushError(t *testing.T) {
	ctx := context.Background()
	published := &nodectl.PublishedFlushError{Err: errors.New("injected parent fsync failure")}

	t.Run("prepare", func(t *testing.T) {
		st := authorityStore(t)
		command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-published-prepare")
		sandbox := &sandboxAdmissionFake{
			prepared: map[string]nodectl.PreparedAdmissionResult{
				command.ObjectID: {
					SandboxID: command.ObjectID, DemandDigest: command.Intent.DemandDigest,
					State: nodectl.PreparedAdmitted, ReservationToken: "token-published",
				},
			},
			prepareErr: published,
			wake:       make(chan struct{}),
		}
		authority := newAuthority(t, st, sandbox)
		if _, err := authority.AdmitAndDispatch(ctx, command); !nodectl.FlushPublished(err) {
			t.Fatalf("published prepare error = %v", err)
		}
		record, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, command.ObjectID)
		if err != nil || record == nil || record.AdmissionState != nodeexec.AdmissionAdmitted {
			t.Fatalf("journal after published prepare = %+v, %v", record, err)
		}
		sandbox.prepareErr = nil
		reply, err := authority.AdmitAndDispatch(ctx, command)
		if err != nil || reply.Outcome != clusterstate.DispatchAcceptedAdmitted || sandbox.prepares != 1 {
			t.Fatalf("retry = %+v, %v prepares=%d", reply, err, sandbox.prepares)
		}
	})

	t.Run("promotion", func(t *testing.T) {
		st := authorityStore(t)
		command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-published-promotion")
		sandbox := &sandboxAdmissionFake{
			prepared: map[string]nodectl.PreparedAdmissionResult{
				command.ObjectID: {
					SandboxID: command.ObjectID, DemandDigest: command.Intent.DemandDigest,
					State: nodectl.PreparedQueued, ReservationToken: "token-published",
				},
			},
			wake: make(chan struct{}),
		}
		authority := newAuthority(t, st, sandbox)
		if reply, err := authority.AdmitAndDispatch(ctx, command); err != nil || reply.Outcome != clusterstate.DispatchAcceptedQueued {
			t.Fatalf("queued dispatch = %+v, %v", reply, err)
		}
		sandbox.promoted = []nodectl.PreparedAdmissionResult{{
			SandboxID: command.ObjectID, DemandDigest: command.Intent.DemandDigest,
			State: nodectl.PreparedAdmitted, ReservationToken: "token-published",
		}}
		sandbox.promoteErr = published
		if err := authority.PromoteSandboxQueue(ctx); !nodectl.FlushPublished(err) {
			t.Fatalf("published promotion error = %v", err)
		}
		record, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, command.ObjectID)
		if err != nil || record == nil || record.AdmissionState != nodeexec.AdmissionAdmitted || !record.ResourceClaimed {
			t.Fatalf("journal after published promotion = %+v, %v", record, err)
		}
	})
}

func TestAuthorityTerminalizesLaunchingSandboxAfterReservationSweep(t *testing.T) {
	ctx := context.Background()
	st := authorityStore(t)
	command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-swept-launch")
	sandbox := &sandboxAdmissionFake{
		prepared: map[string]nodectl.PreparedAdmissionResult{
			command.ObjectID: {
				SandboxID: command.ObjectID, DemandDigest: command.Intent.DemandDigest,
				State: nodectl.PreparedAdmitted, ReservationToken: "token-swept",
			},
		},
		wake: make(chan struct{}),
	}
	authority := newAuthority(t, st, sandbox)
	if _, err := authority.AdmitAndDispatch(ctx, command); err != nil {
		t.Fatal(err)
	}
	record, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, command.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if record, err = authority.ClaimSandbox(ctx, record); err != nil || record.AdmissionState != nodeexec.AdmissionLaunching {
		t.Fatalf("claimed workflow = %+v, %v", record, err)
	}
	prepared := sandbox.prepared[command.ObjectID]
	prepared.State = nodectl.PreparedReleased
	prepared.Reason = "reservation_idle_timeout"
	sandbox.prepared[command.ObjectID] = prepared
	if err := authority.ReconcileSandboxAdmissions(ctx, 10); err != nil {
		t.Fatal(err)
	}
	record, err = st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, command.ObjectID)
	if err != nil || record == nil || record.AdmissionState != nodeexec.AdmissionTerminal || record.ResourceClaimed ||
		record.LatestEvent == nil || record.LatestEvent.State != "ERROR" || record.LatestEvent.Reason != prepared.Reason {
		t.Fatalf("terminal workflow after sweep = %+v, %v", record, err)
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
		record.ObjectID, record.BindingDigest); !errors.Is(err, store.ErrNodeWorkflowState) {
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
		record.ObjectID, record.BindingDigest); !errors.Is(err, nodeexec.ErrFinalOutboxPending) {
		t.Fatalf("finalize before outbox ACK error = %v", err)
	}
	if len(sandbox.finalized) != 1 || !strings.HasPrefix(sandbox.finalized[0], "sandbox-reconcile:") {
		t.Fatalf("controller finalization = %+v", sandbox.finalized)
	}
	record, err = st.GetNodeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox, "sandbox-reconcile")
	if err != nil || record == nil || !record.WorkflowFinalized {
		t.Fatalf("journal finalization = %+v, %v", record, err)
	}
	if err := st.AckExecutionEvent(context.Background(), "node-1", 7, routesync.EventAck{
		ObjectKind: "sandbox", ObjectID: record.ObjectID,
		RegistryGeneration: record.LatestEvent.RegistryGeneration,
		BindingDigest:      record.BindingDigest, EventSeq: record.EventSeq,
	}); err != nil {
		t.Fatal(err)
	}
	if err := authority.FinalizeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox,
		record.ObjectID, record.BindingDigest); err != nil {
		t.Fatalf("finalize after durable outbox ACK = %v", err)
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

func TestFinalizeRemovesAdmissionBeforePersistingCompactionMarker(t *testing.T) {
	st := authorityStore(t)
	command := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-finalize-order")
	dispatch, err := nodeexec.DispatchRecordFromCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	record, err := st.RecordSandboxWorkflow(context.Background(), dispatch, nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionRejected, Result: clusterstate.DispatchDefinitiveReject, Reason: "rejected",
	}, &types.Sandbox{
		ID: dispatch.ObjectID, TemplateID: "bare-img-" + strings.Repeat("5", 64), State: types.StateStarting,
		AuthKey: strings.Repeat("3", 64), ManifestKey: strings.Repeat("4", 64), CreatedUnix: time.Now().Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	sandbox := &sandboxAdmissionFake{prepared: map[string]nodectl.PreparedAdmissionResult{
		record.ObjectID: {
			SandboxID: record.ObjectID, DemandDigest: record.DemandDigest, State: nodectl.PreparedReleased,
		},
	}, wake: make(chan struct{})}
	injected := errors.New("injected workflow finalization failure")
	journal := &failingFinalizeJournal{Store: st, err: injected}
	authority := newAuthority(t, journal, sandbox)
	if err := authority.FinalizeWorkflow(
		context.Background(), record.Kind, record.ObjectID, record.BindingDigest,
	); !errors.Is(err, injected) {
		t.Fatalf("first finalization error = %v", err)
	}
	if _, found := sandbox.prepared[record.ObjectID]; found {
		t.Fatal("Admission survived journal finalization failure")
	}
	stored, err := st.GetNodeWorkflow(context.Background(), record.Kind, record.ObjectID)
	if err != nil || stored == nil || stored.WorkflowFinalized {
		t.Fatalf("workflow after injected failure = %+v, %v", stored, err)
	}
	journal.err = nil
	if err := authority.FinalizeWorkflow(
		context.Background(), record.Kind, record.ObjectID, record.BindingDigest,
	); err != nil {
		t.Fatal(err)
	}
	stored, err = st.GetNodeWorkflow(context.Background(), record.Kind, record.ObjectID)
	if err != nil || stored == nil || !stored.WorkflowFinalized {
		t.Fatalf("workflow after retry = %+v, %v", stored, err)
	}
}

func TestDispatchCommandFromWireUsesBusinessIDAndExactIntent(t *testing.T) {
	want := authorityCommand(t, clusterstate.ExecutionKindSandbox, "sandbox-wire")
	wire := &routesync.Command{
		Kind: routesync.CmdSandboxAdmitDispatch, SID: want.ObjectID,
		NodeEpoch: want.NodeEpoch, SessionSeq: want.SessionSeq,
		RegistryGeneration: want.Binding.RegistryGeneration,
		Binding:            want.Binding.OpaqueBinding, BindingDigest: want.Binding.BindingDigest,
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
		got.Intent.ProviderPolicyVersion != want.Intent.ProviderPolicyVersion ||
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
		AuthKey: strings.Repeat("7", 64), ManifestKey: strings.Repeat("6", 64), State: types.StateRunning,
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
