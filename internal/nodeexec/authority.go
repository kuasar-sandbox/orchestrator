package nodeexec

import (
	"context"
	"errors"
	"fmt"
	"sync"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type IdentitySource func(context.Context) (LocalSessionIdentity, error)

type BuildCapacitySource func(context.Context) (BuildCapacity, string, error)

type SandboxDemandSource func(context.Context, DispatchRecord) (nodectl.SandboxAdmissionDemand, error)

// SandboxObjectSource resolves the exact group/AuthKey/ManifestKey lease named
// by the immutable dispatch and copies that key material into a persistable
// node-local object before resource Admission can be accepted.
type SandboxObjectSource func(context.Context, DispatchRecord) (*types.Sandbox, error)

type BuildObjectSource func(context.Context, DispatchRecord) (*types.Build, error)

type SandboxAdmissionController interface {
	GetAdmission(string, string) (nodectl.PreparedAdmissionResult, error)
	PrepareAdmission(string, string, nodectl.SandboxAdmissionDemand) (nodectl.PreparedAdmissionResult, error)
	ClaimAdmission(string, string) (nodectl.PreparedAdmissionResult, error)
	ReleaseAdmission(string, string, string) (nodectl.PreparedAdmissionResult, error)
	FinalizeAdmission(string, string) error
	PromoteQueued() ([]nodectl.PreparedAdmissionResult, error)
	Wake() <-chan struct{}
}

// SandboxAdmissionOrphanReconciler is implemented only when the Admission
// controller and workflow journal share a crash-consistent local store.
type SandboxAdmissionOrphanReconciler interface {
	ReconcileOrphanSandboxAdmissions(context.Context) error
}

// WorkflowJournal is implemented by the node-local SQLite store. Its methods
// are deliberately keyed by the business sandbox/build IDs from the RFC.
type WorkflowJournal interface {
	GetNodeWorkflow(context.Context, clusterstate.ExecutionKind, string) (*WorkflowRecord, error)
	ExecutionObjectExists(context.Context, clusterstate.ExecutionKind, string) (bool, error)
	RecordSandboxWorkflow(context.Context, DispatchRecord, AdmissionDecision, *types.Sandbox) (*WorkflowRecord, error)
	AdmitQueuedSandbox(context.Context, string, string, string) (*WorkflowRecord, error)
	ClaimSandboxWorkflow(context.Context, string, string, string) (*WorkflowRecord, error)
	PrepareBuildWorkflow(context.Context, DispatchRecord, *types.Build, BuildCapacity, string) (*WorkflowRecord, error)
	PromoteBuildQueue(context.Context, string, uint64, BuildCapacity, int) ([]*WorkflowRecord, error)
	FailQueuedBuildWorkflows(context.Context, string, uint64, string, int) ([]*WorkflowRecord, error)
	ClaimBuildWorkflow(context.Context, string, string) (*WorkflowRecord, error)
	LaunchableNodeWorkflows(context.Context, clusterstate.ExecutionKind, string, uint64, string, int) ([]*WorkflowRecord, error)
	SandboxWorkflowsForReconcile(context.Context, string, uint64, string, int) ([]*WorkflowRecord, error)
	ReleaseSandboxWorkflow(context.Context, string, string, string) (*WorkflowRecord, error)
	FailPendingNodeWorkflow(context.Context, clusterstate.ExecutionKind, string, string, string) (*WorkflowRecord, error)
	FinalizeNodeWorkflow(context.Context, clusterstate.ExecutionKind, string, string) error
}

// Authority is the node-side Admission and command-dedupe endpoint. A separate
// instance belongs to one node-link SessionSeq.
type Authority struct {
	journal        WorkflowJournal
	sandbox        SandboxAdmissionController
	identity       IdentitySource
	buildCapacity  BuildCapacitySource
	buildObject    BuildObjectSource
	sandboxObject  SandboxObjectSource
	sandboxDemand  SandboxDemandSource
	workWake       chan struct{}
	sessionMu      sync.RWMutex
	sessionFenced  bool
	buildBatchSize int
}

func NewAuthority(
	journal WorkflowJournal,
	sandbox SandboxAdmissionController,
	identity IdentitySource,
	buildCapacity BuildCapacitySource,
	buildObject BuildObjectSource,
	sandboxObject SandboxObjectSource,
	sandboxDemand SandboxDemandSource,
) (*Authority, error) {
	if journal == nil || sandbox == nil || identity == nil || buildCapacity == nil || buildObject == nil ||
		sandboxObject == nil || sandboxDemand == nil {
		return nil, errors.New("nodeexec: authority requires journal, Admission, identity, capacity, and demand sources")
	}
	return &Authority{
		journal: journal, sandbox: sandbox, identity: identity,
		buildCapacity: buildCapacity, buildObject: buildObject,
		sandboxObject: sandboxObject, sandboxDemand: sandboxDemand,
		workWake: make(chan struct{}, 1), buildBatchSize: 64,
	}, nil
}

// FenceStaleSession is a mutation barrier. Once it returns, every operation
// admitted by this SessionSeq has completed and no later operation can start.
func (a *Authority) FenceStaleSession() {
	a.sessionMu.Lock()
	a.sessionFenced = true
	a.sessionMu.Unlock()
}

func (a *Authority) beginSessionWork() (func(), bool) {
	a.sessionMu.RLock()
	if a.sessionFenced {
		a.sessionMu.RUnlock()
		return nil, false
	}
	return a.sessionMu.RUnlock, true
}

func (a *Authority) WorkWake() <-chan struct{} { return a.workWake }

func (a *Authority) SandboxAdmissionWake() <-chan struct{} { return a.sandbox.Wake() }

func (a *Authority) notifyWork() {
	select {
	case a.workWake <- struct{}{}:
	default:
	}
}

func (a *Authority) AdmitAndDispatch(
	ctx context.Context,
	command session.DispatchCommand,
) (session.DispatchReply, error) {
	if err := ctx.Err(); err != nil {
		return session.DispatchReply{}, err
	}
	done, active := a.beginSessionWork()
	if !active {
		return session.DispatchReply{Outcome: clusterstate.DispatchSessionMoved, Reason: "node-link session is fenced"}, nil
	}
	defer done()
	identity, err := a.identity(ctx)
	if err != nil {
		return session.DispatchReply{}, err
	}
	if err := identity.Validate(); err != nil {
		return session.DispatchReply{}, err
	}
	if identity.NodeID != command.NodeID || identity.NodeEpoch != command.NodeEpoch ||
		identity.SessionSeq != command.SessionSeq || identity.DataEndpoint != command.DataEndpoint {
		return session.DispatchReply{Outcome: clusterstate.DispatchSessionMoved, Reason: "node-link session tuple changed"}, nil
	}
	dispatch, err := DispatchRecordFromCommand(command)
	if err != nil {
		return session.DispatchReply{Outcome: clusterstate.DispatchWrongBinding, Reason: err.Error()}, nil
	}
	existing, err := a.journal.GetNodeWorkflow(ctx, dispatch.Kind, dispatch.ObjectID)
	if err != nil {
		return session.DispatchReply{}, err
	}
	if existing != nil {
		if !existing.DispatchRecord.SameDispatch(dispatch) {
			return session.DispatchReply{Outcome: clusterstate.DispatchConflict, Reason: "object ID is bound to another dispatch"}, nil
		}
		return session.DispatchReply{Outcome: existing.Result, Reason: existing.Reason}, nil
	}
	occupied, err := a.journal.ExecutionObjectExists(ctx, dispatch.Kind, dispatch.ObjectID)
	if err != nil {
		return session.DispatchReply{}, err
	}
	if occupied {
		if dispatch.Kind == clusterstate.ExecutionKindSandbox {
			prepared, getErr := a.sandbox.GetAdmission(dispatch.ObjectID, dispatch.DemandDigest)
			switch {
			case getErr == nil && prepared.State != nodectl.PreparedRejected && prepared.State != nodectl.PreparedReleased:
				if _, releaseErr := a.sandbox.ReleaseAdmission(dispatch.ObjectID, dispatch.DemandDigest, "object_id_conflict"); releaseErr != nil {
					return session.DispatchReply{}, releaseErr
				}
			case errors.Is(getErr, nodectl.ErrPreparedAdmissionConflict):
				return session.DispatchReply{Outcome: clusterstate.DispatchConflict, Reason: getErr.Error()}, nil
			case getErr != nil && !errors.Is(getErr, nodectl.ErrPreparedAdmissionMissing):
				return session.DispatchReply{}, getErr
			}
		}
		return session.DispatchReply{Outcome: clusterstate.DispatchConflict, Reason: "object ID already exists on node"}, nil
	}

	var record *WorkflowRecord
	switch dispatch.Kind {
	case clusterstate.ExecutionKindSandbox:
		sandboxObject, err := a.sandboxObject(ctx, dispatch)
		if err != nil {
			return session.DispatchReply{}, err
		}
		demand, err := a.sandboxDemand(ctx, dispatch)
		if err != nil {
			return session.DispatchReply{}, err
		}
		prepared, prepareErr := a.sandbox.PrepareAdmission(dispatch.ObjectID, dispatch.DemandDigest, demand)
		if prepareErr != nil && !nodectl.FlushPublished(prepareErr) {
			if errors.Is(prepareErr, nodectl.ErrPreparedAdmissionConflict) {
				return session.DispatchReply{Outcome: clusterstate.DispatchConflict, Reason: prepareErr.Error()}, nil
			}
			return session.DispatchReply{}, prepareErr
		}
		decision, err := sandboxDecision(prepared)
		if err != nil {
			return session.DispatchReply{}, err
		}
		record, err = a.journal.RecordSandboxWorkflow(ctx, dispatch, decision, sandboxObject)
		if err != nil {
			if errors.Is(err, ErrWorkflowConflict) {
				if decision.ReservationToken != "" {
					owned, ownerErr := a.workflowOwnsSandboxReservation(ctx, dispatch, decision.ReservationToken)
					if ownerErr != nil {
						return session.DispatchReply{}, errors.Join(prepareErr, err, ownerErr)
					}
					if !owned {
						if _, releaseErr := a.sandbox.ReleaseAdmission(dispatch.ObjectID, dispatch.DemandDigest, "object_id_conflict"); releaseErr != nil {
							return session.DispatchReply{}, errors.Join(prepareErr, err, releaseErr)
						}
					}
				}
				if prepareErr != nil {
					return session.DispatchReply{}, errors.Join(prepareErr, err)
				}
				return session.DispatchReply{Outcome: clusterstate.DispatchConflict, Reason: err.Error()}, nil
			}
			return session.DispatchReply{}, errors.Join(prepareErr, err)
		}
		if prepareErr != nil {
			// The state-file rename published this Admission, so the journal must
			// describe it before the durability error is returned without an ACK.
			if record.AdmissionState == AdmissionAdmitted {
				a.notifyWork()
			}
			return session.DispatchReply{}, prepareErr
		}
	case clusterstate.ExecutionKindBuild:
		build, err := a.buildObject(ctx, dispatch)
		if err != nil {
			return session.DispatchReply{}, err
		}
		capacity, safetyReason, err := a.buildCapacity(ctx)
		if err != nil {
			return session.DispatchReply{}, err
		}
		record, err = a.journal.PrepareBuildWorkflow(ctx, dispatch, build, capacity, safetyReason)
		if err != nil {
			if errors.Is(err, ErrWorkflowConflict) {
				return session.DispatchReply{Outcome: clusterstate.DispatchConflict, Reason: err.Error()}, nil
			}
			return session.DispatchReply{}, err
		}
	default:
		return session.DispatchReply{}, errors.New("nodeexec: unsupported execution kind")
	}
	if record.AdmissionState == AdmissionAdmitted {
		a.notifyWork()
	}
	return session.DispatchReply{Outcome: record.Result, Reason: record.Reason}, nil
}

func (a *Authority) workflowOwnsSandboxReservation(
	ctx context.Context,
	dispatch DispatchRecord,
	reservationToken string,
) (bool, error) {
	winner, err := a.journal.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, dispatch.ObjectID)
	if err != nil {
		return false, err
	}
	return winner != nil && winner.DemandDigest == dispatch.DemandDigest &&
		winner.ReservationToken == reservationToken, nil
}

func sandboxDecision(prepared nodectl.PreparedAdmissionResult) (AdmissionDecision, error) {
	decision := AdmissionDecision{ReservationToken: prepared.ReservationToken, Reason: prepared.Reason}
	switch prepared.State {
	case nodectl.PreparedAdmitted, nodectl.PreparedClaimed:
		decision.State = AdmissionAdmitted
		decision.Result = clusterstate.DispatchAcceptedAdmitted
		decision.Reason = ""
	case nodectl.PreparedQueued:
		decision.State = AdmissionQueued
		decision.Result = clusterstate.DispatchAcceptedQueued
		decision.Reason = ""
	case nodectl.PreparedRejected, nodectl.PreparedReleased:
		decision.State = AdmissionRejected
		decision.Result = clusterstate.DispatchDefinitiveReject
		decision.ReservationToken = ""
		if decision.Reason == "" {
			decision.Reason = "resource_admission_rejected"
		}
	default:
		return AdmissionDecision{}, fmt.Errorf("nodeexec: unknown Sandbox Admission state %q", prepared.State)
	}
	if err := decision.Validate(); err != nil {
		return AdmissionDecision{}, err
	}
	return decision, nil
}

// PromoteSandboxQueue reconciles durable resource-controller queue changes into
// the command journal. An expired accepted queue entry becomes an ERROR fact,
// never a second placement opportunity.
func (a *Authority) PromoteSandboxQueue(ctx context.Context) error {
	done, active := a.beginSessionWork()
	if !active {
		return ErrSessionFenced
	}
	defer done()
	changed, promoteErr := a.sandbox.PromoteQueued()
	joined := promoteErr
	for _, result := range changed {
		record, getErr := a.journal.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, result.SandboxID)
		if getErr != nil {
			joined = errors.Join(joined, getErr)
			continue
		}
		if record == nil || record.DemandDigest != result.DemandDigest {
			joined = errors.Join(joined, fmt.Errorf("nodeexec: queued Sandbox %s has no matching journal", result.SandboxID))
			continue
		}
		switch result.State {
		case nodectl.PreparedAdmitted:
			if _, err := a.journal.AdmitQueuedSandbox(ctx, result.SandboxID, result.DemandDigest, result.ReservationToken); err != nil {
				joined = errors.Join(joined, err)
				continue
			}
			a.notifyWork()
		case nodectl.PreparedRejected:
			reason := result.Reason
			if reason == "" {
				reason = "resource_admission_rejected"
			}
			if _, err := a.journal.FailPendingNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox,
				result.SandboxID, result.DemandDigest, reason); err != nil {
				joined = errors.Join(joined, err)
			}
		}
	}
	return joined
}

// ReconcileSandboxAdmissions repairs every cross-file crash boundary without
// creating a new Admission record. Missing controller state for active work is
// an error and must fail cluster startup for the current NodeEpoch.
func (a *Authority) ReconcileSandboxAdmissions(ctx context.Context, limit int) error {
	done, active := a.beginSessionWork()
	if !active {
		return ErrSessionFenced
	}
	defer done()
	if reconciler, ok := a.sandbox.(SandboxAdmissionOrphanReconciler); ok {
		if err := reconciler.ReconcileOrphanSandboxAdmissions(ctx); err != nil {
			return fmt.Errorf("nodeexec: reconcile orphan Sandbox Admissions: %w", err)
		}
	}
	identity, err := a.identity(ctx)
	if err != nil {
		return err
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if limit <= 0 {
		limit = 256
	}
	var joined error
	afterObjectID := ""
	for {
		records, err := a.journal.SandboxWorkflowsForReconcile(
			ctx, identity.NodeID, identity.NodeEpoch, afterObjectID, limit)
		if err != nil {
			return errors.Join(joined, err)
		}
		for _, record := range records {
			if record.AdmissionState == AdmissionTerminal && !record.ResourceClaimed {
				continue
			}
			prepared, getErr := a.sandbox.GetAdmission(record.ObjectID, record.DemandDigest)
			if getErr != nil {
				joined = errors.Join(joined, fmt.Errorf("nodeexec: reconcile Sandbox %s: %w", record.ObjectID, getErr))
				continue
			}
			switch record.AdmissionState {
			case AdmissionQueued:
				switch prepared.State {
				case nodectl.PreparedQueued:
				case nodectl.PreparedAdmitted:
					if _, err := a.journal.AdmitQueuedSandbox(ctx, record.ObjectID, record.DemandDigest, prepared.ReservationToken); err != nil {
						joined = errors.Join(joined, err)
					} else {
						a.notifyWork()
					}
				case nodectl.PreparedRejected, nodectl.PreparedReleased:
					joined = errors.Join(joined, a.reconcileSandboxFailure(ctx, record, prepared))
				default:
					joined = errors.Join(joined, fmt.Errorf("nodeexec: queued Sandbox %s has controller state %s", record.ObjectID, prepared.State))
				}
			case AdmissionAdmitted:
				switch prepared.State {
				case nodectl.PreparedAdmitted:
					a.notifyWork()
				case nodectl.PreparedClaimed:
					if _, err := a.journal.ClaimSandboxWorkflow(ctx, record.ObjectID, record.DemandDigest, record.ReservationToken); err != nil {
						joined = errors.Join(joined, err)
					} else {
						a.notifyWork()
					}
				case nodectl.PreparedRejected, nodectl.PreparedReleased:
					joined = errors.Join(joined, a.reconcileSandboxFailure(ctx, record, prepared))
				default:
					joined = errors.Join(joined, fmt.Errorf("nodeexec: admitted Sandbox %s has controller state %s", record.ObjectID, prepared.State))
				}
			case AdmissionLaunching:
				switch prepared.State {
				case nodectl.PreparedClaimed:
					a.notifyWork()
				case nodectl.PreparedRejected, nodectl.PreparedReleased:
					joined = errors.Join(joined, a.reconcileSandboxFailure(ctx, record, prepared))
				default:
					joined = errors.Join(joined, fmt.Errorf("nodeexec: launching Sandbox %s has controller state %s", record.ObjectID, prepared.State))
				}
			case AdmissionRunning:
				if prepared.State != nodectl.PreparedClaimed {
					joined = errors.Join(joined, fmt.Errorf("nodeexec: running Sandbox %s has controller state %s", record.ObjectID, prepared.State))
				}
			case AdmissionTerminal:
				if record.ResourceClaimed {
					if prepared.State != nodectl.PreparedReleased {
						reason := record.Reason
						if record.LatestEvent != nil && record.LatestEvent.Reason != "" {
							reason = record.LatestEvent.Reason
						}
						if _, err := a.sandbox.ReleaseAdmission(record.ObjectID, record.DemandDigest, reason); err != nil {
							joined = errors.Join(joined, err)
							continue
						}
					}
					if _, err := a.journal.ReleaseSandboxWorkflow(ctx, record.ObjectID, record.DemandDigest, record.ReservationToken); err != nil {
						joined = errors.Join(joined, err)
					}
				}
			}
		}
		if len(records) < limit {
			break
		}
		afterObjectID = records[len(records)-1].ObjectID
	}
	return joined
}

func (a *Authority) reconcileSandboxFailure(
	ctx context.Context,
	record *WorkflowRecord,
	prepared nodectl.PreparedAdmissionResult,
) error {
	reason := prepared.Reason
	if reason == "" {
		reason = "resource_admission_released"
	}
	failed, err := a.journal.FailPendingNodeWorkflow(ctx, record.Kind, record.ObjectID, record.DemandDigest, reason)
	if err != nil {
		return err
	}
	if failed.ResourceClaimed {
		_, err = a.journal.ReleaseSandboxWorkflow(ctx, failed.ObjectID, failed.DemandDigest, failed.ReservationToken)
	}
	return err
}

func (a *Authority) PromoteBuildQueue(ctx context.Context) ([]*WorkflowRecord, error) {
	done, active := a.beginSessionWork()
	if !active {
		return nil, ErrSessionFenced
	}
	defer done()
	identity, err := a.identity(ctx)
	if err != nil {
		return nil, err
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	capacity, safetyReason, err := a.buildCapacity(ctx)
	if err != nil {
		return nil, err
	}
	if safetyReason != "" {
		return a.journal.FailQueuedBuildWorkflows(
			ctx, identity.NodeID, identity.NodeEpoch, safetyReason, a.buildBatchSize)
	}
	promoted, err := a.journal.PromoteBuildQueue(
		ctx, identity.NodeID, identity.NodeEpoch, capacity, a.buildBatchSize)
	if err == nil && len(promoted) > 0 {
		a.notifyWork()
	}
	return promoted, err
}

func (a *Authority) Launchable(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	afterObjectID string,
	limit int,
) ([]*WorkflowRecord, error) {
	done, active := a.beginSessionWork()
	if !active {
		return nil, ErrSessionFenced
	}
	defer done()
	identity, err := a.identity(ctx)
	if err != nil {
		return nil, err
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	return a.journal.LaunchableNodeWorkflows(ctx, kind, identity.NodeID, identity.NodeEpoch, afterObjectID, limit)
}

func (a *Authority) ClaimSandbox(ctx context.Context, record *WorkflowRecord) (*WorkflowRecord, error) {
	done, active := a.beginSessionWork()
	if !active {
		return nil, ErrSessionFenced
	}
	defer done()
	if record == nil || record.Kind != clusterstate.ExecutionKindSandbox {
		return nil, errors.New("nodeexec: Sandbox claim requires a Sandbox workflow")
	}
	claimed, err := a.sandbox.ClaimAdmission(record.ObjectID, record.DemandDigest)
	if err != nil && !nodectl.FlushPublished(err) {
		return nil, err
	}
	if claimed.State != nodectl.PreparedClaimed || claimed.ReservationToken != record.ReservationToken {
		return nil, errors.New("nodeexec: resource controller did not claim the journaled reservation")
	}
	journaled, journalErr := a.journal.ClaimSandboxWorkflow(
		ctx, record.ObjectID, record.DemandDigest, record.ReservationToken,
	)
	if journalErr != nil {
		return nil, errors.Join(err, journalErr)
	}
	return journaled, err
}

func (a *Authority) ClaimBuild(ctx context.Context, record *WorkflowRecord) (*WorkflowRecord, error) {
	done, active := a.beginSessionWork()
	if !active {
		return nil, ErrSessionFenced
	}
	defer done()
	if record == nil || record.Kind != clusterstate.ExecutionKindBuild {
		return nil, errors.New("nodeexec: Build claim requires a Build workflow")
	}
	return a.journal.ClaimBuildWorkflow(ctx, record.ObjectID, record.DemandDigest)
}

func (a *Authority) FailSandbox(ctx context.Context, record *WorkflowRecord, reason string) error {
	done, active := a.beginSessionWork()
	if !active {
		return ErrSessionFenced
	}
	defer done()
	if record == nil || record.Kind != clusterstate.ExecutionKindSandbox || reason == "" {
		return errors.New("nodeexec: Sandbox failure requires a workflow and reason")
	}
	if _, err := a.journal.FailPendingNodeWorkflow(ctx, record.Kind, record.ObjectID, record.DemandDigest, reason); err != nil {
		return err
	}
	if _, err := a.sandbox.ReleaseAdmission(record.ObjectID, record.DemandDigest, reason); err != nil {
		return err
	}
	_, err := a.journal.ReleaseSandboxWorkflow(ctx, record.ObjectID, record.DemandDigest, record.ReservationToken)
	return err
}

// ReleaseSandboxResources completes the local terminal resource handoff after
// the object state and terminal outbox event have been committed atomically.
func (a *Authority) ReleaseSandboxResources(ctx context.Context, record *WorkflowRecord, reason string) error {
	done, active := a.beginSessionWork()
	if !active {
		return ErrSessionFenced
	}
	defer done()
	if record == nil || record.Kind != clusterstate.ExecutionKindSandbox {
		return errors.New("nodeexec: Sandbox resource release requires a workflow")
	}
	if _, err := a.sandbox.ReleaseAdmission(record.ObjectID, record.DemandDigest, reason); err != nil {
		return err
	}
	_, err := a.journal.ReleaseSandboxWorkflow(ctx, record.ObjectID, record.DemandDigest, record.ReservationToken)
	return err
}

func (a *Authority) FinalizeWorkflow(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID, bindingDigest string,
) error {
	done, active := a.beginSessionWork()
	if !active {
		return ErrSessionFenced
	}
	defer done()
	if objectID == "" || bindingDigest == "" {
		return errors.New("nodeexec: finalization requires object identity and Binding digest")
	}
	record, err := a.journal.GetNodeWorkflow(ctx, kind, objectID)
	if err != nil {
		return err
	}
	if record == nil {
		return a.journal.FinalizeNodeWorkflow(ctx, kind, objectID, bindingDigest)
	}
	if record.BindingDigest != bindingDigest {
		return ErrWorkflowConflict
	}
	if kind == clusterstate.ExecutionKindSandbox {
		if record.AdmissionState != AdmissionRejected && record.AdmissionState != AdmissionTerminal || record.ResourceClaimed {
			return ErrWorkflowState
		}
		if err := a.sandbox.FinalizeAdmission(objectID, record.DemandDigest); err != nil {
			return err
		}
	}
	return a.journal.FinalizeNodeWorkflow(ctx, kind, objectID, bindingDigest)
}
