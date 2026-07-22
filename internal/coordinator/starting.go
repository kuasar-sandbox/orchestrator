package coordinator

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

const DefaultPlacementRoundLimit uint64 = 2

type RouteCommitter interface {
	CommitRouteWorkflow(context.Context, cluster.Revision, cluster.RouteWorkflowRecord) (cluster.RouteWorkflowRecord, error)
	EnsureExecutionFence(context.Context, cluster.ExecutionFence) error
}

type BuildCommitter interface {
	CommitBuildWorkflow(context.Context, cluster.Revision, cluster.BuildRecord) (cluster.BuildRecord, error)
}

type PairProber interface {
	ProbePair(context.Context, session.ServeIdentity, []placement.PlacementProbeRequest) []session.ProbeResult
}

type Dispatcher interface {
	AdmitAndDispatch(context.Context, session.DispatchCommand) (session.DispatchReply, error)
}

type SandboxRoundSource interface {
	NextSandboxRound(context.Context, string, string, uint64, cluster.DispatchIntent, []string) (SandboxRound, error)
}

type SandboxRound struct {
	SandboxID  string
	Candidates []cluster.PlacementCandidate
	Intent     cluster.DispatchIntent
}

type TieBreaker interface {
	ChooseSecond() (bool, error)
}

type Config struct {
	ServeIdentity       session.ServeIdentity
	PlacementRoundLimit uint64
	Clock               func() time.Time
	TieBreaker          TieBreaker
}

type StartingCoordinator struct {
	identity   session.ServeIdentity
	roundLimit uint64
	clock      func() time.Time
	tie        TieBreaker
	prober     PairProber
	dispatcher Dispatcher
	routes     RouteCommitter
	builds     BuildCommitter
	rounds     SandboxRoundSource
}

func NewStartingCoordinator(config Config, prober PairProber, dispatcher Dispatcher, routes RouteCommitter, builds BuildCommitter, rounds SandboxRoundSource) (*StartingCoordinator, error) {
	if err := config.ServeIdentity.Validate(); err != nil {
		return nil, errors.New("coordinator: complete serving identity is required")
	}
	if prober == nil || dispatcher == nil {
		return nil, errors.New("coordinator: Probe and dispatch clients are required")
	}
	if routes == nil && builds == nil {
		return nil, errors.New("coordinator: at least one workflow committer is required")
	}
	if config.PlacementRoundLimit == 0 {
		config.PlacementRoundLimit = DefaultPlacementRoundLimit
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.TieBreaker == nil {
		config.TieBreaker = cryptoTieBreaker{}
	}
	return &StartingCoordinator{
		identity: config.ServeIdentity, roundLimit: config.PlacementRoundLimit,
		clock: config.Clock, tie: config.TieBreaker, prober: prober, dispatcher: dispatcher,
		routes: routes, builds: builds, rounds: rounds,
	}, nil
}

type RunStatus string

const (
	RunWaitingForEvent RunStatus = "WAITING_FOR_EVENT"
	RunPinnedUnknown   RunStatus = "PINNED_UNKNOWN"
	RunRetrySelected   RunStatus = "RETRY_SELECTED"
	RunNoUsableProbe   RunStatus = "NO_USABLE_PROBE"
	RunConflict        RunStatus = "CONFLICT"
	RunComplete        RunStatus = "COMPLETE"
	RunTerminal        RunStatus = "TERMINAL"
)

type RunResult struct {
	Status  RunStatus
	Outcome cluster.DispatchOutcome
	Reason  string
	Route   *cluster.RouteWorkflowRecord
	Build   *cluster.BuildRecord
}

type cachedProbe struct {
	result     session.ProbeResult
	observedAt time.Time
}

func sandboxPlacementInputs(intent cluster.DispatchIntent) (placement.NormalizedDemand, string, error) {
	demand, err := placement.ParseNormalizedDemand(intent.NormalizedDemand)
	if err != nil {
		return placement.NormalizedDemand{}, "", err
	}
	if demand.Kind != placement.ObjectSandbox {
		return placement.NormalizedDemand{}, "", errors.New("coordinator: Route carries a non-Sandbox demand")
	}
	spec, err := cluster.ParseSandboxDispatchSpec(intent.DispatchSpec)
	if err != nil {
		return placement.NormalizedDemand{}, "", err
	}
	return demand, spec.TargetRuntimeDigest, nil
}

func buildPlacementInputs(intent cluster.DispatchIntent) (placement.NormalizedDemand, string, error) {
	demand, err := placement.ParseNormalizedDemand(intent.NormalizedDemand)
	if err != nil {
		return placement.NormalizedDemand{}, "", err
	}
	if demand.Kind != placement.ObjectBuild {
		return placement.NormalizedDemand{}, "", errors.New("coordinator: Build carries a non-Build demand")
	}
	spec, err := cluster.ParseBuildDispatchSpec(intent.DispatchSpec)
	if err != nil {
		return placement.NormalizedDemand{}, "", err
	}
	maximum := ^uint64(0)
	if uint64(spec.CPUCount) > maximum/1000 || uint64(spec.MemoryMB) > maximum/(1<<20) ||
		demand.Build.Slots != 1 || demand.Build.CPU != uint64(spec.CPUCount)*1000 ||
		demand.Build.Memory != uint64(spec.MemoryMB)*(1<<20) {
		return placement.NormalizedDemand{}, "", errors.New("coordinator: Build demand does not match immutable CPU/memory ceilings")
	}
	return demand, spec.TargetRuntimeDigest, nil
}

func (c *StartingCoordinator) RunRoute(ctx context.Context, record cluster.RouteWorkflowRecord) (RunResult, error) {
	if c.routes == nil {
		return RunResult{}, errors.New("coordinator: Route committer is unavailable")
	}
	if err := record.Validate(); err != nil {
		return RunResult{}, err
	}
	if record.State != cluster.WorkflowRouteStarting || record.Starting == nil {
		return RunResult{}, errors.New("coordinator: Route is not STARTING")
	}
	demand, runtimeDigest, err := sandboxPlacementInputs(record.Starting.Intent)
	if err != nil {
		return RunResult{}, err
	}
	cache := make(map[uint32]cachedProbe)
	for {
		starting := record.Starting
		if starting.SelectedCandidate != nil {
			result := c.dispatch(ctx, cluster.ExecutionKindSandbox, record.Group, record.RouteKey, starting.SandboxID, starting.Intent, *starting.Binding)
			if result.Outcome != cluster.DispatchDefinitiveReject {
				result.Route = &record
				return result, nil
			}
			finalization, finalizationErr := cluster.NewWorkflowFinalizationIntent(
				starting.SandboxID, *starting.Binding, nil,
			)
			if finalizationErr != nil {
				return RunResult{}, finalizationErr
			}
			next := record
			next.Finalizations = append(
				append([]cluster.WorkflowFinalizationIntent(nil), record.Finalizations...), finalization,
			)
			next.Starting = cloneRouteStarting(starting)
			index := *starting.SelectedCandidate
			next.Starting.SelectedCandidate = nil
			next.Starting.Binding = nil
			next.Starting.DefinitivelyRejected = appendRejected(next.Starting.DefinitivelyRejected, index)
			record, err = c.commitRoute(ctx, record.Revision, next)
			if err != nil {
				return RunResult{}, err
			}
			continue
		}
		if allRejected(len(starting.CandidatePool), starting.DefinitivelyRejected) {
			next := record
			next.State = cluster.WorkflowRouteTombstone
			next.Starting = nil
			next.Tombstone = &cluster.RouteTombstoneState{PlacementFailure: &cluster.RoutePlacementFailureState{
				SandboxID: starting.SandboxID, PlacementRound: starting.PlacementRound,
				CandidatePool:        append([]cluster.PlacementCandidate(nil), starting.CandidatePool...),
				DefinitivelyRejected: append([]uint32(nil), starting.DefinitivelyRejected...),
				Intent:               cloneIntent(starting.Intent), Reason: "placement candidate pool exhausted",
			}}
			record, err = c.commitRoute(ctx, record.Revision, next)
			if err != nil {
				return RunResult{}, err
			}
			if err := c.ensurePlacementFence(ctx, record); err != nil {
				return RunResult{}, err
			}
			if placementWindowComplete(starting.PlacementRound, c.roundLimit) {
				return RunResult{Status: RunTerminal, Reason: "placement round limit exhausted", Route: &record}, nil
			}
			record, err = c.StartRouteAfterPlacementFailure(ctx, record)
			if err != nil {
				return RunResult{}, err
			}
			demand, runtimeDigest, err = sandboxPlacementInputs(record.Starting.Intent)
			if err != nil {
				return RunResult{}, err
			}
			cache = make(map[uint32]cachedProbe)
			continue
		}

		index, probe, found, selectErr := c.selectCandidate(
			ctx, demand, runtimeDigest, starting.CandidatePool, starting.DefinitivelyRejected, cache,
		)
		if selectErr != nil {
			return RunResult{}, selectErr
		}
		if !found {
			rejected, changed := appendProbeRejections(starting.DefinitivelyRejected, starting.CandidatePool, cache)
			if changed {
				next := record
				next.Starting = cloneRouteStarting(starting)
				next.Starting.DefinitivelyRejected = rejected
				record, err = c.commitRoute(ctx, record.Revision, next)
				if err != nil {
					return RunResult{}, err
				}
				continue
			}
			return RunResult{Status: RunNoUsableProbe, Reason: "no candidate pair has a current usable Probe", Route: &record}, nil
		}
		binding, bindErr := makeBinding(c.identity.RegistryGeneration, cluster.ExecutionKindSandbox, record.Group, record.RouteKey, starting.SandboxID, starting.Intent, probe.Response)
		if bindErr != nil {
			return RunResult{}, bindErr
		}
		next := record
		next.Starting = cloneRouteStarting(starting)
		next.Starting.SelectedCandidate = uint32Pointer(index)
		next.Starting.Binding = &binding
		record, err = c.commitRoute(ctx, record.Revision, next)
		if err != nil {
			return RunResult{}, err
		}
	}
}

// StartRouteAfterPlacementFailure begins the next globally monotonic round.
// Callers use it automatically inside the current round window, or explicitly
// when a later Reserve restarts a terminal placement-failure workflow.
func (c *StartingCoordinator) StartRouteAfterPlacementFailure(
	ctx context.Context,
	record cluster.RouteWorkflowRecord,
) (cluster.RouteWorkflowRecord, error) {
	if c.routes == nil || c.rounds == nil {
		return cluster.RouteWorkflowRecord{}, errors.New("coordinator: Route committer and Sandbox round source are required")
	}
	if err := record.Validate(); err != nil {
		return cluster.RouteWorkflowRecord{}, err
	}
	if record.State != cluster.WorkflowRouteTombstone || record.Tombstone == nil || record.Tombstone.PlacementFailure == nil {
		return cluster.RouteWorkflowRecord{}, errors.New("coordinator: Route is not a placement-failure TOMBSTONE")
	}
	if err := c.ensurePlacementFence(ctx, record); err != nil {
		return cluster.RouteWorkflowRecord{}, err
	}
	failure := record.Tombstone.PlacementFailure
	nextRound := failure.PlacementRound + 1
	round, err := c.rounds.NextSandboxRound(
		ctx, record.Group, record.RouteKey, nextRound, cloneIntent(failure.Intent), candidateNodeIDs(failure.CandidatePool),
	)
	if err != nil {
		return cluster.RouteWorkflowRecord{}, err
	}
	if round.SandboxID == "" || round.SandboxID == failure.SandboxID || len(round.Candidates) == 0 {
		return cluster.RouteWorkflowRecord{}, errors.New("coordinator: next placement round requires a new Sandbox ID and candidate pool")
	}
	if err := round.Intent.Validate(); err != nil {
		return cluster.RouteWorkflowRecord{}, err
	}
	next := cluster.RouteWorkflowRecord{
		Group: record.Group, RouteKey: record.RouteKey, State: cluster.WorkflowRouteStarting,
		Revision:      record.Revision,
		Finalizations: append([]cluster.WorkflowFinalizationIntent(nil), record.Finalizations...),
		Starting: &cluster.RouteStartingState{
			SandboxID: round.SandboxID, PlacementRound: nextRound,
			CandidatePool: append([]cluster.PlacementCandidate(nil), round.Candidates...),
			Intent:        cloneIntent(round.Intent),
		},
	}
	return c.commitRoute(ctx, record.Revision, next)
}

func candidateNodeIDs(candidates []cluster.PlacementCandidate) []string {
	result := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.NodeID != "" {
			result = append(result, candidate.NodeID)
		}
	}
	return result
}

func (c *StartingCoordinator) ensurePlacementFence(ctx context.Context, record cluster.RouteWorkflowRecord) error {
	failure := record.Tombstone.PlacementFailure
	fence, err := cluster.NewPlacementFailureFence(
		record.Group, record.RouteKey, record.Revision.RegistryGeneration, *failure,
	)
	if err != nil {
		return err
	}
	return c.routes.EnsureExecutionFence(ctx, fence)
}

func (c *StartingCoordinator) RunBuild(ctx context.Context, record cluster.BuildRecord) (RunResult, error) {
	if c.builds == nil {
		return RunResult{}, errors.New("coordinator: Build committer is unavailable")
	}
	if err := record.Validate(); err != nil {
		return RunResult{}, err
	}
	if record.State != cluster.BuildStarting || record.Starting == nil {
		return RunResult{}, errors.New("coordinator: Build is not BUILD_STARTING")
	}
	demand, runtimeDigest, err := buildPlacementInputs(record.Starting.Intent)
	if err != nil {
		return RunResult{}, err
	}
	cache := make(map[uint32]cachedProbe)
	for {
		starting := record.Starting
		if starting.SelectedCandidate != nil {
			result := c.dispatch(ctx, cluster.ExecutionKindBuild, record.Group, "", starting.BuildID, starting.Intent, *starting.Binding)
			if result.Outcome == cluster.DispatchAcceptedAdmitted || result.Outcome == cluster.DispatchAcceptedQueued {
				projection, projectionErr := buildRegistrationProjection(record)
				if projectionErr != nil {
					return RunResult{}, projectionErr
				}
				next := cluster.BuildRecord{
					Group: record.Group, BuildID: record.BuildID, State: cluster.BuildRegistered,
					Revision: record.Revision, Projection: &projection,
					Finalizations: append([]cluster.WorkflowFinalizationIntent(nil), record.Finalizations...),
				}
				record, err = c.commitBuild(ctx, record.Revision, next)
				if err != nil {
					return RunResult{}, err
				}
				result.Status, result.Build = RunComplete, &record
				return result, nil
			}
			if result.Outcome != cluster.DispatchDefinitiveReject {
				result.Build = &record
				return result, nil
			}
			finalization, finalizationErr := cluster.NewWorkflowFinalizationIntent(
				starting.BuildID, *starting.Binding, nil,
			)
			if finalizationErr != nil {
				return RunResult{}, finalizationErr
			}
			next := record
			next.Finalizations = append(
				append([]cluster.WorkflowFinalizationIntent(nil), record.Finalizations...), finalization,
			)
			next.Starting = cloneBuildStarting(starting)
			index := *starting.SelectedCandidate
			next.Starting.SelectedCandidate = nil
			next.Starting.Binding = nil
			next.Starting.DefinitivelyRejected = appendRejected(next.Starting.DefinitivelyRejected, index)
			record, err = c.commitBuild(ctx, record.Revision, next)
			if err != nil {
				return RunResult{}, err
			}
			continue
		}
		if allRejected(len(starting.CandidatePool), starting.DefinitivelyRejected) {
			next := cluster.BuildRecord{
				Group: record.Group, BuildID: record.BuildID, State: cluster.BuildTombstone,
				Revision:      record.Revision,
				Finalizations: append([]cluster.WorkflowFinalizationIntent(nil), record.Finalizations...),
			}
			next.Tombstone = &cluster.BuildTombstoneState{PlacementFailure: cluster.BuildPlacementFailureState{
				BuildID: starting.BuildID, CandidatePool: append([]cluster.PlacementCandidate(nil), starting.CandidatePool...),
				DefinitivelyRejected: append([]uint32(nil), starting.DefinitivelyRejected...),
				Intent:               cloneIntent(starting.Intent), Reason: "Build candidate pool exhausted",
			}}
			record, err = c.commitBuild(ctx, record.Revision, next)
			if err != nil {
				return RunResult{}, err
			}
			return RunResult{Status: RunTerminal, Reason: "Build candidate pool exhausted", Build: &record}, nil
		}

		index, probe, found, selectErr := c.selectCandidate(
			ctx, demand, runtimeDigest, starting.CandidatePool, starting.DefinitivelyRejected, cache,
		)
		if selectErr != nil {
			return RunResult{}, selectErr
		}
		if !found {
			rejected, changed := appendProbeRejections(starting.DefinitivelyRejected, starting.CandidatePool, cache)
			if changed {
				next := record
				next.Starting = cloneBuildStarting(starting)
				next.Starting.DefinitivelyRejected = rejected
				record, err = c.commitBuild(ctx, record.Revision, next)
				if err != nil {
					return RunResult{}, err
				}
				continue
			}
			return RunResult{Status: RunNoUsableProbe, Reason: "no candidate pair has a current usable Probe", Build: &record}, nil
		}
		binding, bindErr := makeBinding(c.identity.RegistryGeneration, cluster.ExecutionKindBuild, record.Group, "", starting.BuildID, starting.Intent, probe.Response)
		if bindErr != nil {
			return RunResult{}, bindErr
		}
		next := record
		next.Starting = cloneBuildStarting(starting)
		next.Starting.SelectedCandidate = uint32Pointer(index)
		next.Starting.Binding = &binding
		record, err = c.commitBuild(ctx, record.Revision, next)
		if err != nil {
			return RunResult{}, err
		}
	}
}

func buildRegistrationProjection(record cluster.BuildRecord) (cluster.BuildProjection, error) {
	if record.Starting == nil || record.Starting.Binding == nil {
		return cluster.BuildProjection{}, errors.New("coordinator: accepted Build registration has no committed Binding")
	}
	spec, err := cluster.ParseBuildDispatchSpec(record.Starting.Intent.DispatchSpec)
	if err != nil {
		return cluster.BuildProjection{}, err
	}
	binding := record.Starting.Binding
	projection := cluster.BuildProjection{
		BuildID: record.BuildID, NodeID: binding.NodeID, NodeEpoch: binding.NodeEpoch,
		DataEndpoint: binding.DataEndpoint, RegistryGeneration: binding.RegistryGeneration,
		OpaqueBinding: binding.OpaqueBinding, BindingDigest: binding.BindingDigest,
		Intent:      cloneIntent(record.Starting.Intent),
		TemplateRef: spec.TemplateID,
	}
	return projection, projection.ValidateWorkflow(record.Group)
}

func (c *StartingCoordinator) selectCandidate(ctx context.Context, demand placement.NormalizedDemand, runtimeDigest string, candidates []cluster.PlacementCandidate, rejected []uint32, cache map[uint32]cachedProbe) (uint32, session.ProbeResult, bool, error) {
	rejectedSet := make(map[uint32]struct{}, len(rejected))
	for _, index := range rejected {
		rejectedSet[index] = struct{}{}
	}
	for pairStart := 0; pairStart < len(candidates); pairStart += 2 {
		indices := make([]uint32, 0, 2)
		requests := make([]placement.PlacementProbeRequest, 0, 2)
		results := make([]session.ProbeResult, 0, 2)
		for offset := 0; offset < 2 && pairStart+offset < len(candidates); offset++ {
			index := uint32(pairStart + offset)
			if _, excluded := rejectedSet[index]; excluded {
				continue
			}
			indices = append(indices, index)
			if cached, ok := cache[index]; ok && probeStillFresh(c.clock(), cached) {
				results = append(results, cached.result)
				continue
			}
			candidate := candidates[index]
			requests = append(requests, demand.ProbeRequest(candidate.NodeID, runtimeDigest))
			results = append(results, session.ProbeResult{})
		}
		if len(indices) == 0 {
			continue
		}
		if len(requests) > 0 {
			probeStarted := c.clock()
			probed := c.prober.ProbePair(ctx, c.identity, requests)
			probeFinished := c.clock()
			probeElapsed := probeFinished.Sub(probeStarted)
			if len(probed) != len(requests) {
				return 0, session.ProbeResult{}, false, errors.New("coordinator: Pair Prober returned the wrong result count")
			}
			probeIndex := 0
			for resultIndex := range results {
				if results[resultIndex].Response.NodeID != "" {
					continue
				}
				result := accountProbeTransit(probed[probeIndex], probeElapsed)
				probeIndex++
				results[resultIndex] = result
				cache[indices[resultIndex]] = cachedProbe{result: result, observedAt: probeFinished}
			}
		}
		usable := make([]int, 0, 2)
		for index, result := range results {
			if result.Response.Class == placement.ProbeImmediate || result.Response.Class == placement.ProbeWouldQueue {
				usable = append(usable, index)
			}
		}
		switch len(usable) {
		case 0:
			continue
		case 1:
			selected := usable[0]
			return indices[selected], results[selected], true, nil
		default:
			chooseSecond := false
			if results[usable[0]].Response.Class == results[usable[1]].Response.Class &&
				results[usable[0]].Response.RatePPM == results[usable[1]].Response.RatePPM {
				var err error
				chooseSecond, err = c.tie.ChooseSecond()
				if err != nil {
					return 0, session.ProbeResult{}, false, err
				}
			}
			winner := placement.ChooseP2C(results[usable[0]].Response, results[usable[1]].Response, func() bool { return chooseSecond })
			selected := usable[0]
			if winner == results[usable[1]].Response {
				selected = usable[1]
			}
			return indices[selected], results[selected], true, nil
		}
	}
	return 0, session.ProbeResult{}, false, nil
}

func (c *StartingCoordinator) dispatch(ctx context.Context, kind cluster.ExecutionKind, group, routeKey, objectID string, intent cluster.DispatchIntent, binding cluster.ExecutionBindingIntent) RunResult {
	request := session.DispatchCommand{
		ServeIdentity: c.identity, Kind: kind, Group: group, RouteKey: routeKey, ObjectID: objectID,
		NodeID: binding.NodeID, NodeEpoch: binding.NodeEpoch, DataEndpoint: binding.DataEndpoint,
		Intent: cloneIntent(intent), Binding: binding,
	}
	dispatched, err := c.dispatcher.AdmitAndDispatch(ctx, request)
	if err != nil {
		if errors.Is(err, session.ErrDispatchNotSent) {
			return RunResult{Status: RunRetrySelected, Reason: err.Error()}
		}
		return RunResult{Status: RunPinnedUnknown, Outcome: cluster.DispatchUnknown, Reason: err.Error()}
	}
	if err := dispatched.Outcome.Validate(); err != nil {
		return RunResult{Status: RunPinnedUnknown, Outcome: cluster.DispatchUnknown, Reason: err.Error()}
	}
	result := RunResult{Outcome: dispatched.Outcome, Reason: dispatched.Reason}
	switch dispatched.Outcome {
	case cluster.DispatchAcceptedAdmitted, cluster.DispatchAcceptedQueued:
		result.Status = RunWaitingForEvent
	case cluster.DispatchDefinitiveReject:
		result.Status = RunRetrySelected
	case cluster.DispatchSessionMoved:
		result.Status = RunRetrySelected
	case cluster.DispatchConflict, cluster.DispatchWrongBinding:
		result.Status = RunConflict
	case cluster.DispatchUnknown:
		result.Status = RunPinnedUnknown
	}
	return result
}

func (c *StartingCoordinator) commitRoute(ctx context.Context, expected cluster.Revision, next cluster.RouteWorkflowRecord) (cluster.RouteWorkflowRecord, error) {
	if err := next.Validate(); err != nil {
		return cluster.RouteWorkflowRecord{}, err
	}
	committed, err := c.routes.CommitRouteWorkflow(ctx, expected, next)
	if err != nil {
		return cluster.RouteWorkflowRecord{}, err
	}
	if err := committed.Validate(); err != nil {
		return cluster.RouteWorkflowRecord{}, err
	}
	if committed.Group != next.Group || committed.RouteKey != next.RouteKey || !revisionAdvanced(expected, committed.Revision) {
		return cluster.RouteWorkflowRecord{}, errors.New("coordinator: Route committer returned a mismatched workflow")
	}
	want, got := next, committed
	got.Revision = want.Revision
	if !reflect.DeepEqual(got, want) {
		return cluster.RouteWorkflowRecord{}, errors.New("coordinator: Route committer changed the requested transition")
	}
	return committed, nil
}

func (c *StartingCoordinator) commitBuild(ctx context.Context, expected cluster.Revision, next cluster.BuildRecord) (cluster.BuildRecord, error) {
	if err := next.Validate(); err != nil {
		return cluster.BuildRecord{}, err
	}
	committed, err := c.builds.CommitBuildWorkflow(ctx, expected, next)
	if err != nil {
		return cluster.BuildRecord{}, err
	}
	if err := committed.Validate(); err != nil {
		return cluster.BuildRecord{}, err
	}
	if committed.Group != next.Group || committed.BuildID != next.BuildID || !revisionAdvanced(expected, committed.Revision) {
		return cluster.BuildRecord{}, errors.New("coordinator: Build committer returned a mismatched workflow")
	}
	want, got := next, committed
	got.Revision = want.Revision
	if !reflect.DeepEqual(got, want) {
		return cluster.BuildRecord{}, errors.New("coordinator: Build committer changed the requested transition")
	}
	return committed, nil
}

func makeBinding(registryGeneration string, kind cluster.ExecutionKind, group, routeKey, objectID string, intent cluster.DispatchIntent, probe placement.PlacementProbeResponse) (cluster.ExecutionBindingIntent, error) {
	if probe.NodeID == "" || probe.NodeEpoch == 0 || probe.DataEndpoint == "" {
		return cluster.ExecutionBindingIntent{}, errors.New("coordinator: selected Probe lacks a stable execution target")
	}
	demandDigest, err := decodeDigest(intent.DemandDigest)
	if err != nil {
		return cluster.ExecutionBindingIntent{}, err
	}
	specDigest, err := decodeDigest(intent.DispatchSpecDigest)
	if err != nil {
		return cluster.ExecutionBindingIntent{}, err
	}
	opaque, err := cluster.EncodeExecutionBinding(cluster.ExecutionBinding{
		RegistryGeneration: registryGeneration, Kind: kind, ObjectID: objectID,
		Group: group, RouteKey: routeKey, NodeID: probe.NodeID, NodeEpoch: probe.NodeEpoch,
		DemandDigest: demandDigest, DispatchSpecDigest: specDigest,
	})
	if err != nil {
		return cluster.ExecutionBindingIntent{}, err
	}
	digest, err := cluster.ExecutionBindingDigest(opaque)
	if err != nil {
		return cluster.ExecutionBindingIntent{}, err
	}
	return cluster.ExecutionBindingIntent{
		NodeID: probe.NodeID, NodeEpoch: probe.NodeEpoch, DataEndpoint: probe.DataEndpoint,
		RegistryGeneration: registryGeneration, OpaqueBinding: opaque, BindingDigest: digest,
	}, nil
}

func decodeDigest(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return digest, errors.New("coordinator: invalid dispatch digest")
	}
	copy(digest[:], decoded)
	return digest, nil
}

func probeStillFresh(now time.Time, cached cachedProbe) bool {
	if cached.result.Response.Class != placement.ProbeImmediate && cached.result.Response.Class != placement.ProbeWouldQueue {
		return false
	}
	elapsed := now.Sub(cached.observedAt)
	return elapsed >= 0 && cached.result.Response.SampleAge >= 0 &&
		cached.result.Response.SampleAge+elapsed <= placement.MaximumProbeSampleAge
}

func accountProbeTransit(result session.ProbeResult, elapsed time.Duration) session.ProbeResult {
	response := &result.Response
	if response.Class != placement.ProbeImmediate && response.Class != placement.ProbeWouldQueue {
		return result
	}
	if elapsed < 0 || response.SampleAge < 0 || elapsed > placement.MaximumProbeSampleAge-response.SampleAge {
		response.Class = placement.ProbeStale
		response.Reason = "placement sample expired during Probe RPC"
		return result
	}
	response.SampleAge += elapsed
	return result
}

func revisionAdvanced(previous, next cluster.Revision) bool {
	return previous.RegistryGeneration == next.RegistryGeneration && previous.ShardID == next.ShardID && next.LogIndex > previous.LogIndex
}

func allRejected(candidateCount int, rejected []uint32) bool {
	if candidateCount == 0 || len(rejected) != candidateCount {
		return false
	}
	seen := make(map[uint32]struct{}, len(rejected))
	for _, index := range rejected {
		if int(index) >= candidateCount {
			return false
		}
		seen[index] = struct{}{}
	}
	return len(seen) == candidateCount
}

func placementWindowComplete(round, limit uint64) bool {
	return limit == 0 || round%limit == 0
}

func appendRejected(rejected []uint32, index uint32) []uint32 {
	for _, current := range rejected {
		if current == index {
			return append([]uint32(nil), rejected...)
		}
	}
	out := append([]uint32(nil), rejected...)
	return append(out, index)
}

func appendProbeRejections(
	rejected []uint32,
	candidates []cluster.PlacementCandidate,
	cache map[uint32]cachedProbe,
) ([]uint32, bool) {
	result := append([]uint32(nil), rejected...)
	changed := false
	for index := range candidates {
		cached, found := cache[uint32(index)]
		if !found || cached.result.Response.Class != placement.ProbeReject {
			continue
		}
		next := appendRejected(result, uint32(index))
		if len(next) != len(result) {
			changed = true
		}
		result = next
	}
	return result, changed
}

func cloneRouteStarting(source *cluster.RouteStartingState) *cluster.RouteStartingState {
	if source == nil {
		return nil
	}
	clone := *source
	clone.CandidatePool = append([]cluster.PlacementCandidate(nil), source.CandidatePool...)
	clone.DefinitivelyRejected = append([]uint32(nil), source.DefinitivelyRejected...)
	clone.Intent = cloneIntent(source.Intent)
	if source.SelectedCandidate != nil {
		clone.SelectedCandidate = uint32Pointer(*source.SelectedCandidate)
	}
	if source.Binding != nil {
		binding := *source.Binding
		clone.Binding = &binding
	}
	return &clone
}

func cloneBuildStarting(source *cluster.BuildStartingState) *cluster.BuildStartingState {
	if source == nil {
		return nil
	}
	clone := *source
	clone.CandidatePool = append([]cluster.PlacementCandidate(nil), source.CandidatePool...)
	clone.DefinitivelyRejected = append([]uint32(nil), source.DefinitivelyRejected...)
	clone.Intent = cloneIntent(source.Intent)
	if source.SelectedCandidate != nil {
		clone.SelectedCandidate = uint32Pointer(*source.SelectedCandidate)
	}
	if source.Binding != nil {
		binding := *source.Binding
		clone.Binding = &binding
	}
	return &clone
}

func cloneIntent(source cluster.DispatchIntent) cluster.DispatchIntent {
	source.NormalizedDemand = append([]byte(nil), source.NormalizedDemand...)
	source.DispatchSpec = append([]byte(nil), source.DispatchSpec...)
	return source
}

func uint32Pointer(value uint32) *uint32 { return &value }

type cryptoTieBreaker struct{}

func (cryptoTieBreaker) ChooseSecond() (bool, error) {
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(2))
	if err != nil {
		return false, fmt.Errorf("coordinator: random P2C tie-break: %w", err)
	}
	return value.Sign() != 0, nil
}
