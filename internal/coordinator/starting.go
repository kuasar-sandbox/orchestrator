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
	NextSandboxRound(context.Context, string, string, uint64, cluster.DispatchIntent) (string, []cluster.PlacementCandidate, error)
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
	if config.ServeIdentity.ClusterID == "" || config.ServeIdentity.StorageGeneration == "" || config.ServeIdentity.SystemEpoch == 0 {
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
	demand, err := placement.ParseNormalizedDemand(record.Starting.Intent.NormalizedDemand)
	if err != nil {
		return RunResult{}, err
	}
	if demand.Kind != placement.ObjectSandbox {
		return RunResult{}, errors.New("coordinator: Route carries a non-Sandbox demand")
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
			next := record
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
			if starting.PlacementRound >= c.roundLimit {
				next := record
				next.State = cluster.WorkflowRouteTombstone
				next.Starting = nil
				next.Tombstone = &cluster.RouteTombstoneState{PlacementFailure: &cluster.RoutePlacementFailureState{
					SandboxID: starting.SandboxID, PlacementRound: starting.PlacementRound,
					CandidatePool:        append([]cluster.PlacementCandidate(nil), starting.CandidatePool...),
					DefinitivelyRejected: append([]uint32(nil), starting.DefinitivelyRejected...),
					Intent:               cloneIntent(starting.Intent), Reason: "placement round limit exhausted",
				}}
				record, err = c.commitRoute(ctx, record.Revision, next)
				if err != nil {
					return RunResult{}, err
				}
				return RunResult{Status: RunTerminal, Reason: "placement round limit exhausted", Route: &record}, nil
			}
			if c.rounds == nil {
				return RunResult{}, errors.New("coordinator: Sandbox round source is unavailable")
			}
			nextRound := starting.PlacementRound + 1
			sandboxID, candidates, roundErr := c.rounds.NextSandboxRound(ctx, record.Group, record.RouteKey, nextRound, cloneIntent(starting.Intent))
			if roundErr != nil {
				return RunResult{}, roundErr
			}
			if sandboxID == "" || sandboxID == starting.SandboxID {
				return RunResult{}, errors.New("coordinator: next placement round requires a new Sandbox ID")
			}
			next := record
			next.Starting = &cluster.RouteStartingState{
				SandboxID: sandboxID, PlacementRound: nextRound,
				CandidatePool: append([]cluster.PlacementCandidate(nil), candidates...), Intent: cloneIntent(starting.Intent),
			}
			record, err = c.commitRoute(ctx, record.Revision, next)
			if err != nil {
				return RunResult{}, err
			}
			cache = make(map[uint32]cachedProbe)
			continue
		}

		index, probe, found, selectErr := c.selectCandidate(ctx, demand, starting.CandidatePool, starting.DefinitivelyRejected, cache)
		if selectErr != nil {
			return RunResult{}, selectErr
		}
		if !found {
			return RunResult{Status: RunNoUsableProbe, Reason: "no candidate pair has a current usable Probe", Route: &record}, nil
		}
		binding, bindErr := makeBinding(c.identity.StorageGeneration, cluster.ExecutionKindSandbox, record.Group, record.RouteKey, starting.SandboxID, starting.Intent, probe.Response)
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
	demand, err := placement.ParseNormalizedDemand(record.Starting.Intent.NormalizedDemand)
	if err != nil {
		return RunResult{}, err
	}
	if demand.Kind != placement.ObjectBuild {
		return RunResult{}, errors.New("coordinator: Build carries a non-Build demand")
	}
	cache := make(map[uint32]cachedProbe)
	for {
		starting := record.Starting
		if starting.SelectedCandidate != nil {
			result := c.dispatch(ctx, cluster.ExecutionKindBuild, record.Group, "", starting.BuildID, starting.Intent, *starting.Binding)
			if result.Outcome != cluster.DispatchDefinitiveReject {
				result.Build = &record
				return result, nil
			}
			next := record
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
			next := record
			next.State = cluster.BuildError
			next.Starting = nil
			next.Failure = &cluster.BuildPlacementFailureState{
				BuildID: starting.BuildID, CandidatePool: append([]cluster.PlacementCandidate(nil), starting.CandidatePool...),
				DefinitivelyRejected: append([]uint32(nil), starting.DefinitivelyRejected...),
				Intent:               cloneIntent(starting.Intent), Reason: "Build candidate pool exhausted",
			}
			record, err = c.commitBuild(ctx, record.Revision, next)
			if err != nil {
				return RunResult{}, err
			}
			return RunResult{Status: RunTerminal, Reason: "Build candidate pool exhausted", Build: &record}, nil
		}

		index, probe, found, selectErr := c.selectCandidate(ctx, demand, starting.CandidatePool, starting.DefinitivelyRejected, cache)
		if selectErr != nil {
			return RunResult{}, selectErr
		}
		if !found {
			return RunResult{Status: RunNoUsableProbe, Reason: "no candidate pair has a current usable Probe", Build: &record}, nil
		}
		binding, bindErr := makeBinding(c.identity.StorageGeneration, cluster.ExecutionKindBuild, record.Group, "", starting.BuildID, starting.Intent, probe.Response)
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

func (c *StartingCoordinator) selectCandidate(ctx context.Context, demand placement.NormalizedDemand, candidates []cluster.PlacementCandidate, rejected []uint32, cache map[uint32]cachedProbe) (uint32, session.ProbeResult, bool, error) {
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
			requests = append(requests, demand.ProbeRequest(candidate.NodeID, candidate.RuntimeDigest))
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

func makeBinding(storageGeneration string, kind cluster.ExecutionKind, group, routeKey, objectID string, intent cluster.DispatchIntent, probe placement.PlacementProbeResponse) (cluster.ExecutionBindingIntent, error) {
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
		StorageGeneration: storageGeneration, Kind: kind, ObjectID: objectID,
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
		StorageGeneration: storageGeneration, OpaqueBinding: opaque, BindingDigest: digest,
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
	return previous.StorageGeneration == next.StorageGeneration && previous.ShardID == next.ShardID && next.LogIndex > previous.LogIndex
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

func appendRejected(rejected []uint32, index uint32) []uint32 {
	for _, current := range rejected {
		if current == index {
			return append([]uint32(nil), rejected...)
		}
	}
	out := append([]uint32(nil), rejected...)
	return append(out, index)
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
