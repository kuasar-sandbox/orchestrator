package controlplane

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/coordinator"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type NodeCommandSender interface {
	SendNodeCommand(context.Context, session.ServeIdentity, string, uint64, string, *routesync.Command) (routesync.CmdAck, bool, error)
	InstallKeyLease(context.Context, session.ServeIdentity, string, uint64, string, routesync.NodeKeyLeaseV1) (routesync.NodeKeyLeaseRefV1, bool, error)
}

type RegistryServiceConfig struct {
	ParkTimeout             time.Duration
	PollInterval            time.Duration
	PermitRefreshInterval   time.Duration
	PlacementRoundLimit     uint64
	RecoveryScanInterval    time.Duration
	RecoveryShardsPerScan   uint32
	RecoveryWorkers         int
	CompactionWorkers       int
	PendingWorkflowsPerPage uint32
	SandboxLaunchPerSecond  int
	SandboxLaunchBurst      int
	BuildLaunchPerSecond    int
	BuildLaunchBurst        int
	NewObjectID             func() (string, error)
}

func DefaultRegistryServiceConfig() RegistryServiceConfig {
	return RegistryServiceConfig{
		ParkTimeout: 30 * time.Second, PollInterval: 20 * time.Millisecond,
		PermitRefreshInterval: time.Second,
		PlacementRoundLimit:   coordinator.DefaultPlacementRoundLimit,
		RecoveryScanInterval:  250 * time.Millisecond, RecoveryShardsPerScan: 64,
		RecoveryWorkers: 8, CompactionWorkers: 4, PendingWorkflowsPerPage: 256, NewObjectID: newUUIDv7,
		SandboxLaunchPerSecond: 500, SandboxLaunchBurst: 500,
		BuildLaunchPerSecond: 100, BuildLaunchBurst: 100,
	}
}

type RegistryService struct {
	store      *RaftStore
	planner    PlacementPlanner
	prober     coordinator.PairProber
	dispatcher coordinator.Dispatcher
	commands   NodeCommandSender
	config     RegistryServiceConfig

	scanMu           sync.Mutex
	nextShard        uint32
	leaseMu          sync.Mutex
	leaseCursors     map[uint32]string
	leaseExpiries    map[string]int64
	leaseRenewing    map[string]struct{}
	lastLeasePruneAt int64

	compactionCtx    context.Context
	stopCompaction   context.CancelFunc
	compactionSlots  chan struct{}
	compactionMu     sync.Mutex
	activeCompaction map[string]struct{}
}

func NewRegistryService(
	store *RaftStore,
	planner PlacementPlanner,
	prober coordinator.PairProber,
	dispatcher coordinator.Dispatcher,
	commands NodeCommandSender,
	config RegistryServiceConfig,
) (*RegistryService, error) {
	if store == nil || planner == nil || prober == nil || dispatcher == nil || commands == nil {
		return nil, errors.New("controlplane: final Registry service requires consensus, Placer, Session Probe/dispatch, and commands")
	}
	defaults := DefaultRegistryServiceConfig()
	if config.ParkTimeout <= 0 {
		config.ParkTimeout = defaults.ParkTimeout
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.PermitRefreshInterval <= 0 {
		config.PermitRefreshInterval = defaults.PermitRefreshInterval
	}
	registryLayout, _ := store.RegistryLayoutSnapshot()
	if config.PermitRefreshInterval >= time.Duration(registryLayout.ServePermitMaxMillis)*time.Millisecond {
		return nil, errors.New("controlplane: Permit refresh interval must be shorter than its maximum lifetime")
	}
	if config.PlacementRoundLimit == 0 {
		config.PlacementRoundLimit = defaults.PlacementRoundLimit
	}
	if config.RecoveryScanInterval <= 0 {
		config.RecoveryScanInterval = defaults.RecoveryScanInterval
	}
	if config.RecoveryShardsPerScan == 0 {
		config.RecoveryShardsPerScan = defaults.RecoveryShardsPerScan
	}
	if config.RecoveryWorkers <= 0 {
		config.RecoveryWorkers = defaults.RecoveryWorkers
	}
	if config.CompactionWorkers <= 0 || config.CompactionWorkers > 256 {
		config.CompactionWorkers = defaults.CompactionWorkers
	}
	if config.PendingWorkflowsPerPage == 0 || config.PendingWorkflowsPerPage > 4096 {
		config.PendingWorkflowsPerPage = defaults.PendingWorkflowsPerPage
	}
	if config.SandboxLaunchPerSecond <= 0 || config.SandboxLaunchBurst <= 0 {
		config.SandboxLaunchPerSecond, config.SandboxLaunchBurst =
			defaults.SandboxLaunchPerSecond, defaults.SandboxLaunchBurst
	}
	if config.BuildLaunchPerSecond <= 0 || config.BuildLaunchBurst <= 0 {
		config.BuildLaunchPerSecond, config.BuildLaunchBurst =
			defaults.BuildLaunchPerSecond, defaults.BuildLaunchBurst
	}
	if config.SandboxLaunchBurst < len(registryLayout.Members) || config.BuildLaunchBurst < len(registryLayout.Members) {
		return nil, errors.New("controlplane: aggregate launch bursts must be at least the Registry member count")
	}
	if config.NewObjectID == nil {
		config.NewObjectID = defaults.NewObjectID
	}
	compactionCtx, stopCompaction := context.WithCancel(context.Background())
	limitedDispatcher := rateLimitedDispatcher{
		next:    dispatcher,
		sandbox: newLaunchRateLimiter(config.SandboxLaunchPerSecond, config.SandboxLaunchBurst, len(registryLayout.Members)),
		build:   newLaunchRateLimiter(config.BuildLaunchPerSecond, config.BuildLaunchBurst, len(registryLayout.Members)),
	}
	dispatcher = keyLeaseDispatcher{planner: planner, installer: commands, next: limitedDispatcher}
	return &RegistryService{
		store: store, planner: planner, prober: prober, dispatcher: dispatcher,
		commands: commands, config: config, compactionCtx: compactionCtx, stopCompaction: stopCompaction,
		compactionSlots: make(chan struct{}, config.CompactionWorkers), activeCompaction: make(map[string]struct{}),
		leaseCursors: make(map[uint32]string), leaseExpiries: make(map[string]int64),
		leaseRenewing: make(map[string]struct{}),
	}, nil
}

func newUUIDv7() (string, error) {
	value, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return value.String(), nil
}

func (s *RegistryService) ReadRoute(ctx context.Context, request routeapi.ReadRouteRequest) (routeapi.ReadRouteResponse, error) {
	if err := s.validateRouteIdentity(request.RequestIdentity, request.Group, request.RouteKey); err != nil {
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadConflict, Reason: err.Error()}, nil
	}
	return s.store.ReadRoute(ctx, request)
}

func (s *RegistryService) ReadBuild(ctx context.Context, request routeapi.ReadBuildRequest) (routeapi.ReadBuildResponse, error) {
	if err := s.validateBuildIdentity(request.RequestIdentity, request.Group, request.BuildID); err != nil {
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadConflict, Reason: err.Error()}, nil
	}
	return s.store.ReadBuild(ctx, request)
}

func (s *RegistryService) RefreshPermit(ctx context.Context, request routeapi.PermitRequest) (routeapi.PermitResponse, error) {
	registryLayout, digest := s.store.RegistryLayoutSnapshot()
	if request.ClusterID != registryLayout.ClusterID || request.RegistryGeneration != registryLayout.RegistryGeneration || request.RegistryLayoutDigest != digest {
		return routeapi.PermitResponse{}, errors.New("controlplane: Permit request names another signed Registry History Generation")
	}
	grant, err := s.store.RefreshPermitGrant(ctx)
	if err != nil {
		return routeapi.PermitResponse{}, err
	}
	return routeapi.PermitResponse{
		ClusterID: grant.ClusterID, RegistryGeneration: grant.RegistryGeneration,
		SystemEpoch: grant.SystemEpoch, RegistryLayoutDigest: grant.RegistryLayoutDigest,
		CommitIndex: grant.CommitIndex, MaxLifetimeMillis: grant.MaxLifetimeMillis,
		ServeGate: grant.ServeGate, WriteGate: grant.WriteGate,
		CutoverGate: grant.CutoverGate, RecoveryClosed: grant.RecoveryClosed,
	}, nil
}

func (s *RegistryService) ReserveSandbox(
	ctx context.Context,
	request routeapi.ReserveSandboxRequest,
) (routeapi.RouteMutationResponse, error) {
	if err := s.validateRouteIdentity(request.RequestIdentity, request.Group, request.RouteKey); err != nil {
		return routeConflict(request.Group, request.RouteKey, err), nil
	}
	local, err := s.store.ReadRoute(ctx, routeapi.ReadRouteRequest{
		RequestIdentity: request.RequestIdentity, Group: request.Group, RouteKey: request.RouteKey,
		MinRouteRevision: request.MinRouteRevision,
	})
	if err == nil && local.Outcome == routeapi.ReadReady {
		if local.Route == nil || !sandboxInputMatchesIntent(request.Input, local.Route.Intent) {
			return routeConflict(request.Group, request.RouteKey, errors.New("route key is bound to a different Sandbox request")), nil
		}
		return routeapi.RouteMutationResponse{
			Outcome: routeapi.MutationReady, Group: request.Group, RouteKey: request.RouteKey,
			State: clusterstate.WorkflowRouteReady, Route: local.Route, RouteRevision: local.RouteRevision,
		}, nil
	}
	leader, leaderErr := s.store.LocalCoordinator(request.ShardID)
	if leaderErr != nil || !leader {
		response := routeapi.RouteMutationResponse{
			Outcome: routeapi.MutationNeedLeader, Group: request.Group, RouteKey: request.RouteKey,
		}
		if err == nil {
			response.LeaderHint = local.LeaderHint
			response.Reason = local.Reason
		}
		return response, nil
	}

	record, err := s.store.ReadRouteWorkflow(ctx, request.Group, request.RouteKey)
	if err != nil {
		return routeapi.RouteMutationResponse{}, err
	}
	if record == nil {
		record, err = s.createRoute(ctx, request)
		if errors.Is(err, ErrRevisionConflict) {
			record, err = s.store.ReadRouteWorkflow(ctx, request.Group, request.RouteKey)
		}
		if err != nil {
			return routeapi.RouteMutationResponse{}, err
		}
	}
	if record == nil {
		return routeapi.RouteMutationResponse{}, errors.New("controlplane: Route creation produced no workflow")
	}
	if record.State != clusterstate.WorkflowRouteTombstone &&
		(record.State != clusterstate.WorkflowRouteReady || request.MinRouteRevision > record.Revision.LogIndex) {
		fencedRecord, fenced, fenceErr := s.fenceRouteAfterNodeEpochAdvance(ctx, *record)
		if fenceErr != nil {
			return routeapi.RouteMutationResponse{}, fenceErr
		}
		if fenced {
			record = &fencedRecord
		}
	}
	if record.State != clusterstate.WorkflowRouteTombstone || record.Tombstone.PlacementFailure != nil {
		if !sandboxInputMatchesRecord(request.Input, *record) {
			return routeConflict(request.Group, request.RouteKey, errors.New("route key is bound to a different Sandbox request")), nil
		}
	}

	runner, err := s.startingCoordinator()
	if err != nil {
		return routeapi.RouteMutationResponse{}, err
	}
	switch record.State {
	case clusterstate.WorkflowRouteStarting:
		result, runErr := runner.RunRoute(ctx, *record)
		if runErr != nil && !errors.Is(runErr, ErrRevisionConflict) {
			return routeapi.RouteMutationResponse{}, runErr
		}
		if result.Status == coordinator.RunConflict {
			return routeConflict(record.Group, record.RouteKey, errors.New(result.Reason)), nil
		}
	case clusterstate.WorkflowRouteTombstone:
		if record.Tombstone.PlacementFailure != nil {
			restarted, restartErr := runner.StartRouteAfterPlacementFailure(ctx, *record)
			if restartErr != nil {
				return routeapi.RouteMutationResponse{}, restartErr
			}
			if _, runErr := runner.RunRoute(ctx, restarted); runErr != nil && !errors.Is(runErr, ErrRevisionConflict) {
				return routeapi.RouteMutationResponse{}, runErr
			}
		} else {
			restarted, restartErr := s.restartTerminalRoute(ctx, request, *record)
			if restartErr != nil {
				return routeapi.RouteMutationResponse{}, restartErr
			}
			if _, runErr := runner.RunRoute(ctx, restarted); runErr != nil && !errors.Is(runErr, ErrRevisionConflict) {
				return routeapi.RouteMutationResponse{}, runErr
			}
		}
	case clusterstate.WorkflowRouteReady:
		return routeResponse(*record), nil
	case clusterstate.WorkflowRoutePaused:
		next := clusterstate.RouteWorkflowRecord{
			Group: record.Group, RouteKey: record.RouteKey, State: clusterstate.WorkflowRouteResuming,
			Revision: record.Revision,
			Resuming: &clusterstate.ResumingRouteState{
				Execution: record.Paused.Execution, Intent: record.Paused.ResumeIntent,
			},
		}
		committed, commitErr := s.store.CommitRouteWorkflow(ctx, record.Revision, next)
		if commitErr != nil {
			return routeapi.RouteMutationResponse{}, commitErr
		}
		record = &committed
		_ = s.driveResuming(ctx, *record)
	case clusterstate.WorkflowRouteResuming:
		_ = s.driveResuming(ctx, *record)
	case clusterstate.WorkflowRouteDeleting:
		return routeResponse(*record), nil
	}
	return s.waitRoute(ctx, request.Group, request.RouteKey)
}

func (s *RegistryService) createRoute(
	ctx context.Context,
	request routeapi.ReserveSandboxRequest,
) (*clusterstate.RouteWorkflowRecord, error) {
	sandboxID, err := s.config.NewObjectID()
	if err != nil {
		return nil, err
	}
	round, err := s.planSandbox(ctx, request.Group, request.RouteKey, sandboxID, request.Input, nil)
	if err != nil {
		return nil, err
	}
	record := clusterstate.RouteWorkflowRecord{
		Group: request.Group, RouteKey: request.RouteKey, State: clusterstate.WorkflowRouteStarting,
		Starting: &clusterstate.RouteStartingState{
			SandboxID: sandboxID, PlacementRound: 1,
			CandidatePool: round.Candidates, Intent: round.Intent,
		},
	}
	committed, err := s.store.CreateRouteWorkflow(ctx, record)
	return &committed, err
}

func (s *RegistryService) restartTerminalRoute(
	ctx context.Context,
	request routeapi.ReserveSandboxRequest,
	record clusterstate.RouteWorkflowRecord,
) (clusterstate.RouteWorkflowRecord, error) {
	sandboxID, err := s.config.NewObjectID()
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	var excluded []string
	if record.Tombstone != nil && record.Tombstone.PlacementFailure != nil {
		excluded = placementCandidateNodeIDs(record.Tombstone.PlacementFailure.CandidatePool)
	}
	round, err := s.planSandbox(ctx, request.Group, request.RouteKey, sandboxID, request.Input, excluded)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	next := clusterstate.RouteWorkflowRecord{
		Group: record.Group, RouteKey: record.RouteKey, State: clusterstate.WorkflowRouteStarting,
		Revision: record.Revision,
		Starting: &clusterstate.RouteStartingState{
			SandboxID: sandboxID, PlacementRound: 1,
			CandidatePool: round.Candidates, Intent: round.Intent,
		},
	}
	return s.store.CommitRouteWorkflow(ctx, record.Revision, next)
}

func (s *RegistryService) ResumeSandbox(
	ctx context.Context,
	request routeapi.ResumeSandboxRequest,
) (routeapi.RouteMutationResponse, error) {
	if err := s.validateRouteIdentity(request.RequestIdentity, request.Group, request.RouteKey); err != nil {
		return routeConflict(request.Group, request.RouteKey, err), nil
	}
	if response, done := s.requireRouteLeader(ctx, request.RequestIdentity, request.Group, request.RouteKey, request.MinRouteRevision); done {
		return response, nil
	}
	record, err := s.store.ReadRouteWorkflow(ctx, request.Group, request.RouteKey)
	if err != nil {
		return routeapi.RouteMutationResponse{}, err
	}
	if record == nil {
		return routeConflict(request.Group, request.RouteKey, errors.New("Route is not found")), nil
	}
	if record.State != clusterstate.WorkflowRouteTombstone {
		fencedRecord, fenced, fenceErr := s.fenceRouteAfterNodeEpochAdvance(ctx, *record)
		if fenceErr != nil {
			return routeapi.RouteMutationResponse{}, fenceErr
		}
		if fenced {
			return routeResponse(fencedRecord), nil
		}
	}
	switch record.State {
	case clusterstate.WorkflowRouteReady:
		return routeResponse(*record), nil
	case clusterstate.WorkflowRoutePaused:
		next := clusterstate.RouteWorkflowRecord{
			Group: record.Group, RouteKey: record.RouteKey, State: clusterstate.WorkflowRouteResuming,
			Revision: record.Revision,
			Resuming: &clusterstate.ResumingRouteState{
				Execution: record.Paused.Execution, Intent: record.Paused.ResumeIntent,
			},
		}
		committed, commitErr := s.store.CommitRouteWorkflow(ctx, record.Revision, next)
		if commitErr != nil {
			return routeapi.RouteMutationResponse{}, commitErr
		}
		record = &committed
	case clusterstate.WorkflowRouteResuming:
	default:
		return routeConflict(request.Group, request.RouteKey, fmt.Errorf("Route state %s cannot resume", record.State)), nil
	}
	_ = s.driveResuming(ctx, *record)
	return s.waitRoute(ctx, request.Group, request.RouteKey)
}

func (s *RegistryService) DeleteSandbox(
	ctx context.Context,
	request routeapi.DeleteSandboxRequest,
) (routeapi.RouteMutationResponse, error) {
	if err := s.validateRouteIdentity(request.RequestIdentity, request.Group, request.RouteKey); err != nil {
		return routeConflict(request.Group, request.RouteKey, err), nil
	}
	if response, done := s.requireRouteLeader(ctx, request.RequestIdentity, request.Group, request.RouteKey, request.MinRouteRevision); done {
		return response, nil
	}
	record, err := s.store.ReadRouteWorkflow(ctx, request.Group, request.RouteKey)
	if err != nil {
		return routeapi.RouteMutationResponse{}, err
	}
	if record == nil {
		return routeConflict(request.Group, request.RouteKey, errors.New("Route is not found")), nil
	}
	if record.State != clusterstate.WorkflowRouteTombstone {
		fencedRecord, fenced, fenceErr := s.fenceRouteAfterNodeEpochAdvance(ctx, *record)
		if fenceErr != nil {
			return routeapi.RouteMutationResponse{}, fenceErr
		}
		if fenced {
			return routeResponse(fencedRecord), nil
		}
	}
	if record.State == clusterstate.WorkflowRouteTombstone {
		return routeResponse(*record), nil
	}
	if record.State != clusterstate.WorkflowRouteDeleting {
		execution, found := routeExecution(*record)
		if !found {
			return routeConflict(request.Group, request.RouteKey, errors.New("Route is not deletable")), nil
		}
		spec, digest, specErr := immutableDeleteSpec(execution)
		if specErr != nil {
			return routeapi.RouteMutationResponse{}, specErr
		}
		next := clusterstate.RouteWorkflowRecord{
			Group: record.Group, RouteKey: record.RouteKey, State: clusterstate.WorkflowRouteDeleting,
			Revision: record.Revision,
			Deleting: &clusterstate.DeletingRouteState{
				Execution: execution, DeleteSpec: spec, DeleteSpecDigest: digest,
				LastEventSeq: execution.LastEventSeq,
			},
		}
		committed, commitErr := s.store.CommitRouteWorkflow(ctx, record.Revision, next)
		if commitErr != nil {
			return routeapi.RouteMutationResponse{}, commitErr
		}
		record = &committed
	}
	_ = s.driveDeleting(ctx, *record)
	return s.waitRoute(ctx, request.Group, request.RouteKey)
}

func (s *RegistryService) ListRoutes(ctx context.Context, request routeapi.ListRoutesRequest) (routeapi.ListRoutesResponse, error) {
	expected, err := s.store.RouteBucketRequestIdentity(request.Group, request.Bucket)
	if err != nil {
		return routeapi.ListRoutesResponse{}, err
	}
	if expected != request.RequestIdentity {
		return routeapi.ListRoutesResponse{Bucket: request.Bucket, Reason: "signed Registry History Generation or Route bucket identity mismatch"}, nil
	}
	routes := make([]routeapi.ListedRoute, 0)
	result, err := s.store.ReadRouteBucket(
		ctx, request.Group, request.Bucket, request.AfterRouteKey, request.Limit, request.Strong,
	)
	if err != nil {
		return routeapi.ListRoutesResponse{}, err
	}
	if !result.Available {
		return routeapi.ListRoutesResponse{Bucket: request.Bucket, Reason: result.Reason}, nil
	}
	for _, record := range result.Routes {
		routes = append(routes, routeapi.ListedRoute{
			RouteKey: record.RouteKey, State: record.State, NodeID: record.NodeID, TemplateRef: record.TemplateRef,
			Presentation: record.Presentation.Clone(),
		})
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].RouteKey < routes[j].RouteKey })
	return routeapi.ListRoutesResponse{
		Routes: routes, Bucket: request.Bucket, SnapshotRevision: result.SnapshotRevision,
		NextRouteKey: result.NextRouteKey,
	}, nil
}

func (s *RegistryService) WatchRoutes(
	ctx context.Context,
	request routeapi.WatchRoutesRequest,
) (routeapi.WatchRoutesResponse, error) {
	expected, err := s.store.RouteBucketRequestIdentity(request.Group, request.Bucket)
	if err != nil {
		return routeapi.WatchRoutesResponse{}, err
	}
	if expected != request.RequestIdentity {
		return routeapi.WatchRoutesResponse{
			Bucket: request.Bucket, Reason: "signed Registry History Generation or Route bucket identity mismatch",
		}, nil
	}
	result, err := s.store.ReadRouteChangefeed(
		ctx, request.Group, request.Bucket, request.AfterRevision, request.Limit, request.Strong,
	)
	if err != nil {
		return routeapi.WatchRoutesResponse{}, err
	}
	if !result.Available {
		return routeapi.WatchRoutesResponse{Bucket: request.Bucket, Reason: result.Reason}, nil
	}
	changes := make([]routeapi.RouteChange, len(result.Changes))
	for index, change := range result.Changes {
		changes[index] = routeapi.RouteChange{
			Revision: change.Revision, Bucket: change.Bucket, Group: change.Group,
			RouteKey: change.RouteKey, State: change.State,
		}
	}
	return routeapi.WatchRoutesResponse{
		Available: true, Reset: result.Reset, Bucket: request.Bucket,
		FloorRevision: result.FloorRevision, HeadRevision: result.HeadRevision,
		CursorRevision: result.CursorRevision, Changes: changes,
	}, nil
}

func (s *RegistryService) RegisterBuild(
	ctx context.Context,
	request routeapi.RegisterBuildRequest,
) (routeapi.BuildMutationResponse, error) {
	if err := s.validateBuildIdentity(request.RequestIdentity, request.Group, request.BuildID); err != nil {
		return buildConflict(request.Group, err), nil
	}
	local, err := s.store.ReadBuild(ctx, routeapi.ReadBuildRequest{
		RequestIdentity: request.RequestIdentity, Group: request.Group, BuildID: request.BuildID,
		MinBuildRevision: request.MinBuildRevision,
	})
	if err == nil && local.Outcome == routeapi.ReadReady {
		if local.Build == nil || !buildInputMatchesIntent(request.Input, local.Build.Intent) {
			return buildConflict(request.Group, errors.New("Build ID is bound to a different request")), nil
		}
		return routeapi.BuildMutationResponse{
			Outcome: buildOutcome(local.BuildState), Group: request.Group,
			State: local.BuildState, Build: local.Build, BuildRevision: local.BuildRevision,
		}, nil
	}
	leader, leaderErr := s.store.LocalCoordinator(request.ShardID)
	if leaderErr != nil || !leader {
		response := routeapi.BuildMutationResponse{Outcome: routeapi.MutationNeedLeader, Group: request.Group}
		if err == nil {
			response.LeaderHint, response.Reason = local.LeaderHint, local.Reason
		}
		return response, nil
	}
	record, err := s.store.ReadBuildWorkflow(ctx, request.Group, request.BuildID)
	if err != nil {
		return routeapi.BuildMutationResponse{}, err
	}
	if record == nil {
		record, err = s.createBuild(ctx, request)
		if errors.Is(err, ErrRevisionConflict) {
			record, err = s.store.ReadBuildWorkflow(ctx, request.Group, request.BuildID)
		}
		if err != nil {
			return routeapi.BuildMutationResponse{}, err
		}
	}
	if record == nil {
		return routeapi.BuildMutationResponse{}, errors.New("controlplane: Build creation produced no workflow")
	}
	if !buildInputMatchesRecord(request.Input, *record) {
		return buildConflict(request.Group, errors.New("Build ID is bound to a different request")), nil
	}
	if record.State == clusterstate.BuildStarting {
		runner, coordinatorErr := s.startingCoordinator()
		if coordinatorErr != nil {
			return routeapi.BuildMutationResponse{}, coordinatorErr
		}
		result, runErr := runner.RunBuild(ctx, *record)
		if runErr != nil && !errors.Is(runErr, ErrRevisionConflict) {
			return routeapi.BuildMutationResponse{}, runErr
		}
		if result.Status == coordinator.RunConflict {
			return buildConflict(request.Group, errors.New(result.Reason)), nil
		}
	}
	return s.waitBuild(ctx, request.Group, request.BuildID)
}

func (s *RegistryService) createBuild(
	ctx context.Context,
	request routeapi.RegisterBuildRequest,
) (*clusterstate.BuildRecord, error) {
	round, err := s.planBuild(ctx, request.Group, request.BuildID, request.Input)
	if err != nil {
		return nil, err
	}
	record := clusterstate.BuildRecord{
		Group: request.Group, BuildID: request.BuildID, State: clusterstate.BuildStarting,
		Starting: &clusterstate.BuildStartingState{
			BuildID: request.BuildID, CandidatePool: round.Candidates, Intent: round.Intent,
		},
	}
	committed, err := s.store.CreateBuildWorkflow(ctx, record)
	return &committed, err
}

func (s *RegistryService) planSandbox(
	ctx context.Context,
	group string,
	routeKey string,
	sandboxID string,
	input routeapi.SandboxInput,
	excludedNodeIDs []string,
) (coordinator.SandboxRound, error) {
	nodes, err := s.store.Catalog(ctx)
	if err != nil {
		return coordinator.SandboxRound{}, err
	}
	response, err := s.planner.Plan(ctx, placer.PlanRequest{
		Kind: placer.PlanSandbox, Group: group, RouteKey: routeKey, Nodes: nodes,
		ExcludedNodeIDs: excludedNodeIDs, TargetRuntimeDigest: input.TargetRuntimeDigest,
		Sandbox: &placer.SandboxPlanInput{
			SandboxID: sandboxID, TemplateRef: input.TemplateRef, Config: cloneStringMap(input.Config),
			TimeoutSeconds: input.TimeoutSeconds, Demand: input.Demand,
			Request: input.Request,
		},
	})
	if err != nil {
		return coordinator.SandboxRound{}, err
	}
	if _, err := clusterstate.ParseSandboxDispatchSpec(response.DispatchSpec); err != nil {
		return coordinator.SandboxRound{}, fmt.Errorf("controlplane: invalid Sandbox dispatch spec: %w", err)
	}
	intent, err := clusterstate.NewDispatchIntent(response.NormalizedDemand, response.DispatchSpec, response.ProviderPolicyVersion)
	if err != nil {
		return coordinator.SandboxRound{}, err
	}
	if len(response.Candidates) == 0 || len(response.Candidates) > placement.DefaultCandidateCount {
		return coordinator.SandboxRound{}, fmt.Errorf("controlplane: Placer returned %d Sandbox candidates, want 1..%d", len(response.Candidates), placement.DefaultCandidateCount)
	}
	if !sandboxInputMatchesIntent(input, intent) {
		return coordinator.SandboxRound{}, errors.New("controlplane: Placer Sandbox intent does not match the requested immutable inputs")
	}
	if err := validatePlanCandidates(response.Candidates, input.TargetRuntimeDigest); err != nil {
		return coordinator.SandboxRound{}, err
	}
	return coordinator.SandboxRound{
		SandboxID: sandboxID, Candidates: append([]clusterstate.PlacementCandidate(nil), response.Candidates...),
		Intent: intent,
	}, nil
}

func (s *RegistryService) planBuild(
	ctx context.Context,
	group string,
	buildID string,
	input routeapi.BuildInput,
) (coordinator.SandboxRound, error) {
	nodes, err := s.store.Catalog(ctx)
	if err != nil {
		return coordinator.SandboxRound{}, err
	}
	response, err := s.planner.Plan(ctx, placer.PlanRequest{
		Kind: placer.PlanBuild, Group: group, Nodes: nodes,
		TargetRuntimeDigest: input.TargetRuntimeDigest,
		Build: &placer.BuildPlanInput{
			BuildID: buildID, TemplateID: input.TemplateID, Profile: input.Profile,
			Names: append([]string(nil), input.Names...), Aliases: append([]string(nil), input.Aliases...),
			Metadata: cloneStringMap(input.Metadata), CPUCount: input.CPUCount, MemoryMB: input.MemoryMB,
			Demand: input.Demand, Request: input.Request,
		},
	})
	if err != nil {
		return coordinator.SandboxRound{}, err
	}
	if _, err := clusterstate.ParseBuildDispatchSpec(response.DispatchSpec); err != nil {
		return coordinator.SandboxRound{}, fmt.Errorf("controlplane: invalid Build dispatch spec: %w", err)
	}
	intent, err := clusterstate.NewDispatchIntent(response.NormalizedDemand, response.DispatchSpec, response.ProviderPolicyVersion)
	if err != nil {
		return coordinator.SandboxRound{}, err
	}
	if len(response.Candidates) == 0 || len(response.Candidates) > placement.DefaultCandidateCount {
		return coordinator.SandboxRound{}, fmt.Errorf("controlplane: Placer returned %d Build candidates, want 1..%d", len(response.Candidates), placement.DefaultCandidateCount)
	}
	if !buildInputMatchesIntent(input, intent) {
		return coordinator.SandboxRound{}, errors.New("controlplane: Placer Build intent does not match the requested immutable inputs")
	}
	if err := validatePlanCandidates(response.Candidates, input.TargetRuntimeDigest); err != nil {
		return coordinator.SandboxRound{}, err
	}
	return coordinator.SandboxRound{Candidates: append([]clusterstate.PlacementCandidate(nil), response.Candidates...), Intent: intent}, nil
}

func validatePlanCandidates(
	candidates []clusterstate.PlacementCandidate,
	targetRuntimeDigest string,
) error {
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate.NodeID == "" {
			return errors.New("controlplane: Placer returned an empty node ID")
		}
		if _, duplicate := seen[candidate.NodeID]; duplicate {
			return fmt.Errorf("controlplane: Placer returned duplicate node %q", candidate.NodeID)
		}
		seen[candidate.NodeID] = struct{}{}
		if candidate.RuntimeDigest != targetRuntimeDigest {
			return fmt.Errorf("controlplane: Placer returned node %q with the wrong target runtime", candidate.NodeID)
		}
	}
	return nil
}

func (s *RegistryService) startingCoordinator() (*coordinator.StartingCoordinator, error) {
	identity, err := s.store.ServeIdentity()
	if err != nil {
		return nil, err
	}
	return coordinator.NewStartingCoordinator(coordinator.Config{
		ServeIdentity: identity, PlacementRoundLimit: s.config.PlacementRoundLimit,
	}, s.prober, s.dispatcher, s.store, s.store, sandboxRoundSource{service: s})
}

type sandboxRoundSource struct{ service *RegistryService }

func (r sandboxRoundSource) NextSandboxRound(
	ctx context.Context,
	group string,
	routeKey string,
	_ uint64,
	previous clusterstate.DispatchIntent,
	excludedNodeIDs []string,
) (coordinator.SandboxRound, error) {
	sandboxID, err := r.service.config.NewObjectID()
	if err != nil {
		return coordinator.SandboxRound{}, err
	}
	demand, err := placement.ParseNormalizedDemand(previous.NormalizedDemand)
	if err != nil || demand.Sandbox == nil {
		return coordinator.SandboxRound{}, errors.Join(err, errors.New("controlplane: prior Sandbox demand is unavailable"))
	}
	spec, err := clusterstate.ParseSandboxDispatchSpec(previous.DispatchSpec)
	if err != nil {
		return coordinator.SandboxRound{}, err
	}
	return r.service.planSandbox(ctx, group, routeKey, sandboxID, routeapi.SandboxInput{
		TemplateRef: spec.TemplateRef, Config: spec.RequestedConfig,
		TimeoutSeconds: spec.TimeoutSeconds, Demand: *demand.Sandbox, Request: spec.Request,
	}, excludedNodeIDs)
}

func placementCandidateNodeIDs(candidates []clusterstate.PlacementCandidate) []string {
	result := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.NodeID != "" {
			result = append(result, candidate.NodeID)
		}
	}
	return result
}

func (s *RegistryService) waitRoute(ctx context.Context, group, routeKey string) (routeapi.RouteMutationResponse, error) {
	timer := time.NewTimer(s.config.ParkTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	for {
		record, err := s.store.ReadRouteWorkflow(ctx, group, routeKey)
		if err != nil {
			return routeapi.RouteMutationResponse{}, err
		}
		if record == nil {
			return routeConflict(group, routeKey, errors.New("Route disappeared")), nil
		}
		if record.State == clusterstate.WorkflowRouteReady || record.State == clusterstate.WorkflowRouteTombstone ||
			record.State == clusterstate.WorkflowRoutePaused {
			return routeResponse(*record), nil
		}
		select {
		case <-ctx.Done():
			return routeResponse(*record), nil
		case <-timer.C:
			return routeResponse(*record), nil
		case <-ticker.C:
		}
	}
}

func (s *RegistryService) waitBuild(ctx context.Context, group, buildID string) (routeapi.BuildMutationResponse, error) {
	timer := time.NewTimer(s.config.ParkTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	for {
		record, err := s.store.ReadBuildWorkflow(ctx, group, buildID)
		if err != nil {
			return routeapi.BuildMutationResponse{}, err
		}
		if record == nil {
			return buildConflict(group, errors.New("Build disappeared")), nil
		}
		if record.State != clusterstate.BuildStarting {
			return buildResponse(*record), nil
		}
		select {
		case <-ctx.Done():
			return buildResponse(*record), nil
		case <-timer.C:
			return buildResponse(*record), nil
		case <-ticker.C:
		}
	}
}

func routeResponse(record clusterstate.RouteWorkflowRecord) routeapi.RouteMutationResponse {
	response := routeapi.RouteMutationResponse{
		Outcome: routeapi.MutationPending, Group: record.Group, RouteKey: record.RouteKey,
		State: record.State, RouteRevision: record.Revision.LogIndex,
	}
	switch record.State {
	case clusterstate.WorkflowRouteReady:
		response.Outcome, response.Route = routeapi.MutationReady, record.Ready
	case clusterstate.WorkflowRouteTombstone:
		response.Outcome = routeapi.MutationTerminal
		if record.Tombstone.PlacementFailure != nil {
			response.Reason = record.Tombstone.PlacementFailure.Reason
		} else {
			response.Reason = record.Tombstone.TerminalReason
		}
	}
	return response
}

func routeConflict(group, routeKey string, err error) routeapi.RouteMutationResponse {
	return routeapi.RouteMutationResponse{
		Outcome: routeapi.MutationConflict, Group: group, RouteKey: routeKey, Reason: err.Error(),
	}
}

func buildResponse(record clusterstate.BuildRecord) routeapi.BuildMutationResponse {
	response := routeapi.BuildMutationResponse{
		Outcome: buildOutcome(record.State), Group: record.Group, State: record.State,
		Build: record.Projection, BuildRevision: record.Revision.LogIndex,
	}
	if record.Tombstone != nil {
		response.Reason = record.Tombstone.PlacementFailure.Reason
	}
	return response
}

func buildOutcome(state clusterstate.BuildWorkflowState) string {
	switch state {
	case clusterstate.BuildRegistered:
		return routeapi.MutationReady
	case clusterstate.BuildTombstone:
		return routeapi.MutationTerminal
	default:
		return routeapi.MutationPending
	}
}

func buildConflict(group string, err error) routeapi.BuildMutationResponse {
	return routeapi.BuildMutationResponse{Outcome: routeapi.MutationConflict, Group: group, Reason: err.Error()}
}

func (s *RegistryService) requireRouteLeader(
	ctx context.Context,
	identity routeapi.RequestIdentity,
	group string,
	routeKey string,
	minRevision uint64,
) (routeapi.RouteMutationResponse, bool) {
	leader, err := s.store.LocalCoordinator(identity.ShardID)
	if err == nil && leader {
		return routeapi.RouteMutationResponse{}, false
	}
	local, _ := s.store.ReadRoute(ctx, routeapi.ReadRouteRequest{
		RequestIdentity: identity, Group: group, RouteKey: routeKey, MinRouteRevision: minRevision,
	})
	return routeapi.RouteMutationResponse{
		Outcome: routeapi.MutationNeedLeader, Group: group, RouteKey: routeKey,
		LeaderHint: local.LeaderHint, Reason: local.Reason,
	}, true
}

func (s *RegistryService) validateRouteIdentity(identity routeapi.RequestIdentity, group, routeKey string) error {
	expected, err := s.store.RouteRequestIdentity(group, routeKey)
	if err != nil {
		return err
	}
	if expected != identity {
		return errors.New("controlplane: Route request has a stale Registry History Generation, system epoch, Registry Layout, or shard")
	}
	return nil
}

func (s *RegistryService) validateBuildIdentity(identity routeapi.RequestIdentity, group, buildID string) error {
	expected, err := s.store.BuildRequestIdentity(group, buildID)
	if err != nil {
		return err
	}
	if expected != identity {
		return errors.New("controlplane: Build request has a stale Registry History Generation, system epoch, Registry Layout, or shard")
	}
	return nil
}

func sandboxInputMatchesRecord(input routeapi.SandboxInput, record clusterstate.RouteWorkflowRecord) bool {
	intent, found := routeIntent(record)
	if !found {
		return true
	}
	return sandboxInputMatchesIntent(input, intent)
}

func sandboxInputMatchesIntent(input routeapi.SandboxInput, intent clusterstate.DispatchIntent) bool {
	demand, err := placement.ParseNormalizedDemand(intent.NormalizedDemand)
	if err != nil || demand.Sandbox == nil || !sandboxDemandMatches(input.Demand, *demand.Sandbox) {
		return false
	}
	spec, err := clusterstate.ParseSandboxDispatchSpec(intent.DispatchSpec)
	if err != nil || spec.TimeoutSeconds != input.TimeoutSeconds ||
		spec.TargetRuntimeDigest != input.TargetRuntimeDigest {
		return false
	}
	normalizedRequest, err := api.RewriteSandboxCreateEnvelope(
		input.Request, spec.TemplateRef, spec.TimeoutSeconds, spec.Config,
	)
	return err == nil && reflect.DeepEqual(spec.RequestedConfig, clusterstate.WithoutSystemMetadata(input.Config)) &&
		reflect.DeepEqual(spec.Request, normalizedRequest)
}

func sandboxDemandMatches(input, persisted placement.SandboxDemand) bool {
	if input.SlotUnits != persisted.SlotUnits {
		return false
	}
	return (input.StartupBudgetMemory == 0 || input.StartupBudgetMemory == persisted.StartupBudgetMemory) &&
		(input.FloorMemory == 0 || input.FloorMemory == persisted.FloorMemory) &&
		(input.AllocatableAtSnapshot == 0 || input.AllocatableAtSnapshot == persisted.AllocatableAtSnapshot)
}

func routeIntent(record clusterstate.RouteWorkflowRecord) (clusterstate.DispatchIntent, bool) {
	switch record.State {
	case clusterstate.WorkflowRouteStarting:
		return record.Starting.Intent, true
	case clusterstate.WorkflowRouteReady:
		return record.Ready.Intent, true
	case clusterstate.WorkflowRoutePaused:
		return record.Paused.Execution.Intent, true
	case clusterstate.WorkflowRouteResuming:
		return record.Resuming.Execution.Intent, true
	case clusterstate.WorkflowRouteDeleting:
		return record.Deleting.Execution.Intent, true
	case clusterstate.WorkflowRouteTombstone:
		if record.Tombstone.PlacementFailure != nil {
			return record.Tombstone.PlacementFailure.Intent, true
		}
	}
	return clusterstate.DispatchIntent{}, false
}

func buildInputMatchesRecord(input routeapi.BuildInput, record clusterstate.BuildRecord) bool {
	var intent clusterstate.DispatchIntent
	switch {
	case record.Starting != nil:
		intent = record.Starting.Intent
	case record.Projection != nil:
		intent = record.Projection.Intent
	case record.Tombstone != nil:
		intent = record.Tombstone.PlacementFailure.Intent
	default:
		return false
	}
	return buildInputMatchesIntent(input, intent)
}

func buildInputMatchesIntent(input routeapi.BuildInput, intent clusterstate.DispatchIntent) bool {
	demand, err := placement.NormalizeBuildDemand(input.Demand)
	if err != nil || !bytes.Equal(demand, intent.NormalizedDemand) {
		return false
	}
	spec, err := clusterstate.ParseBuildDispatchSpec(intent.DispatchSpec)
	if err != nil {
		return false
	}
	return spec.TemplateID == input.TemplateID && spec.Profile == input.Profile &&
		spec.CPUCount == input.CPUCount && spec.MemoryMB == input.MemoryMB &&
		spec.TargetRuntimeDigest == input.TargetRuntimeDigest &&
		slices.Equal(spec.Names, input.Names) && slices.Equal(spec.Aliases, input.Aliases) &&
		reflect.DeepEqual(spec.Metadata, clusterstate.WithoutSystemMetadata(input.Metadata)) &&
		buildRequestMatches(input.Request, spec)
}

func buildRequestMatches(request clusterstate.NodeRequestEnvelopeV1, spec clusterstate.BuildDispatchSpecV1) bool {
	name := ""
	if len(spec.Names) > 0 {
		name = spec.Names[0]
	}
	normalized, err := api.RewriteBuildRegisterEnvelope(request, api.RegisterSpec{
		Name: name, Tags: append([]string(nil), spec.Aliases...), Profile: spec.Profile,
		CPUCount: spec.CPUCount, MemoryMB: spec.MemoryMB, Metadata: cloneStringMap(spec.Metadata),
	})
	return err == nil && reflect.DeepEqual(normalized, spec.Request)
}

func routeExecution(record clusterstate.RouteWorkflowRecord) (clusterstate.ReadyRoute, bool) {
	switch record.State {
	case clusterstate.WorkflowRouteReady:
		return *record.Ready, true
	case clusterstate.WorkflowRoutePaused:
		return record.Paused.Execution, true
	case clusterstate.WorkflowRouteResuming:
		return record.Resuming.Execution, true
	case clusterstate.WorkflowRouteDeleting:
		return record.Deleting.Execution, true
	default:
		return clusterstate.ReadyRoute{}, false
	}
}

func immutableDeleteSpec(execution clusterstate.ReadyRoute) ([]byte, string, error) {
	value := struct {
		Version       uint16 `json:"version"`
		SandboxID     string `json:"sandbox_id"`
		BindingDigest string `json:"binding_digest"`
	}{Version: 1, SandboxID: execution.SandboxID, BindingDigest: execution.BindingDigest}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(raw)
	return raw, hex.EncodeToString(digest[:]), nil
}

func (s *RegistryService) driveResuming(ctx context.Context, record clusterstate.RouteWorkflowRecord) error {
	if record.State != clusterstate.WorkflowRouteResuming || record.Resuming == nil {
		return errors.New("controlplane: Route is not RESUMING")
	}
	identity, err := s.store.ServeIdentity()
	if err != nil {
		return err
	}
	execution, intent := record.Resuming.Execution, record.Resuming.Intent
	command := &routesync.Command{
		CmdID: newCommandID(), Kind: routesync.CmdSandboxResume, SID: execution.SandboxID,
		RegistryGeneration: execution.RegistryGeneration, BindingDigest: execution.BindingDigest,
		DemandDigest: intent.DemandDigest, DispatchSpecDigest: intent.DispatchSpecDigest,
		Group: record.Group, RouteKey: record.RouteKey,
		NormalizedDemand: append([]byte(nil), intent.NormalizedDemand...),
		DispatchSpec:     append([]byte(nil), intent.DispatchSpec...), ProviderPolicy: intent.ProviderPolicyVersion,
	}
	_, _, err = s.commands.SendNodeCommand(
		ctx, identity, execution.NodeID, execution.NodeEpoch, execution.DataEndpoint, command,
	)
	return err
}

func (s *RegistryService) driveDeleting(ctx context.Context, record clusterstate.RouteWorkflowRecord) error {
	if record.State != clusterstate.WorkflowRouteDeleting || record.Deleting == nil {
		return errors.New("controlplane: Route is not DELETING")
	}
	identity, err := s.store.ServeIdentity()
	if err != nil {
		return err
	}
	execution := record.Deleting.Execution
	command := &routesync.Command{
		CmdID: newCommandID(), Kind: routesync.CmdSandboxDelete, SID: execution.SandboxID,
		RegistryGeneration: execution.RegistryGeneration, BindingDigest: execution.BindingDigest,
		Group: record.Group, RouteKey: record.RouteKey,
		DispatchSpec:       append([]byte(nil), record.Deleting.DeleteSpec...),
		DispatchSpecDigest: record.Deleting.DeleteSpecDigest,
	}
	_, _, err = s.commands.SendNodeCommand(
		ctx, identity, execution.NodeID, execution.NodeEpoch, execution.DataEndpoint, command,
	)
	return err
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

// Run keeps the bounded local Permit fresh and recovers only shards for which
// this member is currently the consensus leader. All actions remain protected
// by the workflow CAS and exact node Binding.
func (s *RegistryService) Run(ctx context.Context) error {
	defer s.stopCompaction()
	if _, err := s.store.RefreshPermitGrant(ctx); err != nil {
		return err
	}
	permitTicker := time.NewTicker(s.config.PermitRefreshInterval)
	recoveryTicker := time.NewTicker(s.config.RecoveryScanInterval)
	defer permitTicker.Stop()
	defer recoveryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-permitTicker.C:
			_, _ = s.store.RefreshPermitGrant(ctx)
		case <-recoveryTicker.C:
			s.scanRecoveryWindow(ctx)
		}
	}
}

func (s *RegistryService) scanRecoveryWindow(ctx context.Context) {
	s.scanMu.Lock()
	start := s.nextShard
	count := min(s.config.RecoveryShardsPerScan, s.store.VirtualShardCount())
	s.nextShard = (start + count) % s.store.VirtualShardCount()
	s.scanMu.Unlock()
	sem := make(chan struct{}, s.config.RecoveryWorkers)
	var workers sync.WaitGroup
	for offset := uint32(0); offset < count; offset++ {
		shardID := (start + offset) % s.store.VirtualShardCount()
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			workers.Wait()
			return
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-sem }()
			_ = s.recoverShard(ctx, shardID)
		}()
	}
	workers.Wait()
}

func (s *RegistryService) recoverShard(ctx context.Context, shardID uint32) error {
	leader, err := s.store.LocalCoordinator(shardID)
	if err != nil || !leader {
		return err
	}
	worker, err := s.startingCoordinator()
	if err != nil {
		return err
	}
	systemState, err := s.store.ReadSystem(ctx)
	if err != nil {
		return err
	}
	after := ""
	for {
		pending, err := s.store.PendingWorkflows(ctx, shardID, after, s.config.PendingWorkflowsPerPage)
		if err != nil {
			return err
		}
		for _, workflow := range pending.Workflows {
			switch {
			case workflow.Fence != nil:
				s.scheduleFenceCompaction(ctx, *workflow.Fence)
			case workflow.Route != nil:
				record := *workflow.Route
				if record.State != clusterstate.WorkflowRouteTombstone {
					fencedRecord, fenced, fenceErr := s.fenceRouteWithSystemState(ctx, systemState, record)
					if fenceErr != nil {
						continue
					}
					if fenced {
						record = fencedRecord
					}
				}
				if len(record.Finalizations) != 0 {
					finalized, changed, finalizeErr := s.completeRouteFinalization(ctx, record)
					if finalizeErr == nil && changed {
						record = finalized
					}
				}
				switch record.State {
				case clusterstate.WorkflowRouteStarting:
					_, _ = worker.RunRoute(ctx, record)
				case clusterstate.WorkflowRouteTombstone:
					var fence clusterstate.ExecutionFence
					failure := record.Tombstone.PlacementFailure
					if failure != nil {
						fence, _ = clusterstate.NewPlacementFailureFence(
							record.Group, record.RouteKey, record.Revision.RegistryGeneration, *failure,
						)
					} else {
						fence = fenceFromTombstone(record)
					}
					if err := s.store.EnsureExecutionFence(ctx, fence); err != nil {
						continue
					}
					if failure != nil && failure.PlacementRound%s.config.PlacementRoundLimit != 0 {
						restarted, restartErr := worker.StartRouteAfterPlacementFailure(ctx, record)
						if restartErr == nil {
							_, _ = worker.RunRoute(ctx, restarted)
						}
					}
				case clusterstate.WorkflowRouteResuming:
					_ = s.driveResuming(ctx, record)
				case clusterstate.WorkflowRouteDeleting:
					_ = s.driveDeleting(ctx, record)
				}
			case workflow.Build != nil:
				record := *workflow.Build
				if len(record.Finalizations) != 0 {
					finalized, changed, finalizeErr := s.completeBuildFinalization(ctx, record)
					if finalizeErr == nil && changed {
						record = finalized
					}
				}
				if record.State == clusterstate.BuildStarting {
					_, _ = worker.RunBuild(ctx, record)
				}
			}
		}
		if pending.NextKey == "" {
			break
		}
		after = pending.NextKey
	}
	return s.renewShardKeyLeases(ctx, shardID)
}

func (s *RegistryService) renewShardKeyLeases(ctx context.Context, shardID uint32) error {
	s.leaseMu.Lock()
	after := s.leaseCursors[shardID]
	s.leaseMu.Unlock()
	page, err := s.store.LeaseBindings(ctx, shardID, after, s.config.PendingWorkflowsPerPage)
	if err != nil {
		return err
	}
	identity, err := s.store.ServeIdentity()
	if err != nil {
		return err
	}
	var renewalErrors []error
	for _, binding := range page.Bindings {
		if err := s.renewKeyLease(ctx, identity, binding); err != nil {
			renewalErrors = append(renewalErrors, err)
		}
	}
	s.leaseMu.Lock()
	s.leaseCursors[shardID] = page.NextKey
	s.leaseMu.Unlock()
	return errors.Join(renewalErrors...)
}

func (s *RegistryService) renewKeyLease(
	ctx context.Context,
	identity session.ServeIdentity,
	binding raftstore.LeaseBinding,
) error {
	if binding.RegistryGeneration != identity.RegistryGeneration {
		return errors.New("controlplane: lease Binding belongs to another Registry History Generation")
	}
	cacheKey := leaseBindingCacheKey(binding)
	now := time.Now().Unix()
	if !s.claimLeaseRenewal(cacheKey, now) {
		return nil
	}
	succeeded := false
	defer func() {
		s.leaseMu.Lock()
		delete(s.leaseRenewing, cacheKey)
		if !succeeded {
			delete(s.leaseExpiries, cacheKey)
		}
		s.leaseMu.Unlock()
	}()
	lease, err := s.planner.ResolveKeyLease(ctx, placer.KeyLeaseRequest{
		Group: binding.Group, AuthKeyFingerprint: binding.AuthKeyFingerprint,
		ManifestKeyFingerprint: binding.ManifestKeyFingerprint,
	})
	if err != nil {
		return err
	}
	if err := lease.Validate(); err != nil || lease.Group != binding.Group ||
		lease.AuthKey.Fingerprint != binding.AuthKeyFingerprint ||
		lease.ManifestKey.Fingerprint != binding.ManifestKeyFingerprint {
		return errors.New("controlplane: Provider returned a mismatched key lease")
	}
	if lease.ExpiresUnix <= now+int64(placer.NodeKeyLeaseRenewBefore/time.Second) {
		return errors.New("controlplane: Provider returned a key lease inside the renewal window")
	}
	want := routesync.NodeKeyLeaseRefV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: binding.Group,
		AuthKeyFingerprint:     binding.AuthKeyFingerprint,
		ManifestKeyFingerprint: binding.ManifestKeyFingerprint,
	}
	ack, sent, err := s.commands.InstallKeyLease(
		ctx, identity, binding.NodeID, binding.NodeEpoch, binding.DataEndpoint, lease,
	)
	if err != nil {
		return err
	}
	if !sent || ack != want {
		return errors.New("controlplane: node did not durably acknowledge the renewed key lease")
	}
	s.leaseMu.Lock()
	s.leaseExpiries[cacheKey] = lease.ExpiresUnix
	s.leaseMu.Unlock()
	succeeded = true
	return nil
}

func (s *RegistryService) claimLeaseRenewal(cacheKey string, now int64) bool {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if s.lastLeasePruneAt == 0 || now-s.lastLeasePruneAt >= int64(time.Hour/time.Second) {
		for key, expires := range s.leaseExpiries {
			if expires <= now {
				delete(s.leaseExpiries, key)
			}
		}
		s.lastLeasePruneAt = now
	}
	if _, active := s.leaseRenewing[cacheKey]; active ||
		s.leaseExpiries[cacheKey] > now+int64(placer.NodeKeyLeaseRenewBefore/time.Second) {
		return false
	}
	s.leaseRenewing[cacheKey] = struct{}{}
	return true
}

func leaseBindingCacheKey(binding raftstore.LeaseBinding) string {
	return strings.Join([]string{
		binding.RegistryGeneration, binding.NodeID, fmt.Sprint(binding.NodeEpoch), binding.DataEndpoint,
		binding.Group, binding.AuthKeyFingerprint, binding.ManifestKeyFingerprint,
	}, "\x00")
}

func (s *RegistryService) scheduleFenceCompaction(ctx context.Context, fence clusterstate.ExecutionFence) {
	if ctx.Err() != nil || fence.Revision.LogIndex == 0 {
		return
	}
	key := fence.Group + "\x00" + fence.RouteKey + "\x00" + fence.SandboxID
	s.compactionMu.Lock()
	if _, active := s.activeCompaction[key]; active {
		s.compactionMu.Unlock()
		return
	}
	select {
	case s.compactionSlots <- struct{}{}:
		s.activeCompaction[key] = struct{}{}
	default:
		s.compactionMu.Unlock()
		return
	}
	s.compactionMu.Unlock()
	go func() {
		defer func() {
			<-s.compactionSlots
			s.compactionMu.Lock()
			delete(s.activeCompaction, key)
			s.compactionMu.Unlock()
		}()
		_ = s.finalizeAndCompactFence(s.compactionCtx, fence)
	}()
}

func (s *RegistryService) finalizeAndCompactFence(
	ctx context.Context,
	fence clusterstate.ExecutionFence,
) error {
	return s.store.CompactExecutionFence(ctx, fence)
}

func (s *RegistryService) VerifyFenceOutboxAck(
	ctx context.Context,
	request raftstore.FenceOutboxAckRequest,
) (raftstore.FenceOutboxAckEvidence, error) {
	bindingDigest, digestErr := hex.DecodeString(request.BindingDigest)
	if request.Group == "" || request.RouteKey == "" || request.SandboxID == "" ||
		request.NodeID == "" || request.NodeEpoch == 0 || request.RegistryGeneration == "" ||
		digestErr != nil || len(bindingDigest) != sha256.Size || request.FinalOutboxWatermark == 0 {
		return raftstore.FenceOutboxAckEvidence{}, errors.New("controlplane: incomplete final outbox ACK request")
	}
	state, err := s.store.ReadSystem(ctx)
	if err != nil {
		return raftstore.FenceOutboxAckEvidence{}, err
	}
	enrollment, found := state.NodeEnrollments[request.NodeID]
	if !found || enrollment.Retired || enrollment.MaxNodeEpoch != request.NodeEpoch || enrollment.DataEndpoint == "" {
		return raftstore.FenceOutboxAckEvidence{}, session.ErrSessionUnavailable
	}
	identity, err := s.store.ServeIdentity()
	if err != nil {
		return raftstore.FenceOutboxAckEvidence{}, err
	}
	if identity.RegistryGeneration != request.RegistryGeneration {
		return raftstore.FenceOutboxAckEvidence{}, errors.New("controlplane: final outbox ACK belongs to another Registry History Generation")
	}
	command := &routesync.Command{
		CmdID: newCommandID(), Kind: routesync.CmdFinalizeWorkflow, SID: request.SandboxID,
		RegistryGeneration: request.RegistryGeneration, BindingDigest: request.BindingDigest,
	}
	ack, sent, err := s.commands.SendNodeCommand(
		ctx, identity, request.NodeID, request.NodeEpoch, enrollment.DataEndpoint, command,
	)
	if err != nil {
		return raftstore.FenceOutboxAckEvidence{}, err
	}
	if !sent || ack.Status != routesync.AckAccepted {
		return raftstore.FenceOutboxAckEvidence{}, fmt.Errorf(
			"controlplane: node has not confirmed final outbox watermark: %s", ack.Reason,
		)
	}
	return raftstore.FenceOutboxAckEvidence{
		AckedWatermark: request.FinalOutboxWatermark,
		ProofDigest:    finalOutboxAckProof(request),
	}, nil
}

func finalOutboxAckProof(request raftstore.FenceOutboxAckRequest) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"kuasar-final-outbox-ack-v1\x00%s\x00%s\x00%s\x00%s\x00%s\x00%d\x00%s\x00%d",
		request.RegistryGeneration, request.Group, request.RouteKey, request.SandboxID,
		request.NodeID, request.NodeEpoch, request.BindingDigest, request.FinalOutboxWatermark,
	)))
	return hex.EncodeToString(digest[:])
}
