// Package controlplane assembles the final Registry control plane around the
// consensus Route/Build store, Session Holders, and node-authoritative facts.
package controlplane

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"sync"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

var (
	ErrPermitUnavailable = errors.New("controlplane: no current Serve Permit")
	ErrRevisionConflict  = errors.New("controlplane: workflow revision conflict")
)

type consensusRuntime interface {
	RefreshPermit(context.Context) (raftstore.PermitGrant, error)
	AuthorizeServe(raftstore.PermitIdentity, raftstore.PermitOperation) error
	ReadSystemStrong(context.Context) (raftstore.SystemState, error)
	ApplySystem(context.Context, raftstore.SystemCommand) (raftstore.SystemApplyResult, error)
	ApplyData(context.Context, raftstore.DataCommand) (raftstore.DataApplyResult, error)
	ReadData(context.Context, raftstore.DataLookup) (raftstore.DataLookupResult, error)
	CompactExecutionFence(context.Context, raftstore.ShardRequestIdentity, string, string, string) error
}

// RaftStore is the sole final adapter from Registry workflows to Multi-Raft.
// It caches only the identity carried by a quorum-confirmed Serve Permit; all
// business state remains in the underlying state machines.
type RaftStore struct {
	runtime        consensusRuntime
	registryLayout raftstore.RegistryLayout
	digest         string

	permitMu sync.RWMutex
	permit   *raftstore.PermitGrant
	locks    [64]sync.Mutex
}

func NewRaftStore(runtime consensusRuntime, registryLayout raftstore.RegistryLayout, digest string) (*RaftStore, error) {
	if runtime == nil {
		return nil, errors.New("controlplane: consensus runtime is required")
	}
	if err := registryLayout.Validate(); err != nil {
		return nil, err
	}
	wantDigest, err := registryLayout.Digest()
	if err != nil {
		return nil, err
	}
	if digest != wantDigest {
		return nil, errors.New("controlplane: registryLayout digest does not match verified registryLayout")
	}
	return &RaftStore{runtime: runtime, registryLayout: registryLayout, digest: digest}, nil
}

func (s *RaftStore) RefreshPermit(ctx context.Context) (session.ServeIdentity, error) {
	grant, err := s.RefreshPermitGrant(ctx)
	if err != nil {
		return session.ServeIdentity{}, err
	}
	return serveIdentity(grant.PermitIdentity), nil
}

func (s *RaftStore) RefreshPermitGrant(ctx context.Context) (raftstore.PermitGrant, error) {
	grant, err := s.runtime.RefreshPermit(ctx)
	if err != nil {
		return raftstore.PermitGrant{}, err
	}
	if grant.ClusterID != s.registryLayout.ClusterID || grant.RegistryGeneration != s.registryLayout.RegistryGeneration ||
		grant.RegistryLayoutDigest != s.digest {
		return raftstore.PermitGrant{}, errors.New("controlplane: refreshed Permit belongs to another signed Registry History Generation")
	}
	copy := grant
	s.permitMu.Lock()
	s.permit = &copy
	s.permitMu.Unlock()
	return grant, nil
}

func (s *RaftStore) RegistryLayoutSnapshot() (raftstore.RegistryLayout, string) {
	return raftstore.CloneRegistryLayout(s.registryLayout), s.digest
}

func (s *RaftStore) ServeIdentity() (session.ServeIdentity, error) {
	s.permitMu.RLock()
	grant := s.permit
	if grant != nil {
		copy := *grant
		grant = &copy
	}
	s.permitMu.RUnlock()
	if grant == nil {
		return session.ServeIdentity{}, ErrPermitUnavailable
	}
	return serveIdentity(grant.PermitIdentity), nil
}

func serveIdentity(identity raftstore.PermitIdentity) session.ServeIdentity {
	return session.ServeIdentity{
		ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
		SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest,
	}
}

func permitIdentity(identity session.ServeIdentity) raftstore.PermitIdentity {
	return raftstore.PermitIdentity{
		ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
		SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest,
	}
}

func (s *RaftStore) AllowSessionWork(identity session.ServeIdentity, operation session.PermitOperation) bool {
	permitOperation := raftstore.PermitHolderProbe
	if operation == session.PermitDispatch {
		permitOperation = raftstore.PermitHolderDispatch
	} else if operation == session.PermitRecovery {
		permitOperation = raftstore.PermitHolderRecovery
	}
	return s.runtime.AuthorizeServe(permitIdentity(identity), permitOperation) == nil
}

// AuthorizeNodeSession binds Holder work to the node enrollment state applied
// at least through the cached Permit commit. A partitioned member can use its
// older state only until that Permit expires; it cannot renew and keep serving a
// retired NodeEpoch indefinitely.
func (s *RaftStore) AuthorizeNodeSession(identity session.ServeIdentity, registration session.Registration) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if err := registration.Validate(); err != nil {
		return err
	}
	s.permitMu.RLock()
	grant := s.permit
	if grant != nil {
		copy := *grant
		grant = &copy
	}
	s.permitMu.RUnlock()
	if grant == nil || grant.PermitIdentity != permitIdentity(identity) {
		return session.ErrPermitUnavailable
	}
	reader, ok := s.runtime.(interface {
		ReadSystemLocal() (raftstore.SystemState, error)
	})
	if !ok {
		return session.ErrPermitUnavailable
	}
	state, err := reader.ReadSystemLocal()
	if err != nil || state.LastApplied < grant.CommitIndex || state.Identity() != grant.PermitIdentity {
		return session.ErrPermitUnavailable
	}
	enrollment, found := state.NodeEnrollments[registration.NodeID]
	if !found || enrollment.Retired || enrollment.EnrollmentID != registration.EnrollmentID ||
		enrollment.MaxNodeEpoch != registration.NodeEpoch || enrollment.DataEndpoint != registration.DataEndpoint {
		return session.ErrStaleSession
	}
	return nil
}

func (s *RaftStore) AllowDirectoryEntry(entry session.DirectoryEntry) bool {
	s.permitMu.RLock()
	grant := s.permit
	if grant != nil {
		copy := *grant
		grant = &copy
	}
	s.permitMu.RUnlock()
	if grant == nil {
		return false
	}
	reader, ok := s.runtime.(interface {
		ReadSystemLocal() (raftstore.SystemState, error)
	})
	if !ok {
		return false
	}
	state, err := reader.ReadSystemLocal()
	if err != nil || state.LastApplied < grant.CommitIndex || state.Identity() != grant.PermitIdentity {
		return false
	}
	enrollment, found := state.NodeEnrollments[entry.NodeID]
	if !found || enrollment.Retired || enrollment.EnrollmentID != entry.EnrollmentID ||
		enrollment.MaxNodeEpoch != entry.NodeEpoch {
		return false
	}
	for _, member := range s.registryLayout.Members {
		if member.MemberID == entry.HolderMemberID {
			return true
		}
	}
	return false
}

func (s *RaftStore) AuthorizeEvent(identity session.ServeIdentity, acknowledge bool) error {
	operation := raftstore.PermitHolderEvent
	if acknowledge {
		operation = raftstore.PermitHolderEventAck
	}
	return s.runtime.AuthorizeServe(permitIdentity(identity), operation)
}

func (s *RaftStore) AuthorizeRouterCache(identity session.ServeIdentity) error {
	return s.runtime.AuthorizeServe(permitIdentity(identity), raftstore.PermitRouterCacheForward)
}

func (s *RaftStore) RunSessionRegistration(
	ctx context.Context,
	registration session.Registration,
	install func() error,
) error {
	if err := registration.Validate(); err != nil {
		return err
	}
	if install == nil {
		return errors.New("controlplane: Session Holder install callback is required")
	}
	lock := s.nodeLock(registration.NodeID)
	lock.Lock()
	defer lock.Unlock()
	state, err := s.runtime.ReadSystemStrong(ctx)
	if err != nil {
		return err
	}
	if registrationMatchesCommittedCatalog(state, registration) {
		return install()
	}
	result, err := s.runtime.ApplySystem(ctx, raftstore.SystemCommand{
		Type: raftstore.SystemAcceptNodeRegistration,
		Registration: &raftstore.NodeRegistrationCommand{
			NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
			NodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
			RuntimeDigest: registration.RuntimeDigest, Labels: cloneStrings(registration.Labels),
			Capabilities: cloneBools(registration.Capabilities), FailureDomain: registration.FailureDomain,
			LoadModelVersion: registration.LoadModelVersion, SandboxSlots: registration.SandboxSlots,
			BuildSlots: registration.BuildSlots, BuildCPU: registration.BuildCPU,
			BuildMemory: registration.BuildMemory, BuildStorage: registration.BuildStorage,
			Draining: registration.Draining,
		},
	})
	if err != nil {
		return err
	}
	if result.Conflict || !result.Applied {
		return fmt.Errorf("controlplane: node registration rejected: %s", result.Reason)
	}
	return install()
}

func registrationMatchesCommittedCatalog(state raftstore.SystemState, registration session.Registration) bool {
	if !state.Initialized || state.Retired {
		return false
	}
	enrollment, found := state.NodeEnrollments[registration.NodeID]
	if !found || enrollment.Retired || enrollment.EnrollmentID != registration.EnrollmentID ||
		enrollment.MaxNodeEpoch != registration.NodeEpoch || enrollment.DataEndpoint != registration.DataEndpoint ||
		enrollment.Catalog == nil {
		return false
	}
	catalog := enrollment.Catalog
	return catalog.RuntimeDigest == registration.RuntimeDigest &&
		reflect.DeepEqual(catalog.Labels, registration.Labels) &&
		reflect.DeepEqual(catalog.Capabilities, registration.Capabilities) &&
		catalog.FailureDomain == registration.FailureDomain &&
		catalog.LoadModelVersion == registration.LoadModelVersion &&
		catalog.SandboxSlots == registration.SandboxSlots && catalog.BuildSlots == registration.BuildSlots &&
		catalog.BuildCPU == registration.BuildCPU && catalog.BuildMemory == registration.BuildMemory &&
		catalog.BuildStorage == registration.BuildStorage && catalog.Draining == registration.Draining
}

func (s *RaftStore) EnrollNode(ctx context.Context, enrollment session.NodeEnrollment) error {
	if err := enrollment.Validate(); err != nil {
		return err
	}
	result, err := s.runtime.ApplySystem(ctx, raftstore.SystemCommand{
		Type: raftstore.SystemEnrollNode,
		Enrollment: &raftstore.NodeEnrollmentCommand{
			NodeID: enrollment.NodeID, EnrollmentID: enrollment.EnrollmentID,
			NodeEpoch: enrollment.NodeEpoch, DataEndpoint: enrollment.DataEndpoint,
		},
	})
	if err != nil {
		return err
	}
	if result.Conflict || !result.Applied {
		return fmt.Errorf("controlplane: node enrollment rejected: %s", result.Reason)
	}
	return nil
}

func (s *RaftStore) RunIdentityRetirement(
	ctx context.Context,
	retirement session.IdentityRetirement,
	remove func() (bool, error),
) (bool, error) {
	if err := retirement.Validate(); err != nil {
		return false, err
	}
	if remove == nil {
		return false, errors.New("controlplane: Session Holder removal callback is required")
	}
	lock := s.nodeLock(retirement.NodeID)
	lock.Lock()
	defer lock.Unlock()
	result, err := s.runtime.ApplySystem(ctx, raftstore.SystemCommand{
		Type: raftstore.SystemRetireNode,
		Retirement: &raftstore.NodeRetirementCommand{
			NodeID: retirement.NodeID, EnrollmentID: retirement.EnrollmentID,
			LastNodeEpoch: retirement.LastNodeEpoch,
		},
	})
	if err != nil {
		return false, err
	}
	if result.Conflict || !result.Applied {
		return false, fmt.Errorf("controlplane: node retirement rejected: %s", result.Reason)
	}
	return remove()
}

func (s *RaftStore) nodeLock(nodeID string) *sync.Mutex {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(nodeID))
	return &s.locks[hash.Sum32()%uint32(len(s.locks))]
}

func (s *RaftStore) Catalog(ctx context.Context) (placement.CatalogSnapshot, error) {
	state, err := s.runtime.ReadSystemStrong(ctx)
	if err != nil {
		return placement.CatalogSnapshot{}, err
	}
	if state.ClusterID != s.registryLayout.ClusterID || state.RegistryGeneration != s.registryLayout.RegistryGeneration ||
		state.ActiveRegistryLayoutDigest != s.digest {
		return placement.CatalogSnapshot{}, errors.New("controlplane: Node Catalog belongs to another Registry History Generation")
	}
	nodes := make([]placement.CatalogNode, 0, len(state.NodeEnrollments))
	revision := uint64(0)
	for _, enrollment := range state.NodeEnrollments {
		revision = max(revision, enrollment.LastAppliedIndex)
		if enrollment.Retired || enrollment.Catalog == nil {
			continue
		}
		catalog := enrollment.Catalog
		nodes = append(nodes, placement.CatalogNode{
			NodeID: enrollment.NodeID, Labels: cloneStrings(catalog.Labels),
			Capabilities: cloneBools(catalog.Capabilities), FailureDomain: catalog.FailureDomain,
			RuntimeDigest: catalog.RuntimeDigest, Draining: catalog.Draining,
			SandboxSlotCapacity: catalog.SandboxSlots, BuildSlotCapacity: catalog.BuildSlots,
			BuildCPUCapacity: catalog.BuildCPU, BuildMemoryCapacity: catalog.BuildMemory,
			BuildStorageCapacity: catalog.BuildStorage,
		})
	}
	return placement.NewCatalogSnapshot(
		state.ClusterID, state.RegistryGeneration, state.SystemEpoch,
		state.ActiveRegistryLayoutDigest, revision, nodes,
	)
}

func (s *RaftStore) CommitRouteWorkflow(
	ctx context.Context,
	expected clusterstate.Revision,
	next clusterstate.RouteWorkflowRecord,
) (clusterstate.RouteWorkflowRecord, error) {
	if err := expected.Validate(); err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	identity, err := s.routeIdentity(next.Group, next.RouteKey)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	if expected.RegistryGeneration != identity.RegistryGeneration || expected.ShardID != identity.ShardID {
		return clusterstate.RouteWorkflowRecord{}, ErrRevisionConflict
	}
	return s.putRoute(ctx, identity, raftstore.RevisionExpectation{LogIndex: expected.LogIndex}, next)
}

func (s *RaftStore) CreateRouteWorkflow(
	ctx context.Context,
	next clusterstate.RouteWorkflowRecord,
) (clusterstate.RouteWorkflowRecord, error) {
	identity, err := s.routeIdentity(next.Group, next.RouteKey)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	if next.State != clusterstate.WorkflowRouteStarting || next.Starting == nil {
		return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: new Route must contain complete STARTING intent")
	}
	return s.putRoute(ctx, identity, raftstore.RevisionExpectation{Absent: true}, next)
}

func (s *RaftStore) putRoute(
	ctx context.Context,
	identity raftstore.ShardRequestIdentity,
	expect raftstore.RevisionExpectation,
	next clusterstate.RouteWorkflowRecord,
) (clusterstate.RouteWorkflowRecord, error) {
	result, err := s.applyDataCommand(ctx, raftstore.DataCommand{
		Type: raftstore.DataPutRoute, Identity: identity, Expect: expect, Route: &next,
	})
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	if result.Conflict || !result.Applied {
		return clusterstate.RouteWorkflowRecord{}, fmt.Errorf("%w: %s (current=%d)", ErrRevisionConflict, result.Reason, result.CurrentRevision)
	}
	workflow, err := s.ReadRouteWorkflow(ctx, next.Group, next.RouteKey)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	if workflow == nil || workflow.Revision.LogIndex != result.Revision {
		return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: committed Route revision is not visible")
	}
	return *workflow, nil
}

func (s *RaftStore) CommitBuildWorkflow(
	ctx context.Context,
	expected clusterstate.Revision,
	next clusterstate.BuildRecord,
) (clusterstate.BuildRecord, error) {
	if err := expected.Validate(); err != nil {
		return clusterstate.BuildRecord{}, err
	}
	identity, err := s.buildIdentity(next.Group, next.BuildID)
	if err != nil {
		return clusterstate.BuildRecord{}, err
	}
	if expected.RegistryGeneration != identity.RegistryGeneration || expected.ShardID != identity.ShardID {
		return clusterstate.BuildRecord{}, ErrRevisionConflict
	}
	return s.putBuild(ctx, identity, raftstore.RevisionExpectation{LogIndex: expected.LogIndex}, next)
}

func (s *RaftStore) CreateBuildWorkflow(ctx context.Context, next clusterstate.BuildRecord) (clusterstate.BuildRecord, error) {
	identity, err := s.buildIdentity(next.Group, next.BuildID)
	if err != nil {
		return clusterstate.BuildRecord{}, err
	}
	if next.State != clusterstate.BuildStarting || next.Starting == nil {
		return clusterstate.BuildRecord{}, errors.New("controlplane: new Build must contain complete BUILD_STARTING intent")
	}
	return s.putBuild(ctx, identity, raftstore.RevisionExpectation{Absent: true}, next)
}

func (s *RaftStore) putBuild(
	ctx context.Context,
	identity raftstore.ShardRequestIdentity,
	expect raftstore.RevisionExpectation,
	next clusterstate.BuildRecord,
) (clusterstate.BuildRecord, error) {
	result, err := s.runtime.ApplyData(ctx, raftstore.DataCommand{
		Type: raftstore.DataPutBuild, Identity: identity, Expect: expect, Build: &next,
	})
	if err != nil {
		return clusterstate.BuildRecord{}, err
	}
	if result.Conflict || !result.Applied {
		return clusterstate.BuildRecord{}, fmt.Errorf("%w: %s (current=%d)", ErrRevisionConflict, result.Reason, result.CurrentRevision)
	}
	workflow, err := s.ReadBuildWorkflow(ctx, next.Group, next.BuildID)
	if err != nil {
		return clusterstate.BuildRecord{}, err
	}
	if workflow == nil || workflow.Revision.LogIndex != result.Revision {
		return clusterstate.BuildRecord{}, errors.New("controlplane: committed Build revision is not visible")
	}
	return *workflow, nil
}

func (s *RaftStore) ReadRouteWorkflow(ctx context.Context, group, routeKey string) (*clusterstate.RouteWorkflowRecord, error) {
	identity, err := s.routeIdentity(group, routeKey)
	if err != nil {
		return nil, err
	}
	result, err := s.runtime.ReadData(ctx, raftstore.DataLookup{Workflow: &raftstore.WorkflowLookup{
		Identity: identity, Group: group, RouteKey: routeKey,
	}})
	if err != nil {
		return nil, err
	}
	if result.Workflow == nil || !result.Workflow.Available {
		return nil, errors.New("controlplane: Route workflow shard unavailable")
	}
	return result.Workflow.Route, nil
}

func (s *RaftStore) ReadBuildWorkflow(ctx context.Context, group, buildID string) (*clusterstate.BuildRecord, error) {
	identity, err := s.buildIdentity(group, buildID)
	if err != nil {
		return nil, err
	}
	result, err := s.runtime.ReadData(ctx, raftstore.DataLookup{Workflow: &raftstore.WorkflowLookup{
		Identity: identity, Group: group, BuildID: buildID,
	}})
	if err != nil {
		return nil, err
	}
	if result.Workflow == nil || !result.Workflow.Available {
		return nil, errors.New("controlplane: Build workflow shard unavailable")
	}
	return result.Workflow.Build, nil
}

func (s *RaftStore) EnsureExecutionFence(ctx context.Context, fence clusterstate.ExecutionFence) error {
	identity, err := s.routeIdentity(fence.Group, fence.RouteKey)
	if err != nil {
		return err
	}
	result, err := s.applyDataCommand(ctx, raftstore.DataCommand{
		Type: raftstore.DataPutFence, Identity: identity,
		Expect: raftstore.RevisionExpectation{Absent: true}, Fence: &fence,
	})
	if err != nil {
		return err
	}
	if result.Applied && !result.Conflict {
		return nil
	}
	lookup, err := s.runtime.ReadData(ctx, raftstore.DataLookup{Fence: &raftstore.FenceLookup{
		Identity: identity, Group: fence.Group, RouteKey: fence.RouteKey, SandboxID: fence.SandboxID,
	}})
	if err != nil {
		return err
	}
	if lookup.Fence == nil || lookup.Fence.Fence == nil {
		return fmt.Errorf("%w: %s", ErrRevisionConflict, result.Reason)
	}
	current, wanted := *lookup.Fence.Fence, fence
	current.Revision, wanted.Revision = clusterstate.Revision{}, clusterstate.Revision{}
	if !reflect.DeepEqual(current, wanted) {
		return fmt.Errorf("%w: execution fence differs", ErrRevisionConflict)
	}
	return nil
}

func (s *RaftStore) ReadRoute(ctx context.Context, request routeapi.ReadRouteRequest) (routeapi.ReadRouteResponse, error) {
	result, err := s.runtime.ReadData(ctx, raftstore.DataLookup{Route: &request})
	if err != nil {
		return routeapi.ReadRouteResponse{}, err
	}
	if result.Route == nil {
		return routeapi.ReadRouteResponse{}, errors.New("controlplane: missing Route read result")
	}
	return *result.Route, nil
}

func (s *RaftStore) ReadBuild(ctx context.Context, request routeapi.ReadBuildRequest) (routeapi.ReadBuildResponse, error) {
	result, err := s.runtime.ReadData(ctx, raftstore.DataLookup{Build: &request})
	if err != nil {
		return routeapi.ReadBuildResponse{}, err
	}
	if result.Build == nil {
		return routeapi.ReadBuildResponse{}, errors.New("controlplane: missing Build read result")
	}
	return *result.Build, nil
}

func (s *RaftStore) RouteRequestIdentity(group, routeKey string) (routeapi.RequestIdentity, error) {
	identity, err := s.routeIdentity(group, routeKey)
	if err != nil {
		return routeapi.RequestIdentity{}, err
	}
	return requestIdentity(identity), nil
}

func (s *RaftStore) BuildRequestIdentity(group, buildID string) (routeapi.RequestIdentity, error) {
	identity, err := s.buildIdentity(group, buildID)
	if err != nil {
		return routeapi.RequestIdentity{}, err
	}
	return requestIdentity(identity), nil
}

func (s *RaftStore) RouteBucketRequestIdentity(group string, bucket uint32) (routeapi.RequestIdentity, error) {
	if group == "" || bucket >= s.registryLayout.RouteBucketCount {
		return routeapi.RequestIdentity{}, errors.New("controlplane: invalid Route bucket identity")
	}
	hash, err := clusterstate.RouteShardHash(group, bucket)
	if err != nil {
		return routeapi.RequestIdentity{}, err
	}
	identity, err := s.shardIdentity(uint32(hash % uint64(s.registryLayout.VirtualShardCount)))
	if err != nil {
		return routeapi.RequestIdentity{}, err
	}
	return requestIdentity(identity), nil
}

func requestIdentity(identity raftstore.ShardRequestIdentity) routeapi.RequestIdentity {
	return routeapi.RequestIdentity{
		ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
		SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest, ShardID: identity.ShardID,
	}
}

func (s *RaftStore) PendingWorkflows(ctx context.Context, shardID uint32, after string, limit uint32) (raftstore.PendingLookupResult, error) {
	identity, err := s.shardIdentity(shardID)
	if err != nil {
		return raftstore.PendingLookupResult{}, err
	}
	result, err := s.runtime.ReadData(ctx, raftstore.DataLookup{Pending: &raftstore.PendingLookup{
		Identity: identity, AfterKey: after, Limit: limit,
	}})
	if err != nil {
		return raftstore.PendingLookupResult{}, err
	}
	if result.Pending == nil {
		return raftstore.PendingLookupResult{}, errors.New("controlplane: missing pending-workflow result")
	}
	return *result.Pending, nil
}

func (s *RaftStore) LeaseBindings(
	ctx context.Context,
	shardID uint32,
	after string,
	limit uint32,
) (raftstore.LeaseBindingLookupResult, error) {
	identity, err := s.shardIdentity(shardID)
	if err != nil {
		return raftstore.LeaseBindingLookupResult{}, err
	}
	result, err := s.runtime.ReadData(ctx, raftstore.DataLookup{LeaseBindings: &raftstore.LeaseBindingLookup{
		Identity: identity, AfterKey: after, Limit: limit,
	}})
	if err != nil {
		return raftstore.LeaseBindingLookupResult{}, err
	}
	if result.LeaseBindings == nil {
		return raftstore.LeaseBindingLookupResult{}, errors.New("controlplane: missing lease-Binding result")
	}
	return *result.LeaseBindings, nil
}

func (s *RaftStore) CompactExecutionFence(
	ctx context.Context,
	fence clusterstate.ExecutionFence,
) error {
	identity, err := s.routeIdentity(fence.Group, fence.RouteKey)
	if err != nil {
		return err
	}
	if identity.ShardID != fence.Revision.ShardID || identity.RegistryGeneration != fence.RegistryGeneration {
		return errors.New("controlplane: execution fence belongs to another signed Registry History Generation or shard")
	}
	return s.runtime.CompactExecutionFence(
		ctx, identity, fence.Group, fence.RouteKey, fence.SandboxID,
	)
}

func (s *RaftStore) LocalCoordinator(shardID uint32) (bool, error) {
	leader, ok := s.runtime.(interface {
		LocalDataShardLeader(uint32) (bool, error)
	})
	if !ok {
		return true, nil
	}
	return leader.LocalDataShardLeader(shardID)
}

func (s *RaftStore) LocalRecoveryCoordinator() (bool, error) {
	leader, ok := s.runtime.(interface{ LocalSystemLeader() (bool, error) })
	if !ok {
		return true, nil
	}
	return leader.LocalSystemLeader()
}

func (s *RaftStore) BeginRecovery(ctx context.Context) (raftstore.SystemState, error) {
	runtime, ok := s.runtime.(interface {
		BeginRecovery(context.Context) (raftstore.SystemState, error)
	})
	if !ok {
		return raftstore.SystemState{}, errors.New("controlplane: consensus runtime has no recovery workflow")
	}
	return runtime.BeginRecovery(ctx)
}

func (s *RaftStore) AdvanceRecovery(ctx context.Context, from, to raftstore.RecoveryPhase) (raftstore.SystemState, error) {
	runtime, ok := s.runtime.(interface {
		AdvanceRecovery(context.Context, raftstore.RecoveryPhase, raftstore.RecoveryPhase) (raftstore.SystemState, error)
	})
	if !ok {
		return raftstore.SystemState{}, errors.New("controlplane: consensus runtime has no recovery workflow")
	}
	return runtime.AdvanceRecovery(ctx, from, to)
}

func (s *RaftStore) ConfirmRecoveryPermitDrain(ctx context.Context) (raftstore.SystemState, error) {
	runtime, ok := s.runtime.(interface {
		ConfirmRecoveryPermitDrain(context.Context) (raftstore.SystemState, error)
	})
	if !ok {
		return raftstore.SystemState{}, errors.New("controlplane: consensus runtime has no recovery-drain workflow")
	}
	return runtime.ConfirmRecoveryPermitDrain(ctx)
}

func (s *RaftStore) SetServingGates(ctx context.Context, gates raftstore.GateUpdate) (raftstore.SystemState, error) {
	runtime, ok := s.runtime.(interface {
		SetServingGates(context.Context, raftstore.GateUpdate) (raftstore.SystemState, error)
	})
	if !ok {
		return raftstore.SystemState{}, errors.New("controlplane: consensus runtime has no serving-gate workflow")
	}
	return runtime.SetServingGates(ctx, gates)
}

func (s *RaftStore) CloseRegistryGeneration(ctx context.Context, successor raftstore.RegistryLayout) (raftstore.SystemState, error) {
	runtime, ok := s.runtime.(interface {
		CloseRegistryGeneration(context.Context, raftstore.RegistryLayout) (raftstore.SystemState, error)
	})
	if !ok {
		return raftstore.SystemState{}, errors.New("controlplane: consensus runtime has no Registry History Generation closure workflow")
	}
	return runtime.CloseRegistryGeneration(ctx, successor)
}

func (s *RaftStore) ConfirmPredecessorPermitDrain(ctx context.Context, evidenceDigest string) (raftstore.SystemState, error) {
	runtime, ok := s.runtime.(interface {
		ConfirmPredecessorPermitDrain(context.Context, string) (raftstore.SystemState, error)
	})
	if !ok {
		return raftstore.SystemState{}, errors.New("controlplane: consensus runtime has no predecessor-drain workflow")
	}
	return runtime.ConfirmPredecessorPermitDrain(ctx, evidenceDigest)
}

func (s *RaftStore) UpdateRecoveryNode(ctx context.Context, update raftstore.RecoveryNodeUpdate) error {
	result, err := s.runtime.ApplySystem(ctx, raftstore.SystemCommand{
		Type: raftstore.SystemUpdateRecoveryNode, RecoveryNode: &update,
	})
	if err != nil {
		return err
	}
	if result.Conflict || !result.Applied {
		return fmt.Errorf("controlplane: recovery node progress conflict: %s", result.Reason)
	}
	return nil
}

func (s *RaftStore) ApplyRecoveryData(ctx context.Context, command raftstore.DataCommand) (raftstore.DataApplyResult, error) {
	runtime, ok := s.runtime.(interface {
		ApplyRecoveryData(context.Context, raftstore.DataCommand) (raftstore.DataApplyResult, error)
	})
	if !ok {
		return raftstore.DataApplyResult{}, errors.New("controlplane: consensus runtime has no data recovery workflow")
	}
	return runtime.ApplyRecoveryData(ctx, command)
}

func (s *RaftStore) ReadRecoveryData(ctx context.Context, query raftstore.RecoveryLookup) (raftstore.RecoveryLookupResult, error) {
	runtime, ok := s.runtime.(interface {
		ReadRecoveryData(context.Context, raftstore.RecoveryLookup) (raftstore.RecoveryLookupResult, error)
	})
	if !ok {
		return raftstore.RecoveryLookupResult{}, errors.New("controlplane: consensus runtime has no data recovery workflow")
	}
	return runtime.ReadRecoveryData(ctx, query)
}

func (s *RaftStore) VirtualShardCount() uint32 { return s.registryLayout.VirtualShardCount }

func (s *RaftStore) ReadSystem(ctx context.Context) (raftstore.SystemState, error) {
	return s.runtime.ReadSystemStrong(ctx)
}

func (s *RaftStore) RegistryServeIdentity() (routeapi.RegistryServeIdentity, error) {
	identity, err := s.ServeIdentity()
	if err != nil {
		return routeapi.RegistryServeIdentity{}, err
	}
	return routeapi.RegistryServeIdentity{
		ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
		SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest,
	}, nil
}

func (s *RaftStore) ReadRouteBucket(
	ctx context.Context,
	group string,
	bucket uint32,
	state clusterstate.RouteWorkflowState,
	afterRouteKey string,
	limit uint32,
	strong bool,
) (raftstore.RouteBucketResult, error) {
	if bucket >= s.registryLayout.RouteBucketCount {
		return raftstore.RouteBucketResult{}, errors.New("controlplane: Route bucket is out of range")
	}
	hash, err := clusterstate.RouteShardHash(group, bucket)
	if err != nil {
		return raftstore.RouteBucketResult{}, err
	}
	identity, err := s.shardIdentity(uint32(hash % uint64(s.registryLayout.VirtualShardCount)))
	if err != nil {
		return raftstore.RouteBucketResult{}, err
	}
	result, err := s.runtime.ReadData(ctx, raftstore.DataLookup{RouteBucket: &raftstore.RouteBucketLookup{
		Identity: identity, Group: group, Bucket: bucket, State: state,
		AfterRouteKey: afterRouteKey, Limit: limit, Strong: strong,
	}})
	if err != nil {
		return raftstore.RouteBucketResult{}, err
	}
	if result.RouteBucket == nil {
		return raftstore.RouteBucketResult{}, errors.New("controlplane: missing Route bucket result")
	}
	return *result.RouteBucket, nil
}

func (s *RaftStore) ReadRouteChangefeed(
	ctx context.Context,
	group string,
	bucket uint32,
	afterRevision uint64,
	limit uint32,
	strong bool,
) (raftstore.RouteChangefeedResult, error) {
	if bucket >= s.registryLayout.RouteBucketCount {
		return raftstore.RouteChangefeedResult{}, errors.New("controlplane: Route bucket is out of range")
	}
	hash, err := clusterstate.RouteShardHash(group, bucket)
	if err != nil {
		return raftstore.RouteChangefeedResult{}, err
	}
	identity, err := s.shardIdentity(uint32(hash % uint64(s.registryLayout.VirtualShardCount)))
	if err != nil {
		return raftstore.RouteChangefeedResult{}, err
	}
	result, err := s.runtime.ReadData(ctx, raftstore.DataLookup{Changefeed: &raftstore.RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: afterRevision, Limit: limit, Strong: strong,
	}})
	if err != nil {
		return raftstore.RouteChangefeedResult{}, err
	}
	if result.Changefeed == nil {
		return raftstore.RouteChangefeedResult{}, errors.New("controlplane: missing Route changefeed result")
	}
	return *result.Changefeed, nil
}

func (s *RaftStore) RouteBucketCount() uint32 { return s.registryLayout.RouteBucketCount }

func (s *RaftStore) routeIdentity(group, routeKey string) (raftstore.ShardRequestIdentity, error) {
	_, shardID, err := clusterstate.RouteShardFor(
		group, routeKey, s.registryLayout.RouteBucketCount, s.registryLayout.VirtualShardCount,
	)
	if err != nil {
		return raftstore.ShardRequestIdentity{}, err
	}
	return s.shardIdentity(shardID)
}

func (s *RaftStore) buildIdentity(group, buildID string) (raftstore.ShardRequestIdentity, error) {
	_, shardID, err := clusterstate.BuildShardFor(
		group, buildID, s.registryLayout.BuildBucketCount, s.registryLayout.VirtualShardCount,
	)
	if err != nil {
		return raftstore.ShardRequestIdentity{}, err
	}
	return s.shardIdentity(shardID)
}

func (s *RaftStore) shardIdentity(shardID uint32) (raftstore.ShardRequestIdentity, error) {
	identity, err := s.ServeIdentity()
	if err != nil {
		return raftstore.ShardRequestIdentity{}, err
	}
	return raftstore.ShardRequestIdentity{PermitIdentity: permitIdentity(identity), ShardID: shardID}, nil
}

func cloneStrings(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneBools(source map[string]bool) map[string]bool {
	if source == nil {
		return nil
	}
	clone := make(map[string]bool, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
