package routeapi

import (
	"errors"
	"fmt"
	"slices"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	MutationReady       = "READY"
	MutationPending     = "PENDING"
	MutationTerminal    = "TERMINAL"
	MutationConflict    = "CONFLICT"
	MutationNeedLeader  = "NEED_LEADER"
	MutationUnavailable = "UNAVAILABLE"
)

type PermitRequest struct {
	ClusterID            string `json:"cluster_id"`
	RegistryGeneration   string `json:"registry_generation"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
}

type RegistryServeIdentity struct {
	ClusterID            string `json:"cluster_id"`
	RegistryGeneration   string `json:"registry_generation"`
	SystemEpoch          uint64 `json:"system_epoch"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
}

func (i RegistryServeIdentity) Validate() error {
	if i.ClusterID == "" || i.RegistryGeneration == "" || i.SystemEpoch == 0 || i.RegistryLayoutDigest == "" {
		return errors.New("routeapi: incomplete Registry serving identity")
	}
	return nil
}

func (r PermitRequest) Validate() error {
	if r.ClusterID == "" || r.RegistryGeneration == "" || r.RegistryLayoutDigest == "" {
		return errors.New("routeapi: complete signed Registry History Generation identity is required")
	}
	return nil
}

type PermitResponse struct {
	ClusterID            string `json:"cluster_id"`
	RegistryGeneration   string `json:"registry_generation"`
	SystemEpoch          uint64 `json:"system_epoch"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
	CommitIndex          uint64 `json:"commit_index"`
	MaxLifetimeMillis    uint64 `json:"max_lifetime_millis"`
	ServeGate            bool   `json:"serve_gate"`
	WriteGate            bool   `json:"write_gate"`
	CutoverGate          bool   `json:"cutover_gate"`
	RecoveryClosed       bool   `json:"recovery_closed"`
}

func (r PermitResponse) ValidateFor(request PermitRequest) error {
	if r.ClusterID != request.ClusterID || r.RegistryGeneration != request.RegistryGeneration ||
		r.RegistryLayoutDigest != request.RegistryLayoutDigest || r.SystemEpoch == 0 || r.CommitIndex == 0 ||
		r.MaxLifetimeMillis == 0 {
		return errors.New("routeapi: Permit does not authorize the requested signed Registry History Generation")
	}
	return nil
}

type SandboxInput struct {
	Config              map[string]string       `json:"config,omitempty"`
	TimeoutSeconds      int                     `json:"timeout_seconds,omitempty"`
	Demand              placement.SandboxDemand `json:"demand"`
	TargetRuntimeDigest string                  `json:"target_runtime_digest,omitempty"`
}

func (i SandboxInput) Validate() error {
	if i.TimeoutSeconds < 0 {
		return errors.New("routeapi: Sandbox timeout cannot be negative")
	}
	_, err := placement.NormalizeSandboxDemand(i.Demand)
	return err
}

type ReserveSandboxRequest struct {
	RequestIdentity
	Group            string       `json:"group"`
	RouteKey         string       `json:"route_key"`
	MinRouteRevision uint64       `json:"min_route_revision,omitempty"`
	Input            SandboxInput `json:"input"`
}

func (r ReserveSandboxRequest) Validate() error {
	if err := r.RequestIdentity.Validate(); err != nil {
		return err
	}
	if r.Group == "" || r.RouteKey == "" {
		return errors.New("routeapi: Reserve requires group and route key")
	}
	return r.Input.Validate()
}

type RouteMutationResponse struct {
	Outcome       string                          `json:"outcome"`
	Group         string                          `json:"group,omitempty"`
	RouteKey      string                          `json:"route_key,omitempty"`
	State         clusterstate.RouteWorkflowState `json:"state,omitempty"`
	Route         *clusterstate.ReadyRoute        `json:"route,omitempty"`
	RouteRevision uint64                          `json:"route_revision,omitempty"`
	LeaderHint    *LeaderHint                     `json:"leader_hint,omitempty"`
	Reason        string                          `json:"reason,omitempty"`
}

func (r RouteMutationResponse) ValidateFor(identity RequestIdentity, group, routeKey string) error {
	if r.Group != "" && r.Group != group || r.RouteKey != "" && r.RouteKey != routeKey {
		return errors.New("routeapi: Route mutation response identity mismatch")
	}
	switch r.Outcome {
	case MutationReady:
		if r.Route == nil || r.State != clusterstate.WorkflowRouteReady || r.RouteRevision == 0 ||
			r.Route.RegistryGeneration != identity.RegistryGeneration {
			return errors.New("routeapi: READY mutation lacks a matching committed Route")
		}
		return r.Route.Validate()
	case MutationPending:
		if r.Route != nil || r.RouteRevision == 0 {
			return errors.New("routeapi: PENDING mutation has an invalid projection")
		}
		return nil
	case MutationTerminal, MutationConflict, MutationUnavailable:
		if r.Route != nil {
			return errors.New("routeapi: failed Route mutation carries a forwarding projection")
		}
		return nil
	case MutationNeedLeader:
		if r.Route != nil {
			return errors.New("routeapi: NEED_LEADER carries a forwarding projection")
		}
		if r.LeaderHint != nil {
			return r.LeaderHint.Validate()
		}
		return nil
	default:
		return fmt.Errorf("routeapi: unknown Route mutation outcome %q", r.Outcome)
	}
}

type ResumeSandboxRequest struct {
	RequestIdentity
	Group            string `json:"group"`
	RouteKey         string `json:"route_key"`
	MinRouteRevision uint64 `json:"min_route_revision,omitempty"`
}

func (r ResumeSandboxRequest) Validate() error {
	if err := r.RequestIdentity.Validate(); err != nil {
		return err
	}
	if r.Group == "" || r.RouteKey == "" {
		return errors.New("routeapi: Resume requires group and route key")
	}
	return nil
}

type DeleteSandboxRequest = ResumeSandboxRequest

type BuildInput struct {
	TemplateID          string                `json:"template_id"`
	Profile             types.Profile         `json:"profile"`
	Names               []string              `json:"names,omitempty"`
	Aliases             []string              `json:"aliases,omitempty"`
	Metadata            map[string]string     `json:"metadata,omitempty"`
	Builder             types.BuildOptions    `json:"builder,omitempty"`
	Demand              placement.BuildDemand `json:"demand"`
	TargetRuntimeDigest string                `json:"target_runtime_digest,omitempty"`
}

func (i BuildInput) Validate() error {
	if i.TemplateID == "" || !i.Profile.Valid() {
		return errors.New("routeapi: Build template ID and profile are required")
	}
	if !slices.IsSorted(i.Names) || !slices.IsSorted(i.Aliases) {
		return errors.New("routeapi: Build names and aliases must be canonical sorted lists")
	}
	_, err := placement.NormalizeBuildDemand(i.Demand)
	return err
}

type RegisterBuildRequest struct {
	RequestIdentity
	Group            string     `json:"group"`
	BuildID          string     `json:"build_id"`
	MinBuildRevision uint64     `json:"min_build_revision,omitempty"`
	Input            BuildInput `json:"input"`
}

func (r RegisterBuildRequest) Validate() error {
	if err := r.RequestIdentity.Validate(); err != nil {
		return err
	}
	if r.Group == "" || r.BuildID == "" {
		return errors.New("routeapi: Build register requires group and Build ID")
	}
	return r.Input.Validate()
}

type BuildMutationResponse struct {
	Outcome       string                          `json:"outcome"`
	Group         string                          `json:"group,omitempty"`
	State         clusterstate.BuildWorkflowState `json:"state,omitempty"`
	Build         *clusterstate.BuildProjection   `json:"build,omitempty"`
	BuildRevision uint64                          `json:"build_revision,omitempty"`
	LeaderHint    *LeaderHint                     `json:"leader_hint,omitempty"`
	Reason        string                          `json:"reason,omitempty"`
}

func (r BuildMutationResponse) ValidateFor(request RegisterBuildRequest) error {
	if r.Group != "" && r.Group != request.Group {
		return errors.New("routeapi: Build mutation response group mismatch")
	}
	switch r.Outcome {
	case MutationReady:
		if r.Build == nil || r.Build.BuildID != request.BuildID || r.BuildRevision == 0 ||
			r.Build.RegistryGeneration != request.RegistryGeneration || r.State != clusterstate.BuildRegistered {
			return errors.New("routeapi: Build mutation lacks a matching projection")
		}
		return r.Build.Validate()
	case MutationPending:
		if r.BuildRevision == 0 {
			return errors.New("routeapi: pending Build lacks a committed revision")
		}
		return nil
	case MutationTerminal, MutationConflict, MutationUnavailable:
		return nil
	case MutationNeedLeader:
		if r.LeaderHint != nil {
			return r.LeaderHint.Validate()
		}
		return nil
	default:
		return fmt.Errorf("routeapi: unknown Build mutation outcome %q", r.Outcome)
	}
}

type ListRoutesRequest struct {
	RequestIdentity
	Group  string `json:"group"`
	Bucket uint32 `json:"bucket"`
	Strong bool   `json:"strong,omitempty"`
}

func (r ListRoutesRequest) Validate() error {
	if err := r.RequestIdentity.Validate(); err != nil {
		return err
	}
	if r.Group == "" {
		return errors.New("routeapi: Route list requires group")
	}
	return nil
}

type ListedRoute struct {
	RouteKey string                  `json:"route_key"`
	Route    clusterstate.ReadyRoute `json:"route"`
}

type ListRoutesResponse struct {
	Routes           []ListedRoute `json:"routes"`
	Bucket           uint32        `json:"bucket"`
	SnapshotRevision uint64        `json:"snapshot_revision,omitempty"`
	Reason           string        `json:"reason,omitempty"`
}

func (r ListRoutesResponse) ValidateFor(request ListRoutesRequest) error {
	if r.Bucket != request.Bucket {
		return errors.New("routeapi: Route list response bucket mismatch")
	}
	if r.Reason != "" {
		if len(r.Routes) != 0 || r.SnapshotRevision != 0 {
			return errors.New("routeapi: unavailable Route list carries forwarding projections")
		}
		return nil
	}
	if r.SnapshotRevision == 0 {
		return errors.New("routeapi: available Route list requires a bucket snapshot revision")
	}
	for index := range r.Routes {
		if r.Routes[index].RouteKey == "" {
			return errors.New("routeapi: Route list entry has no route key")
		}
		if err := r.Routes[index].Route.Validate(); err != nil {
			return fmt.Errorf("routeapi: invalid Route list projection: %w", err)
		}
		if r.Routes[index].Route.RegistryGeneration != request.RegistryGeneration {
			return errors.New("routeapi: Route list projection belongs to another Registry History Generation")
		}
	}
	return nil
}
