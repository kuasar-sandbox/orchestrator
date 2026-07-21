// Package routeapi defines the trusted Router-to-Registry Route/Build API.
// Caller credentials are intentionally absent: Router authenticates callers
// before constructing these internal requests.
package routeapi

import (
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	ReadReady         = "READY"
	ReadNeedLeader    = "NEED_LEADER"
	ReadReplicaBehind = "REPLICA_BEHIND"
	ReadNotFound      = "NOT_FOUND"
	ReadConflict      = "CONFLICT"
	ReadUnavailable   = "UNAVAILABLE"
)

type RequestIdentity struct {
	ClusterID            string `json:"cluster_id"`
	RegistryGeneration   string `json:"registry_generation"`
	SystemEpoch          uint64 `json:"system_epoch"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
	ShardID              uint32 `json:"shard_id"`
}

func (i RequestIdentity) Validate() error {
	if i.ClusterID == "" || i.RegistryGeneration == "" || i.SystemEpoch == 0 || i.RegistryLayoutDigest == "" {
		return errors.New("routeapi: incomplete cluster/Registry History Generation identity")
	}
	return nil
}

type LeaderHint struct {
	MemberID string `json:"member_id"`
	Endpoint string `json:"endpoint"`
	Term     uint64 `json:"term"`
}

func (h LeaderHint) Validate() error {
	if h.MemberID == "" || h.Endpoint == "" || h.Term == 0 {
		return errors.New("routeapi: incomplete leader hint")
	}
	return nil
}

type ReadRouteRequest struct {
	RequestIdentity
	Group            string `json:"group"`
	RouteKey         string `json:"route_key"`
	MinRouteRevision uint64 `json:"min_route_revision,omitempty"`
	Strong           bool   `json:"strong,omitempty"`
	Addressable      bool   `json:"addressable,omitempty"`
}

func (r ReadRouteRequest) Validate() error {
	if err := r.RequestIdentity.Validate(); err != nil {
		return err
	}
	if r.Group == "" || r.RouteKey == "" {
		return errors.New("routeapi: group and route key are required")
	}
	if r.Addressable && !r.Strong {
		return errors.New("routeapi: addressable Route projection requires a strong read")
	}
	return nil
}

type ReadRouteResponse struct {
	Outcome       string                          `json:"outcome"`
	Group         string                          `json:"group,omitempty"`
	RouteKey      string                          `json:"route_key,omitempty"`
	Route         *clusterstate.ReadyRoute        `json:"route,omitempty"`
	State         clusterstate.RouteWorkflowState `json:"state,omitempty"`
	RouteRevision uint64                          `json:"route_revision,omitempty"`
	LeaderHint    *LeaderHint                     `json:"leader_hint,omitempty"`
	Reason        string                          `json:"reason,omitempty"`
}

func (r ReadRouteResponse) ValidateFor(request ReadRouteRequest) error {
	switch r.Outcome {
	case ReadReady:
		if r.Route == nil || r.RouteRevision == 0 {
			return errors.New("routeapi: READY requires route and revision")
		}
		if err := r.Route.Validate(); err != nil {
			return err
		}
		if r.Group != request.Group || r.RouteKey != request.RouteKey ||
			r.Route.RegistryGeneration != request.RegistryGeneration ||
			r.RouteRevision < request.MinRouteRevision {
			return errors.New("routeapi: READY does not satisfy request fence")
		}
		if r.LeaderHint != nil {
			return errors.New("routeapi: READY cannot carry a leader hint")
		}
		if r.State != clusterstate.WorkflowRouteReady &&
			!(request.Strong && request.Addressable && r.State == clusterstate.WorkflowRoutePaused) {
			return errors.New("routeapi: READY projection carries a non-addressable Route state")
		}
		return nil
	case ReadNeedLeader, ReadReplicaBehind:
		if r.Route != nil {
			return errors.New("routeapi: non-positive read cannot carry a Route")
		}
		if r.LeaderHint != nil {
			return r.LeaderHint.Validate()
		}
		return nil
	case ReadNotFound:
		if !request.Strong {
			return errors.New("routeapi: replica-local read returned final NOT_FOUND")
		}
		if r.Route != nil {
			return errors.New("routeapi: NOT_FOUND cannot carry a Route")
		}
		if r.LeaderHint != nil {
			return errors.New("routeapi: NOT_FOUND cannot carry a leader hint")
		}
		return nil
	case ReadConflict, ReadUnavailable:
		if r.Route != nil {
			return errors.New("routeapi: failed read cannot carry a Route")
		}
		if r.LeaderHint != nil {
			return errors.New("routeapi: failed read cannot carry a leader hint")
		}
		return nil
	default:
		return fmt.Errorf("routeapi: unknown Route read outcome %q", r.Outcome)
	}
}

type ReadBuildRequest struct {
	RequestIdentity
	Group            string `json:"group"`
	BuildID          string `json:"build_id"`
	MinBuildRevision uint64 `json:"min_build_revision,omitempty"`
	Strong           bool   `json:"strong,omitempty"`
}

func (r ReadBuildRequest) Validate() error {
	if err := r.RequestIdentity.Validate(); err != nil {
		return err
	}
	if r.Group == "" || r.BuildID == "" {
		return errors.New("routeapi: group and build ID are required")
	}
	return nil
}

type ReadBuildResponse struct {
	Outcome       string                          `json:"outcome"`
	Group         string                          `json:"group,omitempty"`
	Build         *clusterstate.BuildProjection   `json:"build,omitempty"`
	Pending       *PendingBuildProjection         `json:"pending,omitempty"`
	BuildState    clusterstate.BuildWorkflowState `json:"build_state,omitempty"`
	BuildRevision uint64                          `json:"build_revision,omitempty"`
	LeaderHint    *LeaderHint                     `json:"leader_hint,omitempty"`
	Reason        string                          `json:"reason,omitempty"`
}

// PendingBuildProjection identifies an existing BUILD_STARTING registration
// without exposing a node Binding that has not yet been acknowledged.
type PendingBuildProjection struct {
	BuildID     string        `json:"build_id"`
	TemplateRef string        `json:"template_ref"`
	Profile     types.Profile `json:"profile"`
}

func (p PendingBuildProjection) Validate() error {
	if p.BuildID == "" || p.TemplateRef == "" || !p.Profile.Valid() {
		return errors.New("routeapi: incomplete pending Build projection")
	}
	return nil
}

func (r ReadBuildResponse) ValidateFor(request ReadBuildRequest) error {
	switch r.Outcome {
	case ReadReady:
		if r.Build == nil || r.Pending != nil || r.BuildRevision == 0 || r.Group != request.Group || r.Build.BuildID != request.BuildID ||
			r.Build.RegistryGeneration != request.RegistryGeneration || r.BuildRevision < request.MinBuildRevision {
			return errors.New("routeapi: positive Build read does not satisfy request fence")
		}
		if err := r.Build.Validate(); err != nil {
			return err
		}
		if r.LeaderHint != nil {
			return errors.New("routeapi: positive Build read cannot carry a leader hint")
		}
		if r.BuildState != clusterstate.BuildRegistered {
			return errors.New("routeapi: local Build read returned an unbound registration")
		}
		return nil
	case ReadNeedLeader, ReadReplicaBehind:
		if r.Build != nil || r.Pending != nil {
			return errors.New("routeapi: non-positive Build read cannot carry a projection")
		}
		if r.LeaderHint != nil {
			return r.LeaderHint.Validate()
		}
		return nil
	case ReadNotFound:
		if !request.Strong {
			return errors.New("routeapi: replica-local Build read returned final NOT_FOUND")
		}
		if r.Build != nil || r.Pending != nil {
			return errors.New("routeapi: NOT_FOUND Build read cannot carry a projection")
		}
		if r.LeaderHint != nil {
			return errors.New("routeapi: NOT_FOUND Build read cannot carry a leader hint")
		}
		return nil
	case ReadConflict:
		if r.Build != nil {
			return errors.New("routeapi: failed Build read cannot carry a projection")
		}
		if r.LeaderHint != nil {
			return errors.New("routeapi: failed Build read cannot carry a leader hint")
		}
		if r.Pending == nil {
			return nil
		}
		if !request.Strong || r.Group != request.Group || r.Pending.BuildID != request.BuildID ||
			r.BuildState != clusterstate.BuildStarting || r.BuildRevision == 0 || r.BuildRevision < request.MinBuildRevision {
			return errors.New("routeapi: pending Build read does not satisfy request fence")
		}
		return r.Pending.Validate()
	case ReadUnavailable:
		if r.Build != nil || r.Pending != nil {
			return errors.New("routeapi: unavailable Build read cannot carry a projection")
		}
		if r.LeaderHint != nil {
			return errors.New("routeapi: unavailable Build read cannot carry a leader hint")
		}
		return nil
	default:
		return fmt.Errorf("routeapi: unknown Build read outcome %q", r.Outcome)
	}
}
