package routeapi

import (
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

type WatchRoutesRequest struct {
	RequestIdentity
	Group         string `json:"group"`
	Bucket        uint32 `json:"bucket"`
	AfterRevision uint64 `json:"after_revision"`
	Limit         uint32 `json:"limit"`
	Strong        bool   `json:"strong,omitempty"`
}

func (r WatchRoutesRequest) Validate() error {
	if err := r.RequestIdentity.Validate(); err != nil {
		return err
	}
	if r.Group == "" {
		return errors.New("routeapi: Route watch requires group")
	}
	if r.Limit == 0 || r.Limit > 4096 {
		return errors.New("routeapi: Route watch limit must be between 1 and 4096")
	}
	return nil
}

type RouteChange struct {
	Revision uint64                          `json:"revision"`
	Bucket   uint32                          `json:"bucket"`
	Group    string                          `json:"group"`
	RouteKey string                          `json:"route_key"`
	State    clusterstate.RouteWorkflowState `json:"state"`
}

type WatchRoutesResponse struct {
	Available      bool          `json:"available"`
	Reset          bool          `json:"reset"`
	Bucket         uint32        `json:"bucket"`
	FloorRevision  uint64        `json:"floor_revision,omitempty"`
	HeadRevision   uint64        `json:"head_revision,omitempty"`
	CursorRevision uint64        `json:"cursor_revision,omitempty"`
	Changes        []RouteChange `json:"changes"`
	LeaderHint     *LeaderHint   `json:"leader_hint,omitempty"`
	Reason         string        `json:"reason,omitempty"`
}

func (r WatchRoutesResponse) ValidateFor(request WatchRoutesRequest) error {
	if r.Bucket != request.Bucket {
		return errors.New("routeapi: Route watch response bucket mismatch")
	}
	if !r.Available {
		if r.Reason == "" || r.Reset || r.FloorRevision != 0 || r.HeadRevision != 0 ||
			r.CursorRevision != 0 || len(r.Changes) != 0 {
			return errors.New("routeapi: unavailable Route watch carries changefeed state")
		}
		if r.LeaderHint != nil {
			return r.LeaderHint.Validate()
		}
		return nil
	}
	if r.Reason != "" || r.LeaderHint != nil || r.HeadRevision == 0 ||
		r.FloorRevision > r.HeadRevision || r.CursorRevision > r.HeadRevision {
		return errors.New("routeapi: available Route watch has invalid revision bounds")
	}
	if r.Reset {
		if len(r.Changes) != 0 || r.CursorRevision != r.HeadRevision {
			return errors.New("routeapi: reset Route watch must advance directly to its snapshot head")
		}
		return nil
	}
	if r.CursorRevision < request.AfterRevision {
		return errors.New("routeapi: Route watch cursor regressed")
	}
	previous := request.AfterRevision
	for _, change := range r.Changes {
		if change.Revision <= previous || change.Revision > r.CursorRevision || change.Group != request.Group ||
			change.Bucket != request.Bucket || change.RouteKey == "" || !validRouteChangeState(change.State) {
			return fmt.Errorf("routeapi: invalid Route change at revision %d", change.Revision)
		}
		previous = change.Revision
	}
	return nil
}

func validRouteChangeState(state clusterstate.RouteWorkflowState) bool {
	switch state {
	case clusterstate.WorkflowRouteStarting, clusterstate.WorkflowRouteReady, clusterstate.WorkflowRoutePaused,
		clusterstate.WorkflowRouteResuming, clusterstate.WorkflowRouteDeleting, clusterstate.WorkflowRouteTombstone:
		return true
	default:
		return false
	}
}
