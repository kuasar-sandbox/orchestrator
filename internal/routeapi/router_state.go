package routeapi

import "sync"

type routeIdentity struct {
	group    string
	routeKey string
}

// RouterState carries the Registry-History-Generation-fenced request identity, monotonic
// per-Route minimum revisions, and cached shard leader hints. It is not a Route
// authority and stores no workflow state.
type RouterState struct {
	mu       sync.Mutex
	identity RequestIdentity
	minimum  map[routeIdentity]uint64
	leaders  map[uint32]LeaderHint
}

func NewRouterState(identity RequestIdentity) (*RouterState, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	return &RouterState{
		identity: identity,
		minimum:  make(map[routeIdentity]uint64),
		leaders:  make(map[uint32]LeaderHint),
	}, nil
}

func (s *RouterState) RouteRequest(group, routeKey, sandboxID string, shardID uint32, strong bool) ReadRouteRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity := s.identity
	identity.ShardID = shardID
	return ReadRouteRequest{
		RequestIdentity:  identity,
		Group:            group,
		RouteKey:         routeKey,
		SandboxID:        sandboxID,
		MinRouteRevision: s.minimum[routeIdentity{group: group, routeKey: routeKey}],
		Strong:           strong,
	}
}

func (s *RouterState) ObserveRoute(request ReadRouteRequest, response ReadRouteResponse) error {
	if err := response.ValidateFor(request); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.ClusterID != s.identity.ClusterID || request.RegistryGeneration != s.identity.RegistryGeneration ||
		request.SystemEpoch != s.identity.SystemEpoch || request.RegistryLayoutDigest != s.identity.RegistryLayoutDigest {
		return nil
	}
	if response.Outcome == ReadReady {
		key := routeIdentity{group: request.Group, routeKey: request.RouteKey}
		if response.RouteRevision > s.minimum[key] {
			s.minimum[key] = response.RouteRevision
		}
	}
	if response.LeaderHint != nil {
		current := s.leaders[request.ShardID]
		if response.LeaderHint.Term >= current.Term {
			s.leaders[request.ShardID] = *response.LeaderHint
		}
	}
	return nil
}

func (s *RouterState) LeaderHint(shardID uint32) (LeaderHint, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hint, found := s.leaders[shardID]
	return hint, found
}
