package routeclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

var (
	ErrPermitUnavailable      = errors.New("routeclient: matching Serve Permit is unavailable")
	ErrMutationOutcomeUnknown = errors.New("routeclient: mutation outcome is unknown")
	ErrInvalidRouteListToken  = errors.New("routeclient: invalid Route list token")
)

const (
	maximumResponseBytes      = 4 << 20
	routeListRPCPageSize      = 8
	nonMutationAttemptTimeout = 6 * time.Second
)

type Endpoint struct {
	MemberID string
	BaseURL  string
	Client   *http.Client
}

type cachedPermit struct {
	response routeapi.PermitResponse
	expires  time.Time
}

type requestDeliveryError struct {
	err            error
	mayHaveReached bool
}

func (e *requestDeliveryError) Error() string { return e.err.Error() }
func (e *requestDeliveryError) Unwrap() error { return e.err }

func requestMayHaveReached(err error) bool {
	var delivery *requestDeliveryError
	return errors.As(err, &delivery) && delivery.mayHaveReached
}

type Client struct {
	registryLayout raftstore.RegistryLayout
	digest         string
	endpoints      map[string]Endpoint
	now            func() time.Time

	mu      sync.RWMutex
	permit  *cachedPermit
	leaders map[uint32]routeapi.LeaderHint
}

type RouteMutationResult struct {
	Response      routeapi.RouteMutationResponse
	ServeIdentity routeapi.RegistryServeIdentity
}

type RouteReadResult struct {
	Response      routeapi.ReadRouteResponse
	ServeIdentity routeapi.RegistryServeIdentity
}

type BuildMutationResult struct {
	Response      routeapi.BuildMutationResponse
	ServeIdentity routeapi.RegistryServeIdentity
}

type BuildReadResult struct {
	Response      routeapi.ReadBuildResponse
	ServeIdentity routeapi.RegistryServeIdentity
}

type RouteListResult struct {
	Routes          []routeapi.ListedRoute
	BucketRevisions []uint64
	ServeIdentity   routeapi.RegistryServeIdentity
}

type RouteListPageResult struct {
	Routes        []routeapi.ListedRoute
	NextToken     string
	ServeIdentity routeapi.RegistryServeIdentity
}

type RouteWatchResult struct {
	Response      routeapi.WatchRoutesResponse
	ServeIdentity routeapi.RegistryServeIdentity
}

func New(registryLayout raftstore.RegistryLayout, digest string, endpoints []Endpoint) (*Client, error) {
	if err := registryLayout.Validate(); err != nil {
		return nil, err
	}
	wantDigest, err := registryLayout.Digest()
	if err != nil || digest != wantDigest {
		return nil, errors.New("routeclient: verified registryLayout digest mismatch")
	}
	byID := make(map[string]Endpoint, len(endpoints))
	for _, endpoint := range endpoints {
		member, found := registryLayoutMember(registryLayout, endpoint.MemberID)
		if !found || endpoint.BaseURL != member.InternalEndpoint || endpoint.Client == nil {
			return nil, errors.New("routeclient: endpoint is not an exact registryLayout member")
		}
		if _, duplicate := byID[endpoint.MemberID]; duplicate {
			return nil, errors.New("routeclient: duplicate endpoint member")
		}
		byID[endpoint.MemberID] = endpoint
	}
	if len(byID) != len(registryLayout.Members) {
		return nil, errors.New("routeclient: every registryLayout member requires an authenticated client")
	}
	return &Client{
		registryLayout: raftstore.CloneRegistryLayout(registryLayout), digest: digest, endpoints: byID,
		now: time.Now, leaders: make(map[uint32]routeapi.LeaderHint),
	}, nil
}

func (c *Client) RegistryLayout() raftstore.RegistryLayout {
	return raftstore.CloneRegistryLayout(c.registryLayout)
}

func (c *Client) RefreshPermit(ctx context.Context) (routeapi.RegistryServeIdentity, error) {
	request := routeapi.PermitRequest{
		ClusterID: c.registryLayout.ClusterID, RegistryGeneration: c.registryLayout.RegistryGeneration,
		RegistryLayoutDigest: c.digest,
	}
	var lastErr error
	for _, endpoint := range c.systemEndpoints() {
		started := c.now()
		var response routeapi.PermitResponse
		if err := postJSONBounded(ctx, endpoint, routeapi.PermitPath, request, &response); err != nil {
			lastErr = err
			continue
		}
		if err := response.ValidateFor(request); err != nil {
			lastErr = err
			continue
		}
		if response.MaxLifetimeMillis != c.registryLayout.ServePermitMaxMillis {
			lastErr = errors.New("routeclient: Permit lifetime does not match the signed Registry Layout")
			continue
		}
		expires := started.Add(time.Duration(response.MaxLifetimeMillis) * time.Millisecond)
		if !c.now().Before(expires) {
			lastErr = ErrPermitUnavailable
			continue
		}
		permit := &cachedPermit{
			response: response,
			expires:  expires,
		}
		c.mu.Lock()
		c.permit = permit
		c.mu.Unlock()
		return serveIdentity(response), nil
	}
	if lastErr == nil {
		lastErr = ErrPermitUnavailable
	}
	return routeapi.RegistryServeIdentity{}, lastErr
}

func (c *Client) Run(ctx context.Context) error {
	if _, err := c.currentPermit(); err != nil {
		if _, err := c.RefreshPermit(ctx); err != nil {
			return err
		}
	}
	for {
		remaining := c.permitRemaining()
		wait := remaining / 3
		if wait < 50*time.Millisecond {
			wait = 50 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
			_, _ = c.RefreshPermit(ctx)
		}
	}
}

func (c *Client) CurrentServeIdentity(write bool) (routeapi.RegistryServeIdentity, error) {
	permit, err := c.currentPermit()
	if err != nil || !permitAuthorizesServeIdentity(permit) || write && !permit.WriteGate {
		return routeapi.RegistryServeIdentity{}, ErrPermitUnavailable
	}
	return serveIdentity(permit), nil
}

func (c *Client) CacheAuthorized(identity routeapi.RegistryServeIdentity) bool {
	permit, err := c.currentPermit()
	return err == nil && permitAuthorizesServeIdentity(permit) && serveIdentity(permit) == identity
}

func (c *Client) ReserveSandbox(
	ctx context.Context,
	group, routeKey string,
	minRevision uint64,
	input routeapi.SandboxInput,
) (RouteMutationResult, error) {
	identity, err := c.routeIdentity(group, routeKey, true)
	if err != nil {
		return RouteMutationResult{}, err
	}
	request := routeapi.ReserveSandboxRequest{
		RequestIdentity: identity, Group: group, RouteKey: routeKey,
		MinRouteRevision: minRevision, Input: input,
	}
	response, err := c.routeMutation(ctx, identity.ShardID, routeapi.ReserveRoutePath, request, func(response routeapi.RouteMutationResponse) error {
		return response.ValidateFor(identity, group, routeKey)
	})
	return RouteMutationResult{Response: response, ServeIdentity: serveIdentityFromRequest(identity)}, err
}

func (c *Client) ResumeSandbox(
	ctx context.Context,
	group, routeKey string,
	minRevision uint64,
) (RouteMutationResult, error) {
	identity, err := c.routeIdentity(group, routeKey, true)
	if err != nil {
		return RouteMutationResult{}, err
	}
	request := routeapi.ResumeSandboxRequest{
		RequestIdentity: identity, Group: group, RouteKey: routeKey,
		MinRouteRevision: minRevision,
	}
	response, err := c.routeMutation(ctx, identity.ShardID, routeapi.ResumeRoutePath, request, func(response routeapi.RouteMutationResponse) error {
		return response.ValidateFor(identity, group, routeKey)
	})
	return RouteMutationResult{Response: response, ServeIdentity: serveIdentityFromRequest(identity)}, err
}

func (c *Client) DeleteSandbox(
	ctx context.Context,
	group, routeKey string,
	minRevision uint64,
) (RouteMutationResult, error) {
	identity, err := c.routeIdentity(group, routeKey, true)
	if err != nil {
		return RouteMutationResult{}, err
	}
	request := routeapi.DeleteSandboxRequest{
		RequestIdentity: identity, Group: group, RouteKey: routeKey,
		MinRouteRevision: minRevision,
	}
	response, err := c.routeMutation(ctx, identity.ShardID, routeapi.DeleteRoutePath, request, func(response routeapi.RouteMutationResponse) error {
		return response.ValidateFor(identity, group, routeKey)
	})
	return RouteMutationResult{Response: response, ServeIdentity: serveIdentityFromRequest(identity)}, err
}

func (c *Client) ReadRoute(
	ctx context.Context,
	group, routeKey string,
	minRevision uint64,
) (RouteReadResult, error) {
	identity, err := c.routeIdentity(group, routeKey, false)
	if err != nil {
		return RouteReadResult{}, err
	}
	request := routeapi.ReadRouteRequest{
		RequestIdentity: identity, Group: group, RouteKey: routeKey,
		MinRouteRevision: minRevision,
	}
	for _, endpoint := range c.localReadEndpoints(identity.ShardID, group+"\x00"+routeKey) {
		var response routeapi.ReadRouteResponse
		if err := postJSONBounded(ctx, endpoint, routeapi.ReadRoutePath, request, &response); err != nil {
			continue
		}
		if err := response.ValidateFor(request); err != nil {
			continue
		}
		if response.Outcome == routeapi.ReadReady || response.Outcome == routeapi.ReadConflict {
			return RouteReadResult{Response: response, ServeIdentity: serveIdentityFromRequest(identity)}, nil
		}
		c.observeLeader(identity.ShardID, response.LeaderHint)
	}
	request.Strong = true
	response, err := c.readRouteStrong(ctx, request)
	return RouteReadResult{Response: response, ServeIdentity: serveIdentityFromRequest(identity)}, err
}

// ReadAddressableRoute resolves an execution for direct control forwarding.
// It is deliberately leader-only: replica-local positive reads remain limited
// to READY Routes, while the strong path may also expose a committed PAUSED
// execution without making it eligible for data-plane routing.
func (c *Client) ReadAddressableRoute(
	ctx context.Context,
	group, routeKey string,
	minRevision uint64,
) (RouteReadResult, error) {
	identity, err := c.routeIdentity(group, routeKey, false)
	if err != nil {
		return RouteReadResult{}, err
	}
	request := routeapi.ReadRouteRequest{
		RequestIdentity: identity, Group: group, RouteKey: routeKey,
		MinRouteRevision: minRevision, Strong: true, Addressable: true,
	}
	response, err := c.readRouteStrong(ctx, request)
	return RouteReadResult{Response: response, ServeIdentity: serveIdentityFromRequest(identity)}, err
}

func (c *Client) RegisterBuild(
	ctx context.Context,
	group, buildID string,
	minRevision uint64,
	input routeapi.BuildInput,
) (BuildMutationResult, error) {
	identity, err := c.buildIdentity(group, buildID, true)
	if err != nil {
		return BuildMutationResult{}, err
	}
	request := routeapi.RegisterBuildRequest{
		RequestIdentity: identity, Group: group, BuildID: buildID,
		MinBuildRevision: minRevision, Input: input,
	}
	response, err := c.buildMutation(ctx, identity.ShardID, routeapi.RegisterBuildPath, request)
	return BuildMutationResult{Response: response, ServeIdentity: serveIdentityFromRequest(identity)}, err
}

func (c *Client) ReadBuild(
	ctx context.Context,
	group, buildID string,
	minRevision uint64,
) (BuildReadResult, error) {
	identity, err := c.buildIdentity(group, buildID, false)
	if err != nil {
		return BuildReadResult{}, err
	}
	request := routeapi.ReadBuildRequest{
		RequestIdentity: identity, Group: group, BuildID: buildID,
		MinBuildRevision: minRevision,
	}
	for _, endpoint := range c.localReadEndpoints(identity.ShardID, group+"\x00"+buildID) {
		var response routeapi.ReadBuildResponse
		if err := postJSONBounded(ctx, endpoint, routeapi.ReadBuildPath, request, &response); err != nil {
			continue
		}
		if err := response.ValidateFor(request); err != nil {
			continue
		}
		if response.Outcome == routeapi.ReadReady || response.Outcome == routeapi.ReadConflict {
			return BuildReadResult{Response: response, ServeIdentity: serveIdentityFromRequest(identity)}, nil
		}
		c.observeLeader(identity.ShardID, response.LeaderHint)
	}
	request.Strong = true
	response, err := c.readBuildStrong(ctx, request)
	return BuildReadResult{Response: response, ServeIdentity: serveIdentityFromRequest(identity)}, err
}

type routeListCursorV1 struct {
	Version       uint8                           `json:"version"`
	Group         string                          `json:"group"`
	State         clusterstate.RouteWorkflowState `json:"state,omitempty"`
	Bucket        uint32                          `json:"bucket"`
	AfterRouteKey string                          `json:"after_route_key"`
}

func (c *Client) ListRoutesPage(
	ctx context.Context,
	group string,
	state clusterstate.RouteWorkflowState,
	limit uint32,
	nextToken string,
) (RouteListPageResult, error) {
	if group == "" || limit == 0 || limit > 1000 ||
		(state != "" && state != clusterstate.WorkflowRouteReady && state != clusterstate.WorkflowRoutePaused) {
		return RouteListPageResult{}, errors.New("routeclient: invalid Route list page")
	}
	cursor := routeListCursorV1{Version: 1, Group: group, State: state}
	if nextToken != "" {
		decoded, err := decodeRouteListCursor(nextToken)
		if err != nil || decoded.Group != group || decoded.State != state ||
			decoded.Bucket >= c.registryLayout.RouteBucketCount || decoded.AfterRouteKey == "" {
			return RouteListPageResult{}, ErrInvalidRouteListToken
		}
		cursor = decoded
	}
	serveIdentity, err := c.CurrentServeIdentity(false)
	if err != nil {
		return RouteListPageResult{}, err
	}
	type positionedRoute struct {
		route  routeapi.ListedRoute
		bucket uint32
	}
	target := int(limit) + 1
	routes := make([]positionedRoute, 0, target)
	for bucket := cursor.Bucket; bucket < c.registryLayout.RouteBucketCount && len(routes) < target; bucket++ {
		if !c.CacheAuthorized(serveIdentity) {
			return RouteListPageResult{}, ErrPermitUnavailable
		}
		identity, err := c.routeBucketIdentity(serveIdentity, group, bucket)
		if err != nil {
			return RouteListPageResult{}, err
		}
		afterRouteKey := ""
		if bucket == cursor.Bucket {
			afterRouteKey = cursor.AfterRouteKey
		}
		readKey := group + "\x00" + strconv.FormatUint(uint64(bucket), 10)
		for len(routes) < target {
			requestLimit := min(target-len(routes), routeListRPCPageSize)
			request := routeapi.ListRoutesRequest{
				RequestIdentity: identity, Group: group, Bucket: bucket, State: state,
				AfterRouteKey: afterRouteKey, Limit: uint32(requestLimit),
			}
			response, err := c.listRouteBucketPage(ctx, readKey, request)
			if err != nil {
				return RouteListPageResult{}, err
			}
			if !c.CacheAuthorized(serveIdentity) {
				return RouteListPageResult{}, ErrPermitUnavailable
			}
			for _, route := range response.Routes {
				routes = append(routes, positionedRoute{route: route, bucket: bucket})
			}
			if response.NextRouteKey == "" {
				break
			}
			afterRouteKey = response.NextRouteKey
		}
	}
	result := RouteListPageResult{ServeIdentity: serveIdentity}
	if len(routes) > int(limit) {
		last := routes[limit-1]
		result.NextToken, err = encodeRouteListCursor(routeListCursorV1{
			Version: 1, Group: group, State: state, Bucket: last.bucket, AfterRouteKey: last.route.RouteKey,
		})
		if err != nil {
			return RouteListPageResult{}, err
		}
		routes = routes[:limit]
	}
	result.Routes = make([]routeapi.ListedRoute, len(routes))
	for index := range routes {
		result.Routes[index] = routes[index].route
	}
	if !c.CacheAuthorized(serveIdentity) {
		return RouteListPageResult{}, ErrPermitUnavailable
	}
	return result, nil
}

func (c *Client) listRouteBucketPage(
	ctx context.Context,
	readKey string,
	request routeapi.ListRoutesRequest,
) (routeapi.ListRoutesResponse, error) {
	var lastErr error
	for pass := 0; pass < 2; pass++ {
		request.Strong = pass == 1
		endpoints := c.localReadEndpoints(request.ShardID, readKey)
		if request.Strong {
			endpoints = c.shardEndpoints(request.ShardID)
		}
		for _, endpoint := range endpoints {
			var response routeapi.ListRoutesResponse
			if err := postJSONBounded(ctx, endpoint, routeapi.ListRoutesPath, request, &response); err != nil {
				lastErr = err
				continue
			}
			if err := response.ValidateFor(request); err != nil {
				lastErr = err
				continue
			}
			if response.Reason != "" {
				lastErr = fmt.Errorf("routeclient: Route bucket is unavailable: %s", response.Reason)
				continue
			}
			return response, nil
		}
	}
	if lastErr == nil {
		lastErr = errors.New("routeclient: Route bucket has no serving replica")
	}
	return routeapi.ListRoutesResponse{}, lastErr
}

func encodeRouteListCursor(cursor routeListCursorV1) (string, error) {
	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeRouteListCursor(token string) (routeListCursorV1, error) {
	if len(token) > 16<<10 {
		return routeListCursorV1{}, ErrInvalidRouteListToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return routeListCursorV1{}, ErrInvalidRouteListToken
	}
	var cursor routeListCursorV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return routeListCursorV1{}, ErrInvalidRouteListToken
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || cursor.Version != 1 {
		return routeListCursorV1{}, ErrInvalidRouteListToken
	}
	return cursor, nil
}

func (c *Client) ListRoutes(ctx context.Context, group string) (RouteListResult, error) {
	const (
		pageLimit               uint32 = routeListRPCPageSize
		maximumSnapshotRestarts        = 3
	)

	serveIdentity, err := c.CurrentServeIdentity(false)
	if err != nil {
		return RouteListResult{}, err
	}
	routes := make([]routeapi.ListedRoute, 0)
	bucketRevisions := make([]uint64, c.registryLayout.RouteBucketCount)
	for bucket := uint32(0); bucket < c.registryLayout.RouteBucketCount; bucket++ {
		if !c.CacheAuthorized(serveIdentity) {
			return RouteListResult{}, ErrPermitUnavailable
		}
		identity, err := c.routeBucketIdentity(serveIdentity, group, bucket)
		if err != nil {
			return RouteListResult{}, err
		}
		readKey := group + "\x00" + strconv.FormatUint(uint64(bucket), 10)
		request := routeapi.ListRoutesRequest{
			RequestIdentity: identity, Group: group, Bucket: bucket, Limit: pageLimit,
		}
		bucketRoutes := make([]routeapi.ListedRoute, 0)
		snapshotRevision := uint64(0)
		restarts := 0
		for {
			var (
				response routeapi.ListRoutesResponse
				lastErr  error
				found    bool
			)
			for pass := 0; pass < 2 && !found; pass++ {
				request.Strong = pass == 1
				endpoints := c.localReadEndpoints(identity.ShardID, readKey)
				if request.Strong {
					endpoints = c.shardEndpoints(identity.ShardID)
				}
				for _, endpoint := range endpoints {
					response = routeapi.ListRoutesResponse{}
					if err := postJSONBounded(ctx, endpoint, routeapi.ListRoutesPath, request, &response); err != nil {
						lastErr = err
						continue
					}
					if err := response.ValidateFor(request); err != nil {
						lastErr = err
						continue
					}
					if response.Reason != "" {
						lastErr = fmt.Errorf("routeclient: Route bucket is unavailable: %s", response.Reason)
						continue
					}
					found = true
					break
				}
			}
			if !found {
				if lastErr == nil {
					lastErr = errors.New("routeclient: Route bucket has no serving replica")
				}
				return RouteListResult{}, lastErr
			}
			if !c.CacheAuthorized(serveIdentity) {
				return RouteListResult{}, ErrPermitUnavailable
			}
			if snapshotRevision != 0 && response.SnapshotRevision != snapshotRevision {
				restarts++
				if restarts > maximumSnapshotRestarts {
					return RouteListResult{}, errors.New("routeclient: Route bucket snapshot changed repeatedly during pagination")
				}
				bucketRoutes = bucketRoutes[:0]
				snapshotRevision = 0
				request.AfterRouteKey = ""
				continue
			}
			if snapshotRevision == 0 {
				snapshotRevision = response.SnapshotRevision
			}
			bucketRoutes = append(bucketRoutes, response.Routes...)
			if response.NextRouteKey == "" {
				break
			}
			request.AfterRouteKey = response.NextRouteKey
		}
		routes = append(routes, bucketRoutes...)
		bucketRevisions[bucket] = snapshotRevision
	}
	if !c.CacheAuthorized(serveIdentity) {
		return RouteListResult{}, ErrPermitUnavailable
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].RouteKey < routes[j].RouteKey })
	return RouteListResult{Routes: routes, BucketRevisions: bucketRevisions, ServeIdentity: serveIdentity}, nil
}

func (c *Client) WatchRoutes(
	ctx context.Context,
	group string,
	bucket uint32,
	afterRevision uint64,
	limit uint32,
) (RouteWatchResult, error) {
	serveIdentity, err := c.CurrentServeIdentity(false)
	if err != nil {
		return RouteWatchResult{}, err
	}
	identity, err := c.routeBucketIdentity(serveIdentity, group, bucket)
	if err != nil {
		return RouteWatchResult{}, err
	}
	request := routeapi.WatchRoutesRequest{
		RequestIdentity: identity, Group: group, Bucket: bucket,
		AfterRevision: afterRevision, Limit: limit,
	}
	readKey := group + "\x00" + strconv.FormatUint(uint64(bucket), 10)
	var lastErr error
	for pass := 0; pass < 2; pass++ {
		request.Strong = pass == 1
		endpoints := c.localReadEndpoints(identity.ShardID, readKey)
		if request.Strong {
			endpoints = c.shardEndpoints(identity.ShardID)
		}
		for _, endpoint := range endpoints {
			if !c.CacheAuthorized(serveIdentity) {
				return RouteWatchResult{}, ErrPermitUnavailable
			}
			var response routeapi.WatchRoutesResponse
			if err := postJSONBounded(ctx, endpoint, routeapi.WatchRoutesPath, request, &response); err != nil {
				lastErr = err
				continue
			}
			if err := response.ValidateFor(request); err != nil {
				lastErr = err
				continue
			}
			if !response.Available {
				c.observeLeader(identity.ShardID, response.LeaderHint)
				lastErr = fmt.Errorf("routeclient: Route changefeed is unavailable: %s", response.Reason)
				continue
			}
			if !c.CacheAuthorized(serveIdentity) {
				return RouteWatchResult{}, ErrPermitUnavailable
			}
			return RouteWatchResult{Response: response, ServeIdentity: serveIdentity}, nil
		}
	}
	if lastErr == nil {
		lastErr = errors.New("routeclient: Route changefeed has no serving replica")
	}
	return RouteWatchResult{}, lastErr
}

func (c *Client) routeMutation(
	ctx context.Context,
	shardID uint32,
	path string,
	request any,
	validate func(routeapi.RouteMutationResponse) error,
) (routeapi.RouteMutationResponse, error) {
	var (
		attemptErrors  []error
		outcomeUnknown bool
	)
	for attempt := 0; attempt < 2; attempt++ {
		for _, endpoint := range c.shardEndpoints(shardID) {
			var response routeapi.RouteMutationResponse
			if err := postJSON(ctx, endpoint, path, request, &response); err != nil {
				outcomeUnknown = outcomeUnknown || requestMayHaveReached(err)
				attemptErrors = append(attemptErrors, fmt.Errorf("member %s: %w", endpoint.MemberID, err))
				continue
			}
			if err := validate(response); err != nil {
				outcomeUnknown = true
				attemptErrors = append(attemptErrors, fmt.Errorf("member %s returned an invalid response: %w", endpoint.MemberID, err))
				continue
			}
			if response.Outcome == routeapi.MutationNeedLeader {
				c.observeLeader(shardID, response.LeaderHint)
				attemptErrors = append(attemptErrors, fmt.Errorf("member %s is not the shard leader: %s", endpoint.MemberID, response.Reason))
				continue
			}
			return response, nil
		}
	}
	if len(attemptErrors) == 0 {
		attemptErrors = append(attemptErrors, errors.New("no Registry Layout member serves the Route shard"))
	}
	err := fmt.Errorf("routeclient: Route shard leader is unavailable: %w", errors.Join(attemptErrors...))
	if outcomeUnknown {
		err = errors.Join(ErrMutationOutcomeUnknown, err)
	}
	return routeapi.RouteMutationResponse{}, err
}

func (c *Client) buildMutation(
	ctx context.Context,
	shardID uint32,
	path string,
	request routeapi.RegisterBuildRequest,
) (routeapi.BuildMutationResponse, error) {
	var (
		attemptErrors  []error
		outcomeUnknown bool
	)
	for attempt := 0; attempt < 2; attempt++ {
		for _, endpoint := range c.shardEndpoints(shardID) {
			var response routeapi.BuildMutationResponse
			if err := postJSON(ctx, endpoint, path, request, &response); err != nil {
				outcomeUnknown = outcomeUnknown || requestMayHaveReached(err)
				attemptErrors = append(attemptErrors, fmt.Errorf("member %s: %w", endpoint.MemberID, err))
				continue
			}
			if err := response.ValidateFor(request); err != nil {
				outcomeUnknown = true
				attemptErrors = append(attemptErrors, fmt.Errorf("member %s returned an invalid response: %w", endpoint.MemberID, err))
				continue
			}
			if response.Outcome == routeapi.MutationNeedLeader {
				c.observeLeader(shardID, response.LeaderHint)
				attemptErrors = append(attemptErrors, fmt.Errorf("member %s is not the shard leader: %s", endpoint.MemberID, response.Reason))
				continue
			}
			return response, nil
		}
	}
	if len(attemptErrors) == 0 {
		attemptErrors = append(attemptErrors, errors.New("no Registry Layout member serves the Build shard"))
	}
	err := fmt.Errorf("routeclient: Build shard leader is unavailable: %w", errors.Join(attemptErrors...))
	if outcomeUnknown {
		err = errors.Join(ErrMutationOutcomeUnknown, err)
	}
	return routeapi.BuildMutationResponse{}, err
}

func (c *Client) readRouteStrong(ctx context.Context, request routeapi.ReadRouteRequest) (routeapi.ReadRouteResponse, error) {
	maxPasses := len(c.shardEndpoints(request.ShardID)) + 1
	for pass := 0; pass < maxPasses; pass++ {
		for _, endpoint := range c.shardEndpoints(request.ShardID) {
			var response routeapi.ReadRouteResponse
			if err := postJSONBounded(ctx, endpoint, routeapi.ReadRoutePath, request, &response); err != nil {
				continue
			}
			if err := response.ValidateFor(request); err != nil {
				continue
			}
			if response.Outcome == routeapi.ReadNeedLeader || response.Outcome == routeapi.ReadReplicaBehind {
				if c.observeLeader(request.ShardID, response.LeaderHint) {
					break
				}
				continue
			}
			return response, nil
		}
	}
	return routeapi.ReadRouteResponse{}, errors.New("routeclient: Route shard is unavailable")
}

func (c *Client) readBuildStrong(ctx context.Context, request routeapi.ReadBuildRequest) (routeapi.ReadBuildResponse, error) {
	maxPasses := len(c.shardEndpoints(request.ShardID)) + 1
	for pass := 0; pass < maxPasses; pass++ {
		for _, endpoint := range c.shardEndpoints(request.ShardID) {
			var response routeapi.ReadBuildResponse
			if err := postJSONBounded(ctx, endpoint, routeapi.ReadBuildPath, request, &response); err != nil {
				continue
			}
			if err := response.ValidateFor(request); err != nil {
				continue
			}
			if response.Outcome == routeapi.ReadNeedLeader || response.Outcome == routeapi.ReadReplicaBehind {
				if c.observeLeader(request.ShardID, response.LeaderHint) {
					break
				}
				continue
			}
			return response, nil
		}
	}
	return routeapi.ReadBuildResponse{}, errors.New("routeclient: Build shard is unavailable")
}

func (c *Client) routeIdentity(group, routeKey string, write bool) (routeapi.RequestIdentity, error) {
	serveIdentity, err := c.CurrentServeIdentity(write)
	if err != nil {
		return routeapi.RequestIdentity{}, err
	}
	_, shardID, err := clusterstate.RouteShardFor(group, routeKey, c.registryLayout.RouteBucketCount, c.registryLayout.VirtualShardCount)
	if err != nil {
		return routeapi.RequestIdentity{}, err
	}
	return requestIdentity(serveIdentity, shardID), nil
}

func (c *Client) buildIdentity(group, buildID string, write bool) (routeapi.RequestIdentity, error) {
	serveIdentity, err := c.CurrentServeIdentity(write)
	if err != nil {
		return routeapi.RequestIdentity{}, err
	}
	_, shardID, err := clusterstate.BuildShardFor(group, buildID, c.registryLayout.BuildBucketCount, c.registryLayout.VirtualShardCount)
	if err != nil {
		return routeapi.RequestIdentity{}, err
	}
	return requestIdentity(serveIdentity, shardID), nil
}

func (c *Client) routeBucketIdentity(serveIdentity routeapi.RegistryServeIdentity, group string, bucket uint32) (routeapi.RequestIdentity, error) {
	if err := serveIdentity.Validate(); err != nil {
		return routeapi.RequestIdentity{}, err
	}
	if group == "" || bucket >= c.registryLayout.RouteBucketCount {
		return routeapi.RequestIdentity{}, errors.New("routeclient: invalid Route bucket")
	}
	hash, err := clusterstate.RouteShardHash(group, bucket)
	if err != nil {
		return routeapi.RequestIdentity{}, err
	}
	return requestIdentity(serveIdentity, uint32(hash%uint64(c.registryLayout.VirtualShardCount))), nil
}

func requestIdentity(serveIdentity routeapi.RegistryServeIdentity, shardID uint32) routeapi.RequestIdentity {
	return routeapi.RequestIdentity{
		ClusterID: serveIdentity.ClusterID, RegistryGeneration: serveIdentity.RegistryGeneration,
		SystemEpoch: serveIdentity.SystemEpoch, RegistryLayoutDigest: serveIdentity.RegistryLayoutDigest, ShardID: shardID,
	}
}

func serveIdentityFromRequest(identity routeapi.RequestIdentity) routeapi.RegistryServeIdentity {
	return routeapi.RegistryServeIdentity{
		ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
		SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest,
	}
}

func serveIdentity(permit routeapi.PermitResponse) routeapi.RegistryServeIdentity {
	return routeapi.RegistryServeIdentity{
		ClusterID: permit.ClusterID, RegistryGeneration: permit.RegistryGeneration,
		SystemEpoch: permit.SystemEpoch, RegistryLayoutDigest: permit.RegistryLayoutDigest,
	}
}

func (c *Client) currentPermit() (routeapi.PermitResponse, error) {
	now := c.now()
	c.mu.RLock()
	permit := c.permit
	if permit != nil {
		copy := *permit
		permit = &copy
	}
	c.mu.RUnlock()
	if permit == nil || !now.Before(permit.expires) {
		return routeapi.PermitResponse{}, ErrPermitUnavailable
	}
	return permit.response, nil
}

func (c *Client) permitRemaining() time.Duration {
	c.mu.RLock()
	permit := c.permit
	c.mu.RUnlock()
	if permit == nil {
		return 0
	}
	remaining := permit.expires.Sub(c.now())
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (c *Client) observeLeader(shardID uint32, hint *routeapi.LeaderHint) bool {
	if hint == nil || !c.memberServesShard(hint.MemberID, shardID) {
		return false
	}
	member, found := registryLayoutMember(c.registryLayout, hint.MemberID)
	if !found || member.InternalEndpoint != hint.Endpoint {
		return false
	}
	c.mu.Lock()
	current := c.leaders[shardID]
	accepted := false
	if hint.Term >= current.Term {
		c.leaders[shardID] = *hint
		accepted = true
	}
	c.mu.Unlock()
	return accepted
}

func (c *Client) shardEndpoints(shardID uint32) []Endpoint {
	if shardID >= uint32(len(c.registryLayout.DataShards)) {
		return nil
	}
	c.mu.RLock()
	hint := c.leaders[shardID]
	c.mu.RUnlock()
	placements := c.registryLayout.DataShards[shardID].Replicas
	result := make([]Endpoint, 0, len(placements))
	if hint.MemberID != "" {
		result = append(result, c.endpoints[hint.MemberID])
	}
	for _, placement := range placements {
		if placement.MemberID != hint.MemberID {
			result = append(result, c.endpoints[placement.MemberID])
		}
	}
	return result
}

func (c *Client) localReadEndpoints(shardID uint32, key string) []Endpoint {
	if shardID >= uint32(len(c.registryLayout.DataShards)) {
		return nil
	}
	type rankedEndpoint struct {
		endpoint Endpoint
		score    uint64
	}
	placements := c.registryLayout.DataShards[shardID].Replicas
	ranked := make([]rankedEndpoint, 0, len(placements))
	for _, placement := range placements {
		memberID := placement.MemberID
		hashInput := "kuasar-route-local-read-v1\x00" + c.registryLayout.RegistryGeneration + "\x00" +
			strconv.FormatUint(uint64(shardID), 10) + "\x00" + key + "\x00" + memberID
		ranked = append(ranked, rankedEndpoint{endpoint: c.endpoints[memberID], score: xxhash.Sum64String(hashInput)})
	}
	sort.Slice(ranked, func(left, right int) bool {
		if ranked[left].score == ranked[right].score {
			return ranked[left].endpoint.MemberID < ranked[right].endpoint.MemberID
		}
		return ranked[left].score > ranked[right].score
	})
	result := make([]Endpoint, len(ranked))
	for index := range ranked {
		result[index] = ranked[index].endpoint
	}
	return result
}

func permitAuthorizesServeIdentity(permit routeapi.PermitResponse) bool {
	return permit.ServeGate && permit.CutoverGate && permit.RecoveryClosed
}

func (c *Client) systemEndpoints() []Endpoint {
	result := make([]Endpoint, 0, len(c.registryLayout.SystemReplicas))
	for _, placement := range c.registryLayout.SystemReplicas {
		result = append(result, c.endpoints[placement.MemberID])
	}
	return result
}

func (c *Client) memberServesShard(memberID string, shardID uint32) bool {
	if shardID >= uint32(len(c.registryLayout.DataShards)) {
		return false
	}
	for _, placement := range c.registryLayout.DataShards[shardID].Replicas {
		if placement.MemberID == memberID {
			return true
		}
	}
	return false
}

func registryLayoutMember(registryLayout raftstore.RegistryLayout, memberID string) (raftstore.RegistryMember, bool) {
	index := sort.Search(len(registryLayout.Members), func(index int) bool {
		return registryLayout.Members[index].MemberID >= memberID
	})
	if index >= len(registryLayout.Members) || registryLayout.Members[index].MemberID != memberID {
		return raftstore.RegistryMember{}, false
	}
	return registryLayout.Members[index], true
}

func postJSON(ctx context.Context, endpoint Endpoint, path string, input, output any) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.BaseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	var wroteRequest atomic.Bool
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) { wroteRequest.Store(true) },
	}))
	request.Header.Set("Content-Type", "application/json")
	response, err := endpoint.Client.Do(request)
	if err != nil {
		return &requestDeliveryError{err: err, mayHaveReached: wroteRequest.Load()}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		message := strings.TrimSpace(string(detail))
		mayHaveReached := response.StatusCode < http.StatusBadRequest || response.StatusCode >= http.StatusInternalServerError
		if message == "" {
			return &requestDeliveryError{err: fmt.Errorf("routeclient: member %s returned %s", endpoint.MemberID, response.Status), mayHaveReached: mayHaveReached}
		}
		return &requestDeliveryError{err: fmt.Errorf("routeclient: member %s returned %s: %s", endpoint.MemberID, response.Status, message), mayHaveReached: mayHaveReached}
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maximumResponseBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return &requestDeliveryError{err: err, mayHaveReached: true}
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return &requestDeliveryError{err: errors.New("routeclient: response contains trailing data"), mayHaveReached: true}
	}
	return nil
}

func postJSONBounded(ctx context.Context, endpoint Endpoint, path string, input, output any) error {
	attemptCtx, cancel := context.WithTimeout(ctx, nonMutationAttemptTimeout)
	defer cancel()
	return postJSON(attemptCtx, endpoint, path, input, output)
}
