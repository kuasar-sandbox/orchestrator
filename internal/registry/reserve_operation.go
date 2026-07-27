package registry

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ReserveOperation selects the single lifecycle action performed by
// /route-link/reserve.
type ReserveOperation string

const (
	ReserveCreate  ReserveOperation = "create"
	ReserveConnect ReserveOperation = "connect"
	ReserveData    ReserveOperation = "data"
)

func (op ReserveOperation) Valid() bool {
	return op == ReserveCreate || op == ReserveConnect || op == ReserveData
}

// SandboxReserveRequest is the explicit trusted Router-to-Registry reserve
// context. Config is consumed only by create; the other fields come from query
// parameters and credential headers rather than a shared request body.
type SandboxReserveRequest struct {
	Operation         ReserveOperation
	Group             string
	RouteKey          string
	ExpectedSandboxID string
	Port              int
	TimeoutSeconds    int
	APIKey            string
	AccessToken       string
	MigrationToken    string
	Config            map[string]string
}

// ReserveSandbox performs exactly one operation. Authentication precedes every
// lifecycle mutation and command dispatch.
func (r *Registry) ReserveSandbox(ctx context.Context, req SandboxReserveRequest) (*ReserveResult, error) {
	if !req.Operation.Valid() || req.Group == "" || req.RouteKey == "" ||
		req.Port < 0 || req.Port > 65535 || req.TimeoutSeconds < 0 {
		return nil, ErrReserveBadRequest
	}
	if len(req.MigrationToken) > migrationtoken.MaxWireSize {
		return nil, fmt.Errorf("%w: migration token is too large", ErrReserveBadRequest)
	}
	switch req.Operation {
	case ReserveCreate:
		if req.ExpectedSandboxID != "" || req.Port != 0 || req.TimeoutSeconds != 0 ||
			req.AccessToken != "" || req.MigrationToken != "" {
			return nil, fmt.Errorf("%w: create contains fields for another operation", ErrReserveBadRequest)
		}
		if err := r.authenticateCreate(ctx, req.Group, req.APIKey); err != nil {
			return nil, err
		}
		return r.reserveCreate(ctx, req.Group, req.RouteKey, req.Config)
	case ReserveConnect:
		if req.ExpectedSandboxID == "" || req.Port != 0 || req.AccessToken != "" || len(req.Config) != 0 {
			return nil, fmt.Errorf("%w: connect contains fields for another operation", ErrReserveBadRequest)
		}
		return r.reserveConnect(ctx, req)
	case ReserveData:
		if req.ExpectedSandboxID == "" || req.TimeoutSeconds != 0 || req.APIKey != "" ||
			req.MigrationToken != "" || len(req.Config) != 0 {
			return nil, fmt.Errorf("%w: data contains fields for another operation", ErrReserveBadRequest)
		}
		return r.reserveData(ctx, req)
	default:
		return nil, ErrReserveBadRequest
	}
}

func (r *Registry) authenticateCreate(ctx context.Context, group, apiKey string) error {
	if apiKey == "" {
		return ErrReserveUnauthorized
	}
	replicas, minReady, timeout := r.scalePolicy()
	ok, err := r.VerifyAPIKeyWithMinReady(ctx, group, apiKey, replicas, minReady, timeout)
	if err != nil {
		return err
	}
	if !ok {
		return ErrReserveForbidden
	}
	return nil
}

func authenticateRecordAPIKey(rec *SandboxRecord, apiKey string) error {
	if apiKey == "" {
		return ErrReserveUnauthorized
	}
	if rec == nil || rec.APISecret == "" {
		return errors.New("registry: sandbox API credential is unavailable")
	}
	parsed, err := apikey.Parse(apiKey)
	secret, decodeErr := hex.DecodeString(rec.APISecret)
	if err != nil || decodeErr != nil || len(secret) != 32 || !apikey.Verify(parsed, secret) {
		return ErrReserveForbidden
	}
	return nil
}

func authenticateRecordAccessToken(rec *SandboxRecord, requestedPort int, accessToken string) error {
	if accessToken == "" {
		return ErrReserveUnauthorized
	}
	if rec == nil {
		return ErrSandboxNotFound
	}
	port, err := reserveDataPort(rec, requestedPort)
	if err != nil {
		return err
	}
	expected := rec.ForwardAccessToken
	if types.Profile(rec.Profile) == types.ProfileE2B && (port == 49983 || port == 49999) {
		expected = rec.EnvdAccessToken
	}
	if expected == "" {
		return errors.New("registry: sandbox data credential is unavailable")
	}
	if !constantTimeStringEqual(expected, accessToken) {
		return ErrReserveUnauthorized
	}
	return nil
}

func reserveDataPort(rec *SandboxRecord, requestedPort int) (int, error) {
	if requestedPort < 0 || requestedPort > 65535 {
		return 0, fmt.Errorf("%w: invalid data port", ErrReserveBadRequest)
	}
	if rec != nil && rec.TargetPort > 0 {
		if requestedPort > 0 && requestedPort != rec.TargetPort {
			return 0, fmt.Errorf("%w: target port mismatch", ErrReserveBadRequest)
		}
		return rec.TargetPort, nil
	}
	return requestedPort, nil
}

func (r *Registry) reserveConnect(ctx context.Context, req SandboxReserveRequest) (*ReserveResult, error) {
	for attempt := 0; attempt < 5; attempt++ {
		rec, rev, found, err := r.getSandboxForReserve(ctx, req.Group, req.RouteKey)
		if err != nil {
			return nil, err
		}
		if !found || rec.SandboxID != req.ExpectedSandboxID {
			return nil, ErrSandboxNotFound
		}
		if err := authenticateRecordAPIKey(rec, req.APIKey); err != nil {
			return nil, err
		}
		if _, _, err := replacementCredentials(rec); err != nil {
			return nil, err
		}
		switch rec.State {
		case StateReserved, StateReady, StatePaused:
		default:
			return nil, ErrSandboxNotFound
		}

		if r.nodeOwner == nil {
			if req.MigrationToken == "" {
				return nil, ErrNodeGone
			}
			return r.placeAndConnect(ctx, req, rec)
		}
		if err := r.nodeOwner.Connected(ctx, rec.NodeID); err != nil {
			if !errors.Is(err, ErrNodeGone) || req.MigrationToken == "" {
				return nil, err
			}
			return r.placeAndConnect(ctx, req, rec)
		}
		result, retry, err := r.connectCurrent(ctx, req, rec, rev)
		if retry {
			continue
		}
		if errors.Is(err, ErrNodeGone) && req.MigrationToken != "" {
			return r.placeAndConnect(ctx, req, rec)
		}
		return result, err
	}
	return nil, clusterstate.ErrConflict
}

func (r *Registry) connectCurrent(ctx context.Context, req SandboxReserveRequest, rec *SandboxRecord, rev int64) (*ReserveResult, bool, error) {
	before := *rec
	current := *rec
	if current.State == StatePaused {
		current.State = StateReserved
	}
	currentRev, ok, err := r.stores.CASSandbox(ctx, &current, rev)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, true, nil
	}
	if err := r.stores.AddNodeSandboxRef(ctx, current.NodeID, clusterstate.NodeSandboxRef{
		Group: current.Group, RouteKey: current.RouteKey, SandboxID: current.SandboxID,
		SandboxGeneration: current.SandboxGeneration, NodeSandboxID: current.NodeSandboxID,
		Profile: current.Profile, APISecretFingerprint: current.APISecretFingerprint,
	}); err != nil {
		r.rollbackConnect(&current, currentRev, &before, true)
		return nil, false, err
	}
	ack, err := r.nodeOwner.SendCommandAndWait(ctx, current.NodeID, connectCommand(req, &current), lifecycleAckTimeout)
	if err != nil {
		r.rollbackConnect(&current, currentRev, &before, !errors.Is(err, ErrNodeGone))
		return nil, false, err
	}
	if ack == nil || ack.Status != routesync.AckAccepted {
		r.rollbackConnect(&current, currentRev, &before, false)
		reason := ""
		if ack != nil {
			reason = ack.Reason
		}
		return nil, false, fmt.Errorf("registry: connect rejected: %s", reason)
	}
	if err := validateConnectResult(ack.Connect, &current); err != nil {
		r.rollbackConnect(&current, currentRev, &before, true)
		return nil, false, err
	}
	route, err := r.routeWithDataEndpoint(ctx, &current, currentRev)
	if err != nil {
		return nil, false, err
	}
	return &ReserveResult{Route: *route, Connect: cloneConnectResult(ack.Connect)}, false, nil
}

func connectCommand(req SandboxReserveRequest, rec *SandboxRecord) *routesync.Command {
	return &routesync.Command{
		CmdID: newID(), Kind: routesync.CmdConnect, SID: rec.NodeSandboxID,
		Profile: rec.Profile, APISecretFingerprint: rec.APISecretFingerprint,
		Cluster: &routesync.ClusterSandboxContext{
			Group: rec.Group, RouteKey: rec.RouteKey, AuthSandboxID: rec.AuthSandboxID,
		},
		MigrationToken: req.MigrationToken,
		TimeoutSeconds: req.TimeoutSeconds,
	}
}

func validateConnectResult(result *routesync.ConnectResult, rec *SandboxRecord) error {
	if result == nil || rec == nil || result.NodeSandboxID != rec.NodeSandboxID ||
		result.Profile != rec.Profile || result.TemplateID == "" || result.TemplateID != rec.TemplateID ||
		result.ForwardAccessToken == "" ||
		!constantTimeStringEqual(result.EnvdAccessToken, rec.EnvdAccessToken) ||
		!constantTimeStringEqual(result.TrafficAccessToken, rec.TrafficAccessToken) ||
		!constantTimeStringEqual(result.ForwardAccessToken, rec.ForwardAccessToken) {
		return errors.New("registry: connect result does not match sandbox route")
	}
	return nil
}

func cloneConnectResult(result *routesync.ConnectResult) *routesync.ConnectResult {
	if result == nil {
		return nil
	}
	clone := *result
	return &clone
}

func (r *Registry) rollbackConnect(current *SandboxRecord, currentRev int64, before *SandboxRecord, retainOwnership bool) {
	if current == nil || before == nil {
		return
	}
	_ = r.rollbackReservedAtRevision(context.Background(), current.Group, current.RouteKey,
		current, currentRev, before, true, !retainOwnership)
}

// placeAndConnect creates a new node-local instance for the same stable lineage
// only when the current node is gone and a MigrationToken was supplied.
func (r *Registry) placeAndConnect(ctx context.Context, req SandboxReserveRequest, original *SandboxRecord) (*ReserveResult, error) {
	if original == nil || req.MigrationToken == "" || original.SandboxID != req.ExpectedSandboxID {
		return nil, ErrSandboxNotFound
	}
	excluded := placementExclusions{}
	excluded.add(original.NodeID)
	var lastFailure error
	for {
		placement, err := r.placer.Place(ctx, PlaceRequest{
			Group: req.Group, RouteKey: req.RouteKey, SandboxID: original.SandboxID,
			ExcludeNodeIDs: excluded.values(),
		})
		if err != nil {
			if errors.Is(err, ErrNoNode) && lastFailure != nil {
				return nil, lastFailure
			}
			return nil, err
		}
		if placement == nil || placement.NodeID == "" {
			if lastFailure != nil {
				return nil, lastFailure
			}
			return nil, ErrNoNode
		}
		if excluded.has(placement.NodeID) {
			if lastFailure != nil {
				return nil, lastFailure
			}
			return nil, ErrNoNode
		}
		if placement.APISecretFingerprint != original.APISecretFingerprint {
			return nil, errors.New("registry: migration credential binding mismatch")
		}
		if err := r.nodeRuntimeLive(ctx, placement.NodeID); err != nil {
			if !errors.Is(err, ErrNodeGone) {
				return nil, err
			}
			lastFailure = err
			excluded.add(placement.NodeID)
			continue
		}

		current, rev, found, err := r.getSandboxForReserve(ctx, req.Group, req.RouteKey)
		if err != nil {
			return nil, err
		}
		if !found || current.SandboxID != req.ExpectedSandboxID {
			return nil, ErrSandboxNotFound
		}
		if !sameSandboxGeneration(current, original) ||
			(current.State != StateReserved && current.State != StateReady && current.State != StatePaused) {
			return nil, clusterstate.ErrConflict
		}
		if current.NextSandboxGeneration == math.MaxUint64 {
			return nil, errors.New("registry: sandbox generation exhausted")
		}
		generation := current.NextSandboxGeneration
		target := *current
		target.NodeID = placement.NodeID
		target.NodeSandboxID = EncodeNodeSandboxID(target.SandboxID, generation)
		target.SandboxGeneration = generation
		target.NextSandboxGeneration = generation + 1
		target.State = StateReserved
		if !validNodeSandboxIdentity(target.SandboxID, target.NodeSandboxID, target.SandboxGeneration) {
			return nil, errors.New("registry: generated node sandbox identity is invalid")
		}
		targetRev, ok, err := r.stores.CASSandbox(ctx, &target, rev)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, clusterstate.ErrConflict
		}
		if err := r.stores.AddNodeSandboxRef(ctx, target.NodeID, clusterstate.NodeSandboxRef{
			Group: target.Group, RouteKey: target.RouteKey, SandboxID: target.SandboxID,
			SandboxGeneration: target.SandboxGeneration, NodeSandboxID: target.NodeSandboxID,
			Profile: target.Profile, APISecretFingerprint: target.APISecretFingerprint,
		}); err != nil {
			r.rollbackConnect(&target, targetRev, original, true)
			if errors.Is(err, errNodeSandboxIDConflict) {
				lastFailure = err
				excluded.add(target.NodeID)
				continue
			}
			return nil, err
		}
		ack, err := r.nodeOwner.SendCommandAndWait(ctx, target.NodeID, connectCommand(req, &target), lifecycleAckTimeout)
		if err != nil {
			ambiguous := !errors.Is(err, ErrNodeGone)
			r.rollbackConnect(&target, targetRev, original, ambiguous)
			if errors.Is(err, ErrNodeGone) {
				lastFailure = err
				excluded.add(target.NodeID)
				continue
			}
			return nil, err
		}
		if ack == nil || ack.Status != routesync.AckAccepted {
			r.rollbackConnect(&target, targetRev, original, false)
			reason := ""
			if ack != nil {
				reason = ack.Reason
			}
			lastFailure = fmt.Errorf("registry: connect rejected: %s", reason)
			excluded.add(target.NodeID)
			continue
		}
		if err := validateConnectResult(ack.Connect, &target); err != nil {
			r.rollbackConnect(&target, targetRev, original, true)
			return nil, err
		}
		route, err := r.routeWithDataEndpoint(ctx, &target, targetRev)
		if err != nil {
			return nil, err
		}
		return &ReserveResult{Route: *route, Connect: cloneConnectResult(ack.Connect)}, nil
	}
}

func (r *Registry) reserveData(ctx context.Context, req SandboxReserveRequest) (*ReserveResult, error) {
	return r.reserveDataAttempt(ctx, req, 0)
}

func (r *Registry) reserveDataAttempt(ctx context.Context, req SandboxReserveRequest, attempt int) (*ReserveResult, error) {
	if attempt >= 5 {
		return nil, clusterstate.ErrConflict
	}
	rec, rev, found, err := r.getSandboxForReserve(ctx, req.Group, req.RouteKey)
	if err != nil {
		return nil, err
	}
	if !found || rec.SandboxID != req.ExpectedSandboxID {
		return nil, ErrSandboxNotFound
	}
	if _, _, err := replacementCredentials(rec); err != nil {
		return nil, err
	}
	if err := authenticateRecordAccessToken(rec, req.Port, req.AccessToken); err != nil {
		return nil, err
	}
	switch rec.State {
	case StateReady:
		route, err := r.routeWithDataEndpoint(ctx, rec, rev)
		if err != nil {
			return nil, err
		}
		return &ReserveResult{Route: *route}, nil
	case StateReserved:
		ready, readyRev, err := r.waitForLineageTransition(ctx, req.Group, req.RouteKey, req.ExpectedSandboxID)
		if err != nil {
			return nil, err
		}
		if ready.State == StatePaused {
			return r.reserveDataAttempt(ctx, req, attempt+1)
		}
		route, err := r.routeWithDataEndpoint(ctx, ready, readyRev)
		if err != nil {
			return nil, err
		}
		return &ReserveResult{Route: *route}, nil
	case StatePaused:
		if r.nodeOwner == nil {
			return nil, ErrNodeGone
		}
		if err := r.nodeOwner.Connected(ctx, rec.NodeID); err != nil {
			return nil, err
		}
		connectReq := SandboxReserveRequest{
			Operation: ReserveData, Group: req.Group, RouteKey: req.RouteKey,
			ExpectedSandboxID: req.ExpectedSandboxID,
		}
		_, retry, err := r.connectCurrent(ctx, connectReq, rec, rev)
		if retry {
			return r.reserveDataAttempt(ctx, req, attempt+1)
		}
		if err != nil {
			return nil, err
		}
		ready, readyRev, err := r.waitForLineageTransition(ctx, req.Group, req.RouteKey, req.ExpectedSandboxID)
		if err != nil {
			return nil, err
		}
		if ready.State == StatePaused {
			return r.reserveDataAttempt(ctx, req, attempt+1)
		}
		route, err := r.routeWithDataEndpoint(ctx, ready, readyRev)
		if err != nil {
			return nil, err
		}
		return &ReserveResult{Route: *route}, nil
	default:
		return nil, ErrSandboxNotFound
	}
}

func (r *Registry) waitForLineageTransition(ctx context.Context, group, routeKey, expectedSandboxID string) (*SandboxRecord, int64, error) {
	wctx, cancel := context.WithTimeout(ctx, r.parkTimeout)
	defer cancel()
	rev, _ := r.stores.RouteGroupRev(wctx, group)
	watch, _ := r.stores.WatchRouteGroup(wctx, group, rev)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		rec, routeRev, found, err := r.getSandboxForReserve(wctx, group, routeKey)
		if err != nil {
			return nil, 0, err
		}
		if !found || rec.SandboxID != expectedSandboxID {
			return nil, 0, ErrSandboxNotFound
		}
		if rec.State == StateReady || rec.State == StatePaused {
			return rec, routeRev, nil
		}
		select {
		case <-watch:
		case <-ticker.C:
		case <-wctx.Done():
			return nil, 0, wctx.Err()
		}
	}
}

func (r *Registry) routeWithDataEndpoint(ctx context.Context, rec *SandboxRecord, routeRevision int64) (*RouteResolve, error) {
	route, err := r.routeFromRecord(ctx, rec, routeRevision)
	if err != nil {
		return nil, err
	}
	if r.nodeOwner == nil || rec.NodeID == "" {
		return nil, ErrNodeGone
	}
	runtime, found, err := r.nodeOwner.Runtime(ctx, rec.NodeID)
	if err != nil {
		return nil, err
	}
	if !found || runtime == nil || runtime.DataEndpoint == "" {
		return nil, ErrNodeGone
	}
	route.DataEndpoint = runtime.DataEndpoint
	return route, nil
}

func (r *Registry) routeFromRecord(ctx context.Context, rec *SandboxRecord, routeRevision int64) (*RouteResolve, error) {
	if rec == nil || rec.SandboxID == "" || rec.NodeSandboxID == "" || routeRevision <= 0 {
		return nil, errors.New("registry: sandbox route is incomplete")
	}
	if _, _, err := replacementCredentials(rec); err != nil {
		return nil, err
	}
	return &RouteResolve{
		SandboxID: rec.SandboxID, NodeSandboxID: rec.NodeSandboxID,
		Group: rec.Group, RouteKey: rec.RouteKey, NodeID: rec.NodeID,
		DataEndpoint: r.nodeDataEndpoint(ctx, rec.NodeID), Profile: rec.Profile, TemplateID: rec.TemplateID,
		AuthSandboxID: rec.AuthSandboxID, APISecret: rec.APISecret,
		APISecretFingerprint: rec.APISecretFingerprint, ManifestKeyFingerprint: rec.ManifestKeyFingerprint,
		ServiceSecret: rec.ServiceSecret, EnvdAccessToken: rec.EnvdAccessToken,
		TrafficAccessToken: rec.TrafficAccessToken, ForwardAccessToken: rec.ForwardAccessToken,
		TargetPort: rec.TargetPort, State: string(rec.State), RouteRevision: routeRevision,
	}, nil
}
