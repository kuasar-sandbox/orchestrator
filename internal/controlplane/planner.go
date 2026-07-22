package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/kuasar-sandbox/orchestrator/internal/placer"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type PlacementPlanner interface {
	Plan(context.Context, placer.PlanRequest) (placer.PlanResponse, error)
	ResolveKeyLease(context.Context, placer.KeyLeaseRequest) (routesync.NodeKeyLeaseV1, error)
}

func (p *HTTPPlacementPlanner) ResolveKeyLease(
	ctx context.Context,
	request placer.KeyLeaseRequest,
) (routesync.NodeKeyLeaseV1, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	start := int(p.next.Add(1)-1) % len(p.endpoints)
	errorsByEndpoint := make([]string, 0, len(p.endpoints))
	for offset := range p.endpoints {
		endpoint := p.endpoints[(start+offset)%len(p.endpoints)]
		lease, callErr := callKeyLeaseResolver(ctx, endpoint, raw, request)
		if callErr == nil {
			return lease, nil
		}
		errorsByEndpoint = append(errorsByEndpoint, fmt.Sprintf("%s: %v", endpoint.Name, callErr))
	}
	return routesync.NodeKeyLeaseV1{}, fmt.Errorf(
		"controlplane: Provider key lease unavailable: %s", strings.Join(errorsByEndpoint, "; "),
	)
}

type PlannerEndpoint struct {
	Name     string
	Endpoint string
	Client   *http.Client
}

type HTTPPlacementPlanner struct {
	endpoints []PlannerEndpoint
	next      atomic.Uint64
}

func NewHTTPPlacementPlanner(endpoints []PlannerEndpoint) (*HTTPPlacementPlanner, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("controlplane: at least one Placer endpoint is required")
	}
	seen := make(map[string]struct{}, len(endpoints))
	copyEndpoints := make([]PlannerEndpoint, len(endpoints))
	for index, endpoint := range endpoints {
		if endpoint.Name == "" || endpoint.Endpoint == "" || strings.HasSuffix(endpoint.Endpoint, "/") || endpoint.Client == nil {
			return nil, errors.New("controlplane: incomplete Placer endpoint")
		}
		if _, duplicate := seen[endpoint.Name]; duplicate {
			return nil, errors.New("controlplane: duplicate Placer endpoint name")
		}
		seen[endpoint.Name] = struct{}{}
		copyEndpoints[index] = endpoint
	}
	return &HTTPPlacementPlanner{endpoints: copyEndpoints}, nil
}

func (p *HTTPPlacementPlanner) Plan(ctx context.Context, request placer.PlanRequest) (placer.PlanResponse, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return placer.PlanResponse{}, err
	}
	start := int(p.next.Add(1)-1) % len(p.endpoints)
	errorsByEndpoint := make([]string, 0, len(p.endpoints))
	for offset := range p.endpoints {
		endpoint := p.endpoints[(start+offset)%len(p.endpoints)]
		response, callErr := callPlacementPlanner(ctx, endpoint, raw)
		if callErr == nil && response.Error == "" {
			return response, nil
		}
		if callErr == nil {
			callErr = errors.New(response.Error)
		}
		errorsByEndpoint = append(errorsByEndpoint, fmt.Sprintf("%s: %v", endpoint.Name, callErr))
	}
	return placer.PlanResponse{}, fmt.Errorf("controlplane: Placer unavailable: %s", strings.Join(errorsByEndpoint, "; "))
}

func callPlacementPlanner(ctx context.Context, endpoint PlannerEndpoint, raw []byte) (placer.PlanResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.Endpoint+placer.FinalPlanPath, bytes.NewReader(raw))
	if err != nil {
		return placer.PlanResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := endpoint.Client.Do(request)
	if err != nil {
		return placer.PlanResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return placer.PlanResponse{}, fmt.Errorf("Placer returned %s", response.Status)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 4<<20))
	decoder.DisallowUnknownFields()
	var result placer.PlanResponse
	if err := decoder.Decode(&result); err != nil {
		return placer.PlanResponse{}, err
	}
	return result, nil
}

func callKeyLeaseResolver(
	ctx context.Context,
	endpoint PlannerEndpoint,
	raw []byte,
	expected placer.KeyLeaseRequest,
) (routesync.NodeKeyLeaseV1, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.Endpoint+placer.FinalKeyLeasePath, bytes.NewReader(raw))
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := endpoint.Client.Do(request)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return routesync.NodeKeyLeaseV1{}, fmt.Errorf("Placer returned %s", response.Status)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var result placer.KeyLeaseResponse
	if err := decoder.Decode(&result); err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	if result.Error != "" {
		return routesync.NodeKeyLeaseV1{}, errors.New(result.Error)
	}
	if result.Lease == nil || result.Lease.Validate() != nil || result.Lease.Group != expected.Group ||
		result.Lease.AuthKey.Fingerprint != expected.AuthKeyFingerprint ||
		result.Lease.ManifestKey.Fingerprint != expected.ManifestKeyFingerprint {
		return routesync.NodeKeyLeaseV1{}, errors.New("controlplane: Placer returned a mismatched key lease")
	}
	return *result.Lease, nil
}
