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
	"sync"
	"sync/atomic"

	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type PlacementPlanner interface {
	Plan(context.Context, placement.CatalogSnapshot, placer.PlanRequest) (placer.PlanResponse, error)
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
	catalogs  map[string]*plannerCatalogState
}

type plannerCatalogState struct {
	mu     sync.Mutex
	digest string
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
	catalogs := make(map[string]*plannerCatalogState, len(copyEndpoints))
	for _, endpoint := range copyEndpoints {
		catalogs[endpoint.Name] = &plannerCatalogState{}
	}
	return &HTTPPlacementPlanner{endpoints: copyEndpoints, catalogs: catalogs}, nil
}

func (p *HTTPPlacementPlanner) Plan(
	ctx context.Context,
	catalog placement.CatalogSnapshot,
	request placer.PlanRequest,
) (placer.PlanResponse, error) {
	if err := catalog.Validate(); err != nil {
		return placer.PlanResponse{}, err
	}
	request.Catalog = catalog.Reference
	raw, err := json.Marshal(request)
	if err != nil {
		return placer.PlanResponse{}, err
	}
	start := int(p.next.Add(1)-1) % len(p.endpoints)
	errorsByEndpoint := make([]string, 0, len(p.endpoints))
	for offset := range p.endpoints {
		endpoint := p.endpoints[(start+offset)%len(p.endpoints)]
		if syncErr := p.ensureCatalog(ctx, endpoint, catalog, false); syncErr != nil {
			errorsByEndpoint = append(errorsByEndpoint, fmt.Sprintf("%s: %v", endpoint.Name, syncErr))
			continue
		}
		response, callErr := callPlacementPlanner(ctx, endpoint, raw)
		if callErr == nil && response.ErrorCode == placer.PlanErrorCatalogUnavailable {
			if syncErr := p.ensureCatalog(ctx, endpoint, catalog, true); syncErr != nil {
				callErr = syncErr
			} else {
				response, callErr = callPlacementPlanner(ctx, endpoint, raw)
			}
		}
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

func (p *HTTPPlacementPlanner) ensureCatalog(
	ctx context.Context,
	endpoint PlannerEndpoint,
	catalog placement.CatalogSnapshot,
	force bool,
) error {
	state := p.catalogs[endpoint.Name]
	state.mu.Lock()
	defer state.mu.Unlock()
	if !force && state.digest == catalog.Reference.Digest {
		return nil
	}
	if err := syncCatalogToPlacer(ctx, endpoint, catalog); err != nil {
		state.digest = ""
		return err
	}
	state.digest = catalog.Reference.Digest
	return nil
}

func syncCatalogToPlacer(ctx context.Context, endpoint PlannerEndpoint, catalog placement.CatalogSnapshot) error {
	start := uint32(0)
	for {
		page, err := nextCatalogSyncPage(catalog, start)
		if err != nil {
			return err
		}
		response, err := callCatalogSync(ctx, endpoint, page)
		if err != nil {
			return err
		}
		if err := response.ValidateFor(page); err != nil {
			return err
		}
		if response.Installed {
			return nil
		}
		if response.NextStart <= start {
			return errors.New("controlplane: Placer did not advance Node Catalog sync")
		}
		start = response.NextStart
	}
}

func nextCatalogSyncPage(catalog placement.CatalogSnapshot, start uint32) (placer.CatalogSyncRequest, error) {
	if start > catalog.Reference.NodeCount || int(start) > len(catalog.Nodes) {
		return placer.CatalogSyncRequest{}, errors.New("controlplane: invalid Node Catalog sync cursor")
	}
	if catalog.Reference.NodeCount == 0 {
		return placer.CatalogSyncRequest{Reference: catalog.Reference, Final: true}, nil
	}
	maximumEnd := min(len(catalog.Nodes), int(start)+placer.MaxCatalogSyncPageNodes)
	candidate := func(end int) (placer.CatalogSyncRequest, int, error) {
		page := placer.CatalogSyncRequest{
			Reference: catalog.Reference, Start: start, Nodes: catalog.Nodes[start:end],
			Final: end == len(catalog.Nodes),
		}
		raw, err := json.Marshal(page)
		return page, len(raw), err
	}
	page, size, err := candidate(maximumEnd)
	if err != nil {
		return placer.CatalogSyncRequest{}, err
	}
	if size > placer.MaximumCatalogSyncRequestBytes {
		low, high := int(start)+1, maximumEnd
		page = placer.CatalogSyncRequest{}
		for low <= high {
			end := low + (high-low)/2
			current, currentSize, err := candidate(end)
			if err != nil {
				return placer.CatalogSyncRequest{}, err
			}
			if currentSize <= placer.MaximumCatalogSyncRequestBytes {
				page = current
				low = end + 1
			} else {
				high = end - 1
			}
		}
		if len(page.Nodes) == 0 {
			return placer.CatalogSyncRequest{}, errors.New("controlplane: one Node Catalog row exceeds the sync request bound")
		}
	}
	return page, page.Validate()
}

func callCatalogSync(
	ctx context.Context,
	endpoint PlannerEndpoint,
	input placer.CatalogSyncRequest,
) (placer.CatalogSyncResponse, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return placer.CatalogSyncResponse{}, err
	}
	if len(raw) > placer.MaximumCatalogSyncRequestBytes {
		return placer.CatalogSyncResponse{}, errors.New("controlplane: Node Catalog sync request exceeds its bound")
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint.Endpoint+placer.FinalCatalogSyncPath, bytes.NewReader(raw),
	)
	if err != nil {
		return placer.CatalogSyncResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := endpoint.Client.Do(request)
	if err != nil {
		return placer.CatalogSyncResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return placer.CatalogSyncResponse{}, fmt.Errorf(
			"Placer Node Catalog sync returned %s: %s", response.Status, strings.TrimSpace(string(detail)),
		)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	decoder.DisallowUnknownFields()
	var result placer.CatalogSyncResponse
	if err := decoder.Decode(&result); err != nil {
		return placer.CatalogSyncResponse{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return placer.CatalogSyncResponse{}, errors.New("controlplane: Placer Node Catalog response has trailing JSON")
	}
	return result, nil
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
