package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/maglev"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const placerPeerMaxAge = 30 * time.Second

type HTTPPlacer struct {
	r            *Registry
	replicaCount int
	minReady     int
	timeout      time.Duration
	client       *http.Client
}

func NewHTTPPlacer(r *Registry, replicaCount int, timeout time.Duration) Placer {
	return NewHTTPPlacerWithMinReady(r, replicaCount, 1, timeout)
}

func NewHTTPPlacerWithMinReady(r *Registry, replicaCount, minReady int, timeout time.Duration) Placer {
	if replicaCount <= 0 {
		replicaCount = 1
	}
	if minReady <= 0 {
		minReady = 1
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &HTTPPlacer{
		r:            r,
		replicaCount: replicaCount,
		minReady:     minReady,
		timeout:      timeout,
		client:       &http.Client{Timeout: timeout},
	}
}

func (p *HTTPPlacer) Place(ctx context.Context, req PlaceRequest) (*Placement, error) {
	peers := p.r.readyPlacerPeers(placerPeerMaxAge)
	if len(peers) < p.minReady {
		return nil, ErrNoNode
	}
	byID := make(map[string]PlacerPeer, len(peers))
	ids := make([]string, 0, len(peers))
	for _, peer := range peers {
		byID[peer.ID] = peer
		ids = append(ids, peer.ID)
	}
	sort.Strings(ids)
	n := p.replicaCount
	if n > len(ids) {
		n = len(ids)
	}
	candidates, err := maglev.LocateN([]byte(req.Group), ids, n)
	if err != nil {
		return nil, err
	}
	var last error
	for _, id := range candidates {
		placement, err := p.placeOne(ctx, byID[id], req)
		if err == nil && placement != nil && placement.NodeID != "" {
			return placement, nil
		}
		if err != nil {
			if errors.Is(err, errInvalidSandboxConfig) {
				return nil, err
			}
			last = err
		}
	}
	if last != nil {
		return nil, last
	}
	return nil, ErrNoNode
}

func (p *HTTPPlacer) placeOne(ctx context.Context, peer PlacerPeer, req PlaceRequest) (*Placement, error) {
	if peer.Advertise == "" {
		return nil, ErrNoNode
	}
	body, _ := json.Marshal(&routesync.PlaceReq{
		ReqID:               newID(),
		Group:               req.Group,
		RouteKey:            req.RouteKey,
		SandboxID:           req.SandboxID,
		Config:              req.Config,
		Build:               req.Build,
		BuildResources:      req.BuildResources,
		TargetRuntimeDigest: req.TargetRuntimeDigest,
		ExcludeNodeIDs:      req.ExcludeNodeIDs,
	})
	u, err := placerURL(peer.Advertise, PlacerLinkPlacePath)
	if err != nil {
		return nil, err
	}
	wctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(wctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("placer_link place %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var out routesync.PlaceResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		if out.InvalidConfig {
			return nil, fmt.Errorf("%w: %s", errInvalidSandboxConfig, out.Error)
		}
		return nil, fmt.Errorf("placer_link place: %s", out.Error)
	}
	if out.NoNode || out.NodeID == "" {
		return nil, ErrNoNode
	}
	return &Placement{
		NodeID: out.NodeID, TemplateRef: out.TemplateRef, Config: out.Config,
		TargetPort:           out.TargetPort,
		APISecretFingerprint: out.APISecretFingerprint,
		ImageRepo:            out.ImageRepo, RegistryAuth: out.RegistryAuth,
	}, nil
}

func (r *Registry) VerifyAPIKey(ctx context.Context, group, apiKey string, replicaCount int, timeout time.Duration) (bool, error) {
	return r.VerifyAPIKeyWithMinReady(ctx, group, apiKey, replicaCount, 1, timeout)
}

func (r *Registry) VerifyAPIKeyWithMinReady(ctx context.Context, group, apiKey string, replicaCount, minReady int, timeout time.Duration) (bool, error) {
	if replicaCount <= 0 {
		replicaCount = 1
	}
	if minReady <= 0 {
		minReady = 1
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	peers := r.readyPlacerPeers(placerPeerMaxAge)
	if len(peers) < minReady {
		return false, ErrNoNode
	}
	byID := make(map[string]PlacerPeer, len(peers))
	ids := make([]string, 0, len(peers))
	for _, peer := range peers {
		byID[peer.ID] = peer
		ids = append(ids, peer.ID)
	}
	sort.Strings(ids)
	n := replicaCount
	if n > len(ids) {
		n = len(ids)
	}
	candidates, err := maglev.LocateN([]byte(group), ids, n)
	if err != nil {
		return false, err
	}
	client := &http.Client{Timeout: timeout}
	var last error
	for _, id := range candidates {
		ok, err := verifyAPIKeyOne(ctx, client, timeout, byID[id], group, apiKey)
		if err == nil {
			return ok, nil
		}
		last = err
	}
	if last != nil {
		return false, last
	}
	return false, ErrNoNode
}

func verifyAPIKeyOne(ctx context.Context, client *http.Client, timeout time.Duration, peer PlacerPeer, group, apiKey string) (bool, error) {
	if peer.Advertise == "" {
		return false, ErrNoNode
	}
	u, err := placerURL(peer.Advertise, PlacerLinkVerifyKeyPath)
	if err != nil {
		return false, err
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(wctx, http.MethodGet, u, nil)
	if err != nil {
		return false, err
	}
	q := req.URL.Query()
	q.Set("group", group)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("X-API-KEY", apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusForbidden, http.StatusNotFound:
		return false, nil
	default:
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return false, fmt.Errorf("placer_link verify-key %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
}

func placerURL(advertise, path string) (string, error) {
	if strings.HasPrefix(advertise, "http://") || strings.HasPrefix(advertise, "https://") {
		u, err := url.Parse(advertise)
		if err != nil || u.Host == "" {
			return "", fmt.Errorf("placer_link: invalid placer advertise %q", advertise)
		}
		u.Path = strings.TrimRight(u.Path, "/") + path
		return u.String(), nil
	}
	return "http://" + strings.TrimRight(advertise, "/") + path, nil
}
