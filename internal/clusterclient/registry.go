// Package clusterclient contains small client-side helpers shared by cluster
// roles that consume registry membership.
package clusterclient

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
)

const MembershipPath = "/cluster/membership"

type Endpoint struct {
	MemberID string
	BaseURL  string
	Client   *http.Client
}

type Registry struct {
	bootstrap string
	baseURL   string
	tlsConfig *tls.Config
	client    *http.Client

	mu         sync.RWMutex
	membership clustercfg.MembershipConfig
}

func NewRegistry(bootstrap string, tlsConfig *tls.Config) (*Registry, error) {
	base, client, err := HTTPBase(bootstrap, tlsConfig)
	if err != nil {
		return nil, err
	}
	return &Registry{bootstrap: bootstrap, baseURL: base, tlsConfig: tlsConfig, client: client}, nil
}

func NewRegistryWithClient(baseURL string, client *http.Client) *Registry {
	if client == nil {
		client = http.DefaultClient
	}
	return &Registry{bootstrap: baseURL, baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

func HTTPBase(endpoint string, tlsConfig *tls.Config) (string, *http.Client, error) {
	if endpoint == "" {
		return "", nil, fmt.Errorf("clusterclient: registry bootstrap is required")
	}
	if strings.HasPrefix(endpoint, "/") {
		dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", endpoint)
		}
		return "http://registry", &http.Client{Transport: &http.Transport{DialContext: dial}}, nil
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" {
			return "", nil, fmt.Errorf("clusterclient: invalid endpoint %q", endpoint)
		}
		return strings.TrimRight(endpoint, "/"), clientForScheme(u.Scheme, tlsConfig), nil
	}
	if tlsConfig != nil {
		return "https://" + endpoint, clientForScheme("https", tlsConfig), nil
	}
	return "http://" + endpoint, clientForScheme("http", nil), nil
}

func clientForScheme(scheme string, tlsConfig *tls.Config) *http.Client {
	if scheme == "https" {
		return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true}}
	}
	return &http.Client{Transport: &http.Transport{}}
}

func (r *Registry) FetchMembership(ctx context.Context) (clustercfg.MembershipConfig, error) {
	targets := r.membershipFetchTargets()
	var out clustercfg.MembershipConfig
	var got bool
	var errs []string
	for _, target := range targets {
		next, err := fetchMembershipFrom(ctx, target)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", target.MemberID, err))
			continue
		}
		next = next.WithComputedLabels()
		if !got || membershipNewer(next, out) {
			out = next
			got = true
		}
	}
	if !got {
		if len(errs) == 0 {
			return clustercfg.MembershipConfig{}, fmt.Errorf("clusterclient: no membership endpoints")
		}
		return clustercfg.MembershipConfig{}, fmt.Errorf("clusterclient: membership refresh failed: %s", strings.Join(errs, "; "))
	}
	r.mu.Lock()
	r.membership = out
	r.mu.Unlock()
	return out, nil
}

func fetchMembershipFrom(ctx context.Context, ep Endpoint) (clustercfg.MembershipConfig, error) {
	if ep.Client == nil {
		return clustercfg.MembershipConfig{}, fmt.Errorf("client is nil")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.BaseURL+MembershipPath, nil)
	if err != nil {
		return clustercfg.MembershipConfig{}, err
	}
	resp, err := ep.Client.Do(req)
	if err != nil {
		return clustercfg.MembershipConfig{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return clustercfg.MembershipConfig{}, fmt.Errorf("membership %s", resp.Status)
	}
	var out clustercfg.MembershipConfig
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return clustercfg.MembershipConfig{}, err
	}
	return out, nil
}

func membershipNewer(a, b clustercfg.MembershipConfig) bool {
	if a.Active != b.Active {
		return a.Active > b.Active
	}
	return a.Next > b.Next
}

func (r *Registry) membershipFetchTargets() []Endpoint {
	out := []Endpoint{{MemberID: "bootstrap", BaseURL: r.baseURL, Client: r.client}}
	r.mu.RLock()
	m := r.membership
	r.mu.RUnlock()
	if len(m.Versions) == 0 {
		return out
	}
	seen := map[string]bool{r.baseURL: true}
	for _, v := range m.MemberVersions() {
		for _, member := range v.Members {
			if member.ID == "" {
				continue
			}
			ep, err := r.endpointForMember(v, member)
			if err != nil || ep.BaseURL == "" || seen[ep.BaseURL] {
				continue
			}
			seen[ep.BaseURL] = true
			out = append(out, ep)
		}
	}
	return out
}

func (r *Registry) Membership(ctx context.Context) (clustercfg.MembershipConfig, error) {
	r.mu.RLock()
	m := r.membership
	r.mu.RUnlock()
	if len(m.Versions) != 0 {
		return m, nil
	}
	return r.FetchMembership(ctx)
}

func (r *Registry) Refresh(ctx context.Context) error {
	_, err := r.FetchMembership(ctx)
	return err
}

func (r *Registry) RouteCandidates(ctx context.Context, group string) ([]Endpoint, error) {
	m, err := r.Membership(ctx)
	if err != nil {
		return nil, err
	}
	return r.candidates(ctx, m, group, m.Owners.RouteLink)
}

func (r *Registry) NodeCandidates(ctx context.Context, nodeID string) ([]Endpoint, error) {
	m, err := r.Membership(ctx)
	if err != nil {
		return nil, err
	}
	return r.candidates(ctx, m, nodeID, m.Owners.NodeLink)
}

func (r *Registry) NodeListEndpoints(ctx context.Context) ([]Endpoint, error) {
	m, err := r.Membership(ctx)
	if err != nil {
		return nil, err
	}
	return r.jointOwnerEndpoints(ctx, m, clusterstate.NamespaceNodeList, m.Owners.NodeList)
}

func (r *Registry) ActiveEndpoints(ctx context.Context) ([]Endpoint, error) {
	m, err := r.Membership(ctx)
	if err != nil {
		return nil, err
	}
	v, ok := m.ActiveVersion()
	if !ok {
		return nil, fmt.Errorf("clusterclient: active membership %d not found", m.Active)
	}
	members := append([]clustercfg.MembershipMember(nil), v.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	out := make([]Endpoint, 0, len(members))
	for _, member := range members {
		ep, err := r.endpointForMember(v, member)
		if err != nil {
			return nil, err
		}
		out = append(out, ep)
	}
	return out, nil
}

func (r *Registry) jointOwnerEndpoints(ctx context.Context, m clustercfg.MembershipConfig, key string, ownerCount int) ([]Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ownerCount <= 0 {
		return nil, fmt.Errorf("clusterclient: owner count must be positive")
	}
	byID := map[string]clustercfg.MembershipMember{}
	versionByID := map[string]clustercfg.MembershipVersion{}
	for _, v := range m.OwnerVersions() {
		view := clusterstate.MemberView{Version: v.Version, Members: v.MemberIDs()}
		owners, err := view.Owners(key, ownerCount)
		if err != nil {
			return nil, err
		}
		members := map[string]clustercfg.MembershipMember{}
		for _, member := range v.Members {
			members[member.ID] = member
		}
		for _, id := range owners {
			member := members[id]
			if member.ID == "" {
				continue
			}
			if _, ok := byID[id]; ok {
				continue
			}
			byID[id] = member
			versionByID[id] = v
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Endpoint, 0, len(ids))
	for _, id := range ids {
		ep, err := r.endpointForMember(versionByID[id], byID[id])
		if err != nil {
			return nil, err
		}
		out = append(out, ep)
	}
	return out, nil
}

func (r *Registry) OwnerEndpoints(ctx context.Context) ([]Endpoint, error) {
	m, err := r.Membership(ctx)
	if err != nil {
		return nil, err
	}
	byID := map[string]clustercfg.MembershipMember{}
	versionByID := map[string]clustercfg.MembershipVersion{}
	for _, v := range m.OwnerVersions() {
		for _, member := range v.Members {
			if member.ID == "" {
				continue
			}
			if _, ok := byID[member.ID]; ok {
				continue
			}
			byID[member.ID] = member
			versionByID[member.ID] = v
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Endpoint, 0, len(ids))
	for _, id := range ids {
		ep, err := r.endpointForMember(versionByID[id], byID[id])
		if err != nil {
			return nil, err
		}
		out = append(out, ep)
	}
	return out, nil
}

func (r *Registry) candidates(ctx context.Context, m clustercfg.MembershipConfig, key string, ownerCount int) ([]Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ownerCount <= 0 {
		return nil, fmt.Errorf("clusterclient: owner count must be positive")
	}
	v, ok := m.ActiveVersion()
	if !ok {
		return nil, fmt.Errorf("clusterclient: active membership %d not found", m.Active)
	}
	view := clusterstate.MemberView{Version: v.Version, Members: v.MemberIDs()}
	owners, err := view.Owners(key, ownerCount)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]clustercfg.MembershipMember, len(v.Members))
	for _, member := range v.Members {
		byID[member.ID] = member
	}
	out := make([]Endpoint, 0, len(owners))
	for _, id := range owners {
		ep, err := r.endpointForMember(v, byID[id])
		if err != nil {
			return nil, err
		}
		out = append(out, ep)
	}
	return out, nil
}

func (r *Registry) endpointForMember(v clustercfg.MembershipVersion, member clustercfg.MembershipMember) (Endpoint, error) {
	base := member.Advertise
	client := (*http.Client)(nil)
	if base == "" && len(v.Members) == 1 {
		base, client = r.baseURL, r.client
	} else {
		var err error
		base, client, err = HTTPBase(base, r.tlsConfig)
		if err != nil {
			return Endpoint{}, fmt.Errorf("clusterclient: member %q: %w", member.ID, err)
		}
	}
	return Endpoint{MemberID: member.ID, BaseURL: strings.TrimRight(base, "/"), Client: client}, nil
}

func (r *Registry) ActiveLabel(ctx context.Context) (string, error) {
	m, err := r.Membership(ctx)
	if err != nil {
		return "", err
	}
	v, ok := m.ActiveVersion()
	if !ok {
		return "", fmt.Errorf("clusterclient: active membership %d not found", m.Active)
	}
	return v.Label, nil
}

func SortEndpointsForTest(eps []Endpoint) {
	sort.Slice(eps, func(i, j int) bool { return eps[i].MemberID < eps[j].MemberID })
}

func NewTimeoutClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout}
}
