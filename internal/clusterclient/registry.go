// Package clusterclient contains small client-side helpers shared by cluster
// roles that consume registry membership.
package clusterclient

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
)

const idleConnTimeout = 90 * time.Second

const MembershipPath = "/cluster/membership"

type Endpoint struct {
	MemberID string
	BaseURL  string
	Client   *http.Client
}

type Registry struct {
	bootstrap     string
	baseURL       string
	tlsConfig     *tls.Config
	client        *http.Client
	poolsOnce     sync.Once
	httpClient    *http.Client
	httpsClient   *http.Client
	unixTransport *http.Transport

	mu         sync.RWMutex
	membership clustercfg.MembershipConfig
}

func NewRegistry(bootstrap string, tlsConfig *tls.Config) (*Registry, error) {
	r := &Registry{bootstrap: bootstrap, tlsConfig: tlsConfig}
	base, client, err := r.httpBase(bootstrap)
	if err != nil {
		return nil, err
	}
	r.baseURL, r.client = base, client
	return r, nil
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
		return "http://registry", &http.Client{Transport: &http.Transport{DialContext: dial, IdleConnTimeout: idleConnTimeout}}, nil
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
		return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true, IdleConnTimeout: idleConnTimeout}}
	}
	return &http.Client{Transport: &http.Transport{IdleConnTimeout: idleConnTimeout}}
}

// httpBase shares a fixed set of transports. No member-address cache is retained.
// The caller-owned bootstrap client is deliberately separate from these pools.
func (r *Registry) httpBase(endpoint string) (string, *http.Client, error) {
	r.poolsOnce.Do(func() {
		r.httpClient = clientForScheme("http", nil)
		r.httpsClient = clientForScheme("https", r.tlsConfig)
		r.unixTransport = &http.Transport{IdleConnTimeout: idleConnTimeout, DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			path, err := hex.DecodeString(host)
			if err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, "unix", string(path))
		}}
	})
	if strings.HasPrefix(endpoint, "/") {
		return "http://registry", &http.Client{Transport: unixRoundTripper{r.unixTransport, endpoint}}, nil
	}
	if endpoint == "" {
		return "", nil, fmt.Errorf("clusterclient: registry bootstrap is required")
	}
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		if r.tlsConfig != nil {
			endpoint = "https://" + endpoint
		} else {
			endpoint = "http://" + endpoint
		}
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return "", nil, fmt.Errorf("clusterclient: invalid endpoint %q", endpoint)
	}
	client := r.httpClient
	if u.Scheme == "https" {
		client = r.httpsClient
	}
	return strings.TrimRight(endpoint, "/"), client, nil
}

// A private authority lets Transport key Unix pools by socket path while callers
// and servers retain the existing http://registry URL and Host header.
type unixRoundTripper struct {
	transport *http.Transport
	path      string
}

func (u unixRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	if clone.Host == "" {
		clone.Host = req.URL.Host
	}
	clone.URL.Host = hex.EncodeToString([]byte(u.path))
	return u.transport.RoundTrip(clone)
}

func (r *Registry) endpointKey(endpoint string) string {
	if strings.HasPrefix(endpoint, "/") {
		return endpoint
	}
	if endpoint == "" {
		return r.endpointKey(r.bootstrap)
	}
	if !strings.Contains(endpoint, "://") {
		if r.tlsConfig != nil {
			endpoint = "https://" + endpoint
		} else {
			endpoint = "http://" + endpoint
		}
	}
	return strings.TrimRight(endpoint, "/")
}

// DecodeResponse consumes the bounded tail of a successful finite JSON response
// so HTTP/1 chunk terminators reach Transport. Invalid or excessive tails are not
// drained indefinitely; request contexts still bound network reads.
func DecodeResponse(body io.ReadCloser, out any) error {
	if err := json.NewDecoder(body).Decode(out); err != nil {
		return err
	}
	return DrainResponse(body)
}

// DrainResponse spends a bounded byte budget on connection reuse.
// The request context controls cancellation and network deadlines.
// It is for finite control responses, never WATCH_LIST or other streams.
func DrainResponse(body io.ReadCloser) error {
	_, err := io.CopyN(io.Discard, body, 64<<10)
	if err == io.EOF {
		return nil
	}
	return err
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
	if err := DecodeResponse(resp.Body, &out); err != nil {
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
	seen := map[string]bool{r.endpointKey(r.bootstrap): true}
	for _, v := range m.MemberVersions() {
		for _, member := range v.Members {
			if member.ID == "" {
				continue
			}
			ep, err := r.endpointForMember(v, member)
			if err != nil || ep.BaseURL == "" || seen[r.endpointKey(member.Advertise)] {
				continue
			}
			seen[r.endpointKey(member.Advertise)] = true
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
	return r.jointOwnerEndpoints(ctx, m, string(clusterstate.NodeListShard), m.Owners.NodeList)
}

func (r *Registry) PlacerLinkEndpoints(ctx context.Context, shardKey string) ([]Endpoint, error) {
	m, err := r.Membership(ctx)
	if err != nil {
		return nil, err
	}
	return r.jointOwnerEndpoints(ctx, m, shardKey, m.Owners.PlacerLink)
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
		base, client, err = r.httpBase(base)
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
