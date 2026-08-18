package placer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/maglev"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const placerRegisterInterval = 15 * time.Second

var (
	placerRegisterRetryInterval = time.Second
	placerReadyPollInterval     = time.Second
	scaleLinkRetryDelay         = 200 * time.Millisecond
)

const scaleLinkFailureRetries = 2

// Service is the standalone placer placement engine. It keeps node_list/group
// views synced, computes placement over the local view, and returns suggestions
// that the registry commits through route/node owner state.
type Service struct {
	linksMu     sync.RWMutex
	links       []RegistryLink
	watchLinks  []RegistryLink
	watchLabel  string
	watchCancel context.CancelFunc
	startedCtx  context.Context

	cfg clustercfg.PlacementConfig
	log *slog.Logger

	nodes    *nodeView
	provider clusterstate.SandboxGroupProvider

	sourceMu           sync.RWMutex
	runID              string
	importSources      []ImportSource
	sourceOwnerID      string
	sourcePeerSource   func() []string
	sourceLeaseTTL     time.Duration
	selectorPatchEvery time.Duration
	scaleLinkResolver  func(context.Context, string) ([]RegistryLink, error)
	scaleLinkRefresher func(context.Context) error
	nodeChanges        chan struct{}
}

type RegistryLink struct {
	Name    string
	BaseURL string
	Client  *http.Client
}

// NewRemote builds a standalone placer connected to registry control paths. The
// address may be a UDS path, host:port, or http(s) URL.
func NewRemote(scaleAddr string, scaleTLS *tls.Config, cfg clustercfg.PlacementConfig, log *slog.Logger) *Service {
	return NewRemoteLinks([]RegistryLink{registryLinkFromAddress("registry", scaleAddr, scaleTLS)}, cfg, log)
}

func NewRemoteLinks(links []RegistryLink, cfg clustercfg.PlacementConfig, log *slog.Logger) *Service {
	return NewRemoteLinksWithGroups(links, emptyGroupProvider{}, nil, cfg, log)
}

func NewRemoteLinksWithGroups(links []RegistryLink, provider clusterstate.SandboxGroupProvider, sources []ImportSource, cfg clustercfg.PlacementConfig, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Candidates <= 0 {
		cfg.Candidates = 2
	}
	if cfg.ImportSourceOwnerCount <= 0 {
		cfg.ImportSourceOwnerCount = 3
	}
	sourceLeaseTTL := cfg.ImportSourceLeaseTTLDur()
	if sourceLeaseTTL <= 0 {
		sourceLeaseTTL = 15 * time.Second
	}
	selectorPatchEvery := cfg.SelectorPatchRefreshDur()
	if selectorPatchEvery <= 0 {
		selectorPatchEvery = time.Minute
	}
	links = normalizeRegistryLinks(links)
	if provider == nil {
		provider = emptyGroupProvider{}
	}
	runID := newRunID()
	return &Service{
		links: links, watchLinks: links,
		cfg: cfg, log: log,
		nodes:              newNodeView(1),
		provider:           provider,
		runID:              runID,
		importSources:      normalizeImportSources(sources),
		sourceOwnerID:      "local",
		sourceLeaseTTL:     sourceLeaseTTL,
		selectorPatchEvery: selectorPatchEvery,
		nodeChanges:        make(chan struct{}, 1),
	}
}

func newRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func normalizeImportSources(sources []ImportSource) []ImportSource {
	out := make([]ImportSource, 0, len(sources))
	seen := map[string]bool{}
	for _, source := range sources {
		if source.SourceID == "" || source.Importer == nil || seen[source.SourceID] {
			continue
		}
		seen[source.SourceID] = true
		out = append(out, source)
	}
	return out
}

func registryLinkFromAddress(name, scaleAddr string, scaleTLS *tls.Config) RegistryLink {
	link := RegistryLink{Name: name}
	switch {
	case strings.HasPrefix(scaleAddr, "http://") || strings.HasPrefix(scaleAddr, "https://"):
		u, err := url.Parse(scaleAddr)
		if err == nil && u.Host != "" {
			link.BaseURL = strings.TrimRight(scaleAddr, "/")
			if u.Scheme == "https" {
				link.Client = &http.Client{Transport: &http.Transport{TLSClientConfig: scaleTLS, ForceAttemptHTTP2: true}}
			} else {
				link.Client = &http.Client{Transport: &http.Transport{}}
			}
			return link
		}
	case strings.HasPrefix(scaleAddr, "/"):
		link.BaseURL = "http://placer-link"
		dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", scaleAddr)
		}
		link.Client = &http.Client{Transport: &http.Transport{DialContext: dial}}
		return link
	case scaleTLS != nil:
		link.BaseURL = "https://" + scaleAddr
		link.Client = &http.Client{Transport: &http.Transport{TLSClientConfig: scaleTLS, ForceAttemptHTTP2: true}}
		return link
	default:
		link.BaseURL = "http://" + scaleAddr
		link.Client = &http.Client{Transport: &http.Transport{}}
		return link
	}
	link.BaseURL = strings.TrimRight(scaleAddr, "/")
	link.Client = &http.Client{Transport: &http.Transport{}}
	return link
}

func normalizeRegistryLinks(links []RegistryLink) []RegistryLink {
	out := make([]RegistryLink, 0, len(links))
	seen := map[string]int{}
	for _, link := range links {
		link.BaseURL = strings.TrimRight(link.BaseURL, "/")
		if link.BaseURL == "" {
			continue
		}
		if link.Name == "" {
			link.Name = link.BaseURL
		}
		baseName := link.Name
		if n := seen[baseName]; n > 0 {
			link.Name = fmt.Sprintf("%s#%d", baseName, n+1)
		}
		seen[baseName]++
		if link.Client == nil {
			link.Client = &http.Client{Transport: &http.Transport{}}
		}
		out = append(out, link)
	}
	return out
}

func minReadyLinks(n int) int {
	switch {
	case n <= 1:
		return 1
	default:
		return n - 1
	}
}

// Start launches the node_list watch loop and selector patch reconcile.
// sandbox-group data is imported into the placer/provider side, not watched from
// registry. node_list is low-frequency catalog data; hot load/budget is validated
// by registry/node owner on the cold placement path.
func (s *Service) Start(ctx context.Context) {
	startReconcile := false
	s.linksMu.Lock()
	if s.startedCtx == nil {
		s.startedCtx = ctx
		if len(s.watchLinks) > 0 {
			s.startWatchLinkLocked(ctx)
		}
		startReconcile = true
	}
	s.linksMu.Unlock()
	if startReconcile {
		go s.reconcileSelectorPatches(ctx)
	}
}

func (s *Service) SetRegistryLinks(ctx context.Context, links []RegistryLink) {
	links = normalizeRegistryLinks(links)
	s.linksMu.Lock()
	defer s.linksMu.Unlock()
	s.links = links
}

func (s *Service) SetNodeListLinks(ctx context.Context, links []RegistryLink) {
	s.SetNodeListLinksForLabel(ctx, links, "")
}

func (s *Service) SetNodeListLinksForLabel(ctx context.Context, links []RegistryLink, label string) {
	links = normalizeRegistryLinks(links)
	s.linksMu.Lock()
	defer s.linksMu.Unlock()
	if sameRegistryLinkTargets(s.watchLinks, links) && s.watchLabel == label {
		s.watchLinks = links
		return
	}
	s.watchLinks = links
	s.watchLabel = label
	s.nodes.removeSource("node_list")
	if s.watchCancel != nil {
		s.watchCancel()
		s.watchCancel = nil
	}
	if s.startedCtx != nil {
		s.startWatchLinkLocked(ctx)
	}
}

func (s *Service) SetImportSourceOwnerSource(ownerID string, peerSource func() []string) {
	if ownerID == "" {
		ownerID = "local"
	}
	s.sourceMu.Lock()
	s.sourceOwnerID = ownerID
	s.sourcePeerSource = peerSource
	s.sourceMu.Unlock()
}

func (s *Service) SetPlacerLinkResolver(resolver func(context.Context, string) ([]RegistryLink, error)) {
	s.linksMu.Lock()
	s.scaleLinkResolver = resolver
	s.linksMu.Unlock()
}

func (s *Service) SetPlacerLinkRefresher(refresher func(context.Context) error) {
	s.linksMu.Lock()
	s.scaleLinkRefresher = refresher
	s.linksMu.Unlock()
}

func sameRegistryLinkTargets(a, b []RegistryLink) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].BaseURL != b[i].BaseURL {
			return false
		}
	}
	return true
}

func (s *Service) RegistryLinks() []RegistryLink {
	s.linksMu.RLock()
	defer s.linksMu.RUnlock()
	return append([]RegistryLink(nil), s.links...)
}

func (s *Service) startWatchLinkLocked(ctx context.Context) {
	if s.watchCancel != nil {
		return
	}
	wctx, cancel := context.WithCancel(ctx)
	s.watchCancel = cancel
	go s.watchNodeList(wctx)
}

func (s *Service) ServePlacerLink(mux *http.ServeMux) {
	mux.HandleFunc(registry.PlacerLinkPlacePath, s.servePlace)
	mux.HandleFunc(registry.PlacerLinkVerifyKeyPath, s.serveVerifyKey)
}

func (s *Service) servePlace(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in routesync.PlaceReq
	if err := json.NewDecoder(io.LimitReader(req.Body, 1<<20)).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.answer(req.Context(), &in))
}

func (s *Service) serveVerifyKey(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	group := req.URL.Query().Get("group")
	apiSecret, apiSecretFound, err := s.provider.GetAPISecret(req.Context(), group)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	credentialFound := apiSecretFound
	var manifestKey clusterstate.Secret
	if apiSecret.Value == "" {
		var manifestKeyFound bool
		manifestKey, manifestKeyFound, err = s.provider.GetKey(req.Context(), group)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		credentialFound = credentialFound || manifestKeyFound
	}
	apiSecret, err = materializeAPISecret(manifestKey, apiSecret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !credentialFound {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	apiSecretValue, err := inlineSecret("api_secret", apiSecret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if apiSecretValue == "" || !verifyAPIKey(apiSecretValue, req.Header.Get("X-API-KEY")) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) RegisterLoop(ctx context.Context, id, advertise, memberlistLabel string) {
	if id == "" || advertise == "" {
		s.log.Warn("placer: registration disabled; member id/advertise missing")
		return
	}
	for {
		ok := true
		for _, link := range s.RegistryLinks() {
			if err := s.postJSON(ctx, link, registry.PlacerLinkRegisterPath, registry.PlacerRegister{
				ID: id, Advertise: advertise,
				MemberlistLabel: memberlistLabel,
			}); err != nil {
				ok = false
				s.log.Warn("placer: register", "registry", link.Name, "err", err)
			}
		}
		wait := placerRegisterInterval
		if !ok {
			wait = placerRegisterRetryInterval
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (s *Service) Ready() bool {
	return s.nodes.ready()
}

func (s *Service) ReadyForLabel(label string) bool {
	return s.nodes.readyForLabel(label)
}

// answer computes a placement for a reverse request over the local view. It
// requires a complete node_list snapshot, then resolves only the requested group;
// there is no global "all groups imported" readiness gate.
func (s *Service) answer(ctx context.Context, req *routesync.PlaceReq) *routesync.PlaceResult {
	res := &routesync.PlaceResult{ReqID: req.ReqID}
	if !s.nodes.ready() {
		res.NoNode = true
		return res
	}
	g, ok, err := s.groupForPlace(ctx, req.Group)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	if !ok {
		res.NoNode = true
		return res
	}
	apiFP, _, _, _, _, _, _, _, err := credentialPairPatch(req.Group, g.manifestKey, g.apiSecret)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	var sandboxConfig map[string]string
	if !req.Build {
		sandboxConfig, err = mergeConfig(g.group.Config, req.Config)
		if err != nil {
			res.Error = fmt.Sprintf("placer: group %q sandbox config: %v", req.Group, err)
			res.InvalidConfig = true
			return res
		}
	}
	p := placeParams{
		group: req.Group, nodes: s.nodes.values(), selectors: g.hint.NodeSelectors,
		rules: s.cfg.ShuffleSharding, candidates: s.cfg.Candidates,
		zoneAdmitMax: s.cfg.ZoneAdmitMax, excludedNodeIDs: nodeIDSet(req.ExcludeNodeIDs),
		targetRuntimeDigest: req.TargetRuntimeDigest,
		buildResources:      req.BuildResources,
	}
	var node string
	var placeErr error
	if req.Build {
		node, placeErr = placeBuild(p)
	} else {
		node, placeErr = placeSandbox(p)
	}
	if placeErr != nil || node == "" {
		res.NoNode = true
	} else {
		res.NodeID = node
		res.APISecretFingerprint = apiFP
		if req.Build {
			res.ImageRepo = g.group.ImageRepo
			if g.group.RegistryAuth.Value != "" {
				registryAuth, err := inlineSecret("registry_auth", g.group.RegistryAuth)
				if err != nil {
					res.Error = err.Error()
					res.NodeID = ""
					return res
				}
				res.RegistryAuth = registryAuth
			}
		} else {
			res.TemplateRef = g.group.TemplateRef
			res.TargetPort = g.group.TargetPort
			res.Config = sandboxConfig
		}
	}
	return res
}

type groupView struct {
	group       clusterstate.SandboxGroup
	hint        clusterstate.PlacementHint
	manifestKey clusterstate.Secret
	apiSecret   clusterstate.Secret
}

func (s *Service) groupForPlace(ctx context.Context, group string) (groupView, bool, error) {
	g, found, err := s.provider.Get(ctx, group)
	if err != nil || !found {
		return groupView{}, false, err
	}
	hint, _, err := s.provider.GetPlacementHint(ctx, group)
	if err != nil {
		return groupView{}, false, err
	}
	manifestKey, _, err := s.provider.GetKey(ctx, group)
	if err != nil {
		return groupView{}, false, err
	}
	apiSecret, _, err := s.provider.GetAPISecret(ctx, group)
	if err != nil {
		return groupView{}, false, err
	}
	apiSecret, err = materializeAPISecret(manifestKey, apiSecret)
	if err != nil {
		return groupView{}, false, fmt.Errorf("placer: group %q: %w", group, err)
	}
	return groupView{group: g, hint: hint, manifestKey: manifestKey, apiSecret: apiSecret}, true, nil
}

type selectorPatchGroup struct {
	group       string
	hint        clusterstate.PlacementHint
	manifestKey clusterstate.Secret
	apiSecret   clusterstate.Secret
}

type selectorPatchState struct {
	key    string
	sentAt time.Time
}

type importLeaseToken struct {
	sourceID string
	lease    registry.ImportSourceLease
}

// reconcileSelectorPatches computes each imported group's shuffle-effective
// selectors and credential-pair node set, then pushes them through placer_link. Only
// the source lease winner runs Range for a source_id; unchanged patches are
// refreshed so node_link credential-pair TTLs stay live.
func (s *Service) reconcileSelectorPatches(ctx context.Context) {
	last := map[string]selectorPatchState{} // group -> derived key + owner-set signature + last successful push
	t := time.NewTicker(s.reconcileInterval())
	defer t.Stop()
	readyPoll := time.NewTicker(placerReadyPollInterval)
	defer readyPoll.Stop()
	wasReady := false
	reconcileIfReady := func() {
		ready := s.nodes.ready()
		if ready && !wasReady {
			s.reconcileImportSources(ctx, s.nodes.values(), last)
		}
		wasReady = ready
	}
	reconcileIfReady()
	for {
		select {
		case <-ctx.Done():
			return
		case <-readyPoll.C:
			reconcileIfReady()
		case <-s.nodeChanges:
			if s.nodes.ready() {
				wasReady = true
				s.reconcileImportSources(ctx, s.nodes.values(), last)
			}
		case <-t.C:
			ready := s.nodes.ready()
			wasReady = ready
			if !ready {
				continue
			}
			s.reconcileImportSources(ctx, s.nodes.values(), last)
		}
	}
}

func (s *Service) reconcileInterval() time.Duration {
	interval := s.selectorPatchEvery
	if interval <= 0 {
		interval = time.Minute
	}
	if interval <= 0 {
		return time.Second
	}
	return interval
}

func (s *Service) reconcileImportSources(ctx context.Context, nodes []*registry.NodeRecord, last map[string]selectorPatchState) {
	for _, source := range s.importSources {
		if source.SourceID == "" || source.Importer == nil {
			continue
		}
		if !s.isImportSourceCandidate(source.SourceID) {
			continue
		}
		s.reconcileImportSource(ctx, source, nodes, last)
	}
}

func (s *Service) reconcileImportSource(ctx context.Context, source ImportSource, nodes []*registry.NodeRecord, last map[string]selectorPatchState) {
	retries := 0
	for {
		lease, ok, err := s.acquireImportSourceLeaseToken(ctx, source.SourceID)
		if err != nil {
			if isPlacerShutdownError(ctx, err) {
				return
			}
			if s.retryPlacerLinkFailure(ctx, source.SourceID, err, &retries) {
				continue
			}
			s.log.Warn("placer: import source lease", "source", source.SourceID, "err", err)
			return
		}
		if !ok {
			return
		}
		page, err := source.Importer.Range(ctx, lease.lease.Cursor, defaultGroupPageLimit)
		if err != nil {
			s.log.Warn("placer: group import", "source", source.SourceID, "cursor", lease.lease.Cursor, "err", err)
			return
		}
		groups, err := s.selectorPatchGroupsForImportPage(ctx, page.Groups)
		if err != nil {
			s.log.Warn("placer: group import", "source", source.SourceID, "err", err)
			return
		}
		if err := s.pushSelectorPatches(ctx, source.SourceID, nodes, groups, lease, last); err != nil {
			if isPlacerShutdownError(ctx, err) {
				return
			}
			if s.retryPlacerLinkFailure(ctx, source.SourceID, err, &retries) {
				continue
			}
			s.log.Warn("placer: selector patch", "source", source.SourceID, "err", err)
			return
		}
		complete := page.NextCursor == ""
		if err := s.checkpointImportSource(ctx, source.SourceID, lease, page.NextCursor, complete, ""); err != nil {
			if isPlacerShutdownError(ctx, err) {
				return
			}
			if s.retryPlacerLinkFailure(ctx, source.SourceID, err, &retries) {
				continue
			}
			s.log.Warn("placer: source cursor", "source", source.SourceID, "cursor", page.NextCursor, "err", err)
			return
		}
		retries = 0
		if complete {
			return
		}
	}
}

func (s *Service) retryPlacerLinkFailure(ctx context.Context, sourceID string, err error, attempts *int) bool {
	if isPlacerShutdownError(ctx, err) {
		return false
	}
	if attempts == nil || *attempts >= scaleLinkFailureRetries {
		return false
	}
	s.linksMu.RLock()
	refresher := s.scaleLinkRefresher
	s.linksMu.RUnlock()
	if refresher == nil {
		return false
	}
	*attempts++
	if refreshErr := refresher(ctx); refreshErr != nil {
		s.log.Warn("placer: refresh registry membership after placer_link failure", "source", sourceID, "err", refreshErr, "cause", err)
	} else {
		s.log.Warn("placer: retry placer_link operation after membership refresh", "source", sourceID, "attempt", *attempts, "err", err)
	}
	timer := time.NewTimer(scaleLinkRetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func isPlacerShutdownError(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "terminated signal received") || strings.Contains(msg, "context canceled")
}

func (s *Service) selectorPatchGroupsForImportPage(ctx context.Context, groups []string) ([]selectorPatchGroup, error) {
	out := make([]selectorPatchGroup, 0, len(groups))
	for _, group := range groups {
		hint, found, err := s.provider.GetPlacementHint(ctx, group)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		key, _, err := s.provider.GetKey(ctx, group)
		if err != nil {
			return nil, err
		}
		apiSecret, _, err := s.provider.GetAPISecret(ctx, group)
		if err != nil {
			return nil, err
		}
		out = append(out, selectorPatchGroup{group: group, hint: hint, manifestKey: key, apiSecret: apiSecret})
	}
	return out, nil
}

func (s *Service) isImportSourceCandidate(sourceID string) bool {
	self, peers := s.sourceOwnerSnapshot()
	if self == "" {
		return true
	}
	if len(peers) == 0 {
		peers = []string{self}
	}
	seen := map[string]bool{}
	ids := make([]string, 0, len(peers)+1)
	for _, id := range peers {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	if !seen[self] {
		ids = append(ids, self)
	}
	sort.Strings(ids)
	n := s.cfg.ImportSourceOwnerCount
	if n <= 0 {
		n = 3
	}
	if n > len(ids) {
		n = len(ids)
	}
	owners, err := maglev.LocateN([]byte(sourceID), ids, n)
	if err != nil {
		return false
	}
	for _, id := range owners {
		if id == self {
			return true
		}
	}
	return false
}

func (s *Service) sourceOwnerSnapshot() (string, []string) {
	s.sourceMu.RLock()
	self := s.sourceOwnerID
	source := s.sourcePeerSource
	s.sourceMu.RUnlock()
	if self == "" {
		self = "local"
	}
	if source == nil {
		return self, []string{self}
	}
	return self, source()
}

func (s *Service) acquireImportSourceLeaseToken(ctx context.Context, sourceID string) (importLeaseToken, bool, error) {
	self, _ := s.sourceOwnerSnapshot()
	if self == "" {
		self = "local"
	}
	links := s.scaleLinkLinks(ctx, string(clusterstate.PlacerImportSourceShard(sourceID)))
	var lastErr error
	for _, link := range links {
		resp, err := s.acquireImportSourceLease(ctx, link, sourceID, self)
		if err != nil {
			if isPlacerShutdownError(ctx, err) {
				return importLeaseToken{}, false, err
			}
			lastErr = err
			s.log.Warn("placer: import source lease", "registry", link.Name, "source", sourceID, "err", err)
			continue
		}
		if !resp.Acquired {
			return importLeaseToken{sourceID: sourceID, lease: resp.Lease}, false, nil
		}
		return importLeaseToken{sourceID: sourceID, lease: resp.Lease}, true, nil
	}
	if lastErr != nil {
		return importLeaseToken{}, false, lastErr
	}
	return importLeaseToken{}, false, nil
}

func (s *Service) acquireImportSourceLease(ctx context.Context, link RegistryLink, sourceID, ownerID string) (registry.ImportSourceLeaseResponse, error) {
	reqBody := registry.ImportSourceLeaseRequest{
		SourceID: sourceID, OwnerID: ownerID, RunID: s.runID,
		TTLMillis: s.sourceLeaseTTL.Milliseconds(),
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(&reqBody); err != nil {
		return registry.ImportSourceLeaseResponse{}, err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, link.BaseURL+registry.PlacerLinkImportSourcePath, &body)
	if err != nil {
		return registry.ImportSourceLeaseResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := link.Client.Do(req)
	if err != nil {
		return registry.ImportSourceLeaseResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return registry.ImportSourceLeaseResponse{}, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var out registry.ImportSourceLeaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return registry.ImportSourceLeaseResponse{}, err
	}
	return out, nil
}

func (s *Service) checkpointImportSource(ctx context.Context, sourceID string, lease importLeaseToken, cursor string, complete bool, lastErr string) error {
	reqBody := registry.ImportSourceCursorRequest{
		SourceID: sourceID, OwnerID: lease.lease.OwnerID, RunID: lease.lease.RunID, Term: lease.lease.Term,
		Cursor: cursor, Complete: complete, Error: lastErr,
	}
	links := s.scaleLinkLinks(ctx, string(clusterstate.PlacerImportSourceShard(sourceID)))
	var lastPostErr error
	for _, link := range links {
		if err := s.postJSON(ctx, link, registry.PlacerLinkSourceCursorPath, reqBody); err != nil {
			if isPlacerShutdownError(ctx, err) {
				return err
			}
			lastPostErr = err
			s.log.Warn("placer: source cursor", "registry", link.Name, "source", sourceID, "err", err)
			continue
		}
		return nil
	}
	if lastPostErr != nil {
		return lastPostErr
	}
	return fmt.Errorf("placer: no registry link for source %s cursor checkpoint", sourceID)
}

func (s *Service) pushSelectorPatches(ctx context.Context, sourceID string, nodes []*registry.NodeRecord, groups []selectorPatchGroup, lease importLeaseToken, last map[string]selectorPatchState) error {
	now := time.Now()
	for _, g := range groups {
		selectors, nodeIDs := selectorPatchTargets(g.group, nodes, g.hint.NodeSelectors, s.cfg.ShuffleSharding)
		apiFP, apiType, apiValue, apiRef,
			manifestFP, manifestType, manifestValue, manifestRef, err := credentialPairPatch(g.group, g.manifestKey, g.apiSecret)
		if err != nil {
			s.log.Warn("placer: credential pair", "group", g.group, "err", err)
			nodeIDs = nil
			apiFP, apiType, apiValue, apiRef = "", "", "", ""
			manifestFP, manifestType, manifestValue, manifestRef = "", "", "", ""
		}
		links := s.scaleLinkLinks(ctx, string(clusterstate.PlacerImportSourceShard(sourceID)))
		linkSig := registryLinkSignature(links)
		key := selectorPatchSignature(selectors, nodeIDs,
			apiFP, apiType, apiValue, apiRef,
			manifestFP, manifestType, manifestValue, manifestRef,
			linkSig)
		state := last[g.group]
		if state.key == key && now.Sub(state.sentAt) < s.selectorPatchEvery {
			continue
		}
		patch := &routesync.SelectorPatch{
			Group: g.group, Selectors: selectors, NodeIDs: nodeIDs,
			APISecretFingerprint: apiFP, APISecretType: apiType, APISecret: apiValue, APISecretRef: apiRef,
			ManifestKeyFingerprint: manifestFP, ManifestKeyType: manifestType, ManifestKey: manifestValue, ManifestKeyRef: manifestRef,
			ImportSourceID: lease.sourceID, ImportOwnerID: lease.lease.OwnerID, ImportRunID: lease.lease.RunID, ImportTerm: lease.lease.Term,
		}
		pushed := false
		for _, link := range links {
			if err := s.postJSON(ctx, link, registry.PlacerLinkSelectorPatchPath, patch); err != nil {
				if isPlacerShutdownError(ctx, err) {
					return err
				}
				s.log.Warn("placer: selector patch", "registry", link.Name, "source", sourceID, "group", g.group, "err", err)
				continue
			}
			last[g.group] = selectorPatchState{key: key, sentAt: now}
			pushed = true
			break
		}
		if !pushed {
			return fmt.Errorf("placer: selector patch failed for source %s group %s", sourceID, g.group)
		}
	}
	return nil
}

func (s *Service) scaleLinkLinks(ctx context.Context, recordKey string) []RegistryLink {
	s.linksMu.RLock()
	resolver := s.scaleLinkResolver
	fallback := append([]RegistryLink(nil), s.links...)
	s.linksMu.RUnlock()
	if resolver == nil {
		return fallback
	}
	links, err := resolver(ctx, recordKey)
	if err != nil {
		if isPlacerShutdownError(ctx, err) {
			return fallback
		}
		s.log.Warn("placer: placer_link owner links", "key", recordKey, "err", err)
		return fallback
	}
	links = normalizeRegistryLinks(links)
	if len(links) == 0 {
		return fallback
	}
	return links
}

func registryLinkSignature(links []RegistryLink) string {
	ids := make([]string, 0, len(links))
	for _, link := range links {
		ids = append(ids, link.Name+"@"+link.BaseURL)
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

func selectorPatchSignature(selectors []map[string]string, nodeIDs []string,
	apiFP, apiType, apiValue, apiRef,
	manifestFP, manifestType, manifestValue, manifestRef,
	linkSig string,
) string {
	nodes := append([]string(nil), nodeIDs...)
	sort.Strings(nodes)
	return strings.Join([]string{
		strings.Join(canonicalSelectorStrings(selectors), ";"),
		strings.Join(nodes, ","),
		apiFP,
		apiType,
		apiValue,
		apiRef,
		manifestFP,
		manifestType,
		manifestValue,
		manifestRef,
		linkSig,
	}, "|")
}

func canonicalSelectorStrings(selectors []map[string]string) []string {
	out := make([]string, 0, len(selectors))
	for _, selector := range selectors {
		keys := make([]string, 0, len(selector))
		for key := range selector {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, strconv.Quote(key)+"="+strconv.Quote(selector[key]))
		}
		out = append(out, strings.Join(parts, ","))
	}
	sort.Strings(out)
	return out
}

func (s *Service) postJSON(ctx context.Context, link RegistryLink, path string, v any) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(v); err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, link.BaseURL+path, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := link.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

func selectorPatchTargets(group string, nodes []*registry.NodeRecord, selectors []map[string]string, rules []clustercfg.ShuffleRule) ([]map[string]string, []string) {
	effective := selectors
	if eff, ok := effectiveSelectors(group, nodes, selectors, rules); ok {
		effective = eff
	}
	var nodeIDs []string
	for _, n := range nodes {
		if n.Draining || !matchSelectors(n.Labels, effective) {
			continue
		}
		nodeIDs = append(nodeIDs, n.NodeID)
	}
	sort.Strings(nodeIDs)
	return effective, nodeIDs
}

func (s *Service) watchNodeList(ctx context.Context) {
	index := 0
	current := ""
	token := ""
	for ctx.Err() == nil {
		s.linksMu.RLock()
		links := append([]RegistryLink(nil), s.watchLinks...)
		label := s.watchLabel
		s.linksMu.RUnlock()
		if len(links) == 0 {
			s.nodes.removeSource("node_list")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}
		if index >= len(links) {
			index = 0
		}
		link := links[index]
		index++
		if current != link.Name {
			current = link.Name
			token = ""
			s.nodes.removeSource("node_list")
		}
		sink := s.nodes.sourceWithNotifyLabel("node_list", label, s.notifyNodeListChanged)
		nextToken, err := s.subscribeOnce(ctx, link, registry.PlacerLinkNodeListWatchPath, token, sink)
		token = nextToken
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("placer: node_list watch ended; failing over", "registry", link.Name, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (s *Service) notifyNodeListChanged() {
	select {
	case s.nodeChanges <- struct{}{}:
	default:
	}
}

// subscribe keeps a view synced from registry placer_link, reconnecting with
// capped backoff and resuming from the last applied rev (mirrors the router).
func (s *Service) subscribe(ctx context.Context, link RegistryLink, path string, sink viewSink) {
	backoff := 200 * time.Millisecond
	var token string
	for ctx.Err() == nil {
		newToken, err := s.subscribeOnce(ctx, link, path, token, sink)
		token = newToken
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("placer: view watch ended; reconnecting", "registry", link.Name, "path", path, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (s *Service) subscribeOnce(ctx context.Context, link RegistryLink, path, fromToken string, sink viewSink) (string, error) {
	u := fmt.Sprintf("%s%s", link.BaseURL, path)
	if fromToken != "" {
		u += "?from=" + url.QueryEscape(fromToken)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fromToken, err
	}
	resp, err := link.Client.Do(req)
	if err != nil {
		return fromToken, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fromToken, fmt.Errorf("view watch %s: %s", path, resp.Status)
	}
	token, syncing := fromToken, false
	for {
		ev, err := readViewFrame(resp.Body)
		if err != nil {
			return token, err
		}
		switch ev.Type {
		case "reset":
			sink.reset()
			syncing = true
		case "put":
			sink.put(ev.Key, ev.Value)
		case "delete":
			sink.del(ev.Key)
		case "bookmark":
			sink.bookmark()
			syncing = false
		}
		// Advance the resume point only once live (a snapshot interrupted before its
		// bookmark re-snapshots on reconnect — same discipline as the router).
		if !syncing && ev.Token != "" {
			token = ev.Token
		}
	}
}

func readViewFrame(r io.Reader) (*registry.ViewEvent, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	buf := make([]byte, binary.LittleEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var ev registry.ViewEvent
	if err := json.Unmarshal(buf, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}

func decodeNode(raw json.RawMessage) (*registry.NodeRecord, bool) {
	var n registry.NodeRecord
	if json.Unmarshal(raw, &n) != nil {
		return nil, false
	}
	return &n, true
}

func decodeNodeList(raw json.RawMessage) (*registry.NodeRecord, bool) {
	var n clusterstate.NodeListEntry
	if json.Unmarshal(raw, &n) != nil {
		return nil, false
	}
	return &registry.NodeRecord{
		Meta:   n.SourceMeta,
		NodeID: n.NodeID, Labels: n.Labels, Capacity: n.Capacity,
		BuildRegistrationCapacity: n.BuildRegistrationCapacity, BuildExecutionCapacity: n.BuildExecutionCapacity,
		DataEndpoint: n.DataEndpoint, RuntimeDigest: n.RuntimeDigest, Draining: n.Draining,
	}, true
}

// viewSink is the subset of a watch-backed view the watch loop drives.
type viewSink interface {
	reset()
	put(key string, raw json.RawMessage)
	del(key string)
	bookmark()
}

// nodeView holds the single active node_list WATCH_LIST source. A source applies
// snapshot reset/bookmark atomically; failover clears the old source before the
// next owner publishes a full snapshot.
type nodeView struct {
	mu       sync.RWMutex
	sources  map[string]*nodeSource
	minReady int
}

type nodeSource struct {
	live    map[string]*registry.NodeRecord
	shadow  map[string]*registry.NodeRecord
	syncing bool
	synced  bool
	label   string
}

type nodeViewSink struct {
	view     *nodeView
	source   string
	label    string
	onChange func()
}

func newNodeView(minReady int) *nodeView {
	if minReady < 1 {
		minReady = 1
	}
	return &nodeView{sources: map[string]*nodeSource{}, minReady: minReady}
}

func (v *nodeView) source(name string) viewSink {
	return v.sourceWithNotify(name, nil)
}

func (v *nodeView) sourceWithNotify(name string, notify func()) viewSink {
	return v.sourceWithNotifyLabel(name, "", notify)
}

func (v *nodeView) sourceWithNotifyLabel(name, label string, notify func()) viewSink {
	if name == "" {
		name = "registry"
	}
	v.mu.Lock()
	src := v.ensureLocked(name)
	src.label = label
	v.mu.Unlock()
	return nodeViewSink{view: v, source: name, label: label, onChange: notify}
}

func (v *nodeView) setMinReady(n int) {
	if n < 1 {
		n = 1
	}
	v.mu.Lock()
	v.minReady = n
	v.mu.Unlock()
}

func (v *nodeView) removeSource(name string) {
	v.mu.Lock()
	delete(v.sources, name)
	v.mu.Unlock()
}

func (v *nodeView) ensureLocked(name string) *nodeSource {
	src := v.sources[name]
	if src == nil {
		src = &nodeSource{live: map[string]*registry.NodeRecord{}}
		v.sources[name] = src
	}
	return src
}

func (s nodeViewSink) reset() {
	s.view.mu.Lock()
	src := s.view.ensureLocked(s.source)
	src.label = s.label
	src.shadow = map[string]*registry.NodeRecord{}
	src.syncing = true
	s.view.mu.Unlock()
}

func (s nodeViewSink) put(key string, raw json.RawMessage) {
	v, ok := decodeNodeList(raw)
	if !ok || key == "" {
		return
	}
	s.view.mu.Lock()
	src := s.view.ensureLocked(s.source)
	changed := false
	if src.syncing {
		src.shadow[key] = v
	} else {
		changed = true
		src.live[key] = v
	}
	s.view.mu.Unlock()
	if changed {
		s.notifyChange()
	}
}

func (s nodeViewSink) del(key string) {
	s.view.mu.Lock()
	src := s.view.ensureLocked(s.source)
	changed := false
	if src.syncing {
		delete(src.shadow, key)
	} else {
		_, changed = src.live[key]
		delete(src.live, key)
	}
	s.view.mu.Unlock()
	if changed {
		s.notifyChange()
	}
}

func (s nodeViewSink) bookmark() {
	s.view.mu.Lock()
	src := s.view.ensureLocked(s.source)
	oldLabel := src.label
	wasSynced := src.synced
	changed := !wasSynced || oldLabel != s.label
	if src.syncing {
		if wasSynced && !nodeRecordMapsEqual(src.live, src.shadow) {
			changed = true
		}
		src.live, src.shadow = src.shadow, nil
		src.syncing = false
	}
	src.label = s.label
	src.synced = true
	s.view.mu.Unlock()
	if changed {
		s.notifyChange()
	}
}

func nodeRecordMapsEqual(a, b map[string]*registry.NodeRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for key, av := range a {
		bv, ok := b[key]
		if !ok || !reflect.DeepEqual(av, bv) {
			return false
		}
	}
	return true
}

func (s nodeViewSink) notifyChange() {
	if s.onChange != nil {
		s.onChange()
	}
}

func (v *nodeView) ready() bool {
	return v.readyForLabel("")
}

func (v *nodeView) readyForLabel(label string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	ready := 0
	for _, src := range v.sources {
		if src.synced && (label == "" || src.label == label) {
			ready++
		}
	}
	return ready >= v.minReady
}

func (v *nodeView) values() []*registry.NodeRecord {
	v.mu.RLock()
	defer v.mu.RUnlock()
	merged := map[string]*registry.NodeRecord{}
	for _, src := range v.sources {
		if !src.synced {
			continue
		}
		for id, rec := range src.live {
			cur := merged[id]
			if cur == nil || fresherNode(rec, cur) {
				merged[id] = cloneNodeRecord(rec)
			}
		}
	}
	out := make([]*registry.NodeRecord, 0, len(merged))
	for _, rec := range merged {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func fresherNode(a, b *registry.NodeRecord) bool {
	if a.Meta.Ballot.Less(b.Meta.Ballot) {
		return false
	}
	if b.Meta.Ballot.Less(a.Meta.Ballot) {
		return true
	}
	if a.Meta.Rev != b.Meta.Rev {
		return a.Meta.Rev > b.Meta.Rev
	}
	return a.NodeID < b.NodeID
}

func nodeIDSet(ids []string) map[string]struct{} {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			out[id] = struct{}{}
		}
	}
	return out
}

func cloneNodeRecord(in *registry.NodeRecord) *registry.NodeRecord {
	if in == nil {
		return nil
	}
	out := *in
	out.Labels = cloneStringMap(in.Labels)
	out.BuildRegistrationCapacity = cloneAdmissionLimit(in.BuildRegistrationCapacity)
	out.BuildExecutionCapacity = cloneAdmissionLimit(in.BuildExecutionCapacity)
	out.BuildRegistrationUsage = cloneAdmissionUsage(in.BuildRegistrationUsage)
	out.BuildExecutionUsage = cloneAdmissionUsage(in.BuildExecutionUsage)
	return &out
}

func cloneAdmissionLimit(in *routesync.BuildAdmissionLimit) *routesync.BuildAdmissionLimit {
	if in == nil {
		return nil
	}
	out := *in
	out.Resources = cloneBuildResources(in.Resources)
	return &out
}

func cloneAdmissionUsage(in *routesync.BuildAdmissionUsage) *routesync.BuildAdmissionUsage {
	if in == nil {
		return nil
	}
	out := *in
	out.Resources = cloneBuildResources(in.Resources)
	return &out
}

func cloneBuildResources(in *routesync.BuildResources) *routesync.BuildResources {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
