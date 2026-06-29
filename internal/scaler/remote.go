package scaler

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// Service is the standalone scaler placement engine. It keeps node_list/group
// views synced, computes placement over the local view, and returns suggestions
// that the registry commits through route/node owner state.
type Service struct {
	linksMu     sync.RWMutex
	links       []RegistryLink
	watchLinks  []RegistryLink
	watchCancel context.CancelFunc
	startedCtx  context.Context

	cfg       clustercfg.PlacementConfig
	deadAfter int64 // node_dead_after seconds (node-alive eligibility, §4.2)
	log       *slog.Logger

	nodes    *nodeView
	provider clusterstate.SandboxGroupProvider
	importer clusterstate.SandboxGroupImporter
}

type RegistryLink struct {
	Name    string
	BaseURL string
	Client  *http.Client
}

// NewRemote builds a standalone scaler connected to registry control paths. The
// address may be a UDS path, host:port, or http(s) URL.
func NewRemote(scaleAddr string, scaleTLS *tls.Config, cfg clustercfg.PlacementConfig, deadAfter int64, log *slog.Logger) *Service {
	return NewRemoteLinks([]RegistryLink{registryLinkFromAddress("registry", scaleAddr, scaleTLS)}, cfg, deadAfter, log)
}

func NewRemoteLinks(links []RegistryLink, cfg clustercfg.PlacementConfig, deadAfter int64, log *slog.Logger) *Service {
	return NewRemoteLinksWithGroups(links, emptyGroupSource{}, emptyGroupSource{}, cfg, deadAfter, log)
}

func NewRemoteLinksWithGroups(links []RegistryLink, provider clusterstate.SandboxGroupProvider, importer clusterstate.SandboxGroupImporter, cfg clustercfg.PlacementConfig, deadAfter int64, log *slog.Logger) *Service {
	if cfg.Candidates <= 0 {
		cfg.Candidates = 2
	}
	links = normalizeRegistryLinks(links)
	if provider == nil {
		provider = emptyGroupSource{}
	}
	if importer == nil {
		importer = emptyGroupSource{}
	}
	return &Service{
		links: links, watchLinks: links,
		cfg: cfg, deadAfter: deadAfter, log: log,
		nodes:    newNodeView(1),
		provider: provider,
		importer: importer,
	}
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
		link.BaseURL = "http://scale-link"
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

// Start launches the node_list watch loop and key allocation reconcile.
// sandbox-group data is imported into the scaler/provider side, not watched from
// registry. node_list is low-frequency catalog data; hot load/budget is validated
// by registry/node owner on the cold placement path.
func (s *Service) Start(ctx context.Context) {
	s.linksMu.Lock()
	if s.startedCtx == nil {
		s.startedCtx = ctx
		s.startWatchLinkLocked(ctx)
	}
	s.linksMu.Unlock()
	go s.reconcileKeyAllocations(ctx)
}

func (s *Service) SetRegistryLinks(ctx context.Context, links []RegistryLink) {
	links = normalizeRegistryLinks(links)
	s.linksMu.Lock()
	defer s.linksMu.Unlock()
	s.links = links
}

func (s *Service) SetNodeListLinks(ctx context.Context, links []RegistryLink) {
	links = normalizeRegistryLinks(links)
	s.linksMu.Lock()
	defer s.linksMu.Unlock()
	if sameRegistryLinkTargets(s.watchLinks, links) {
		s.watchLinks = links
		return
	}
	s.watchLinks = links
	s.nodes.removeSource("node_list")
	if s.watchCancel != nil {
		s.watchCancel()
		s.watchCancel = nil
	}
	if s.startedCtx != nil {
		s.startWatchLinkLocked(ctx)
	}
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

func (s *Service) ServeScaleLink(mux *http.ServeMux) {
	mux.HandleFunc(registry.ScaleLinkPlacePath, s.servePlace)
	mux.HandleFunc(registry.ScaleLinkVerifyKeyPath, s.serveVerifyKey)
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
	authSecret, ok, err := s.provider.GetAuthKey(req.Context(), req.URL.Query().Get("group"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !ok {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	authKey, err := inlineSecret("auth_key", authSecret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if authKey == "" || !verifyAPIKey(authKey, req.Header.Get("X-API-KEY")) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) RegisterLoop(ctx context.Context, id, advertise, memberlistLabel string) {
	s.RegisterLoopDynamic(ctx, id, advertise, memberlistLabel, advertise)
}

func (s *Service) Ready() bool {
	return s.nodes.ready()
}

func (s *Service) RegisterLoopDynamic(ctx context.Context, id, advertise, memberlistLabel, memberlistAdvertise string) {
	if id == "" || advertise == "" {
		s.log.Warn("scaler: registration disabled; member id/advertise missing")
		return
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		for _, link := range s.RegistryLinks() {
			if err := s.postJSON(ctx, link, registry.ScaleLinkRegisterPath, registry.ScalerRegister{
				ID: id, Advertise: advertise,
				MemberlistLabel: memberlistLabel, MemberlistAdvertise: memberlistAdvertise,
			}); err != nil {
				s.log.Warn("scaler: register", "registry", link.Name, "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
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
	fp, _, _, _, err := manifestKeyPatch(req.Group, g.manifestKey)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	if !req.Build {
		authKey, err := inlineSecret("auth_key", g.authKey)
		if err != nil {
			res.Error = err.Error()
			return res
		}
		if authKey != "" && req.SandboxID != "" {
			tok, err := clusterstate.DeriveAccessToken(authKey, req.SandboxID)
			if err != nil {
				res.Error = err.Error()
				return res
			}
			res.AccessToken = tok
		}
	}
	p := placeParams{
		group: req.Group, nodes: s.nodes.values(), selectors: g.hint.NodeSelectors,
		rules: s.cfg.ShuffleSharding, candidates: s.cfg.Candidates,
		zoneAdmitMax: s.cfg.ZoneAdmitMax, deadAfter: s.deadAfter, now: time.Now().Unix(),
		targetRuntimeDigest: req.TargetRuntimeDigest,
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
		res.KeyFingerprint = fp
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
			res.Config = mergeConfig(g.group.Config, req.Config)
		}
	}
	return res
}

type groupView struct {
	group       clusterstate.SandboxGroup
	hint        clusterstate.PlacementHint
	manifestKey clusterstate.Secret
	authKey     clusterstate.Secret
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
	authKey, _, err := s.provider.GetAuthKey(ctx, group)
	if err != nil {
		return groupView{}, false, err
	}
	return groupView{group: g, hint: hint, manifestKey: manifestKey, authKey: authKey}, true, nil
}

type groupAllocation struct {
	group       string
	hint        clusterstate.PlacementHint
	manifestKey clusterstate.Secret
}

func (s *Service) groupAllocations(ctx context.Context) ([]groupAllocation, error) {
	var out []groupAllocation
	cursor := ""
	for {
		page, err := s.importer.Range(ctx, cursor, defaultGroupPageLimit)
		if err != nil {
			return nil, err
		}
		for _, group := range page.Groups {
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
			out = append(out, groupAllocation{group: group, hint: hint, manifestKey: key})
		}
		if page.NextCursor == "" {
			return out, nil
		}
		cursor = page.NextCursor
	}
}

// reconcileKeyAllocations periodically computes each group's explicit node set for
// manifest-key distribution and pushes it to every registry member. Change
// tracking is per registry link so a down member retries without forcing the
// already updated members to receive every unchanged group again.
func (s *Service) reconcileKeyAllocations(ctx context.Context) {
	last := map[string]map[string]string{} // registry link -> group -> derived key
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !s.nodes.ready() {
				continue
			}
			nodes := s.nodes.values()
			allocations, err := s.groupAllocations(ctx)
			if err != nil {
				s.log.Warn("scaler: group import", "err", err)
				continue
			}
			current := map[string]bool{}
			for _, g := range allocations {
				current[g.group] = true
				selectors, nodeIDs := keyAllocation(g.group, nodes, g.hint.NodeSelectors, s.cfg.ShuffleSharding)
				fp, keyType, keyValue, keyRef, err := manifestKeyPatch(g.group, g.manifestKey)
				if err != nil {
					s.log.Warn("scaler: manifest key", "group", g.group, "err", err)
					nodeIDs = nil
					fp, keyType, keyValue, keyRef = "", "", "", ""
				}
				key := fmt.Sprint(selectors) + "|" + strings.Join(nodeIDs, ",") + "|" + fp + "|" + keyType + "|" + keyValue + "|" + keyRef
				patch := &routesync.SelectorPatch{
					Group: g.group, Selectors: selectors, NodeIDs: nodeIDs, NodeAllocation: true,
					KeyFingerprint: fp, ManifestKeyType: keyType, ManifestKey: keyValue, ManifestKeyRef: keyRef,
				}
				for _, link := range s.RegistryLinks() {
					if last[link.Name] == nil {
						last[link.Name] = map[string]string{}
					}
					if last[link.Name][g.group] == key {
						continue // unchanged since last successful push to this member
					}
					if err := s.postJSON(ctx, link, registry.ScaleLinkSelectorPatchPath, patch); err != nil {
						s.log.Warn("scaler: selector patch", "registry", link.Name, "group", g.group, "err", err)
						continue
					}
					last[link.Name][g.group] = key
				}
			}
			byName := s.linksByName()
			for name, groups := range last {
				link, ok := byName[name]
				if !ok {
					delete(last, name)
					continue
				}
				for group := range groups {
					if current[group] {
						continue
					}
					if err := s.postJSON(ctx, link, registry.ScaleLinkSelectorPatchPath, &routesync.SelectorPatch{Group: group, NodeAllocation: true}); err != nil {
						s.log.Warn("scaler: selector patch delete", "registry", name, "group", group, "err", err)
						continue
					}
					delete(groups, group)
				}
			}
		}
	}
}

func (s *Service) linksByName() map[string]RegistryLink {
	links := s.RegistryLinks()
	out := make(map[string]RegistryLink, len(links))
	for _, link := range links {
		out[link.Name] = link
	}
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

func keyAllocation(group string, nodes []*registry.NodeRecord, selectors []map[string]string, rules []clustercfg.ShuffleRule) ([]map[string]string, []string) {
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
		sink := s.nodes.source("node_list")
		nextToken, err := s.subscribeOnce(ctx, link, registry.ScaleLinkNodeListWatchPath, token, sink)
		token = nextToken
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("scaler: node_list watch ended; failing over", "registry", link.Name, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// subscribe keeps a view synced from registry scale_link, reconnecting with
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
		s.log.Warn("scaler: view watch ended; reconnecting", "registry", link.Name, "path", path, "err", err)
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
		NodeID: n.NodeID, Labels: n.Labels, Capacity: n.Capacity, BuildCapacity: n.BuildCapacity,
		DataEndpoint: n.DataEndpoint, RuntimeDigest: n.RuntimeDigest, Draining: n.Draining,
		LastHeartbeatUnix: n.LastHeartbeatUnix,
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
}

type nodeViewSink struct {
	view   *nodeView
	source string
}

func newNodeView(minReady int) *nodeView {
	if minReady < 1 {
		minReady = 1
	}
	return &nodeView{sources: map[string]*nodeSource{}, minReady: minReady}
}

func (v *nodeView) source(name string) viewSink {
	if name == "" {
		name = "registry"
	}
	v.mu.Lock()
	v.ensureLocked(name)
	v.mu.Unlock()
	return nodeViewSink{view: v, source: name}
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
	if src.syncing {
		src.shadow[key] = v
	} else {
		src.live[key] = v
	}
	s.view.mu.Unlock()
}

func (s nodeViewSink) del(key string) {
	s.view.mu.Lock()
	src := s.view.ensureLocked(s.source)
	if src.syncing {
		delete(src.shadow, key)
	} else {
		delete(src.live, key)
	}
	s.view.mu.Unlock()
}

func (s nodeViewSink) bookmark() {
	s.view.mu.Lock()
	src := s.view.ensureLocked(s.source)
	if src.syncing {
		src.live, src.shadow = src.shadow, nil
		src.syncing = false
	}
	src.synced = true
	s.view.mu.Unlock()
}

func (v *nodeView) ready() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	ready := 0
	for _, src := range v.sources {
		if src.synced {
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
	if a.LastHeartbeatUnix != b.LastHeartbeatUnix {
		return a.LastHeartbeatUnix > b.LastHeartbeatUnix
	}
	return a.NodeID < b.NodeID
}

func cloneNodeRecord(in *registry.NodeRecord) *registry.NodeRecord {
	if in == nil {
		return nil
	}
	out := *in
	out.Labels = cloneStringMap(in.Labels)
	if in.BuildCapacity != nil {
		bc := *in.BuildCapacity
		out.BuildCapacity = &bc
	}
	if in.BuildAlloc != nil {
		ba := *in.BuildAlloc
		out.BuildAlloc = &ba
	}
	return &out
}
