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
	watchCancel map[string]context.CancelFunc
	startedCtx  context.Context

	cfg       clustercfg.PlacementConfig
	deadAfter int64 // node_dead_after seconds (node-alive eligibility, §4.2)
	log       *slog.Logger

	nodes  *nodeView
	groups *groupStore
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
	if cfg.Candidates <= 0 {
		cfg.Candidates = 2
	}
	links = normalizeRegistryLinks(links)
	return &Service{
		links: links, watchLinks: links, watchCancel: map[string]context.CancelFunc{},
		cfg: cfg, deadAfter: deadAfter, log: log,
		nodes:  newNodeView(minReadyLinks(len(links))),
		groups: newGroupStore(),
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
		for _, link := range s.watchLinks {
			s.startWatchLinkLocked(ctx, link)
		}
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
	old := make(map[string]RegistryLink, len(s.watchLinks))
	for _, link := range s.watchLinks {
		old[link.Name] = link
	}
	next := make(map[string]RegistryLink, len(links))
	for _, link := range links {
		next[link.Name] = link
		if cur, ok := old[link.Name]; ok && cur.BaseURL == link.BaseURL {
			continue
		}
		if cancel := s.watchCancel[link.Name]; cancel != nil {
			cancel()
			delete(s.watchCancel, link.Name)
			s.nodes.removeSource(link.Name)
		}
		if s.startedCtx != nil {
			s.startWatchLinkLocked(ctx, link)
		}
	}
	for name := range old {
		if _, ok := next[name]; ok {
			continue
		}
		if cancel := s.watchCancel[name]; cancel != nil {
			cancel()
			delete(s.watchCancel, name)
		}
		s.nodes.removeSource(name)
	}
	s.watchLinks = links
	s.nodes.setMinReady(minReadyLinks(len(links)))
}

func (s *Service) RegistryLinks() []RegistryLink {
	s.linksMu.RLock()
	defer s.linksMu.RUnlock()
	return append([]RegistryLink(nil), s.links...)
}

func (s *Service) startWatchLinkLocked(ctx context.Context, link RegistryLink) {
	if _, exists := s.watchCancel[link.Name]; exists {
		return
	}
	wctx, cancel := context.WithCancel(ctx)
	s.watchCancel[link.Name] = cancel
	go s.subscribe(wctx, link, registry.ScaleLinkNodeListWatchPath, s.nodes.source(link.Name))
}

func (s *Service) ServeScaleLink(mux *http.ServeMux) {
	mux.HandleFunc(registry.ScaleLinkPlacePath, s.servePlace)
	mux.HandleFunc(registry.ScaleLinkVerifyKeyPath, s.serveVerifyKey)
	mux.HandleFunc(GroupImportPath, s.serveGroupImport)
}

func (s *Service) ImportGroups(groups []clusterstate.SandboxGroupRecord) {
	s.groups.replace(groups)
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
	_ = json.NewEncoder(w).Encode(s.answer(&in))
}

func (s *Service) serveGroupImport(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	groups, sum, err := importGroups(io.LimitReader(req.Body, 64<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.groups.replace(groups)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(sum)
}

func (s *Service) serveVerifyKey(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	g, ok := s.groups.get(req.URL.Query().Get("group"))
	if !ok {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	authKey, err := inlineSecret("auth_key", g.AuthKey)
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

func (s *Service) RegisterLoop(ctx context.Context, id, advertise, readyLabel string) {
	s.RegisterLoopDynamic(ctx, id, advertise, func(context.Context) (string, error) { return readyLabel, nil })
}

func (s *Service) RegisterLoopDynamic(ctx context.Context, id, advertise string, readyLabel func(context.Context) (string, error)) {
	if id == "" || advertise == "" {
		s.log.Warn("scaler: registration disabled; member id/advertise missing")
		return
	}
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		label := ""
		if readyLabel != nil {
			var err error
			label, err = readyLabel(ctx)
			if err != nil {
				s.log.Warn("scaler: ready label", "err", err)
			}
		}
		for _, link := range s.RegistryLinks() {
			if err := s.postJSON(ctx, link, registry.ScaleLinkRegisterPath, registry.ScalerRegister{ID: id, Advertise: advertise, ReadyLabel: label}); err != nil {
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
// returns NoNode until both views have synced (never places over a partial node
// set / missing selectors → blast-radius violation) or when nothing is eligible.
func (s *Service) answer(req *routesync.PlaceReq) *routesync.PlaceResult {
	res := &routesync.PlaceResult{ReqID: req.ReqID}
	if !s.nodes.ready() || !s.groups.ready() {
		res.NoNode = true
		return res
	}
	g, ok := s.groups.get(req.Group)
	if !ok {
		res.NoNode = true
		return res
	}
	fp, _, _, _, err := manifestKeyPatch(g)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	if !req.Build {
		authKey, err := inlineSecret("auth_key", g.AuthKey)
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
		group: req.Group, nodes: s.nodes.values(), selectors: g.NodeSelectors,
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
			res.ImageRepo = g.ImageRepo
			if g.RegistryAuth.Value != "" {
				registryAuth, err := inlineSecret("registry_auth", g.RegistryAuth)
				if err != nil {
					res.Error = err.Error()
					res.NodeID = ""
					return res
				}
				res.RegistryAuth = registryAuth
			}
		} else {
			res.TemplateRef = g.TemplateRef
			res.Config = mergeConfig(g.Config, req.Config)
		}
	}
	return res
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
			if !s.nodes.ready() || !s.groups.ready() {
				continue
			}
			nodes := s.nodes.values()
			current := map[string]bool{}
			for _, g := range s.groups.values() {
				current[g.Group] = true
				selectors, nodeIDs := keyAllocation(g.Group, nodes, g.NodeSelectors, s.cfg.ShuffleSharding)
				fp, keyType, keyValue, keyRef, err := manifestKeyPatch(g)
				if err != nil {
					s.log.Warn("scaler: manifest key", "group", g.Group, "err", err)
					nodeIDs = nil
					fp, keyType, keyValue, keyRef = "", "", "", ""
				}
				key := fmt.Sprint(selectors) + "|" + strings.Join(nodeIDs, ",") + "|" + fp + "|" + keyType + "|" + keyValue + "|" + keyRef
				patch := &routesync.SelectorPatch{
					Group: g.Group, Selectors: selectors, NodeIDs: nodeIDs, NodeAllocation: true,
					KeyFingerprint: fp, ManifestKeyType: keyType, ManifestKey: keyValue, ManifestKeyRef: keyRef,
				}
				for _, link := range s.RegistryLinks() {
					if last[link.Name] == nil {
						last[link.Name] = map[string]string{}
					}
					if last[link.Name][g.Group] == key {
						continue // unchanged since last successful push to this member
					}
					if err := s.postJSON(ctx, link, registry.ScaleLinkSelectorPatchPath, patch); err != nil {
						s.log.Warn("scaler: selector patch", "registry", link.Name, "group", g.Group, "err", err)
						continue
					}
					last[link.Name][g.Group] = key
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

// subscribe keeps a view synced from registry scale_link, reconnecting with
// capped backoff and resuming from the last applied rev (mirrors the router).
func (s *Service) subscribe(ctx context.Context, link RegistryLink, path string, sink viewSink) {
	backoff := 200 * time.Millisecond
	var rev int64
	for ctx.Err() == nil {
		newRev, err := s.subscribeOnce(ctx, link, path, rev, sink)
		rev = newRev
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

func (s *Service) subscribeOnce(ctx context.Context, link RegistryLink, path string, fromRev int64, sink viewSink) (int64, error) {
	u := fmt.Sprintf("%s%s?from_rev=%d", link.BaseURL, path, fromRev)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fromRev, err
	}
	resp, err := link.Client.Do(req)
	if err != nil {
		return fromRev, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fromRev, fmt.Errorf("view watch %s: %s", path, resp.Status)
	}
	rev, syncing := fromRev, false
	for {
		ev, err := readViewFrame(resp.Body)
		if err != nil {
			return rev, err
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
		if !syncing && ev.Rev > rev {
			rev = ev.Rev
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

// nodeView merges node_list WATCH_LIST streams from multiple registry members.
// Each source applies snapshot reset/bookmark atomically; the scaler reads a
// node_id-keyed union over synced sources, preferring the freshest heartbeat when
// the same node appears from more than one registry member.
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
