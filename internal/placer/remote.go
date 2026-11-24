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
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/maglev"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
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

// Service imports complete Group definitions onto statically selected Nodes.
// Per-request placement and authorization are owned by Registry and Node.
type Service struct {
	linksMu     sync.RWMutex
	links       []RegistryLink
	watchLinks  []RegistryLink
	watchLabel  string
	watchCancel context.CancelFunc
	startedCtx  context.Context

	cfg clustercfg.PlacementConfig
	log *slog.Logger

	nodes *nodeView

	sourceMu           sync.RWMutex
	runID              string
	importSources      []ImportSource
	sourceOwnerID      string
	sourcePeerSource   func() []string
	sourceLeaseTTL     time.Duration
	importEvery        time.Duration
	scaleLinkResolver  func(context.Context, string) ([]RegistryLink, error)
	scaleLinkRefresher func(context.Context) error
	nodeChanges        chan struct{}
	importMu           sync.Mutex
	importStates       map[importTarget]*importState
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
	return NewRemoteLinksWithImports(links, nil, cfg, log)
}

func NewRemoteLinksWithImports(links []RegistryLink, sources []ImportSource, cfg clustercfg.PlacementConfig, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	if cfg.ImportSourceOwnerCount <= 0 {
		cfg.ImportSourceOwnerCount = 3
	}
	sourceLeaseTTL := cfg.ImportSourceLeaseTTLDur()
	if sourceLeaseTTL <= 0 {
		sourceLeaseTTL = 15 * time.Second
	}
	importEvery := cfg.ImportRefreshDur()
	if importEvery <= 0 {
		importEvery = time.Minute
	}
	links = normalizeRegistryLinks(links)
	runID := newRunID()
	return &Service{
		links: links, watchLinks: links,
		cfg: cfg, log: log,
		nodes:          newNodeView(1),
		runID:          runID,
		importSources:  normalizeImportSources(sources),
		sourceOwnerID:  "local",
		sourceLeaseTTL: sourceLeaseTTL,
		importEvery:    importEvery,
		nodeChanges:    make(chan struct{}, 1),
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

// Start watches the Node catalog and runs conditional background imports.
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
		go s.reconcileGroups(ctx)
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

type importLeaseToken struct {
	sourceID string
	lease    registry.ImportSourceLease
}

// reconcileSelectorPatches computes each imported group's shuffle-effective
// selectors and credential-pair node set, then pushes them through placer_link. Only
// the source lease winner runs Range for a source_id; unchanged patches are
// refreshed so node_link credential-pair TTLs stay live.

func (s *Service) reconcileInterval() time.Duration {
	interval := s.importEvery
	if interval <= 0 {
		interval = time.Minute
	}
	if interval <= 0 {
		return time.Second
	}
	return interval
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
	links := s.scaleLinkLinks(ctx, clusterstate.ImportSourceRoutingKey(sourceID))
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
	if err := clusterclient.DecodeResponse(resp.Body, &out); err != nil {
		return registry.ImportSourceLeaseResponse{}, err
	}
	return out, nil
}

func (s *Service) checkpointImportSource(ctx context.Context, sourceID string, lease importLeaseToken, cursor string, complete bool, lastErr string) error {
	reqBody := registry.ImportSourceCursorRequest{
		SourceID: sourceID, OwnerID: lease.lease.OwnerID, RunID: lease.lease.RunID, Term: lease.lease.Term,
		Cursor: cursor, Complete: complete, Error: lastErr,
	}
	links := s.scaleLinkLinks(ctx, clusterstate.ImportSourceRoutingKey(sourceID))
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
	return clusterclient.DrainResponse(resp.Body)
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
	if json.Unmarshal(raw, &n) != nil || !n.Version.IsCommitted() {
		return nil, false
	}
	return &registry.NodeRecord{
		Version: n.Version,
		NodeID:  n.NodeID, Labels: n.Labels, Capacity: n.Capacity,
		BuildRegistrationCapacity: n.BuildRegistrationCapacity, BuildExecutionCapacity: n.BuildExecutionCapacity,
		APIEndpoint: n.APIEndpoint, DataEndpoint: n.DataEndpoint, RuntimeDigest: n.RuntimeDigest, Draining: n.Draining,
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
	// The merged sources describe the same node_list record in one database.
	// Compare all ten bytes, including the transaction order within a commit.
	if order := a.Version.Compare(b.Version); order != 0 {
		return order > 0
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
