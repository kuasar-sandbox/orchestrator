package scaler

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// Service is the standalone scaler (cluster-scaler.md §4.2/§5): it DIALS the
// registry scale_link, subscribes node_list/group views, and answers the
// registry's reverse placement requests over scale_link (no scaler listen).
// Placement runs over its local view; the registry commits by CAS.
type Service struct {
	scaleBase   string
	scheme      string
	watchClient *http.Client     // no timeout: long-lived view watches
	placeTr     *http2.Transport // full-duplex scale_link session (place_req down / place_result up)
	cfg         clustercfg.PlacementConfig
	deadAfter   int64 // node_dead_after seconds (node-alive eligibility, §4.2)
	log         *slog.Logger

	nodes  *viewMap[*registry.NodeRecord]
	groups *viewMap[*registry.GroupView]
}

// NewRemote builds a standalone scaler dialing registry scale_link (a UDS path,
// or host:port; scaleTLS non-nil = mTLS h2). deadAfter is node_dead_after.
func NewRemote(scaleAddr string, scaleTLS *tls.Config, cfg clustercfg.PlacementConfig, deadAfter int64, log *slog.Logger) *Service {
	if cfg.Candidates <= 0 {
		cfg.Candidates = 2
	}
	s := &Service{
		cfg: cfg, deadAfter: deadAfter, log: log,
		nodes:  newViewMap(decodeNodeList),
		groups: newViewMap(decodeGroup),
	}
	// View watches over a normal client; the scale_link session over an h2 transport
	// (full-duplex: registry writes place_req down, scaler writes place_result up).
	pt := &http2.Transport{}
	switch {
	case strings.HasPrefix(scaleAddr, "/"):
		s.scaleBase, s.scheme = "http://scale-link", "http"
		dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", scaleAddr)
		}
		s.watchClient = &http.Client{Transport: &http.Transport{DialContext: dial}}
		pt.AllowHTTP = true
		pt.DialTLSContext = func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) { return dial(ctx, "", "") }
	case scaleTLS != nil:
		s.scaleBase, s.scheme = "https://"+scaleAddr, "https"
		s.watchClient = &http.Client{Transport: &http.Transport{TLSClientConfig: scaleTLS, ForceAttemptHTTP2: true}}
		pt.DialTLSContext = func(ctx context.Context, _, addr string, _ *tls.Config) (net.Conn, error) {
			d := &net.Dialer{}
			raw, err := d.DialContext(ctx, "tcp", scaleAddr)
			if err != nil {
				return nil, err
			}
			tc := tls.Client(raw, scaleTLS)
			if err := tc.HandshakeContext(ctx); err != nil {
				raw.Close()
				return nil, err
			}
			return tc, nil
		}
	default:
		s.scaleBase, s.scheme = "http://"+scaleAddr, "http"
		s.watchClient = &http.Client{Transport: &http.Transport{}}
		pt.AllowHTTP = true
		pt.DialTLSContext = func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", scaleAddr)
		}
	}
	s.placeTr = pt
	return s
}

// Start launches the node_list + group view-watch loops + the scale_link session
// (background). node_list is low-frequency catalog data; hot load/budget is
// validated by registry/node owner on the cold placement path.
func (s *Service) Start(ctx context.Context) {
	go s.subscribe(ctx, registry.ScaleLinkNodeListWatchPath, s.nodes)
	go s.subscribe(ctx, registry.ScaleLinkGroupWatchPath, s.groups)
	go s.runPlaceLink(ctx)
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
	var selectors []map[string]string
	if g, ok := s.groups.get(req.Group); ok {
		selectors = g.NodeSelectors
	}
	p := placeParams{
		group: req.Group, nodes: s.nodes.values(), selectors: selectors,
		rules: s.cfg.ShuffleSharding, candidates: s.cfg.Candidates,
		zoneAdmitMax: s.cfg.ZoneAdmitMax, deadAfter: s.deadAfter, now: time.Now().Unix(),
		targetRuntimeDigest: req.TargetRuntimeDigest,
	}
	var node string
	var err error
	if req.Build {
		node, err = placeBuild(p)
	} else {
		node, err = placeSandbox(p)
	}
	if err != nil || node == "" {
		res.NoNode = true
	} else {
		res.NodeID = node
	}
	return res
}

// runPlaceLink keeps the scale_link session alive (the registry's reverse-call channel),
// reconnecting with capped backoff until ctx is cancelled.
func (s *Service) runPlaceLink(ctx context.Context) {
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		err := s.placeSession(ctx)
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("scaler: place-link ended; reconnecting", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// placeSession runs one scale_link session: PUT the link (request body = place_result
// stream up), read place_req down (response body), and answer each over the local
// view. Full-duplex h2 (mirrors node-link, inverse roles: the registry commands).
func (s *Service) placeSession(ctx context.Context) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pr, pw := io.Pipe()
	defer pw.Close()
	req, err := http.NewRequestWithContext(sctx, http.MethodPut, s.scaleBase+routesync.ScaleLinkPath, pr)
	if err != nil {
		return err
	}
	resp, err := s.placeTr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := routesync.ReadMsg(resp.Body); err != nil { // registry Hello
		return err
	}
	// Two writers on pw (place_result from the read loop, allocation patch from
	// the reconcile goroutine) → serialize.
	var wmu sync.Mutex
	send := func(m *routesync.Msg) error {
		wmu.Lock()
		defer wmu.Unlock()
		return routesync.WriteMsg(pw, m)
	}
	go s.reconcileKeyAllocations(sctx, send)
	for {
		m, err := routesync.ReadMsg(resp.Body)
		if err != nil {
			return err
		}
		if m.Type == routesync.TypePlaceReq && m.PlaceReq != nil {
			res := s.answer(m.PlaceReq) // placement is in-memory + fast; answer inline
			if err := send(&routesync.Msg{Type: routesync.TypePlaceResult, PlaceResult: res}); err != nil {
				return err
			}
		}
	}
}

// reconcileKeyAllocations periodically computes each group's explicit node set for
// manifest-key distribution and pushes it to registry/node owner. Per-session
// change tracking; a reconnect re-pushes all derived state.
func (s *Service) reconcileKeyAllocations(ctx context.Context, send func(*routesync.Msg) error) {
	last := map[string]string{}
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
			for _, g := range s.groups.values() {
				selectors, nodeIDs := keyAllocation(g.Group, nodes, g.NodeSelectors, s.cfg.ShuffleSharding)
				key := fmt.Sprint(selectors) + "|" + strings.Join(nodeIDs, ",")
				if last[g.Group] == key {
					continue // unchanged since last push
				}
				patch := &routesync.SelectorPatch{
					Group: g.Group, Selectors: selectors, NodeIDs: nodeIDs, NodeAllocation: true,
				}
				if err := send(&routesync.Msg{Type: routesync.TypeSelectorPatch, Patch: patch}); err != nil {
					return
				}
				last[g.Group] = key
			}
		}
	}
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
func (s *Service) subscribe(ctx context.Context, path string, sink viewSink) {
	backoff := 200 * time.Millisecond
	var rev int64
	for ctx.Err() == nil {
		newRev, err := s.subscribeOnce(ctx, path, rev, sink)
		rev = newRev
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("scaler: view watch ended; reconnecting", "path", path, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (s *Service) subscribeOnce(ctx context.Context, path string, fromRev int64, sink viewSink) (int64, error) {
	u := fmt.Sprintf("%s%s?from_rev=%d", s.scaleBase, path, fromRev)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fromRev, err
	}
	resp, err := s.watchClient.Do(req)
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

func decodeGroup(raw json.RawMessage) (*registry.GroupView, bool) {
	var g registry.GroupView
	if json.Unmarshal(raw, &g) != nil {
		return nil, false
	}
	return &g, true
}

// viewSink is the subset of viewMap the watch loop drives (type-erased over the
// element type, since put takes the raw JSON the map decodes itself).
type viewSink interface {
	reset()
	put(key string, raw json.RawMessage)
	del(key string)
	bookmark()
}

// viewMap is a watch-synced map: a shadow accumulates during a snapshot and is
// swapped in atomically at the bookmark; live deltas apply directly. Reads never
// see a half-built set.
type viewMap[T any] struct {
	mu      sync.RWMutex
	live    map[string]T
	shadow  map[string]T
	syncing bool
	synced  bool
	decode  func(json.RawMessage) (T, bool)
}

func newViewMap[T any](decode func(json.RawMessage) (T, bool)) *viewMap[T] {
	return &viewMap[T]{live: map[string]T{}, decode: decode}
}

func (m *viewMap[T]) reset() {
	m.mu.Lock()
	m.shadow = map[string]T{}
	m.syncing = true
	m.mu.Unlock()
}

func (m *viewMap[T]) put(key string, raw json.RawMessage) {
	v, ok := m.decode(raw)
	if !ok {
		return
	}
	m.mu.Lock()
	if m.syncing {
		m.shadow[key] = v
	} else {
		m.live[key] = v
	}
	m.mu.Unlock()
}

func (m *viewMap[T]) del(key string) {
	m.mu.Lock()
	if !m.syncing {
		delete(m.live, key)
	}
	m.mu.Unlock()
}

func (m *viewMap[T]) bookmark() {
	m.mu.Lock()
	if m.syncing {
		m.live, m.shadow = m.shadow, nil
		m.syncing = false
		m.synced = true
	}
	m.mu.Unlock()
}

func (m *viewMap[T]) ready() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.synced
}

func (m *viewMap[T]) values() []T {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]T, 0, len(m.live))
	for _, v := range m.live {
		out = append(out, v)
	}
	return out
}

func (m *viewMap[T]) get(key string) (T, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.live[key]
	return v, ok
}
