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
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

// Service is the standalone scaler (cluster-scaler.md §5): it mirrors the
// registry's node table + group placement config over the op view-watch (a local
// view) and answers placement over POST /scaler/place, which the registry's
// remotePlacer calls. The in-process Placer (New) stays the default deployment.
type Service struct {
	opBase      string
	watchClient *http.Client // no timeout: long-lived view watches
	cfg         clustercfg.ScalerConfig
	log         *slog.Logger

	nodes  *viewMap[*registry.NodeRecord]
	groups *viewMap[*registry.GroupView]
}

var errViewNotReady = fmt.Errorf("scaler: view not synced")

// NewRemote builds a standalone scaler dialing the registry op endpoint (a UDS
// path, or host:port; opTLS non-nil = mTLS h2).
func NewRemote(opAddr string, opTLS *tls.Config, cfg clustercfg.ScalerConfig, log *slog.Logger) *Service {
	if cfg.PlaceCandidates <= 0 {
		cfg.PlaceCandidates = 2
	}
	s := &Service{
		cfg: cfg, log: log,
		nodes:  newViewMap(decodeNode),
		groups: newViewMap(decodeGroup),
	}
	var transport http.RoundTripper
	switch {
	case strings.HasPrefix(opAddr, "/"):
		s.opBase = "http://op"
		transport = &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", opAddr)
		}}
	case opTLS != nil:
		s.opBase = "https://" + opAddr
		transport = &http.Transport{TLSClientConfig: opTLS, ForceAttemptHTTP2: true}
	default:
		s.opBase = "http://" + opAddr
		transport = &http.Transport{}
	}
	s.watchClient = &http.Client{Transport: transport}
	return s
}

// Start launches the node + group view-watch loops (background).
func (s *Service) Start(ctx context.Context) {
	go s.subscribe(ctx, registry.OpNodeWatchPath, s.nodes)
	go s.subscribe(ctx, registry.OpGroupWatchPath, s.groups)
}

// Handler serves the placement op the registry's remotePlacer calls.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/scaler/place", s.servePlace)
	return mux
}

func (s *Service) servePlace(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	nodeID, err := s.Place(q.Get("group"), q.Get("route_key"))
	switch {
	case err == registry.ErrNoNode:
		http.Error(w, "no eligible node", http.StatusConflict)
	case err == errViewNotReady:
		http.Error(w, "scaler view not synced", http.StatusServiceUnavailable)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"node_id": nodeID})
	}
}

// Place runs the placement algorithm over the local view. It refuses until both
// views have completed their first snapshot (so it never places over a partial
// node set or missing selectors → blast-radius violation).
func (s *Service) Place(group, routeKey string) (string, error) {
	if !s.nodes.ready() || !s.groups.ready() {
		return "", errViewNotReady
	}
	var selectors []map[string]string
	if g, ok := s.groups.get(group); ok {
		selectors = g.NodeSelectors
	}
	return placeOver(group, s.nodes.values(), selectors, s.cfg.ShuffleSharding, s.cfg.PlaceCandidates)
}

// subscribe keeps a view synced from the registry op watch, reconnecting with
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
	u := fmt.Sprintf("%s%s?from_rev=%d", s.opBase, path, fromRev)
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
