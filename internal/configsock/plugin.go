package configsock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"slices"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// --- plugin plane: subscriber registry + registration handler ---
//
// A subscriber (the external proxy master, or a route observer such as the platform
// agent) holds a single PUT /internal/plugin/{id}/register request open: that
// connection is its lease + its route stream. Closing it deregisters; a second
// registration with the same id evicts (and closes the stream of) the first.

// Plugin is one live registration on the plugin plane.
type Plugin struct {
	ID     string
	Caps   routesync.Register
	cancel context.CancelFunc
	epoch  uint64
	ready  bool
}

// Registry tracks live plugin registrations. The plugin-plane handler Adds on
// register and Removes on disconnect; the external-mode proxyForwarder reads
// ProxyTargets to forward data-plane requests to a registered proxy endpoint.
// Concurrency-safe and shared between the config-socket server and the proxyForwarder.
type Registry struct {
	mu        sync.Mutex
	m         map[string]*Plugin
	nextEpoch uint64
	barriers  map[string]*proxyRouteBarrier
}

var (
	ErrProxyRouteUnavailable  = errors.New("configsock: external proxy route stream unavailable")
	ErrProxyRouteDisconnected = errors.New("configsock: external proxy route stream disconnected")
	ErrProxyRouteLeaseChanged = errors.New("configsock: external proxy route lease changed")
)

func NewRegistry() *Registry {
	return &Registry{m: map[string]*Plugin{}, barriers: map[string]*proxyRouteBarrier{}}
}

// Add registers p, evicting (and closing the stream of) any existing registration
// with the same id — a re-register deregisters the prior. The lock serializes this
// against the evicted plugin's Remove, so the successor is never dropped.
func (r *Registry) Add(p *Plugin) {
	r.mu.Lock()
	if old := r.m[p.ID]; old != nil && old != p {
		r.failBarriersLocked(old, ErrProxyRouteLeaseChanged)
		if old.cancel != nil {
			old.cancel() // ends the prior handler; its deferred Remove sees it is no longer current
		}
	}
	r.nextEpoch++
	p.epoch = r.nextEpoch
	p.ready = false
	r.m[p.ID] = p
	r.mu.Unlock()
}

// Remove deregisters p only if it is still the current registration for its id, so
// an evicted plugin's deferred Remove never drops its successor.
func (r *Registry) Remove(p *Plugin) {
	r.mu.Lock()
	if r.m[p.ID] == p {
		r.failBarriersLocked(p, ErrProxyRouteDisconnected)
		delete(r.m, p.ID)
	}
	r.mu.Unlock()
}

// markRouteStreamReady admits p to new barriers only after ServeStream has
// installed its live event subscription. This closes the registration/subscription
// race in which an otherwise healthy barrier could be published into no channel.
func (r *Registry) markRouteStreamReady(p *Plugin) {
	r.mu.Lock()
	if r.m[p.ID] == p {
		p.ready = true
	}
	r.mu.Unlock()
}

// routeBarrierAck applies an ACK only to the exact registration epoch captured
// by the barrier. Unknown, duplicate, stale, and successor-session ACKs are no-ops.
func (r *Registry) routeBarrierAck(p *Plugin, barrierID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.barriers[barrierID]
	if b == nil || b.err != nil || r.m[p.ID] != p || !p.ready {
		return
	}
	if required := b.required[p.epoch]; required != p {
		return
	}
	b.acked[p.epoch] = struct{}{}
	if len(b.acked) == len(b.required) {
		b.signalLocked()
	}
}

// BeginProxyRouteBarrier captures the complete current traffic-serving proxy
// participant set. V1 has one trusted ProxyPluginID lease, but proxyRouteBarrier
// deliberately implements all-of completion over a set.
func (r *Registry) BeginProxyRouteBarrier() (routesync.RouteBarrier, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.m[routesync.ProxyPluginID]
	if !proxyRouteParticipantReady(p) {
		return nil, ErrProxyRouteUnavailable
	}
	return r.beginRouteBarrierLocked([]*Plugin{p})
}

func proxyRouteParticipantReady(p *Plugin) bool {
	return p != nil && p.ready && p.ID == routesync.ProxyPluginID &&
		p.Caps.Subscribe != nil && p.Caps.Subscribe.Kind == routesync.KindRouteWake &&
		p.Caps.Proxy != nil && p.Caps.Proxy.Socket.Path != ""
}

func (r *Registry) beginRouteBarrierLocked(required []*Plugin) (*proxyRouteBarrier, error) {
	id, err := newRouteBarrierID()
	if err != nil {
		return nil, err
	}
	b := &proxyRouteBarrier{
		registry: r,
		id:       id,
		required: make(map[uint64]*Plugin, len(required)),
		acked:    make(map[uint64]struct{}, len(required)),
		done:     make(chan struct{}),
	}
	for _, p := range required {
		if p == nil || p.epoch == 0 {
			return nil, ErrProxyRouteUnavailable
		}
		b.required[p.epoch] = p
	}
	if len(b.required) == 0 {
		return nil, ErrProxyRouteUnavailable
	}
	r.barriers[id] = b
	return b, nil
}

func newRouteBarrierID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (r *Registry) failBarriersLocked(p *Plugin, err error) {
	for _, b := range r.barriers {
		if b.required[p.epoch] == p {
			b.failLocked(err)
		}
	}
}

type proxyRouteBarrier struct {
	registry *Registry
	id       string
	required map[uint64]*Plugin
	acked    map[uint64]struct{}
	done     chan struct{}
	signaled bool
	err      error
}

func (b *proxyRouteBarrier) ID() string { return b.id }

func (b *proxyRouteBarrier) Wait(ctx context.Context) error {
	select {
	case <-b.done:
		b.registry.mu.Lock()
		err := b.err
		b.registry.mu.Unlock()
		return err
	default:
	}
	select {
	case <-b.done:
		b.registry.mu.Lock()
		err := b.err
		b.registry.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Commit is the barrier linearization point. ACK completion alone is
// insufficient: every captured participant must still be the current ready
// registration while this check holds the registry lock.
func (b *proxyRouteBarrier) Commit() error {
	r := b.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.barriers[b.id] != b {
		return ErrProxyRouteLeaseChanged
	}
	if b.err != nil {
		delete(r.barriers, b.id)
		return b.err
	}
	if len(b.acked) != len(b.required) {
		return ErrProxyRouteUnavailable
	}
	for epoch, p := range b.required {
		if p.epoch != epoch || !p.ready || r.m[p.ID] != p {
			delete(r.barriers, b.id)
			return ErrProxyRouteLeaseChanged
		}
	}
	delete(r.barriers, b.id)
	return nil
}

func (b *proxyRouteBarrier) Cancel() {
	r := b.registry
	r.mu.Lock()
	if r.barriers[b.id] == b {
		delete(r.barriers, b.id)
		b.failLocked(context.Canceled)
	}
	r.mu.Unlock()
}

func (b *proxyRouteBarrier) signalLocked() {
	if !b.signaled {
		b.signaled = true
		close(b.done)
	}
}

func (b *proxyRouteBarrier) failLocked(err error) {
	if b.err == nil {
		b.err = err
	}
	b.signalLocked()
}

// ProxyTargets returns the data-forward UDS paths of registered proxy plugins, in
// stable id order.
func (r *Registry) ProxyTargets() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.m))
	for id, p := range r.m {
		if p.Caps.Proxy != nil && p.Caps.Proxy.Socket.Path != "" {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = r.m[id].Caps.Proxy.Socket.Path
	}
	return out
}

// ProxyStatsTarget returns the master-only stats endpoint from the current
// trusted proxy registration. The registration connection remains its lease.
func (r *Registry) ProxyStatsTarget() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	plugin := r.m[routesync.ProxyPluginID]
	if plugin == nil || plugin.Caps.Proxy == nil || plugin.Caps.Proxy.StatsSocket == nil || plugin.Caps.Proxy.StatsSocket.Path == "" {
		return "", false
	}
	return plugin.Caps.Proxy.StatsSocket.Path, true
}

// handlePluginRegister authenticates the subscriber (plugin_pidfile / socket perms),
// reads its Register frame, registers it (evicting any same-id holder), then runs the
// route stream until the connection drops — which deregisters it. The held h2c
// request IS the lease; there is no explicit deregister call.
func (s *Server) handlePluginRegister(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.pluginAuthed(peer) {
		http.Error(w, "not authorized (plugin)", http.StatusForbidden)
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing plugin id", http.StatusBadRequest)
		return
	}
	reg, err := routesync.ReadRegister(r.Body)
	if err != nil {
		http.Error(w, "bad register frame", http.StatusBadRequest)
		return
	}
	if reg.Mmds && !isTrustedMMDSProxyRegistration(id, reg) {
		http.Error(w, "not authorized (mmds proxy)", http.StatusForbidden)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	p := &Plugin{ID: id, Caps: reg, cancel: cancel}
	s.deps.Plugins.Add(p)
	defer s.deps.Plugins.Remove(p)
	s.log.Info("plugin registered", "id", id, "subscribe", reg.SubscribeKind(), "proxy", reg.Proxy != nil, "mmds", reg.Mmds)
	hooks := &routesync.StreamHooks{
		RouteStreamReady: func() { s.deps.Plugins.markRouteStreamReady(p) },
		RouteBarrierAck:  func(barrierID string) { s.deps.Plugins.routeBarrierAck(p, barrierID) },
	}
	routesync.ServeStream(ctx, w, r.Body, s.deps.RouteSource, reg, hooks, s.log)
	s.log.Info("plugin deregistered", "id", id)
}

func isTrustedMMDSProxyRegistration(id string, reg routesync.Register) bool {
	return id == routesync.ProxyPluginID &&
		reg.Subscribe != nil && reg.Subscribe.Kind == routesync.KindRouteWake &&
		reg.Proxy != nil && reg.Mmds
}

// pluginAuthed gates the plugin plane: when plugin_pidfile is set the peer pid must
// be listed; otherwise the socket's 0600 perms (same uid / root) are the only gate.
func (s *Server) pluginAuthed(peer int) bool {
	if s.deps.PluginPidfile == "" {
		return true
	}
	pids, err := readPIDs(s.deps.PluginPidfile)
	if err != nil {
		s.log.Warn("configsock plugin pidfile", "path", s.deps.PluginPidfile, "err", err)
		return false
	}
	if slices.Contains(pids, peer) {
		return true
	}
	s.log.Warn("configsock plugin pid not allowlisted", "peer", peer, "pidfile", s.deps.PluginPidfile)
	return false
}
