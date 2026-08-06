package configsock

import (
	"context"
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
}

// Registry tracks live plugin registrations. The plugin-plane handler Adds on
// register and Removes on disconnect; the external-mode proxyForwarder reads
// ProxyTargets to forward data-plane requests to a registered proxy endpoint.
// Concurrency-safe and shared between the config-socket server and the proxyForwarder.
type Registry struct {
	mu             sync.Mutex
	m              map[string]*Plugin
	mmdsChangeHook func()
}

func NewRegistry() *Registry { return &Registry{m: map[string]*Plugin{}} }

// Add registers p, evicting (and closing the stream of) any existing registration
// with the same id — a re-register deregisters the prior. The lock serializes this
// against the evicted plugin's Remove, so the successor is never dropped.
func (r *Registry) Add(p *Plugin) {
	r.mu.Lock()
	before := r.mmdsAvailableLocked()
	if old := r.m[p.ID]; old != nil && old != p {
		old.cancel() // ends the prior handler; its deferred Remove sees it is no longer current
	}
	r.m[p.ID] = p
	changed := before != r.mmdsAvailableLocked()
	hook := r.mmdsChangeHook
	r.mu.Unlock()
	if changed && hook != nil {
		hook()
	}
}

// Remove deregisters p only if it is still the current registration for its id, so
// an evicted plugin's deferred Remove never drops its successor.
func (r *Registry) Remove(p *Plugin) {
	r.mu.Lock()
	before := r.mmdsAvailableLocked()
	if r.m[p.ID] == p {
		delete(r.m, p.ID)
	}
	changed := before != r.mmdsAvailableLocked()
	hook := r.mmdsChangeHook
	r.mu.Unlock()
	if changed && hook != nil {
		hook()
	}
}

// SetMMDSChangeHook installs a notification for changes to the live external
// proxy MMDS capability. The hook receives no captured value: it must query
// MMDSAvailable, so callbacks that race with a reconnect always observe the
// latest registration rather than publishing stale state.
func (r *Registry) SetMMDSChangeHook(hook func()) {
	r.mu.Lock()
	r.mmdsChangeHook = hook
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
}

// MMDSAvailable reports whether the external proxy master's live registration
// proves that it has bound an MMDS listener. The held registration stream is
// the lease; disconnecting it removes the capability.
func (r *Registry) MMDSAvailable() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mmdsAvailableLocked()
}

func (r *Registry) mmdsAvailableLocked() bool {
	p := r.m[routesync.ProxyPluginID]
	return p != nil && p.Caps.Proxy != nil && p.Caps.Proxy.Socket.Path != "" &&
		p.Caps.Subscribe != nil && p.Caps.Subscribe.Kind == routesync.KindRouteWake && p.Caps.Mmds
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
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	p := &Plugin{ID: id, Caps: reg, cancel: cancel}
	s.deps.Plugins.Add(p)
	defer s.deps.Plugins.Remove(p)
	s.log.Info("plugin registered", "id", id, "subscribe", reg.SubscribeKind(), "proxy", reg.Proxy != nil, "mmds", reg.Mmds)
	routesync.ServeStream(ctx, w, r.Body, s.deps.RouteSource, reg, s.log)
	s.log.Info("plugin deregistered", "id", id)
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
