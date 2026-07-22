// Package mmdsauth is the internal-mode "endpoint authority" for MMDS
// endpoints: it reads sandbox_mmds_endpoints
// directly from the store and implements the bounded value-wait semantics
// for store-backend endpoints. It satisfies internal/mmds's
// EndpointAuthority interface directly (conductor process, one shared
// instance). External mode's counterpart (proxy master process) is
// internal/mmdsrpc.Client — the same internal/external split
// internal/mmds.Source already has.
package mmdsauth

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrelay"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// relayFetcher is the narrow surface ServeRelay needs from a relay client —
// satisfied structurally by *mmdsrelay.Client (New's production wiring) and
// stubbable in tests without spinning up real DNS/TLS.
type relayFetcher interface {
	Fetch(ctx context.Context, key, rawURL, headerName, headerValue string) mmdsrelay.Result
}

// Authority reads endpoint definitions/values from st and parks a bounded
// wait for a never-configured store or relay-auth value, waking waiters as
// soon as an admin mutation lands (Notify) rather than only on timeout.
type Authority struct {
	st      *store.Store
	relay   relayFetcher  // nil = relay-backend endpoints are unavailable (503)
	timeout time.Duration // value-wait timeout for a never-configured endpoint (2-5s)

	mu      sync.Mutex
	waiters map[string]chan struct{} // key = sid+"\x00"+name; closed+recreated to broadcast a change
}

// New builds an Authority. timeout is the node-policy value_wait_timeout
// (config.MMDSEndpointsConfig.ValueWaitTimeoutDur()); non-positive falls back
// to 3s, mirroring that accessor's own default. relay may be nil (relay
// endpoints unavailable, e.g. before Phase 6's relay config is wired); pass
// a *mmdsrelay.Client in production.
func New(st *store.Store, relay relayFetcher, timeout time.Duration) *Authority {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Authority{st: st, relay: relay, timeout: timeout, waiters: map[string]chan struct{}{}}
}

// Lookup resolves the exact (sandbox_id, path) to its declared name+backend
// type. found=false means no endpoint owns that exact path for that
// sandbox — the caller (internal/mmds) falls through to the built-in envd
// token/metadata response.
func (a *Authority) Lookup(ctx context.Context, sandboxID, path string) (name, backendType string, found bool) {
	name, backendType, found, err := a.st.MMDSEndpointByPath(ctx, sandboxID, path)
	if err != nil {
		return "", "", false
	}
	return name, backendType, found
}

// ServeStore answers a store-backend GET. Only a never-configured endpoint
// (revision=0, value_present=false) waits, bounded by the configured
// timeout; a PUT/DELETE landing during the wait wakes it immediately via
// Notify. Deleted (revision>0, value_present=false) or expired values return
// present=false immediately, matching the guest-visible 404 in every case —
// the caller cannot distinguish "never configured, timed out" from "deleted"
// from "expired", and is not required to.
// revision is returned even when present=false is not used by the caller,
// but is needed on the present=true path for the X-Kuasar-MMDS-Revision
// response header.
func (a *Authority) ServeStore(ctx context.Context, sandboxID, name string) (value []byte, contentType string, revision int64, present bool) {
	v, ok, err := a.st.GetMMDSStoreValue(ctx, sandboxID, name)
	if err != nil || !ok {
		return nil, "", 0, false
	}
	if v.Revision == 0 && !v.Present {
		if !a.wait(ctx, sandboxID, name) {
			return nil, "", 0, false // timed out without a PUT landing
		}
		if v, ok, err = a.st.GetMMDSStoreValue(ctx, sandboxID, name); err != nil || !ok {
			return nil, "", 0, false
		}
	}
	if !v.Present {
		return nil, "", v.Revision, false
	}
	if v.ExpiresUnix > 0 && time.Now().Unix() >= v.ExpiresUnix {
		return nil, "", v.Revision, false
	}
	return v.Value, v.ContentType, v.Revision, true
}

// relayPublicConfig mirrors the JSON shape internal/orch's
// mmdsRelayPublicConfig writes into public_config_json at Create — the
// relay URL and auth header name, both immutable for the sandbox's
// lifetime.
type relayPublicConfig struct {
	URL            string `json:"url"`
	AuthHeaderName string `json:"auth_header_name"`
}

// ServeRelay answers a relay-backend GET, applying the same never-configured
// bounded-wait rule as ServeStore (the same uniform node-policy value
// timeout used by store). ok=false means "treat as
// absent" — the caller responds 404 without ever having contacted the
// upstream (never-configured-then-timed-out, or revoked auth). ok=true
// means status is the exact code to return: the relay's own classification
// (a passthrough 2xx/4xx/5xx, or 429/502/504) — a real upstream attempt was
// made.
func (a *Authority) ServeRelay(ctx context.Context, sandboxID, name string) (status int, contentType string, body []byte, ok bool) {
	if a.relay == nil {
		return http.StatusServiceUnavailable, "", nil, true
	}
	v, found, err := a.st.GetMMDSRelayAuth(ctx, sandboxID, name)
	if err != nil || !found {
		return 0, "", nil, false
	}
	if v.Revision == 0 && !v.Present {
		if !a.wait(ctx, sandboxID, name) {
			return 0, "", nil, false // never configured, timed out -> 404, upstream never contacted
		}
		if v, found, err = a.st.GetMMDSRelayAuth(ctx, sandboxID, name); err != nil || !found {
			return 0, "", nil, false
		}
	}
	if !v.Present {
		return 0, "", nil, false // revoked -> 404, upstream never contacted
	}

	cfgJSON, _, cfgFound, err := a.st.GetMMDSEndpointPublicConfig(ctx, sandboxID, name)
	if err != nil || !cfgFound {
		return 0, "", nil, false
	}
	var cfg relayPublicConfig
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil || cfg.URL == "" || cfg.AuthHeaderName == "" {
		return http.StatusBadGateway, "", nil, true
	}

	// Cancel the fetch if this endpoint's auth is rotated/revoked while the
	// upstream call is outstanding.
	watchCtx, cancel := a.watch(ctx, sandboxID, name)
	defer cancel()

	res := a.relay.Fetch(watchCtx, waitKey(sandboxID, name), cfg.URL, cfg.AuthHeaderName, string(v.Value))
	return res.Status, res.ContentType, res.Body, true
}

// Notify wakes any goroutine currently parked in wait() for (sid,name) — the
// admin mutation path (internal/orch's SetMMDSStoreValue/ClearMMDSStoreValue
// and their relay-auth counterparts) calls this right after a successful
// store write, so a parked request observes the change on the very next
// check instead of waiting out the full timeout.
func (a *Authority) Notify(sandboxID, name string) {
	key := waitKey(sandboxID, name)
	a.mu.Lock()
	defer a.mu.Unlock()
	if ch, ok := a.waiters[key]; ok {
		close(ch)
		delete(a.waiters, key) // the next waiter creates a fresh channel
	}
}

func (a *Authority) wait(ctx context.Context, sandboxID, name string) bool {
	key := waitKey(sandboxID, name)
	a.mu.Lock()
	ch, ok := a.waiters[key]
	if !ok {
		ch = make(chan struct{})
		a.waiters[key] = ch
	}
	a.mu.Unlock()

	timer := time.NewTimer(a.timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// watch derives a context from parent that is cancelled the next time
// Notify(sandboxID, name) fires — used by ServeRelay to abort an in-flight
// upstream fetch when its auth value is rotated or revoked mid-flight. The
// returned CancelFunc must always be called (it also stops the internal
// watcher goroutine once the caller is done, whether or not a Notify ever
// arrived).
func (a *Authority) watch(parent context.Context, sandboxID, name string) (context.Context, context.CancelFunc) {
	key := waitKey(sandboxID, name)
	a.mu.Lock()
	ch, ok := a.waiters[key]
	if !ok {
		ch = make(chan struct{})
		a.waiters[key] = ch
	}
	a.mu.Unlock()

	cctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-ch:
			cancel()
		case <-cctx.Done():
		}
	}()
	return cctx, cancel
}

func waitKey(sandboxID, name string) string { return sandboxID + "\x00" + name }
