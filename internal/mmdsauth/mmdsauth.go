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

// Counter is the narrow metrics surface Authority needs. Add (unlike
// internal/mmds.Counter's Inc-only surface) is safe here since Authority
// only ever runs in the conductor process against a real *metrics.M, never
// through the worker-side metrics pipe.
type Counter interface {
	Inc(name string)
	Add(name string, n int64)
}

type noopCounter struct{}

func (noopCounter) Inc(string)        {}
func (noopCounter) Add(string, int64) {}

// Authority reads endpoint definitions/values from st and parks a bounded
// wait for a never-configured store or relay-auth value, waking waiters as
// soon as an admin mutation lands (Notify) rather than only on timeout.
type Authority struct {
	st      *store.Store
	relay   relayFetcher  // nil = relay-backend endpoints are unavailable (503)
	timeout time.Duration // value-wait timeout for a never-configured endpoint (2-5s)
	mx      Counter

	mu      sync.Mutex
	waiters map[string]chan struct{} // key = sid+"\x00"+name; closed+recreated to broadcast a change
}

// New builds an Authority. timeout is the node-policy value_wait_timeout
// (config.MMDSEndpointsConfig.ValueWaitTimeoutDur()); non-positive falls back
// to 3s, mirroring that accessor's own default. relay may be nil (relay
// endpoints unavailable, e.g. before Phase 6's relay config is wired); pass
// a *mmdsrelay.Client in production. mx may be nil (metrics off).
func New(st *store.Store, relay relayFetcher, timeout time.Duration, mx Counter) *Authority {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	if mx == nil {
		mx = noopCounter{}
	}
	return &Authority{st: st, relay: relay, timeout: timeout, mx: mx, waiters: map[string]chan struct{}{}}
}

// Lookup resolves the exact (sandbox_id, path) to its declared name+backend
// type. found=false (err=nil) means no endpoint owns that exact path for
// that sandbox — the caller (internal/mmds) falls through to the built-in
// envd token/metadata response. A non-nil err (the store read itself
// failed) must never be treated the same as found=false: the caller must
// respond 503, not silently fall through.
func (a *Authority) Lookup(ctx context.Context, sandboxID, path string) (name, backendType string, found bool, err error) {
	return a.st.MMDSEndpointByPath(ctx, sandboxID, path)
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
// response header. A non-nil err (the store read itself failed) must never
// be treated the same as present=false: the caller must respond 503.
func (a *Authority) ServeStore(ctx context.Context, sandboxID, name string) (value []byte, contentType string, revision int64, present bool, err error) {
	v, ok, err := a.st.GetMMDSStoreValue(ctx, sandboxID, name)
	if err != nil {
		return nil, "", 0, false, err
	}
	if !ok {
		return nil, "", 0, false, nil
	}
	if v.Revision == 0 && !v.Present {
		if !a.timedWait(ctx, sandboxID, name) {
			return nil, "", 0, false, nil // timed out without a PUT landing
		}
		if v, ok, err = a.st.GetMMDSStoreValue(ctx, sandboxID, name); err != nil {
			return nil, "", 0, false, err
		} else if !ok {
			return nil, "", 0, false, nil
		}
	}
	if !v.Present {
		return nil, "", v.Revision, false, nil
	}
	if v.ExpiresUnix > 0 && time.Now().Unix() >= v.ExpiresUnix {
		return nil, "", v.Revision, false, nil
	}
	return v.Value, v.ContentType, v.Revision, true, nil
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
// timeout used by store). ok=false (err=nil) means "treat as
// absent" — the caller responds 404 without ever having contacted the
// upstream (never-configured-then-timed-out, or revoked auth). ok=true
// means status is the exact code to return: the relay's own classification
// (a passthrough 2xx/4xx/5xx, or 429/502/504) — a real upstream attempt was
// made. A non-nil err (the store read itself failed) must never be treated
// the same as ok=false: the caller must respond 503.
func (a *Authority) ServeRelay(ctx context.Context, sandboxID, name string) (status int, contentType string, body []byte, ok bool, err error) {
	if a.relay == nil {
		return http.StatusServiceUnavailable, "", nil, true, nil
	}
	v, found, err := a.st.GetMMDSRelayAuth(ctx, sandboxID, name)
	if err != nil {
		return 0, "", nil, false, err
	}
	if !found {
		return 0, "", nil, false, nil
	}
	if v.Revision == 0 && !v.Present {
		if !a.timedWait(ctx, sandboxID, name) {
			return 0, "", nil, false, nil // never configured, timed out -> 404, upstream never contacted
		}
		if v, found, err = a.st.GetMMDSRelayAuth(ctx, sandboxID, name); err != nil {
			return 0, "", nil, false, err
		} else if !found {
			return 0, "", nil, false, nil
		}
	}
	if !v.Present {
		return 0, "", nil, false, nil // revoked -> 404, upstream never contacted
	}

	cfgJSON, _, cfgFound, err := a.st.GetMMDSEndpointPublicConfig(ctx, sandboxID, name)
	if err != nil {
		return 0, "", nil, false, err
	}
	if !cfgFound {
		return 0, "", nil, false, nil
	}
	var cfg relayPublicConfig
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil || cfg.URL == "" || cfg.AuthHeaderName == "" {
		return http.StatusBadGateway, "", nil, true, nil
	}

	// Cancel the fetch if this endpoint's auth is rotated/revoked while the
	// upstream call is outstanding.
	watchCtx, cancel := a.watch(ctx, sandboxID, name)
	defer cancel()

	res := a.relay.Fetch(watchCtx, waitKey(sandboxID, name), cfg.URL, cfg.AuthHeaderName, string(v.Value))
	return res.Status, res.ContentType, res.Body, true, nil
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

// timedWait wraps wait with mmds_value_wait_seconds observation — only a
// genuinely parked (never-configured) request reaches this, so the metric
// reflects real park latency rather than being diluted by already-answered
// reads. metrics.M has no histogram/float support, so the sum is tracked in
// milliseconds (mmds_value_wait_seconds_sum_ms), documented via the name
// itself rather than the doc's literal seconds unit.
func (a *Authority) timedWait(ctx context.Context, sandboxID, name string) bool {
	start := time.Now()
	woke := a.wait(ctx, sandboxID, name)
	a.mx.Add("mmds_value_wait_seconds_sum_ms", time.Since(start).Milliseconds())
	a.mx.Inc("mmds_value_wait_seconds_count")
	return woke
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
