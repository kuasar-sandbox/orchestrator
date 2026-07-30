// Package mmdssvc implements the guest-facing side of a type:"service" MMDS
// route: Proxy authenticates the sandbox, resolves the specified route to a
// node-operator-registered local trusted service, and delegates the request
// to it over a Unix domain socket -- never an arbitrary tenant-supplied URL
// (see internal/sandboxcfg.MMDSServiceSpec's doc comment for the confirmed
// design this replaces). Shared by both internal mode (internal/orch calls
// Call directly, in-process) and external mode (each proxy worker calls it
// independently, after internal/mmdsrpc resolves just the target name).
package mmdssvc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

// Fixed identity headers Call injects on the outgoing request. These are the
// only caller-controlled data a registered service ever receives -- the
// guest's own headers, query, body, Host, and tokens are never forwarded.
const (
	HeaderSandboxID      = "X-Kuasar-Sandbox-Id"      // node-local sandbox ID
	HeaderSandboxSubject = "X-Kuasar-Sandbox-Subject" // stable AuthSandboxID(), the credential subject
	HeaderService        = "X-Kuasar-MMDS-Service"    // the sandbox's own service alias, never the operator's target name
)

// Entry is one dial-ready, node-operator-registered local service.
type Entry struct {
	SocketPath       string
	Timeout          time.Duration
	MaxResponseBytes int
}

// Registry maps a config.MMDSServiceRegistryEntry key (the name a sandbox's
// own services[].target references) to its dial-ready Entry.
type Registry map[string]Entry

// BuildRegistry converts the node operator's config-level registry
// (config.MMDSConfig.Services or config.ProxyFileConfig.Services -- the same
// map type in both files) into a dial-ready Registry. Config.validate/
// ProxyFileConfig.validate already reject a malformed endpoint/timeout at
// load time and applyDefaults already fills Timeout/MaxResponseBytes, so
// this mostly just converts types; the endpoint-scheme check is
// defense-in-depth for a caller that builds the map directly.
func BuildRegistry(cfg map[string]config.MMDSServiceRegistryEntry) (Registry, error) {
	reg := make(Registry, len(cfg))
	for name, e := range cfg {
		if !strings.HasPrefix(e.Endpoint, "unix://") {
			return nil, fmt.Errorf("mmdssvc: service %q: endpoint %q: v1 supports unix:// endpoints only", name, e.Endpoint)
		}
		path := strings.TrimPrefix(e.Endpoint, "unix://")
		if !strings.HasPrefix(path, "/") {
			return nil, fmt.Errorf("mmdssvc: service %q: endpoint %q: unix:// path must be absolute", name, e.Endpoint)
		}
		maxBytes := e.MaxResponseBytes
		if maxBytes <= 0 {
			maxBytes = 64 * 1024
		}
		reg[name] = Entry{SocketPath: path, Timeout: e.TimeoutDur(), MaxResponseBytes: maxBytes}
	}
	return reg, nil
}

// Result is the filtered, guest-visible outcome of one Call.
type Result struct {
	StatusCode  int
	ContentType string
	Body        string
	RetryAfter  string // "" = omit
}

const (
	maxContentTypeBytes = 256
	maxRetryAfterBytes  = 32
)

// Call resolves target in reg and, if registered, dials it over its unix://
// socket, builds a brand-new GET path request carrying exactly the three
// fixed headers above and nothing else -- this signature has no
// *http.Request parameter, structurally preventing any guest-controlled data
// from reaching a registered service -- and returns a bounded, filtered
// Result. Every failure mode folds into Result.StatusCode, never a Go error,
// so callers (internal/orch.Orchestrator.MMDSRoute,
// internal/proxyshm.WorkerView.MMDSRoute) always get a well-formed
// guest-visible outcome. Never follows redirects (a service's own 3xx passes
// through verbatim, per the design's "no redirect-following" invariant).
func Call(ctx context.Context, reg Registry, target, path, sandboxID, authSubject, serviceAlias string) Result {
	entry, ok := reg[target]
	if target == "" || !ok {
		return Result{StatusCode: http.StatusServiceUnavailable}
	}

	var dialed atomic.Bool
	client := &http.Client{
		Timeout: entry.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			// A fresh Transport is built per Call and never reused, so a
			// kept-alive connection would never be returned to a pool anyone
			// drains -- its persistConn read loop (and the underlying socket)
			// would leak for as long as the registered service keeps it open.
			// Disabling keep-alive makes the transport close the connection
			// itself once the response body is fully read.
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				conn, err := d.DialContext(ctx, "unix", entry.SocketPath)
				if err == nil {
					dialed.Store(true)
				}
				return conn, err
			},
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://mmds-service"+path, nil)
	if err != nil {
		return Result{StatusCode: http.StatusBadGateway}
	}
	req.Header.Set(HeaderSandboxID, sandboxID)
	req.Header.Set(HeaderSandboxSubject, authSubject)
	req.Header.Set(HeaderService, serviceAlias)

	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return Result{StatusCode: http.StatusGatewayTimeout}
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return Result{StatusCode: http.StatusGatewayTimeout}
		}
		if !dialed.Load() {
			return Result{StatusCode: http.StatusServiceUnavailable}
		}
		return Result{StatusCode: http.StatusBadGateway} // dialed fine, but the call itself failed (reset, malformed framing)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(entry.MaxResponseBytes)+1))
	if err != nil || len(body) > entry.MaxResponseBytes {
		return Result{StatusCode: http.StatusBadGateway}
	}

	return Result{
		StatusCode:  resp.StatusCode,
		ContentType: sanitizeContentType(resp.Header.Get("Content-Type")),
		Body:        string(body),
		RetryAfter:  sanitizeRetryAfter(resp.Header.Get("Retry-After")),
	}
}

// sanitizeContentType drops (rather than fails the whole response for) an
// unparseable or oversized Content-Type.
func sanitizeContentType(v string) string {
	if v == "" || len(v) > maxContentTypeBytes {
		return ""
	}
	if _, _, err := mime.ParseMediaType(v); err != nil {
		return ""
	}
	return v
}

// sanitizeRetryAfter accepts only the two HTTP-valid Retry-After forms
// (delay-seconds or an HTTP-date), dropping anything else. delay-seconds is
// RFC 9110's 1*DIGIT -- a non-negative integer with no sign -- so this uses
// ParseUint rather than Atoi/ParseInt, which would wrongly accept "-1".
func sanitizeRetryAfter(v string) string {
	if v == "" || len(v) > maxRetryAfterBytes {
		return ""
	}
	if _, err := strconv.ParseUint(v, 10, 64); err == nil {
		return v
	}
	if _, err := http.ParseTime(v); err == nil {
		return v
	}
	return ""
}
