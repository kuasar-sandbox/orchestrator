// Package mmds serves a minimal Firecracker MMDS v2 metadata service so envd — when
// launched in FC mode (without -isnotfc) — can fetch its access-token hash and accept
// being re-keyed to a fresh per-identity token at /init. This is what makes snapshot
// forks SDK-usable with envd-side enforcement (defense-in-depth) instead of relying on
// the proxy alone.
//
// Two-stage flow (Firecracker MMDS v2), so it composes with the proxy's "wait for the
// route to sync" semantics and has no ordering constraint between sandbox launch and
// the first poll:
//
//   - PUT /latest/api/token : resolve the request's source IP — the guest's
//     vswitch-SNAT'd floating IP — to a running sandbox id, PARKING (bounded) until it
//     registers; return an HMAC-signed session token that binds this session to that id.
//   - GET /                 : verify + decode the session token (the in-guest code is
//     untrusted, so we trust the token we minted, not a re-read of the source), then
//     return that sandbox's current {instanceID, envID, accessTokenHash}.
//
// Hosted by the proxy component (proxy_mode=internal: the serve daemon; external:
// proxy workers sharing the master's listener fd). envd hard-codes
// 169.254.169.254:80, so the host redirects that to the configured listen address
// (deployment config; keeps this process off a privileged port / root).
package mmds

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"
)

// Source is the orchestrator's view the MMDS reads. Both the orchestrator
// (proxy_mode=internal) and the synced route table (external) implement it; lookups are
// non-blocking and the server does the parking.
type Source interface {
	// ByFloatingIP returns the running sandbox id whose floating IP is ip (the guest's
	// SNAT'd source). ok=false if none is registered yet.
	ByFloatingIP(ip string) (sandboxID string, ok bool)
	// SandboxInfo returns the running sandbox's current template id + access token.
	SandboxInfo(sandboxID string) (templateID, accessToken string, ok bool)
	// MmdsSecret returns sid's per-sandbox session-token signing key — deterministic
	// from the manifest key + id, so a token minted by any proxy worker verifies in
	// any other through the same shared route view. ok=false for an unknown sandbox.
	MmdsSecret(sandboxID string) (secret []byte, ok bool)
	// CurrentRunID returns sid's current launch/resume incarnation id: a
	// session token binds this value at PUT time, and every GET re-fetches
	// it fresh to compare — pause/resume mints a new RunID, which
	// invalidates tokens minted before the resume. ok=false for an unknown
	// sandbox.
	CurrentRunID(sandboxID string) (runID string, ok bool)
}

// EndpointAuthority resolves and serves MMDS endpoints: the exact
// (sandbox_id, path) dispatch and the store backend's
// bounded value-wait. Two implementers split at the internal/external
// boundary exactly as Source already does: internal/mmdsauth.Authority
// (conductor, in-process store reads) and internal/mmdsrpc.Client (proxy
// worker, RPC to the proxy master). nil disables endpoint dispatch entirely
// (every request falls through to the built-in envd behavior below) — the
// internal-mode wiring in cmd/node-ctl only constructs one when
// mmds.endpoints.enabled is true.
type EndpointAuthority interface {
	// Lookup resolves the exact (sandbox_id, path) to its declared name +
	// backend type. found=false (err=nil) means no endpoint owns that
	// path — fall through to the built-in envd response. A non-nil err
	// means the authority itself failed (store read error, worker RPC
	// timeout/close/backpressure) and must never be treated the same as
	// found=false: the guest gets 503/504, classified via
	// authorityErrStatus, not a silent fallthrough.
	Lookup(ctx context.Context, sandboxID, path string) (name, backendType string, found bool, err error)
	// ServeStore answers a store-backend GET, applying the bounded
	// never-configured wait. present=false (err=nil) means 404. A non-nil
	// err means the authority itself failed — see Lookup.
	ServeStore(ctx context.Context, sandboxID, name string) (value []byte, contentType string, revision int64, present bool, err error)
	// ServeRelay answers a relay-backend GET, applying the same bounded
	// never-configured wait. ok=false (err=nil) means 404 (never
	// configured after the wait, or revoked — the upstream is never
	// contacted in either case). ok=true means status is the exact response
	// code to use (a passthrough upstream 2xx/4xx/5xx, or a relay-specific
	// 429/502/503/504). A non-nil err means the authority itself failed
	// (distinct from a relay upstream failure, which is already classified
	// into status) — see Lookup.
	ServeRelay(ctx context.Context, sandboxID, name string) (status int, contentType string, body []byte, ok bool, err error)
}

// Counter is the narrow metrics surface mmds needs, mirroring
// internal/proxy.Counter so both internal-mode (*metrics.M) and external
// worker-mode (the metrics pipe counter) satisfy it without this package
// depending on either concrete type.
type Counter interface {
	Inc(name string)
}

type noopCounter struct{}

func (noopCounter) Inc(string) {}

// Backend type tags on EndpointAuthority.Lookup's backendType return value.
// Mirrors internal/store's MMDSBackendStore/MMDSBackendRelay as independent
// local constants so this package does not need to import internal/store.
const (
	BackendStore = "store"
	BackendRelay = "relay"
)

// Server is the MMDS handler. Serve it on a listener the host redirects
// 169.254.169.254:80 to.
type Server struct {
	src  Source
	auth EndpointAuthority // nil = endpoint dispatch disabled
	park time.Duration     // max PUT wait for the route to sync (data-plane park budget)
	log  *slog.Logger
	mx   Counter
}

// New builds a Server. auth may be nil (MMDS endpoints disabled or
// not yet wired for this deployment mode); mx may be nil (metrics off).
func New(src Source, auth EndpointAuthority, park time.Duration, log *slog.Logger, mx Counter) *Server {
	if park <= 0 {
		park = 30 * time.Second
	}
	if mx == nil {
		mx = noopCounter{}
	}
	return &Server{src: src, auth: auth, park: park, log: log, mx: mx}
}

// opts is the metadata document envd's host.MMDSOpts unmarshals. address is left empty
// so envd's log exporter stays quiet.
type opts struct {
	InstanceID      string `json:"instanceID"`
	EnvID           string `json:"envID"`
	Address         string `json:"address"`
	AccessTokenHash string `json:"accessTokenHash"`
}

// Handler routes the two Firecracker MMDS v2 calls envd makes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /latest/api/token", s.putToken)
	mux.HandleFunc("GET /", s.getMeta)
	return guardRawRequest(mux)
}

// guardRawRequest rejects a malformed or non-canonical request before it
// ever reaches ServeMux: an outer raw-path guard rejects malformed/encoded
// paths before ServeMux canonicalization, and guest access is exact GET
// only — no body, query, method list, or automatic redirect. net/http's
// ServeMux, left to see these requests itself, would
// 301-redirect a non-canonical path (dot segments, doubled slashes, a
// trailing slash) to its cleaned form instead of rejecting it — that
// redirect is itself the "automatic redirect" the doc rules out, so it must
// never happen; rejecting here, on the raw wire-form request target, before
// routing, is what prevents it.
func guardRawRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := requestTargetPathAndQuery(r.RequestURI)
		rawPath := target
		if i := strings.IndexAny(target, "?#"); i >= 0 {
			rawPath = target[:i]
		}
		if rawPath != target || strings.Contains(rawPath, "%") || path.Clean(rawPath) != rawPath {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		if requestHasBody(r) {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requestTargetPathAndQuery returns the path[?query] portion of a raw
// request-target. Origin-form (the only form a direct-connecting guest ever
// sends: "/latest/api/token?...") passes through unchanged. Absolute-form
// ("http://host/latest/...", valid per RFC 7230 3.1.1 and what
// httptest.NewRequest's convenience API produces) has its scheme+authority
// stripped first so the guard validates the same path/query bytes either
// way, rather than tripping over the "//" in "http://".
func requestTargetPathAndQuery(target string) string {
	if strings.HasPrefix(target, "/") {
		return target
	}
	if i := strings.Index(target, "://"); i >= 0 {
		rest := target[i+len("://"):]
		if j := strings.IndexByte(rest, '/'); j >= 0 {
			return rest[j:]
		}
		return "/"
	}
	return target
}

// requestHasBody reports whether r carries any request body — a
// Content-Length-declared body is checked directly; a chunked/unknown-length
// body is checked by attempting to read one byte (non-blocking: the server
// has already fully read the request off the wire before invoking the
// handler chain, so this never waits on the network).
func requestHasBody(r *http.Request) bool {
	if r.ContentLength > 0 {
		return true
	}
	if r.Body == nil {
		return false
	}
	var buf [1]byte
	n, _ := r.Body.Read(buf[:])
	return n > 0
}

// Serve runs the MMDS HTTP/1.1 server on ln until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{Handler: s.Handler()}
	go func() { <-ctx.Done(); _ = srv.Close() }()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) putToken(w http.ResponseWriter, r *http.Request) {
	ttlSec, err := parseTTLHeader(r.Header)
	if err != nil {
		if s.log != nil {
			s.log.Debug("mmds: bad token ttl header", "err", err)
		}
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	ip := sourceIP(r.RemoteAddr)
	sid, ok := s.resolve(r.Context(), ip)
	if !ok {
		// Route never synced within the park budget — envd's poll PUTs again.
		if s.log != nil {
			s.log.Debug("mmds: no sandbox for source ip within park", "ip", ip)
		}
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	secret, ok := s.src.MmdsSecret(sid)
	if !ok {
		// A running sandbox without a derivable secret means a missing manifest key —
		// shouldn't happen; fail closed so envd retries rather than gets a bad token.
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	runID, ok := s.src.CurrentRunID(sid)
	if !ok {
		// Same fail-closed rationale as the missing-secret case above: a
		// running sandbox always has a current incarnation.
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	token := mintToken(sid, ip, runID, time.Duration(ttlSec)*time.Second, secret)
	setMMDSResponseHeaders(w)
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set(TTLHeader, strconv.FormatInt(ttlSec, 10))
	_, _ = w.Write([]byte(token))
}

func (s *Server) getMeta(w http.ResponseWriter, r *http.Request) {
	ip := sourceIP(r.RemoteAddr)
	sid, ok := s.verifyToken(r.Header.Get("X-metadata-token"), ip)
	if !ok {
		http.Error(w, "", http.StatusUnauthorized)
		return
	}
	setMMDSResponseHeaders(w)

	// MMDS endpoint dispatch: an exact (sandbox_id, path) match
	// wins over the built-in envd metadata response below.
	// No match (or auth==nil, i.e. the feature is disabled/not wired for
	// this deployment mode) falls straight through, preserving the built-in
	// route unconditionally. An authority failure (err!=nil) is NOT a "no
	// match" — it must return 503/504, never a silent fallthrough to the
	// built-in response.
	if s.auth != nil {
		name, backendType, found, err := s.auth.Lookup(r.Context(), sid, r.URL.Path)
		if err != nil {
			s.mx.Inc(`mmds_requests_total{backend_type="unknown",result="unavailable"}`)
			http.Error(w, "", authorityErrStatus(err))
			return
		}
		if found {
			s.serveEndpoint(w, r, sid, name, backendType)
			return
		}
	}

	tid, token, ok := s.src.SandboxInfo(sid)
	if !ok {
		http.Error(w, "{}", http.StatusNotFound)
		return
	}
	b, _ := json.Marshal(opts{InstanceID: sid, EnvID: tid, AccessTokenHash: HashToken(token)})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

// MMDSRevisionHeader carries a served store value's current revision.
// Relay does not invent a revision for upstream content, so it is never set
// on a relay-backed response.
const MMDSRevisionHeader = "X-Kuasar-MMDS-Revision"

// setMMDSResponseHeaders applies the hardening headers every guest MMDS
// response (built-in and endpoint alike) must carry.
func setMMDSResponseHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

// serveEndpoint dispatches a declared endpoint GET by backend type.
func (s *Server) serveEndpoint(w http.ResponseWriter, r *http.Request, sid, name, backendType string) {
	switch backendType {
	case BackendStore:
		value, contentType, revision, present, err := s.auth.ServeStore(r.Context(), sid, name)
		if err != nil {
			s.mx.Inc(`mmds_requests_total{backend_type="store",result="unavailable"}`)
			http.Error(w, "", authorityErrStatus(err))
			return
		}
		if !present {
			s.mx.Inc(`mmds_requests_total{backend_type="store",result="not_found"}`)
			http.Error(w, "", http.StatusNotFound)
			return
		}
		s.mx.Inc(`mmds_requests_total{backend_type="store",result="ok"}`)
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set(MMDSRevisionHeader, strconv.FormatInt(revision, 10))
		_, _ = w.Write(value)
	case BackendRelay:
		status, contentType, body, ok, err := s.auth.ServeRelay(r.Context(), sid, name)
		if err != nil {
			s.mx.Inc(`mmds_requests_total{backend_type="relay",result="unavailable"}`)
			http.Error(w, "", authorityErrStatus(err))
			return
		}
		if !ok {
			s.mx.Inc(`mmds_requests_total{backend_type="relay",result="not_found"}`)
			http.Error(w, "", http.StatusNotFound)
			return
		}
		s.mx.Inc(`mmds_requests_total{backend_type="relay",result="` + relayResultLabel(status) + `"}`)
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		_, _ = w.Write(body)
	default:
		http.Error(w, "", http.StatusNotFound)
	}
}

// authorityErrStatus classifies an EndpointAuthority failure (store read
// error, worker RPC timeout/close/backpressure) into the guest-visible
// status: a deadline exceeded (relay timeout, RPC deadline) becomes 504;
// every other authority failure (DB error, RPC connection closed, worker
// inflight cap) becomes 503.
func authorityErrStatus(err error) int {
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	return http.StatusServiceUnavailable
}

// relayResultLabel is the bounded mmds_requests_total{result} value for a
// relay dispatch — every distinct value is an enum literal, never derived
// from caller/upstream-controlled data, since only bounded enums may be
// used as metric labels.
func relayResultLabel(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "ok"
	case status == http.StatusTooManyRequests:
		return "rate_limited"
	case status == http.StatusServiceUnavailable:
		return "unavailable"
	case status == http.StatusGatewayTimeout:
		return "timeout"
	case status >= 400 && status < 500:
		return "upstream_4xx"
	case status >= 500:
		return "upstream_5xx_or_blocked"
	default:
		return "other"
	}
}

// resolve parks (bounded by s.park) until a running sandbox owns ip — reusing
// the data-plane "wait for the route to sync" semantics so external mode has
// no launch-before-poll ordering constraint. A legit guest's own floating IP
// resolves immediately; the park only spans the brief route-sync window.
func (s *Server) resolve(ctx context.Context, ip string) (string, bool) {
	deadline := time.Now().Add(s.park)
	for {
		if sid, ok := s.src.ByFloatingIP(ip); ok {
			return sid, true
		}
		if time.Now().After(deadline) {
			return "", false
		}
		select {
		case <-ctx.Done():
			return "", false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TTLHeader is the request/response header carrying the requested/accepted
// session token TTL in seconds (Firecracker-compatible range).
const TTLHeader = "X-metadata-token-ttl-seconds"

const (
	minTTLSeconds = 1
	maxTTLSeconds = 21600 // 6h
	audienceMMDS  = "mmds"
)

// parseTTLHeader requires exactly one TTLHeader value, a decimal integer in
// [minTTLSeconds, maxTTLSeconds]. Any other shape — missing, duplicate,
// malformed, zero, negative, or out-of-range — is an error: return 400
// without issuing a token.
func parseTTLHeader(h http.Header) (int64, error) {
	vals := h.Values(TTLHeader)
	if len(vals) != 1 {
		return 0, fmt.Errorf("mmds: %s header must be present exactly once, got %d", TTLHeader, len(vals))
	}
	n, err := strconv.ParseInt(strings.TrimSpace(vals[0]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("mmds: %s is not a decimal integer: %w", TTLHeader, err)
	}
	if n < minTTLSeconds || n > maxTTLSeconds {
		return 0, fmt.Errorf("mmds: %s %d out of range [%d,%d]", TTLHeader, n, minTTLSeconds, maxTTLSeconds)
	}
	return n, nil
}

// A session token is base64url(payload) + "." + hex(HMAC-SHA256(secret,
// payload)), where payload is sid + sourceIP + runID + audience + expiryUnix
// joined by NUL. This internal encoding is not part of the guest API
// contract — envd treats the whole string as opaque. The sid
// is readable without the secret (by design: the server must extract it to
// look the secret up), but the signature makes every other field
// tamper-evident; verifyToken re-resolves the CURRENT source IP and run ID
// and rejects a mismatch, rather than trusting the token's snapshot as
// ground truth — this is what makes pause/resume invalidate old tokens and
// makes a token unforgeable from a different source.
func mintToken(sid, sourceIP, runID string, ttl time.Duration, secret []byte) string {
	expiry := time.Now().Add(ttl).Unix()
	payload := strings.Join([]string{sid, sourceIP, runID, audienceMMDS, strconv.FormatInt(expiry, 10)}, "\x00")
	sig := hmacSHA256(secret, []byte(payload))
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + hex.EncodeToString(sig)
}

// verifyToken decodes+verifies tok, then checks it against CURRENT live
// state: source IP must match sourceIP (the requester's actual address,
// re-resolved by the caller — untrusted input is never re-read as ground
// truth for anything the signature covers, only compared against it), the
// token must not be expired, and its bound run ID must equal
// Source.CurrentRunID(sid) right now (a stale run ID means the sandbox
// paused/resumed since the token was minted).
func (s *Server) verifyToken(tok, sourceIP string) (string, bool) {
	encPayload, sigHex, found := strings.Cut(tok, ".")
	if !found || encPayload == "" {
		return "", false
	}
	payload, err := base64.RawURLEncoding.DecodeString(encPayload)
	if err != nil {
		return "", false
	}
	parts := strings.Split(string(payload), "\x00")
	if len(parts) != 5 {
		return "", false
	}
	sid, tokIP, tokRunID, audience, expiryStr := parts[0], parts[1], parts[2], parts[3], parts[4]
	if sid == "" || audience != audienceMMDS {
		return "", false
	}
	expiry, err := strconv.ParseInt(expiryStr, 10, 64)
	if err != nil {
		return "", false
	}
	secret, ok := s.src.MmdsSecret(sid)
	if !ok {
		return "", false
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil {
		return "", false
	}
	if !hmac.Equal(sig, hmacSHA256(secret, payload)) {
		return "", false
	}
	if time.Now().Unix() >= expiry {
		return "", false // expired
	}
	if tokIP != sourceIP {
		return "", false // cross-source replay
	}
	curRunID, ok := s.src.CurrentRunID(sid)
	if !ok || curRunID != tokRunID {
		return "", false // unknown sandbox, or a stale pre-pause/resume incarnation
	}
	return sid, true
}

func hmacSHA256(secret, msg []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write(msg)
	return mac.Sum(nil)
}

// HashToken renders exactly what envd's checkMMDSHash compares against —
// keys.HashAccessTokenBytes: hex(sha512(token)), no prefix, no secret.
func HashToken(token string) string {
	sum := sha512.Sum512([]byte(token))
	return hex.EncodeToString(sum[:])
}

func sourceIP(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}
