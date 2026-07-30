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
//     registers; return an HMAC-signed, TTL-bounded session token that binds this
//     session to that id, that exact source IP, and that sandbox's current run
//     incarnation (so pause/resume invalidates it).
//   - GET /                 : verify the session token (the in-guest code is
//     untrusted, so we trust the token we minted, not a re-read of the source) --
//     expiry, audience, source IP, and current incarnation are all re-checked on
//     every call, not just at mint time -- then return that sandbox's current
//     {instanceID, envID, accessTokenHash}.
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
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Source is the orchestrator's view the MMDS reads. Both the orchestrator
// (proxy_mode=internal) and the synced route table (external) implement it; lookups are
// non-blocking and the server does the parking, with one exception: the
// MMDSRoute method below, for a specified-but-never-configured secret.
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
	// Incarnation returns sid's current run incarnation (the launch/resume-scoped
	// run id, e.g. types.Sandbox.RunID) -- a minted token binds to this exact
	// value, so pause/resume (which assigns a fresh incarnation) invalidates
	// every token minted under the prior one, without needing an explicit
	// revocation list. ok=false for an unknown sandbox or one with no
	// incarnation yet (e.g. between Create and its first successful launch) --
	// a token cannot be minted or verified without one.
	Incarnation(sandboxID string) (incarnation string, ok bool)
	// MMDSRoute returns the tenant-specified kuasar-sandbox.mmds route at path for
	// sid. ok=false with err=nil means sid or path is genuinely unspecified — the
	// caller falls through to the existing fixed instance-info response. err!=nil
	// means resolution could not be completed (e.g. an external-mode RPC to the
	// proxy master timed out) — this is distinct from "unspecified" so the caller
	// can fail closed (503) instead of silently serving the wrong response for a
	// route that may well be specified. For a "secret" route whose value has
	// never been configured (revision==0), this call may block, bounded by node
	// policy, waiting for an admin PUT to land before returning -- ok is still
	// true (the route IS specified); the caller distinguishes "present" from
	// "specified but absent" via MMDSRoute.Present. For a "service" route,
	// this call delegates to a node-operator-registered local service (see
	// internal/mmdssvc) and always returns ok=true with err=nil once the
	// route itself is specified -- the outcome of that delegation (success or
	// any failure) is fully encoded in MMDSRoute.StatusCode, never in err.
	MMDSRoute(sandboxID, path string) (route MMDSRoute, ok bool, err error)
}

// MMDSRoute is the resolved backend for one specified guest-visible path.
type MMDSRoute struct {
	Type        string // "secret" | "service" | "static"
	ContentType string // static, secret (once Present), and service
	Data        string // static, secret (once Present), and service
	// Present is meaningful for "secret" routes only: true iff Data holds a
	// real configured value. A specified-but-absent secret is a valid 404, not
	// an error.
	Present bool
	// StatusCode and RetryAfter are meaningful for "service" routes only.
	// StatusCode is the exact status getMeta writes -- already fully
	// classified by the Source implementation (the registered service's own
	// 2xx/4xx/5xx passed through verbatim, or 502/503/504 on a Proxy-side
	// failure -- see internal/mmdssvc.Call). Zero is treated defensively as
	// 503 (should not happen for a "service"-typed route returned with
	// ok=true).
	StatusCode int
	RetryAfter string
}

// Server is the MMDS handler. Serve it on a listener the host redirects
// 169.254.169.254:80 to.
type Server struct {
	src  Source
	park time.Duration // max PUT wait for the route to sync (data-plane park budget)
	log  *slog.Logger
}

func New(src Source, park time.Duration, log *slog.Logger) *Server {
	if park <= 0 {
		park = 30 * time.Second
	}
	return &Server{src: src, park: park, log: log}
}

// opts is the metadata document envd's host.MMDSOpts unmarshals. address is left empty
// so envd's log exporter stays quiet.
type opts struct {
	InstanceID      string `json:"instanceID"`
	EnvID           string `json:"envID"`
	Address         string `json:"address"`
	AccessTokenHash string `json:"accessTokenHash"`
}

// Handler routes the two Firecracker MMDS v2 calls envd makes, plus specified
// MMDS route dispatch (getMeta). Every response -- success and error alike --
// carries Cache-Control: no-store and X-Content-Type-Options: nosniff, per
// design: applied once here rather than at each write/http.Error call site so
// an error path can't accidentally omit them. guardRawRequest runs BEFORE the
// mux: http.ServeMux cleans "/a//b" and "/a/../b" internally and issues a 301
// to the cleaned path before any handler runs (verified against the stdlib),
// which the design's exact-path contract forbids (no auto-redirect/rewrite);
// the guard rejects those, along with percent-escapes and a query string,
// with 400 before ServeMux ever sees the request.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /latest/api/token", s.putToken)
	mux.HandleFunc("GET /", s.getMeta)
	return secureHeaders(guardRawRequest(mux))
}

func secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

// guardRawRequest rejects a request outright -- 400, never a redirect or a
// rewrite -- if its raw (still-escaped) path is not already canonical: any
// percent-escape, a query or fragment, an empty/"."/".." segment, or a
// trailing slash on a non-root path. r.URL.EscapedPath() is used throughout
// (not r.URL.Path) specifically because Path is already percent-decoded --
// checking it would let "/a%2Fb" silently match a specified "/a/b" route.
func guardRawRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" || r.URL.Fragment != "" || r.URL.RawFragment != "" {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		raw := r.URL.EscapedPath()
		if strings.Contains(raw, "%") {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		if raw != "/" {
			if strings.HasSuffix(raw, "/") {
				http.Error(w, "", http.StatusBadRequest)
				return
			}
			for _, seg := range strings.Split(strings.TrimPrefix(raw, "/"), "/") {
				if seg == "" || seg == "." || seg == ".." {
					http.Error(w, "", http.StatusBadRequest)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
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

// mmdsTokenAudience scopes a minted token to this service specifically, so
// it can never be confused with (or replayed into) some other subsystem
// that happened to reuse the same per-sandbox HMAC secret.
const mmdsTokenAudience = "mmds"

// Token TTL bounds, in seconds -- the design's own "1 through 21600" range
// (21600s = 6h), enforced on the requested value before a token is minted.
const (
	minTokenTTLSeconds = 1
	maxTokenTTLSeconds = 21600
)

func (s *Server) putToken(w http.ResponseWriter, r *http.Request) {
	// Exactly one X-metadata-token-ttl-seconds header, a decimal integer in
	// [1, 21600] -- missing, duplicate (Values returns one entry per header
	// line), malformed, zero, negative, or out-of-range all fail before any
	// sandbox resolution/token work happens.
	ttlHeader := r.Header.Values("X-metadata-token-ttl-seconds")
	if len(ttlHeader) != 1 {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	ttlSeconds, err := strconv.ParseInt(ttlHeader[0], 10, 64)
	if err != nil || ttlSeconds < minTokenTTLSeconds || ttlSeconds > maxTokenTTLSeconds {
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
	incarnation, ok := s.src.Incarnation(sid)
	if !ok {
		// No current incarnation to bind to (e.g. between Create and the first
		// successful launch) -- fail closed the same way a missing secret does;
		// envd's poll PUTs again once one exists.
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	token, err := mintToken(tokenPayload{
		SID:         sid,
		SourceIP:    ip,
		Incarnation: incarnation,
		Audience:    mmdsTokenAudience,
		ExpiresUnix: time.Now().Unix() + ttlSeconds,
	}, secret)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	// Echo the accepted (parsed, canonical) value, not the raw header bytes.
	w.Header().Set("X-metadata-token-ttl-seconds", strconv.FormatInt(ttlSeconds, 10))
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(token))
}

func (s *Server) getMeta(w http.ResponseWriter, r *http.Request) {
	sid, ok := s.verifyToken(r.Header.Get("X-metadata-token"), sourceIP(r.RemoteAddr))
	if !ok {
		http.Error(w, "", http.StatusUnauthorized)
		return
	}
	// The built-in instance-info document is served only at the exact root;
	// every other path goes through specified-route dispatch, unspecified or
	// not -- an unspecified path is 404, not a silent alias for "/".
	if r.URL.Path != "/" {
		route, ok, err := s.src.MMDSRoute(sid, r.URL.Path)
		if err != nil {
			// Resolution failed (e.g. an external-mode RPC to the proxy master
			// timed out) rather than the path being genuinely unspecified — fail
			// closed instead of risking a silent fallthrough to the wrong response.
			if s.log != nil {
				s.log.Debug("mmds: route resolution failed", "sid", sid, "path", r.URL.Path, "err", err)
			}
			http.Error(w, "", http.StatusServiceUnavailable)
			return
		}
		if !ok {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		switch route.Type {
		case "static":
			w.Header().Set("Content-Type", route.ContentType)
			_, _ = w.Write([]byte(route.Data))
		case "secret":
			if route.Present {
				w.Header().Set("Content-Type", route.ContentType)
				_, _ = w.Write([]byte(route.Data))
			} else {
				http.Error(w, "", http.StatusNotFound)
			}
		case "service":
			code := route.StatusCode
			if code == 0 {
				code = http.StatusServiceUnavailable
			}
			if route.RetryAfter != "" {
				w.Header().Set("Retry-After", route.RetryAfter)
			}
			if route.ContentType != "" {
				w.Header().Set("Content-Type", route.ContentType)
			}
			w.WriteHeader(code)
			_, _ = w.Write([]byte(route.Data))
		default: // should not happen post-ExtractMMDS
			http.Error(w, "", http.StatusServiceUnavailable)
		}
		return
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

// resolve parks (bounded by s.park) until a running sandbox owns ip — reusing the
// data-plane "wait for the route to sync" semantics so external mode has no
// launch-before-poll ordering constraint. A legit guest's own floating IP resolves
// immediately; the park only spans the brief route-sync window.
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

// tokenPayload is the integrity-protected, HMAC-signed body of a session
// token minted by putToken. Every field is re-checked against live state on
// every protected GET (verifyToken), not just at mint time: Audience scopes
// the token to this service; ExpiresUnix bounds its lifetime to the TTL
// requested at mint; SourceIP must match the current request's source
// exactly; Incarnation must match the sandbox's *current* run incarnation --
// a stale incarnation (paused and resumed since this token was minted) fails
// closed rather than silently authenticating a session from a prior run.
// Reusable (not single-use): the same source/sandbox/incarnation may present
// this token repeatedly until it expires.
type tokenPayload struct {
	SID         string `json:"sid"`
	SourceIP    string `json:"ip"`
	Incarnation string `json:"run"`
	Audience    string `json:"aud"`
	ExpiresUnix int64  `json:"exp"`
}

// mintToken renders p as "<base64url(JSON payload)>.<hex(HMAC-SHA256(secret,
// payload bytes))>" -- opaque to envd, unforgeable by the guest (it lacks
// secret). The internal encoding is not part of the guest API contract (see
// package doc); only mintToken/verifyToken need to agree on it.
func mintToken(p tokenPayload, secret []byte) (string, error) {
	payload, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload) + "." + hex.EncodeToString(signBytes(payload, secret)), nil
}

// verifyToken decodes and fully re-validates tok against live state:
// integrity (HMAC), audience, expiry, the current request's source IP, and
// the sandbox's current run incarnation. currentSourceIP is the caller's own
// fresh sourceIP(r.RemoteAddr) call, never cached from token mint time.
func (s *Server) verifyToken(tok, currentSourceIP string) (string, bool) {
	payloadB64, sig, found := strings.Cut(tok, ".")
	if !found || payloadB64 == "" {
		return "", false
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return "", false
	}
	var p tokenPayload
	if err := json.Unmarshal(payload, &p); err != nil || p.SID == "" {
		return "", false
	}
	secret, ok := s.src.MmdsSecret(p.SID)
	if !ok {
		return "", false
	}
	got, err := hex.DecodeString(sig)
	if err != nil {
		return "", false
	}
	if !hmac.Equal(got, signBytes(payload, secret)) {
		return "", false
	}
	if p.Audience != mmdsTokenAudience {
		return "", false
	}
	if time.Now().Unix() >= p.ExpiresUnix {
		return "", false
	}
	if p.SourceIP != currentSourceIP {
		return "", false
	}
	incarnation, ok := s.src.Incarnation(p.SID)
	if !ok || incarnation != p.Incarnation {
		return "", false
	}
	return p.SID, true
}

func signBytes(payload, secret []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
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
