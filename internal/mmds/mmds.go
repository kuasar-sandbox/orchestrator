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
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
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
	// MMDSRoute returns the tenant-specified kuasar-sandbox.mmds route at path for
	// sid. ok=false with err=nil means sid or path is genuinely unspecified — the
	// caller falls through to the existing fixed instance-info response. err!=nil
	// means resolution could not be completed (e.g. an external-mode RPC to the
	// proxy master timed out) — this is distinct from "unspecified" so the caller
	// can fail closed (503) instead of silently serving the wrong response for a
	// route that may well be specified.
	MMDSRoute(sandboxID, path string) (route MMDSRoute, ok bool, err error)
}

// MMDSRoute is the resolved backend for one specified guest-visible path.
type MMDSRoute struct {
	Type        string // "secret" | "service" | "static"
	ContentType string // static only
	Data        string // static only
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

func (s *Server) putToken(w http.ResponseWriter, r *http.Request) {
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
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(sid + "." + sign(sid, secret)))
}

func (s *Server) getMeta(w http.ResponseWriter, r *http.Request) {
	sid, ok := s.verifyToken(r.Header.Get("X-metadata-token"))
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
		default: // "secret" | "service" — backend lands in a later phase
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

// A session token is "<sid>.<hex(HMAC-SHA256(secret, sid))>" — opaque to envd,
// unforgeable by the guest (it lacks the secret). The sid is not secret (it's the
// sandbox id). The secret is the per-sandbox key from the Source, so any worker mints
// and verifies the same token.
func (s *Server) verifyToken(tok string) (string, bool) {
	sid, sig, found := strings.Cut(tok, ".")
	if !found || sid == "" {
		return "", false
	}
	secret, ok := s.src.MmdsSecret(sid)
	if !ok {
		return "", false
	}
	got, err := hex.DecodeString(sig)
	if err != nil {
		return "", false
	}
	if !hmac.Equal(got, signBytes(sid, secret)) {
		return "", false
	}
	return sid, true
}

func sign(sid string, secret []byte) string { return hex.EncodeToString(signBytes(sid, secret)) }

func signBytes(sid string, secret []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(sid))
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
