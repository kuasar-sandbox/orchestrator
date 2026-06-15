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
// Hosted by the proxy component (proxy_mode=internal: the serve daemon; external: the
// proxy worker). envd hard-codes 169.254.169.254:80, so the host redirects that to the
// configured listen address (deployment config; keeps this process off a privileged
// port / root).
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
	// any other. ok=false for an unknown sandbox.
	MmdsSecret(sandboxID string) (secret []byte, ok bool)
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

// Handler routes the two Firecracker MMDS v2 calls envd makes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /latest/api/token", s.putToken)
	mux.HandleFunc("GET /", s.getMeta)
	return mux
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
