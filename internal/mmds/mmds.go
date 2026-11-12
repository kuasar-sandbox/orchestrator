// Package mmds serves the guest-visible Firecracker MMDS v2 endpoint.
//
// The built-in root document is retained for envd re-keying. Operator-defined
// static, secret, and local-service routes are exact-path additions; no request
// path is cleaned, redirected, or treated as a subtree.
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
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

const (
	mmdsTokenAudience = "mmds"

	minTokenTTLSeconds = 1
	maxTokenTTLSeconds = 21600

	readHeaderTimeout = 5 * time.Second
	idleTimeout       = 30 * time.Second
	maxHeaderBytes    = 16 * 1024

	// peerCloseGrace bounds how long writeResponse waits, after a successful
	// flush, for the peer's own close on a Connection: close exchange.
	peerCloseGrace = 250 * time.Millisecond
)

// Source is the trusted route view used by an independent proxy worker. It must
// fail closed when its backing sync/store is unavailable.
type Source interface {
	MMDSAvailable() bool
	ByFloatingIP(ip string) (sandboxID string, ok bool)
	SandboxInfo(sandboxID string) (templateID, accessToken string, ok bool)
	MmdsSecret(sandboxID string) (secret []byte, ok bool)
	Incarnation(sandboxID string) (incarnation string, ok bool)
	MMDSRoute(ctx context.Context, sandboxID, exactPath string) (route MMDSRoute, ok bool, err error)
	MMDSHold(ctx context.Context, sandboxID, runID, exactPath string, value, ifMatch *int64) (actual *int64, status int)
}

// MMDSRoute describes a matched target. A service lookup cannot perform I/O
// on the provider; ReadService is invoked only after accepting a guest GET.
type MMDSRoute struct {
	Type        string
	ContentType string
	Body        []byte
	Present     bool
	ReadService func(context.Context) mmdssvc.Result
}

type Server struct {
	src  Source
	park time.Duration
	log  *slog.Logger
}

func New(src Source, park time.Duration, log *slog.Logger) *Server {
	if park <= 0 {
		park = 30 * time.Second
	}
	return &Server{src: src, park: park, log: log}
}

type opts struct {
	InstanceID      string `json:"instanceID"`
	EnvID           string `json:"envID"`
	Address         string `json:"address"`
	AccessTokenHash string `json:"accessTokenHash"`
}

// Handler deliberately avoids http.ServeMux so the standard library cannot
// clean a request path and emit a redirect before the exact-path checks run.
func (s *Server) Handler() http.Handler {
	dispatch := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.src.MMDSAvailable() {
			http.Error(w, "", http.StatusServiceUnavailable)
			return
		}
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
			s.putToken(w, r)
		case r.Method == http.MethodGet || holdPutCandidate(r):
			s.metadata(w, r)
		default:
			http.Error(w, "", http.StatusNotFound)
		}
	})
	return secureHeaders(guardRequest(dispatch))
}

// writeResponse publishes one complete, non-streaming MMDS body. The caller
// pre-sets only business headers (e.g. the token TTL); the helper owns the
// response type, a legal Content-Length computed before the first write, the
// write itself, and the conditional peer-close wait. It must be the
// response's only writer: no WriteHeader/Write before, no body appended
// after. Write, short-write, and flush errors are returned for the caller's
// logger; the close-wait itself never fails -- ending by grace expiry is a
// normal return, not a response error.
func writeResponse(w http.ResponseWriter, r *http.Request, status int, contentType string, body []byte) error {
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	bodyAllowed := status >= 200 && status != http.StatusNoContent && status != http.StatusNotModified
	if bodyAllowed {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	} else {
		w.Header().Del("Content-Length")
	}
	w.WriteHeader(status)
	if bodyAllowed && len(body) > 0 {
		n, err := w.Write(body)
		if err != nil {
			return err
		}
		if n != len(body) {
			return io.ErrShortWrite
		}
	}
	if !bodyAllowed {
		return nil
	}
	return waitForPeerClose(w, r)
}

// waitForPeerClose parks briefly after a flushed response so the peer -- not
// this server -- closes first on Connection: close exchanges. TIME_WAIT then
// lands on the guest side, where it dies with the sandbox and is reset by the
// next restore, instead of accumulating on this server, where snapshot
// restores replay a frozen TCP 4-tuple whose fresh SYN then draws a stale
// ACK, an RST, and a 21-40ms retransmission on the launch critical path
// (partial mitigation of the management-plane problem described in #308).
//
// The wait fits only plain HTTP/1.1 requests that already asked to close and
// carry no request body or transfer encoding: such a response is fully
// length-delimited, so a prompt peer can finish reading and close on its own
// while net/http's background read cancels the request context within
// microseconds. Every other shape -- keep-alive reuse, HTTP/1.0, bodied or
// chunked requests, bodiless statuses, a writer without Flusher support --
// skips the wait and keeps net/http's normal close semantics; server-side
// TIME_WAIT after the grace expires is accepted, no RST is ever forced.
// Request-context cancellation is only the wake signal, not proof of the
// peer's FIN; the timer bounds the wait when no signal arrives.
func waitForPeerClose(w http.ResponseWriter, r *http.Request) error {
	if r.ProtoMajor != 1 || r.ProtoMinor != 1 || !r.Close ||
		len(r.TransferEncoding) != 0 || r.ContentLength != 0 {
		return nil
	}
	if err := http.NewResponseController(w).Flush(); err != nil {
		if errors.Is(err, http.ErrNotSupported) {
			// No Flusher: the complete response is already handed to
			// net/http, which will finish and close it; waiting cannot help.
			return nil
		}
		return err
	}
	timer := time.NewTimer(peerCloseGrace)
	defer timer.Stop()
	select {
	case <-r.Context().Done():
	case <-timer.C:
	}
	return nil
}

func secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

// Only declaration-backed PUTs may carry operation parameters. Built-ins,
// normal reads and unsupported methods keep their existing query behavior.
func holdPutCandidate(r *http.Request) bool {
	return (r.Method == http.MethodPut || r.Method == "put") && r.URL.Path != "/" && r.URL.Path != "/latest/api/token"
}

func guardRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL == nil || ((r.URL.ForceQuery || r.URL.RawQuery != "") && !holdPutCandidate(r)) || r.URL.Fragment != "" || r.URL.RawFragment != "" {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		raw := r.URL.Path
		rawTargetPath, _, _ := strings.Cut(r.RequestURI, "?")
		if raw == "" || raw[0] != '/' || r.URL.RawPath != "" ||
			strings.Contains(rawTargetPath, "%") || strings.ContainsAny(raw, "?#\\*") {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		if raw != "/" {
			if strings.HasSuffix(raw, "/") {
				http.Error(w, "", http.StatusBadRequest)
				return
			}
			for _, segment := range strings.Split(strings.TrimPrefix(raw, "/"), "/") {
				if segment == "" || segment == "." || segment == ".." {
					http.Error(w, "", http.StatusBadRequest)
					return
				}
			}
		}
		if r.Method == http.MethodGet && (r.ContentLength != 0 || len(r.TransferEncoding) != 0 || (r.Body != nil && r.Body != http.NoBody)) {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// HTTPServer constructs the existing HTTP/1 endpoint without starting I/O. The
// Proxy worker owns its ordered data/MMDS shutdown through a drain controller.
func (s *Server) HTTPServer(ctx context.Context) *http.Server {
	return &http.Server{
		Handler:           s.Handler(),
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
}

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := s.HTTPServer(ctx)
	go func() {
		<-ctx.Done()
		// Close cancels every in-flight request context, so handlers parked
		// in waitForPeerClose stop waiting immediately instead of each
		// paying the grace period.
		_ = srv.Close()
	}()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) putToken(w http.ResponseWriter, r *http.Request) {
	ttlHeaders := r.Header.Values("X-metadata-token-ttl-seconds")
	if len(ttlHeaders) != 1 {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	ttl, err := strconv.ParseInt(ttlHeaders[0], 10, 64)
	if err != nil || ttl < minTokenTTLSeconds || ttl > maxTokenTTLSeconds {
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	ip := sourceIP(r.RemoteAddr)
	sid, ok := s.resolve(r.Context(), ip)
	if !ok {
		if s.log != nil {
			s.log.Debug("mmds: no sandbox for source ip within park", "ip", ip)
		}
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	secret, ok := s.src.MmdsSecret(sid)
	if !ok {
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	incarnation, ok := s.src.Incarnation(sid)
	if !ok {
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	token, err := mintToken(tokenPayload{
		SID:         sid,
		SourceIP:    ip,
		Incarnation: incarnation,
		Audience:    mmdsTokenAudience,
		ExpiresUnix: time.Now().Unix() + ttl,
	}, secret)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-metadata-token-ttl-seconds", strconv.FormatInt(ttl, 10))
	if err := writeResponse(w, r, http.StatusOK, "text/plain", []byte(token)); err != nil && s.log != nil {
		s.log.Debug("mmds: token response write failed", "err", err)
	}
}

func (s *Server) metadata(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.verifyToken(r.Header.Get("X-metadata-token"), sourceIP(r.RemoteAddr))
	if !ok {
		http.Error(w, "", http.StatusUnauthorized)
		return
	}
	sid := principal.SID
	if r.URL.Path != "/" {
		route, found, err := s.src.MMDSRoute(r.Context(), sid, r.URL.Path)
		if err != nil {
			if s.log != nil {
				s.log.Debug("mmds: route resolution failed", "sid", sid, "path", r.URL.Path, "err", err)
			}
			http.Error(w, "", http.StatusServiceUnavailable)
			return
		}
		if !found {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		if route.Type == sandboxcfg.MMDSRouteHoldTraffic {
			s.holdTraffic(w, r, principal)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		s.writeRoute(w, r, route)
		return
	}
	templateID, accessToken, ok := s.src.SandboxInfo(sid)
	if !ok {
		http.Error(w, "{}", http.StatusNotFound)
		return
	}
	body, _ := json.Marshal(opts{
		InstanceID:      sid,
		EnvID:           templateID,
		AccessTokenHash: HashToken(accessToken),
	})
	if err := writeResponse(w, r, http.StatusOK, "application/json", body); err != nil && s.log != nil {
		s.log.Debug("mmds: metadata response write failed", "err", err)
	}
}

// Decimal text, with no form or JSON decoding. Bound the actual stream (not
// the declared length) so chunked PUTs and curl -d work exactly the same way.
const maxHoldTextBytes = 19 // decimal MaxInt64

func holdDecimal(text string) (*int64, bool) {
	if len(text) == 0 || len(text) > maxHoldTextBytes {
		return nil, false
	}
	for _, c := range []byte(text) {
		if c < '0' || c > '9' {
			return nil, false
		}
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return nil, false
	}
	return &n, true
}

func (s *Server) holdTraffic(w http.ResponseWriter, r *http.Request, principal tokenPayload) {
	var value, ifMatch *int64
	if r.Method != http.MethodGet {
		if r.URL.RawQuery != "" || r.URL.ForceQuery {
			// Exactly one pair, including after percent decoding. ParseQuery
			// errors (including partially parsed input) must never omit CAS.
			params, err := url.ParseQuery(r.URL.RawQuery)
			v := params["if-match"]
			if err != nil || len(params) != 1 || len(v) != 1 || strings.Contains(r.URL.RawQuery, "&") {
				http.Error(w, "", http.StatusBadRequest)
				return
			}
			var valid bool
			ifMatch, valid = holdDecimal(v[0])
			if !valid {
				http.Error(w, "", http.StatusBadRequest)
				return
			}
		}
		if r.Body == nil {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		// An unbounded slow sender must not keep the worker waiting for a digit.
		ctl := http.NewResponseController(w)
		if err := ctl.SetReadDeadline(time.Now().Add(readHeaderTimeout)); err == nil {
			defer ctl.SetReadDeadline(time.Time{})
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxHoldTextBytes+1))
		var valid bool
		value, valid = holdDecimal(string(body))
		if err != nil || !valid {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
	}
	actual, status := s.src.MMDSHold(r.Context(), principal.SID, principal.Incarnation, r.URL.Path, value, ifMatch)
	if actual != nil && *actual >= 0 && (status == http.StatusOK || status == http.StatusConflict) {
		s.writeBody(w, r, status, "text/plain", strconv.AppendInt(nil, *actual, 10))
		return
	}
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable:
	default:
		status = http.StatusServiceUnavailable
	}
	http.Error(w, "", status)
}

func (s *Server) writeRoute(w http.ResponseWriter, r *http.Request, route MMDSRoute) {
	contentType := route.ContentType
	if contentType == "" {
		contentType = "text/plain"
	}
	switch route.Type {
	case "", "static":
		s.writeBody(w, r, http.StatusOK, contentType, route.Body)
	case "secret":
		if !route.Present {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		s.writeBody(w, r, http.StatusOK, contentType, route.Body)
	case "service":
		if route.ReadService == nil {
			http.Error(w, "", http.StatusServiceUnavailable)
			return
		}
		result := route.ReadService(r.Context())
		status := result.StatusCode
		if status < 100 || status > 599 {
			status = http.StatusServiceUnavailable
		}
		s.writeBody(w, r, status, result.ContentType, result.Body)
	default:
		http.Error(w, "", http.StatusServiceUnavailable)
	}
}

// writeBody publishes an assembled route body; a write failure only reaches
// the logger, it can no longer alter the already-committed response.
func (s *Server) writeBody(w http.ResponseWriter, r *http.Request, status int, contentType string, body []byte) {
	if err := writeResponse(w, r, status, contentType, body); err != nil && s.log != nil {
		s.log.Debug("mmds: route response write failed", "err", err)
	}
}

// resolve retains the existing bounded route-sync park used only while minting
// the MMDSv2 token. Secret routes themselves never wait for a value.
func (s *Server) resolve(ctx context.Context, ip string) (string, bool) {
	deadline := time.Now().Add(s.park)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if sid, ok := s.src.ByFloatingIP(ip); ok {
			return sid, true
		}
		if !time.Now().Before(deadline) {
			return "", false
		}
		select {
		case <-ctx.Done():
			return "", false
		case <-ticker.C:
		}
	}
}

type tokenPayload struct {
	SID         string `json:"sid"`
	SourceIP    string `json:"ip"`
	Incarnation string `json:"run"`
	Audience    string `json:"aud"`
	ExpiresUnix int64  `json:"exp"`
}

func mintToken(payload tokenPayload, secret []byte) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(body) + "." + hex.EncodeToString(signBytes(body, secret)), nil
}

func (s *Server) verifyToken(token, currentSourceIP string) (tokenPayload, bool) {
	payloadEncoded, signature, found := strings.Cut(token, ".")
	if !found || payloadEncoded == "" || signature == "" {
		return tokenPayload{}, false
	}
	body, err := base64.RawURLEncoding.DecodeString(payloadEncoded)
	if err != nil {
		return tokenPayload{}, false
	}
	var payload tokenPayload
	if err := json.Unmarshal(body, &payload); err != nil || payload.SID == "" {
		return tokenPayload{}, false
	}
	secret, ok := s.src.MmdsSecret(payload.SID)
	if !ok {
		return tokenPayload{}, false
	}
	got, err := hex.DecodeString(signature)
	if err != nil || !hmac.Equal(got, signBytes(body, secret)) {
		return tokenPayload{}, false
	}
	if payload.Audience != mmdsTokenAudience || payload.ExpiresUnix <= time.Now().Unix() || payload.SourceIP != currentSourceIP {
		return tokenPayload{}, false
	}
	incarnation, ok := s.src.Incarnation(payload.SID)
	if !ok || incarnation != payload.Incarnation {
		return tokenPayload{}, false
	}
	return payload, true
}

func signBytes(payload, secret []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

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
