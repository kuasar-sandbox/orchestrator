package configsock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
)

// Admin routes for MMDS endpoints: a value provider PUTs/DELETEs a store
// value or a relay auth value. Unlike the
// plugin plane's long-lived streaming registration, these are one-shot
// request/response handlers, gated by the same adminAuthed check as the
// manifest-key admin plane. {id} and {name} are Go 1.22 ServeMux wildcards
// (single path segment, no "/") — mmdscfg's name grammar never produces a
// slash, so PathAdminMmdsStore and PathAdminMmdsAuth never collide.
const (
	PathAdminMmdsStore = "/internal/admin/sandboxes/{id}/mmds/{name}"
	PathAdminMmdsAuth  = "/internal/admin/sandboxes/{id}/mmds/{name}/auth"

	// MMDSExpiresHeader carries a store value's absolute Unix expiry (decimal
	// seconds) on PUT; omitted or "0" means no expiry.
	MMDSExpiresHeader = "X-Kuasar-MMDS-Expires-Unix"

	defaultMMDSValueLimit = 16384 // used only if Deps didn't set a limit

	// maxMMDSContentTypeBytes is a fixed implementation limit (not
	// operator-configurable) bounding the PUT store value's Content-Type
	// header length.
	maxMMDSContentTypeBytes = 255
)

// MmdsEndpointsAdmin is the store/relay-auth mutation surface the mmds admin
// routes expose to an authenticated local value provider. Implementations
// should return errors comparable via errors.Is to ErrMMDSEndpointNotFound /
// ErrMMDSEndpointWrongBackend so the handlers can map those to 404; anything
// else becomes 500. revision is the endpoint's new internal revision after a
// successful mutation (0 on failure) — surfaced so the admin audit log can
// record it alongside the authenticated value provider, sandbox, endpoint
// name, operation, and result.
type MmdsEndpointsAdmin interface {
	// SetMMDSStoreValue replaces the complete value for a store-backend
	// endpoint. contentType=="" resets to application/octet-stream;
	// expiresUnix==0 means no expiry.
	SetMMDSStoreValue(ctx context.Context, sid, name string, value []byte, contentType string, expiresUnix int64) (revision int64, err error)
	// ClearMMDSStoreValue deletes the value for a store-backend endpoint.
	ClearMMDSStoreValue(ctx context.Context, sid, name string) (revision int64, err error)
	// SetMMDSRelayAuth replaces the complete auth value for a relay-backend
	// endpoint.
	SetMMDSRelayAuth(ctx context.Context, sid, name string, value []byte) (revision int64, err error)
	// ClearMMDSRelayAuth revokes the auth value for a relay-backend endpoint.
	ClearMMDSRelayAuth(ctx context.Context, sid, name string) (revision int64, err error)
}

// ErrMMDSEndpointNotFound / ErrMMDSEndpointWrongBackend are the sentinel
// errors MmdsEndpointsAdmin implementations wrap so the mmds admin handlers
// can distinguish "unknown or wrong-backend endpoint" (404) from a genuine
// failure (500) without this package importing internal/store's own errors.
var (
	ErrMMDSEndpointNotFound     = errors.New("configsock: mmds endpoint not found")
	ErrMMDSEndpointWrongBackend = errors.New("configsock: mmds endpoint has a different backend type")
)

func (s *Server) handleMmdsStorePut(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	sid, name := r.PathValue("id"), r.PathValue("name")
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.mmdsStoreValueLimit()))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ct := r.Header.Get("Content-Type")
	if err := validateMMDSContentType(ct); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	expiresUnix, err := parseMMDSExpires(r.Header.Get(MMDSExpiresHeader))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	revision, err := s.deps.MmdsEndpoints.SetMMDSStoreValue(r.Context(), sid, name, body, ct, expiresUnix)
	s.auditMMDS(peer, sid, name, "store", "put", revision, err)
	if err != nil {
		s.mmdsAdminStatus(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMmdsStoreDelete(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	sid, name := r.PathValue("id"), r.PathValue("name")
	revision, err := s.deps.MmdsEndpoints.ClearMMDSStoreValue(r.Context(), sid, name)
	s.auditMMDS(peer, sid, name, "store", "delete", revision, err)
	if err != nil {
		s.mmdsAdminStatus(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMmdsRelayAuthPut(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	sid, name := r.PathValue("id"), r.PathValue("name")
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.mmdsRelayAuthLimit()))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := validateMMDSRelayAuthValue(body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	revision, err := s.deps.MmdsEndpoints.SetMMDSRelayAuth(r.Context(), sid, name, body)
	s.auditMMDS(peer, sid, name, "relay", "put_auth", revision, err)
	if err != nil {
		s.mmdsAdminStatus(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMmdsRelayAuthDelete(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	sid, name := r.PathValue("id"), r.PathValue("name")
	revision, err := s.deps.MmdsEndpoints.ClearMMDSRelayAuth(r.Context(), sid, name)
	s.auditMMDS(peer, sid, name, "relay", "delete_auth", revision, err)
	if err != nil {
		s.mmdsAdminStatus(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// auditMMDS logs exactly one audit record per mutation attempt, success or
// failure: the authenticated value provider, sandbox, endpoint name,
// operation, revision, and result. peer (the SO_PEERCRED pid) is the
// authenticated value provider identity; no other caller-identity mechanism
// exists on this local admin plane.
func (s *Server) auditMMDS(peer int, sid, name, backend, op string, revision int64, err error) {
	result := "ok"
	switch {
	case errors.Is(err, ErrMMDSEndpointNotFound):
		result = "not_found"
	case errors.Is(err, ErrMMDSEndpointWrongBackend):
		result = "wrong_backend"
	case err != nil:
		result = "error"
	}
	if result == "error" {
		s.log.Warn("mmds admin mutation", "peer_pid", peer, "sid", sid, "name", name, "backend", backend, "op", op, "revision", revision, "result", result, "err", err)
		return
	}
	s.log.Info("mmds admin mutation", "peer_pid", peer, "sid", sid, "name", name, "backend", backend, "op", op, "revision", revision, "result", result)
}

func (s *Server) mmdsAdminStatus(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrMMDSEndpointNotFound), errors.Is(err, ErrMMDSEndpointWrongBackend):
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *Server) mmdsStoreValueLimit() int64 {
	if s.deps.MMDSMaxStoreValueBytes > 0 {
		return s.deps.MMDSMaxStoreValueBytes
	}
	return defaultMMDSValueLimit
}

func (s *Server) mmdsRelayAuthLimit() int64 {
	if s.deps.MMDSMaxRelayAuthBytes > 0 {
		return s.deps.MMDSMaxRelayAuthBytes
	}
	return defaultMMDSValueLimit
}

// parseMMDSExpires parses the X-Kuasar-MMDS-Expires-Unix header: omitted
// means no expiry (0); present must be a non-negative decimal Unix
// timestamp — a negative value has no meaningful "expired if now >=
// expires_unix" interpretation and is rejected as malformed input rather
// than silently treated as no-expiry.
func parseMMDSExpires(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("configsock: %s must be >= 0, got %d", MMDSExpiresHeader, n)
	}
	return n, nil
}

// validateMMDSContentType enforces the admin-PUT Content-Type rule: a
// bounded length plus "must parse as a media type". Empty is allowed
// (defaults to application/octet-stream downstream).
func validateMMDSContentType(ct string) error {
	if ct == "" {
		return nil
	}
	if len(ct) > maxMMDSContentTypeBytes {
		return fmt.Errorf("configsock: content-type exceeds %d bytes", maxMMDSContentTypeBytes)
	}
	typ, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return fmt.Errorf("configsock: content-type is not a valid media type: %w", err)
	}
	// mime.ParseMediaType is lenient enough to accept a bare token with no
	// "/" (returning it verbatim as typ with no error) — reject that here
	// since a real media type is always "type/subtype" (RFC 6838).
	if !strings.Contains(typ, "/") {
		return fmt.Errorf("configsock: content-type %q is not type/subtype", ct)
	}
	return nil
}

// validateMMDSRelayAuthValue rejects NUL, CR, and LF in a relay auth value —
// the value is later injected as an outbound HTTP header value by
// internal/mmdsrelay, where any of these bytes would allow header/
// request-line injection into the relay request.
func validateMMDSRelayAuthValue(v []byte) error {
	if bytes.ContainsAny(v, "\x00\r\n") {
		return fmt.Errorf("configsock: relay auth value must not contain NUL, CR, or LF")
	}
	return nil
}
