package configsock

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
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
)

// MmdsEndpointsAdmin is the store/relay-auth mutation surface the mmds admin
// routes expose to an authenticated local value provider. Implementations
// should return errors comparable via errors.Is to ErrMMDSEndpointNotFound /
// ErrMMDSEndpointWrongBackend so the handlers can map those to 404; anything
// else becomes 500.
type MmdsEndpointsAdmin interface {
	// SetMMDSStoreValue replaces the complete value for a store-backend
	// endpoint. contentType=="" resets to application/octet-stream;
	// expiresUnix==0 means no expiry.
	SetMMDSStoreValue(ctx context.Context, sid, name string, value []byte, contentType string, expiresUnix int64) error
	// ClearMMDSStoreValue deletes the value for a store-backend endpoint.
	ClearMMDSStoreValue(ctx context.Context, sid, name string) error
	// SetMMDSRelayAuth replaces the complete auth value for a relay-backend
	// endpoint.
	SetMMDSRelayAuth(ctx context.Context, sid, name string, value []byte) error
	// ClearMMDSRelayAuth revokes the auth value for a relay-backend endpoint.
	ClearMMDSRelayAuth(ctx context.Context, sid, name string) error
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
	expiresUnix, err := parseMMDSExpires(r.Header.Get(MMDSExpiresHeader))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := s.deps.MmdsEndpoints.SetMMDSStoreValue(r.Context(), sid, name, body, r.Header.Get("Content-Type"), expiresUnix); err != nil {
		s.mmdsAdminError(w, "put store", sid, name, err)
		return
	}
	s.log.Info("mmds admin mutation", "sid", sid, "name", name, "backend", "store", "op", "put")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMmdsStoreDelete(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	sid, name := r.PathValue("id"), r.PathValue("name")
	if err := s.deps.MmdsEndpoints.ClearMMDSStoreValue(r.Context(), sid, name); err != nil {
		s.mmdsAdminError(w, "delete store", sid, name, err)
		return
	}
	s.log.Info("mmds admin mutation", "sid", sid, "name", name, "backend", "store", "op", "delete")
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
	if err := s.deps.MmdsEndpoints.SetMMDSRelayAuth(r.Context(), sid, name, body); err != nil {
		s.mmdsAdminError(w, "put relay auth", sid, name, err)
		return
	}
	s.log.Info("mmds admin mutation", "sid", sid, "name", name, "backend", "relay", "op", "put_auth")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMmdsRelayAuthDelete(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	sid, name := r.PathValue("id"), r.PathValue("name")
	if err := s.deps.MmdsEndpoints.ClearMMDSRelayAuth(r.Context(), sid, name); err != nil {
		s.mmdsAdminError(w, "delete relay auth", sid, name, err)
		return
	}
	s.log.Info("mmds admin mutation", "sid", sid, "name", name, "backend", "relay", "op", "delete_auth")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) mmdsAdminError(w http.ResponseWriter, op, sid, name string, err error) {
	switch {
	case errors.Is(err, ErrMMDSEndpointNotFound), errors.Is(err, ErrMMDSEndpointWrongBackend):
		w.WriteHeader(http.StatusNotFound)
	default:
		s.log.Warn("configsock mmds admin", "op", op, "sid", sid, "name", name, "err", err)
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

func parseMMDSExpires(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}
	return strconv.ParseInt(v, 10, 64)
}
