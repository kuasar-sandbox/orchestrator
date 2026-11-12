package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

type GuestHoldCore interface {
	GetGuestHold(context.Context, string, string, string) (int64, error)
	SetGuestHold(context.Context, string, string, string, int64, *int64) (int64, error)
}

type guestHoldRequest struct {
	SandboxID string `json:"sandbox_id"`
	RunID     string `json:"run_id"`
	Path      string `json:"path"`
	Value     *int64 `json:"value,omitempty"`
	IfMatch   *int64 `json:"if_match,omitempty"`
}

type guestHoldResult struct {
	Value *int64 `json:"value,omitempty"`
}

// No guest-controlled address or method is relayed. The peer must own a live
// MMDS registration; Conductor separately checks the run and declaration under
// the sandbox lifecycle lock. Never hold the registry lock across that call.
func (s *Server) handleGuestHold(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	peer, ok := peerFrom(r.Context())
	if !ok || !s.pluginAuthed(peer) {
		http.Error(w, "unauthorized proxy", 403)
		return
	}
	registry := s.deps.Plugins
	registry.mu.Lock()
	plugin := registry.m[id]
	var lease context.Context
	if plugin != nil && plugin.peerPID == peer && plugin.ready && isTrustedMMDSProxyRegistration(id, plugin.Caps) {
		lease = plugin.lease
	}
	registry.mu.Unlock()
	if lease == nil || lease.Err() != nil {
		http.Error(w, "proxy registration unavailable", 503)
		return
	}
	var q guestHoldRequest
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	if err != nil || strictjson.Decode(raw, &q) != nil || q.SandboxID == "" || q.RunID == "" || q.Path == "" || q.Value != nil && *q.Value < 0 || q.IfMatch != nil && (q.Value == nil || *q.IfMatch < 0) {
		http.Error(w, "invalid hold request", 400)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(lease, cancel)
	defer stop()
	if lease.Err() != nil {
		http.Error(w, "proxy registration unavailable", 503)
		return
	}
	var v int64
	if q.Value == nil {
		v, err = s.deps.GuestHold.GetGuestHold(ctx, q.SandboxID, q.RunID, q.Path)
	} else {
		v, err = s.deps.GuestHold.SetGuestHold(ctx, q.SandboxID, q.RunID, q.Path, *q.Value, q.IfMatch)
	}
	var result guestHoldResult
	code := http.StatusOK
	switch {
	case err == nil:
		result.Value = &v
	case errors.Is(err, api.ErrHoldNotMatched):
		code = 409
		result.Value = &v
	case errors.Is(err, api.ErrHoldStateConflict):
		code = 409
	case errors.Is(err, api.ErrNotFound):
		code = 404
	case errors.Is(err, api.ErrBadRequest):
		code = 400
	default:
		code = 503
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(result)
}

// GuestHoldClient uses a separate connection from route delivery so waiting
// for a mutation cannot stall its own downstream route update.
func GuestHoldClient(client *http.Client, pluginID string) func(context.Context, string, string, string, *int64, *int64) (*int64, int) {
	endpoint := "http://localhost/internal/plugin/" + url.PathEscape(pluginID) + "/hold"
	return func(ctx context.Context, id, runID, path string, value, ifMatch *int64) (*int64, int) {
		body, err := json.Marshal(guestHoldRequest{id, runID, path, value, ifMatch})
		if err != nil {
			return nil, 503
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, 503
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, 503
		}
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200, 409:
			var result guestHoldResult
			raw, err := io.ReadAll(io.LimitReader(resp.Body, 257))
			if err != nil || len(raw) > 256 || strictjson.Decode(raw, &result) != nil || result.Value != nil && *result.Value < 0 || resp.StatusCode == 200 && result.Value == nil {
				return nil, 503
			}
			return result.Value, resp.StatusCode
		case 400, 404:
			return nil, resp.StatusCode
		default:
			return nil, 503
		}
	}
}
