package api

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

var (
	ErrHoldNotMatched    = errors.New("hold value does not match")
	ErrHoldStateConflict = errors.New("sandbox state does not permit hold operation")
)

type SandboxHoldCore interface {
	GetSandboxHold(ctx context.Context, id, apiKey string) (int64, error)
	SetSandboxHold(ctx context.Context, id, apiKey string, value int64, ifMatch *int64) (int64, error)
}

func (a *API) hold(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	id, key := r.PathValue("id"), apiKeyFrom(r.Context())
	var value int64
	var err error
	if r.Method == http.MethodPut {
		// Decode exact keys: encoding/json's struct matching also accepts
		// "IFMATCH". A misspelt condition must never become a blind write.
		var fields map[string]int64
		raw, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
		if readErr != nil || strictjson.Decode(raw, &fields) != nil || r.URL.RawQuery != "" {
			writeErr(w, 400, "invalid hold update")
			return
		}
		next, ok := fields["value"]
		match, conditional := fields["ifMatch"]
		delete(fields, "value")
		delete(fields, "ifMatch")
		if !ok || next < 0 || match < 0 || len(fields) != 0 {
			writeErr(w, 400, "expected non-negative integer value and optional ifMatch")
			return
		}
		var expected *int64
		if conditional {
			expected = &match
		}
		value, err = a.core.SetSandboxHold(r.Context(), id, key, next, expected)
	} else {
		value, err = a.core.GetSandboxHold(r.Context(), id, key)
	}
	code := http.StatusOK
	switch {
	case errors.Is(err, ErrHoldNotMatched):
		code = http.StatusConflict
	case errors.Is(err, ErrHoldStateConflict):
		writeErr(w, 409, ErrHoldStateConflict.Error())
		return
	case err != nil:
		a.fail(w, err)
		return
	}
	writeJSON(w, code, struct {
		Value int64 `json:"value"`
	}{value})
}
