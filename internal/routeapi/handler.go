package routeapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const (
	ReadRoutePath = "/internal/route/read"
	ReadBuildPath = "/internal/build/read"
	maxBodyBytes  = 128 << 10
)

type Service interface {
	ReadRoute(context.Context, ReadRouteRequest) (ReadRouteResponse, error)
	ReadBuild(context.Context, ReadBuildRequest) (ReadBuildResponse, error)
}

// TrustVerifier authenticates the Router-to-Registry transport identity. It is
// deliberately unrelated to caller/GROUP credentials.
type TrustVerifier func(*http.Request) error

func NewHandler(service Service, trust TrustVerifier) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+ReadRoutePath, func(w http.ResponseWriter, r *http.Request) {
		if !authorizeInternal(w, r, trust) {
			return
		}
		var request ReadRouteRequest
		if err := decodeRequest(r, &request); err != nil || request.Validate() != nil {
			http.Error(w, "invalid Route request", http.StatusBadRequest)
			return
		}
		response, err := service.ReadRoute(r.Context(), request)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, ReadRouteResponse{Outcome: ReadUnavailable, Reason: err.Error()})
			return
		}
		if err := response.ValidateFor(request); err != nil {
			http.Error(w, "invalid Route service response", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, response)
	})
	mux.HandleFunc("POST "+ReadBuildPath, func(w http.ResponseWriter, r *http.Request) {
		if !authorizeInternal(w, r, trust) {
			return
		}
		var request ReadBuildRequest
		if err := decodeRequest(r, &request); err != nil || request.Validate() != nil {
			http.Error(w, "invalid Build request", http.StatusBadRequest)
			return
		}
		response, err := service.ReadBuild(r.Context(), request)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, ReadBuildResponse{Outcome: ReadUnavailable, Reason: err.Error()})
			return
		}
		if err := response.ValidateFor(request); err != nil {
			http.Error(w, "invalid Build service response", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, response)
	})
	return mux
}

func authorizeInternal(w http.ResponseWriter, r *http.Request, trust TrustVerifier) bool {
	if trust == nil || trust(r) != nil {
		http.Error(w, "untrusted internal request", http.StatusForbidden)
		return false
	}
	return true
}

func decodeRequest(r *http.Request, out any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxBodyBytes {
		return errors.New("routeapi: request body exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("routeapi: request must contain one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
