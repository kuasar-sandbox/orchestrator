package configsock

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestWrappedAPIFallbackIsSharedButInternalRoutesBypassIt(t *testing.T) {
	var calls atomic.Int32
	wrappedAPI := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("X-Extension-API", "true")
		_, _ = io.WriteString(w, "wrapped:"+r.URL.Path)
	})
	server := &Server{deps: Deps{API: wrappedAPI}}
	router := server.router()

	// The public listener serves this exact wrapped object directly.
	public := httptest.NewRecorder()
	wrappedAPI.ServeHTTP(public, httptest.NewRequest(http.MethodGet, "/private", nil))
	// The config socket uses the same object as its API-plane fallback.
	fallback := httptest.NewRecorder()
	router.ServeHTTP(fallback, httptest.NewRequest(http.MethodGet, "/private", nil))
	if public.Body.String() != fallback.Body.String() || fallback.Header().Get("X-Extension-API") != "true" || calls.Load() != 2 {
		t.Fatalf("public/fallback responses = %q/%q calls=%d", public.Body.String(), fallback.Body.String(), calls.Load())
	}

	internal := httptest.NewRecorder()
	router.ServeHTTP(internal, httptest.NewRequest(http.MethodPost, PathTaskSandboxBootstrap, nil))
	if internal.Code != http.StatusForbidden {
		t.Fatalf("internal route status = %d", internal.Code)
	}
	if internal.Header().Get("X-Extension-API") != "" || calls.Load() != 2 {
		t.Fatalf("internal route crossed API wrapper: header=%q calls=%d", internal.Header().Get("X-Extension-API"), calls.Load())
	}
}
