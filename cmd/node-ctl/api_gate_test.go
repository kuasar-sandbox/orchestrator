package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIHandlerGateFailsClosedUntilOpened(t *testing.T) {
	gate := &apiHandlerGate{}
	request := httptest.NewRequest(http.MethodGet, "http://api.example.test/sandboxes", nil)

	closed := httptest.NewRecorder()
	gate.ServeHTTP(closed, request)
	if closed.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed gate status = %d, want %d", closed.Code, http.StatusServiceUnavailable)
	}

	gate.Open(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	opened := httptest.NewRecorder()
	gate.ServeHTTP(opened, request)
	if opened.Code != http.StatusNoContent {
		t.Fatalf("opened gate status = %d, want %d", opened.Code, http.StatusNoContent)
	}
}
