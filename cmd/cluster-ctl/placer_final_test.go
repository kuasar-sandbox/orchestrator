package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/placer"
)

func TestFinalPlacerMountsRegistryKeyLeaseEndpoint(t *testing.T) {
	handler := finalPlacerHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("unauthenticated request reached Placer service")
	}))
	request := httptest.NewRequest(http.MethodPost, placer.FinalKeyLeasePath, nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("key-lease status = %d, want %d", response.Code, http.StatusForbidden)
	}
}
