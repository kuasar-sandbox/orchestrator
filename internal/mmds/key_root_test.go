package mmds

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
)

type rootSwitchSource struct {
	fakeSource
	root string
}

func (s *rootSwitchSource) MmdsSecret(sid string) ([]byte, bool) {
	if _, exists := s.info[sid]; !exists {
		return nil, false
	}
	secret := keys.MmdsSecret(s.root, sid)
	return secret, len(secret) != 0
}

func TestTokenRejectsPreviousManifestRootAfterAPISecretSwitch(t *testing.T) {
	const sid = "node-local-sandbox"
	source := &rootSwitchSource{
		fakeSource: fakeSource{fip: map[string]string{"100.100.96.5": sid}, info: map[string][2]string{sid: {"template", "envd-token"}}},
		root:       strings.Repeat("1", 64), // Old deployment's ManifestKey root.
	}
	handler := New(source, time.Millisecond, nil).Handler()
	mint := func() string {
		t.Helper()
		request := httptest.NewRequest(http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
		request.RemoteAddr = "100.100.96.5:1234"
		request.Header.Set("X-metadata-token-ttl-seconds", "60")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("mint: %d", response.Code)
		}
		return response.Body.String()
	}
	check := func(token string, expected int) {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, "http://169.254.169.254/", nil)
		request.RemoteAddr = "100.100.96.5:1234"
		request.Header.Set("X-metadata-token", token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != expected {
			t.Fatalf("token response: %d, want %d", response.Code, expected)
		}
	}
	oldToken := mint()
	check(oldToken, http.StatusOK)
	// Keep sandbox ID and incarnation identical: only the published secret's
	// parent changes to the separately configured APISecret.
	source.root = strings.Repeat("2", 64)
	check(oldToken, http.StatusUnauthorized)
	check(mint(), http.StatusOK)
}
