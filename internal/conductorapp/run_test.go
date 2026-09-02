package conductorapp

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
)

type untouchedAPICore struct{ api.Core }

func TestConductorPublicHandlerDoesNotDispatchSandboxData(t *testing.T) {
	cfg := &publicconfig.Conductor{}
	cfg.API.Domain = "test.local"
	handler := newAPIHandler(cfg, untouchedAPICore{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, request := range []*http.Request{
		func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "http://49983-sb-1.test.local/sandbox-data", nil)
			r.Host = "49983-sb-1.test.local"
			return r
		}(),
		func() *http.Request {
			r := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", nil)
			r.Host = "sandbox:443"
			r.Header.Set("E2b-Sandbox-Id", "sb-1")
			return r
		}(),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s Host=%q status=%d, want API handler's natural 404", request.Method, request.Host, response.Code)
		}
	}
}
