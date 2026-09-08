package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestClusterCreateRejectsStandaloneIdentity(t *testing.T) {
	for _, carrier := range []string{"header", "metadata"} {
		t.Run(carrier, func(t *testing.T) {
			body := `{}`
			if carrier == "metadata" {
				body = `{"metadata":{"kuasar-sandbox.identity":"{}"}}`
			}
			r := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(body))
			if carrier == "header" {
				r.Header.Set(HeaderIdentity, `{}`)
			}
			if _, err := createSandboxMetadata(httptest.NewRecorder(), r); err == nil || !strings.Contains(err.Error(), "identity") {
				t.Fatalf("cluster Create identity = %v", err)
			}
		})
	}
}

func TestClusterBuildRejectsStandaloneIdentity(t *testing.T) {
	for _, carrier := range []string{"header", "metadata"} {
		t.Run(carrier, func(t *testing.T) {
			metadata := map[string]string{}
			header := http.Header{}
			if carrier == "header" {
				header.Set(HeaderIdentity, `{}`)
			} else {
				metadata[sandboxcfg.NsIdentity] = `{}`
			}
			if _, err := mergeBuildRegistrationHeaders(metadata, header); err == nil || !strings.Contains(err.Error(), "identity") {
				t.Fatalf("cluster Build identity = %v", err)
			}
		})
	}
}
