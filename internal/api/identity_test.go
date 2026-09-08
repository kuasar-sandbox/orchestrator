package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestIdentityHeaderWholeObjectPrecedence(t *testing.T) {
	body := map[string]string{sandboxcfg.NsIdentity: `{"id":"body","stable_id":"body-stable"}`, "keep": "value"}
	for _, raw := range []string{`{"id":"header"}`, `{}`, `{"stable_id":"header-stable"}`} {
		h := http.Header{}
		h.Set(identityHeader, raw)
		got, err := mergeCreateConfigHeaders(body, h)
		if err != nil || got[sandboxcfg.NsIdentity] != raw || got["keep"] != "value" {
			t.Fatalf("merge = %+v, %v", got, err)
		}
		if body[sandboxcfg.NsIdentity] != `{"id":"body","stable_id":"body-stable"}` {
			t.Fatal("Header merge mutated body")
		}
	}
	got, err := mergeCreateConfigHeaders(body, http.Header{})
	if err != nil || got[sandboxcfg.NsIdentity] != body[sandboxcfg.NsIdentity] {
		t.Fatalf("metadata-only identity = %+v, %v", got, err)
	}
}

func TestIdentityHeaderRejectsAmbiguity(t *testing.T) {
	for _, values := range [][]string{
		{""}, {"null"}, {`{"id":"a"}`, `{"id":"b"}`}, {`{"id":"a","id":"b"}`},
		{`{"id":"a","group":"injected"}`}, {`{"stable_id":null}`},
	} {
		h := http.Header{http.CanonicalHeaderKey(identityHeader): values}
		if _, err := mergeCreateConfigHeaders(nil, h); err == nil {
			t.Fatalf("accepted Header values %q", values)
		}
	}
	h := http.Header{}
	h.Set(identityHeader, `{"id":"valid"}`)
	if _, err := mergeCreateConfigHeaders(map[string]string{sandboxcfg.NsIdentity: `{"id":1}`}, h); err == nil {
		t.Fatal("valid Header hid malformed body identity")
	}
}

func TestBuildHeadersRejectIdentity(t *testing.T) {
	for _, carrier := range []string{"header", "metadata"} {
		t.Run(carrier, func(t *testing.T) {
			meta := map[string]string{}
			h := http.Header{}
			if carrier == "header" {
				h.Set(identityHeader, `{}`)
			} else {
				meta[sandboxcfg.NsIdentity] = `{}`
			}
			if _, err := mergeBuildConfigHeaders(meta, h); err == nil || !strings.Contains(err.Error(), sandboxcfg.NsIdentity) {
				t.Fatalf("Build identity error = %v", err)
			}
		})
	}
}

func TestCreateIdentityConflictResponse(t *testing.T) {
	recorder := httptest.NewRecorder()
	(&API{}).fail(recorder, ErrAlreadyExists)
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "sandbox already exists") {
		t.Fatalf("conflict response = %d %s", recorder.Code, recorder.Body.String())
	}
}
