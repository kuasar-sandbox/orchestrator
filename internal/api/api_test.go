package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdscfg"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxDetailIncludesRedactedMMDSEndpoints(t *testing.T) {
	a := &API{}
	detail := &types.Sandbox{
		ID: "sb-1", TemplateID: "e2b-img-" + strings.Repeat("1", 64),
		State: types.StateRunning, Metadata: map[string]string{
			mmdscfg.Ns: `{"endpoints":[{"name":"credentials","path":"/latest/credentials","backend_type":"relay","configured":true,"revision":2,"expired":false},{"name":"user-data","path":"/latest/user-data","backend_type":"store","configured":false,"revision":0,"expired":false}]}`,
		},
	}

	body, err := json.Marshal(a.sandboxDetail(detail))
	if err != nil {
		t.Fatal(err)
	}
	raw := string(body)
	if !strings.Contains(raw, `"kuasar-sandbox.mmds"`) ||
		!strings.Contains(raw, `"backend_type":"relay"`) ||
		!strings.Contains(raw, `"configured":true`) {
		t.Fatalf("sandbox detail omitted MMDS status: %s", raw)
	}
	for _, secret := range []string{"https://upstream.example", "X-Upstream-Auth", "secret-value"} {
		if strings.Contains(raw, secret) {
			t.Fatalf("sandbox detail leaked %q: %s", secret, raw)
		}
	}

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	metadata, ok := got["metadata"].(map[string]any)
	if !ok || !strings.Contains(metadata[mmdscfg.Ns].(string), `"endpoints":[`) {
		t.Fatalf("metadata = %#v, want redacted MMDS status", got["metadata"])
	}
}

func TestSandboxDetailOmitsMMDSEndpointMetadataWhenUnset(t *testing.T) {
	a := &API{}
	detail := &types.Sandbox{ID: "sb-1", Metadata: map[string]string{}}
	body, err := json.Marshal(a.sandboxDetail(detail))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), mmdscfg.Ns) {
		t.Fatalf("sandbox detail unexpectedly returned MMDS metadata: %s", body)
	}
}

func TestRequestedBuildProfile(t *testing.T) {
	tests := []struct {
		raw     string
		want    types.Profile
		wantErr bool
	}{
		{raw: "", want: types.ProfileE2B},
		{raw: "e2b", want: types.ProfileE2B},
		{raw: "bare", want: types.ProfileBare},
		{raw: "unknown", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := requestedBuildProfile(tt.raw)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("requestedBuildProfile(%q) = %q, %v; want %q, error=%t", tt.raw, got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestMergeConfigHeaders(t *testing.T) {
	// No headers => no allocation.
	if m := mergeConfigHeaders(nil, http.Header{}); m != nil {
		t.Fatalf("no headers should not allocate: %v", m)
	}

	// Headers populate the matching namespace keys.
	h := http.Header{}
	h.Set("X-Kuasar-Sandbox-Network", `{"hostname":"h1"}`)
	h.Set("X-Kuasar-Sandbox-Resource", `{"capacity":{"cpu":4}}`)
	h.Set("X-Kuasar-Sandbox-Restore", `{"prefetch":"memory"}`)
	m := mergeConfigHeaders(nil, h)
	if m[sandboxcfg.NsNetwork] != `{"hostname":"h1"}` || m[sandboxcfg.NsResource] != `{"capacity":{"cpu":4}}` {
		t.Fatalf("headers not normalized: %+v", m)
	}
	if _, ok := m[sandboxcfg.NsRestore]; ok {
		t.Fatalf("generic/template headers admitted request-scoped restore: %+v", m)
	}
	if got := mergeCreateConfigHeaders(nil, h); got[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("create restore header not normalized: %+v", got)
	}

	// Header wins over an e2b metadata key of the same namespace.
	meta := map[string]string{sandboxcfg.NsNetwork: `{"hostname":"from-metadata"}`}
	got := mergeConfigHeaders(meta, header("X-Kuasar-Sandbox-Network", `{"hostname":"from-header"}`))
	if got[sandboxcfg.NsNetwork] != `{"hostname":"from-header"}` {
		t.Fatalf("header should win over metadata: %+v", got)
	}
	meta = map[string]string{sandboxcfg.NsRestore: `{"prefetch":"memory"}`}
	got = mergeCreateConfigHeaders(meta, header("X-Kuasar-Sandbox-Restore", `{"prefetch":"off"}`))
	if got[sandboxcfg.NsRestore] != `{"prefetch":"off"}` {
		t.Fatalf("restore header should win over metadata: %+v", got)
	}
	emptyRestore := http.Header{}
	emptyRestore.Set("X-Kuasar-Sandbox-Restore", "")
	got = mergeCreateConfigHeaders(nil, emptyRestore)
	if _, ok := got[sandboxcfg.NsRestore]; !ok {
		t.Fatalf("present empty restore header must reach strict validation: %+v", got)
	}

	// Builder is build-only and is not folded by the generic sandbox header path.
	got = mergeConfigHeaders(nil, header("X-Kuasar-Sandbox-Builder", `{"referer":{"enabled":false}}`))
	if got != nil {
		t.Fatalf("builder header should not enter sandbox metadata: %+v", got)
	}
}

func TestMergeConfigHeadersMMDS(t *testing.T) {
	// Header populates the mmds namespace.
	got := mergeConfigHeaders(nil, header("X-Kuasar-Sandbox-MMDS", `{}`))
	if got[mmdscfg.Ns] != `{}` {
		t.Fatalf("mmds header not normalized: %+v", got)
	}

	// Header wins over an e2b metadata key of the same namespace.
	meta := map[string]string{mmdscfg.Ns: `{"endpoints":[]}`}
	got = mergeConfigHeaders(meta, header("X-Kuasar-Sandbox-MMDS", `{"endpoints":["from-header"]}`))
	if got[mmdscfg.Ns] != `{"endpoints":["from-header"]}` {
		t.Fatalf("mmds header should win over metadata: %+v", got)
	}

	// mergeBuildConfigHeaders also folds the header (orch's build path rejects
	// its presence explicitly — see orch/build.go), since it shares the same
	// configHeaderNs precedence table as create.
	got = mergeBuildConfigHeaders(nil, header("X-Kuasar-Sandbox-MMDS", `{}`))
	if got[mmdscfg.Ns] != `{}` {
		t.Fatalf("mmds header not normalized via mergeBuildConfigHeaders: %+v", got)
	}
}

func TestMergeBuildConfigHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("X-Kuasar-Sandbox-Builder", `{"referer":{"enabled":false}}`)
	h.Set("X-Kuasar-Sandbox-Network", `{"hostname":"build"}`)
	got := mergeBuildConfigHeaders(nil, h)
	if got[buildcfg.NsBuilder] != `{"referer":{"enabled":false}}` {
		t.Fatalf("builder header not normalized: %+v", got)
	}
	if got[sandboxcfg.NsNetwork] != `{"hostname":"build"}` {
		t.Fatalf("sandbox build header not normalized: %+v", got)
	}
}

func header(k, v string) http.Header {
	h := http.Header{}
	h.Set(k, v)
	return h
}
