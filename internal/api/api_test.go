package api

import (
	"net/http"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

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
	if m[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("restore header not normalized: %+v", m)
	}

	// Header wins over an e2b metadata key of the same namespace.
	meta := map[string]string{sandboxcfg.NsNetwork: `{"hostname":"from-metadata"}`}
	got := mergeConfigHeaders(meta, header("X-Kuasar-Sandbox-Network", `{"hostname":"from-header"}`))
	if got[sandboxcfg.NsNetwork] != `{"hostname":"from-header"}` {
		t.Fatalf("header should win over metadata: %+v", got)
	}
	meta = map[string]string{sandboxcfg.NsRestore: `{"prefetch":"off"}`}
	got = mergeConfigHeaders(meta, header("X-Kuasar-Sandbox-Restore", `{"prefetch":"memory"}`))
	if got[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("restore header should win over metadata: %+v", got)
	}

	// Builder is build-only and is not folded by the generic sandbox header path.
	got = mergeConfigHeaders(nil, header("X-Kuasar-Sandbox-Builder", `{"referer":{"enabled":false}}`))
	if got != nil {
		t.Fatalf("builder header should not enter sandbox metadata: %+v", got)
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
