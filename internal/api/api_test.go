package api

import (
	"net/http"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestMergeConfigHeaders(t *testing.T) {
	// No headers => no allocation.
	if m := mergeConfigHeaders(nil, http.Header{}); m != nil {
		t.Fatalf("no headers should not allocate: %v", m)
	}

	// Headers populate the matching namespace keys.
	h := http.Header{}
	h.Set("X-Kuasar-Sandbox-Network", `{"hostname":"h1"}`)
	h.Set("X-Kuasar-Sandbox-Resource", `{"capacity":{"cpu":4}}`)
	m := mergeConfigHeaders(nil, h)
	if m[sandboxcfg.NsNetwork] != `{"hostname":"h1"}` || m[sandboxcfg.NsResource] != `{"capacity":{"cpu":4}}` {
		t.Fatalf("headers not normalized: %+v", m)
	}

	// Header wins over an e2b metadata key of the same namespace.
	meta := map[string]string{sandboxcfg.NsNetwork: `{"hostname":"from-metadata"}`}
	got := mergeConfigHeaders(meta, header("X-Kuasar-Sandbox-Network", `{"hostname":"from-header"}`))
	if got[sandboxcfg.NsNetwork] != `{"hostname":"from-header"}` {
		t.Fatalf("header should win over metadata: %+v", got)
	}
}

func header(k, v string) http.Header {
	h := http.Header{}
	h.Set(k, v)
	return h
}
