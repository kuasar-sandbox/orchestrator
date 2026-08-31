package conductorapp

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

func TestProxyAffinityKeyUsesCanonicalSIDOrStableRawRequest(t *testing.T) {
	canonicalHTTP := func(path, host string) *http.Request {
		request := httptest.NewRequest(http.MethodGet, "http://"+host+path, nil)
		request.Host = host
		request.Header.Set(proxy.HeaderSandboxID, "stable-s1")
		request.Header.Set(proxy.HeaderSandboxPort, "8080")
		return request
	}
	if first, second := proxyAffinityKey(canonicalHTTP("/one", "one.example")), proxyAffinityKey(canonicalHTTP("/two", "two.example")); first != second || first != "sandbox\x00stable-s1" {
		t.Fatalf("canonical HTTP keys=%q %q", first, second)
	}

	canonicalConnect := func(port string) *http.Request {
		request := httptest.NewRequest(http.MethodConnect, "http://sandbox:"+port, nil)
		request.Host = "sandbox:" + port
		request.Header.Set(proxy.HeaderSandboxID, "stable-s1")
		request.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceForward))
		request.Header.Set(proxy.HeaderSandboxPort, port)
		return request
	}
	if first, second := proxyAffinityKey(canonicalConnect("8080")), proxyAffinityKey(canonicalConnect("9090")); first != second || first != "sandbox\x00stable-s1" {
		t.Fatalf("canonical CONNECT keys=%q %q", first, second)
	}

	raw := func(method, path string) *http.Request {
		request := httptest.NewRequest(method, "http://private.example"+path, nil)
		request.Host = "private.example"
		request.Header.Set("X-Sandbox-Id", "private-s1")
		return request
	}
	first := proxyAffinityKey(raw(http.MethodGet, "/private?revision=1"))
	second := proxyAffinityKey(raw(http.MethodGet, "/private?revision=2"))
	if first != second || first != "raw\x00private.example\x00GET\x00/private" {
		t.Fatalf("raw keys=%q %q", first, second)
	}
	if changed := proxyAffinityKey(raw(http.MethodPost, "/private?revision=1")); changed == first {
		t.Fatalf("raw method was omitted from affinity key: %q", changed)
	}
}
