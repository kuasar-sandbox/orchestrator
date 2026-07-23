package mmdsrelay

import (
	"compress/gzip"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// newTestClient builds a Client whose resolvePin bypasses real DNS/SSRF
// validation and returns the loopback IP an httptest.NewTLSServer actually
// listens on — the request otherwise runs unmodified production code (dial,
// peer verification against that same loopback IP, TLS, header injection,
// redirect rejection, size bounds), per client.go's resolvePin doc comment.
func newTestClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	c := New(cfg, nil)
	c.resolvePin = func(_ context.Context, _ string) (net.IP, error) {
		return net.ParseIP("127.0.0.1"), nil
	}
	return c
}

// relayURLFor rewrites srv.URL's port onto a fixed https://relay.test hostname
// so Fetch's own URL parsing (which requires a real hostname, not an IP
// literal) is exercised too — resolvePin, not the URL's host, decides where
// the connection actually goes.
func relayURLFor(t *testing.T, srv *httptest.Server, path string) string {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return "https://relay.test:" + u.Port() + path
}

func testCfg() Config {
	return Config{
		RequestTimeout:       2 * time.Second,
		MaxResponseBytes:     1 << 16,
		MaxDecompressedBytes: 1 << 18,
		MaxDNSAnswers:        16,
		MaxInflightPerKey:    4,
		MaxRequestsPerSecond: 100, // high enough to not interfere with non-rate-limit tests
	}
}

func TestFetchHappyPathInjectsHeaderAndReturnsBody(t *testing.T) {
	var gotHeader string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Upstream-Assertion")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("upstream body"))
	}))
	defer srv.Close()
	srv.Client() // ensure TLS config is initialized before we bypass it below

	c := newTestClient(t, testCfg())
	res := c.fetchInsecureForTest(t, "k1", relayURLFor(t, srv, "/creds"), "X-Upstream-Assertion", "secret-token")
	if res.Status != http.StatusOK {
		t.Fatalf("Status = %d, want 200", res.Status)
	}
	if string(res.Body) != "upstream body" {
		t.Fatalf("Body = %q, want %q", res.Body, "upstream body")
	}
	if res.ContentType != "text/plain" {
		t.Fatalf("ContentType = %q, want text/plain", res.ContentType)
	}
	if gotHeader != "secret-token" {
		t.Fatalf("upstream saw header %q, want secret-token", gotHeader)
	}
}

func TestFetchStripsUpstreamHeaders(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Evil-Header", "leak-me")
		w.Header().Set("Set-Cookie", "sneaky=1")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := newTestClient(t, testCfg())
	res := c.fetchInsecureForTest(t, "k2", relayURLFor(t, srv, "/"), "X-Auth", "v")
	if res.Status != http.StatusOK {
		t.Fatalf("Status = %d, want 200", res.Status)
	}
	// Result only carries Status/ContentType/Body — there is no headers map
	// at all, so "every other upstream header is stripped" holds by
	// construction. This test documents that invariant.
	if res.ContentType != "application/json" {
		t.Fatalf("ContentType = %q, want application/json", res.ContentType)
	}
}

func TestFetchRejectsRedirect(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer srv.Close()

	c := newTestClient(t, testCfg())
	res := c.fetchInsecureForTest(t, "k3", relayURLFor(t, srv, "/"), "X-Auth", "v")
	if res.Status != http.StatusBadGateway {
		t.Fatalf("Status = %d, want 502 (redirect rejected)", res.Status)
	}
}

func TestFetchOversizeBodyRejected(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 1024))
	}))
	defer srv.Close()

	cfg := testCfg()
	cfg.MaxResponseBytes = 100
	c := newTestClient(t, cfg)
	res := c.fetchInsecureForTest(t, "k4", relayURLFor(t, srv, "/"), "X-Auth", "v")
	if res.Status != http.StatusBadGateway {
		t.Fatalf("Status = %d, want 502 (oversize)", res.Status)
	}
}

func TestFetchGzipDecompressedOversizeRejected(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		gz := gzip.NewWriter(w)
		_, _ = gz.Write(make([]byte, 1<<20)) // 1MiB decompressed, way over the test bound
		gz.Close()
	}))
	defer srv.Close()

	cfg := testCfg()
	cfg.MaxResponseBytes = 1 << 20  // compressed body fits
	cfg.MaxDecompressedBytes = 1024 // decompressed does not
	c := newTestClient(t, cfg)
	res := c.fetchInsecureForTest(t, "k5", relayURLFor(t, srv, "/"), "X-Auth", "v")
	if res.Status != http.StatusBadGateway {
		t.Fatalf("Status = %d, want 502 (decompressed oversize)", res.Status)
	}
}

func TestFetchGzipRoundTrip(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		gz := gzip.NewWriter(w)
		_, _ = gz.Write([]byte("decompressed body"))
		gz.Close()
	}))
	defer srv.Close()

	c := newTestClient(t, testCfg())
	res := c.fetchInsecureForTest(t, "k6", relayURLFor(t, srv, "/"), "X-Auth", "v")
	if res.Status != http.StatusOK || string(res.Body) != "decompressed body" {
		t.Fatalf("Status=%d Body=%q, want 200 %q", res.Status, res.Body, "decompressed body")
	}
}

func TestFetchUnsupportedEncodingRejected(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "br")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))
	}))
	defer srv.Close()

	c := newTestClient(t, testCfg())
	res := c.fetchInsecureForTest(t, "k7", relayURLFor(t, srv, "/"), "X-Auth", "v")
	if res.Status != http.StatusBadGateway {
		t.Fatalf("Status = %d, want 502 (unsupported encoding)", res.Status)
	}
}

func TestFetchPassesThroughUpstream4xxAnd5xx(t *testing.T) {
	for _, code := range []int{400, 404, 500, 503} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer srv.Close()
			c := newTestClient(t, testCfg())
			res := c.fetchInsecureForTest(t, "k-status-"+http.StatusText(code), relayURLFor(t, srv, "/"), "X-Auth", "v")
			if res.Status != code {
				t.Fatalf("Status = %d, want %d passed through", res.Status, code)
			}
		})
	}
}

func TestFetchRateLimited(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testCfg()
	cfg.MaxRequestsPerSecond = 1
	c := newTestClient(t, cfg)
	u := relayURLFor(t, srv, "/")

	first := c.fetchInsecureForTest(t, "rl-key", u, "X-Auth", "v")
	if first.Status != http.StatusOK {
		t.Fatalf("first request Status = %d, want 200", first.Status)
	}
	second := c.fetchInsecureForTest(t, "rl-key", u, "X-Auth", "v")
	if second.Status != http.StatusTooManyRequests {
		t.Fatalf("second immediate request Status = %d, want 429", second.Status)
	}
}

func TestFetchInflightCapRejectsOverLimit(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testCfg()
	cfg.MaxInflightPerKey = 1
	cfg.MaxRequestsPerSecond = 100
	c := newTestClient(t, cfg)
	u := relayURLFor(t, srv, "/")

	done := make(chan Result, 1)
	go func() { done <- c.fetchInsecureForTest(t, "inflight-key", u, "X-Auth", "v") }()
	time.Sleep(50 * time.Millisecond) // let the first request occupy the one inflight slot

	over := c.fetchInsecureForTest(t, "inflight-key", u, "X-Auth", "v")
	if over.Status != http.StatusTooManyRequests {
		t.Fatalf("over-limit request Status = %d, want 429", over.Status)
	}

	close(release)
	first := <-done
	if first.Status != http.StatusOK {
		t.Fatalf("first request Status = %d, want 200", first.Status)
	}
}

func TestFetchRejectsIPLiteralURL(t *testing.T) {
	c := New(testCfg(), nil)
	res := c.Fetch(context.Background(), "k", "https://169.254.169.254/latest/creds", "X-Auth", "v")
	if res.Status != http.StatusBadGateway {
		t.Fatalf("Status = %d, want 502 (IP literal host rejected)", res.Status)
	}
}

func TestFetchRejectsUserinfoQueryFragment(t *testing.T) {
	c := New(testCfg(), nil)
	for _, u := range []string{
		"https://user:pass@example.com/",
		"https://example.com/?q=1",
		"https://example.com/#frag",
		"http://example.com/", // not https
	} {
		t.Run(u, func(t *testing.T) {
			res := c.Fetch(context.Background(), "k", u, "X-Auth", "v")
			if res.Status != http.StatusBadGateway {
				t.Fatalf("Fetch(%q) Status = %d, want 502", u, res.Status)
			}
		})
	}
}

func TestFetchRejectsInvalidAuthHeaderName(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := newTestClient(t, testCfg())
	u := relayURLFor(t, srv, "/")

	for _, name := range []string{"Host", "Content-Length", "X-Forwarded-For", "bad header", ""} {
		t.Run(name, func(t *testing.T) {
			res := c.fetchInsecureForTest(t, "k-hdr-"+name, u, name, "v")
			if res.Status != http.StatusBadGateway {
				t.Fatalf("Fetch with header name %q Status = %d, want 502", name, res.Status)
			}
		})
	}
}

// TestFetchSSRFBlockedIPFromResolver exercises the REAL defaultResolvePin
// (unlike the other tests, which override resolvePin entirely and so never
// touch the SSRF-validation code path at all): only lookupIPAddr, the raw
// DNS step, is faked, so disallowedIP's rejection of a malicious DNS answer
// runs unmodified.
func TestFetchSSRFBlockedIPFromResolver(t *testing.T) {
	c := New(testCfg(), nil)
	c.lookupIPAddr = func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("169.254.169.254")}}, nil // simulate a DNS answer resolving to the metadata IP
	}
	res := c.Fetch(context.Background(), "k", "https://evil.example.com/", "X-Auth", "v")
	if res.Status != http.StatusBadGateway {
		t.Fatalf("Status = %d, want 502 (blocked IP)", res.Status)
	}
}

// fakeRemoteAddrConn wraps a real net.Conn but reports a different
// RemoteAddr(), so a test can simulate the connected peer disagreeing with
// the pinned IP without actually being able to make that happen over a real
// network stack.
type fakeRemoteAddrConn struct {
	net.Conn
	remote net.Addr
}

func (c *fakeRemoteAddrConn) RemoteAddr() net.Addr { return c.remote }

// TestFetchAbortsOnPinnedPeerMismatch exercises doFetch's post-dial TOCTOU
// defense (a peer differing from the pinned IP must abort before auth) —
// c.dial is overridden to return a real connection whose RemoteAddr()
// disagrees with the pinned IP resolvePin chose, which must abort the
// request before the auth header is ever attached (the request never
// reaches the upstream handler at all).
func TestFetchAbortsOnPinnedPeerMismatch(t *testing.T) {
	reached := false
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, testCfg()) // resolvePin returns 127.0.0.1, the server's real address
	realDial := c.dial
	c.dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := realDial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &fakeRemoteAddrConn{Conn: conn, remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 443}}, nil
	}

	res := c.fetchInsecureForTest(t, "k-mismatch", relayURLFor(t, srv, "/"), "X-Auth", "v")
	if res.Status != http.StatusBadGateway {
		t.Fatalf("Status = %d, want 502 (peer mismatch aborted before auth)", res.Status)
	}
	if reached {
		t.Fatal("upstream handler was reached despite the pinned-peer mismatch — auth header was sent")
	}
}

func TestFetchTimeout(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	// Unblock the handler before srv.Close() waits for it to finish —
	// deferred calls run LIFO, so this must be registered AFTER
	// defer srv.Close() to run BEFORE it.
	defer srv.Close()
	defer close(block)

	cfg := testCfg()
	cfg.RequestTimeout = 50 * time.Millisecond
	c := newTestClient(t, cfg)
	res := c.fetchInsecureForTest(t, "k-timeout", relayURLFor(t, srv, "/"), "X-Auth", "v")
	if res.Status != http.StatusGatewayTimeout {
		t.Fatalf("Status = %d, want 504 (timeout)", res.Status)
	}
}

// fetchInsecureForTest is Fetch, but with certificate verification disabled
// (client.go's insecureSkipVerifyForTests test hook) for httptest.
// NewTLSServer's self-signed cert — production Fetch always verifies
// against the system roots.
func (c *Client) fetchInsecureForTest(t *testing.T, key, rawURL, headerName, headerValue string) Result {
	t.Helper()
	orig := insecureSkipVerifyForTests
	insecureSkipVerifyForTests = true
	defer func() { insecureSkipVerifyForTests = orig }()
	return c.Fetch(context.Background(), key, rawURL, headerName, headerValue)
}
