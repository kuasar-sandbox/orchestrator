// Package mmdsrelay implements the SSRF-hardened relay backend for MMDS
// endpoints: GET a fixed, declared HTTPS upstream with an operator-injected
// auth header, after resolving, validating, and pinning the destination IP
// so a caller-controlled URL can never reach a private, link-local, or
// otherwise non-global address — including the cloud metadata address
// 169.254.169.254 itself.
//
// There is no prior SSRF-hardening code in this repo to build on: this
// package resolve-then-pins by hand rather than relying on Go's default
// dial-by-hostname behavior, which re-resolves and offers no hook to reject
// a disallowed answer before connecting.
package mmdsrelay

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Config bounds one relay client's behavior (node policy, threaded through
// from config.MMDSEndpointsConfig).
type Config struct {
	RequestTimeout       time.Duration // total deadline for one fetch, including DNS+connect+TLS+read
	MaxResponseBytes     int64         // raw (possibly-compressed) body bound
	MaxDecompressedBytes int64         // independent bound on the gzip-decompressed body
	MaxDNSAnswers        int           // reject a resolution with more answers than this
	MaxInflightPerKey    int           // concurrent fetches per (sandbox_id,name)
	MaxRequestsPerSecond float64       // token-bucket rate per (sandbox_id,name); <=0 disables
}

// Result is a fully classified outcome, ready for the caller (mmdsauth) to
// hand straight to the guest response. The upstream's own 2xx/4xx/5xx pass
// through; every other case (1xx, 3xx, malformed, unsupported encoding,
// oversize, blocked, rate limited, timeout) becomes one of 429/502/504 as
// documented on each rejection below.
type Result struct {
	Status      int
	ContentType string
	Body        []byte
}

// Counter is the narrow metrics surface Client needs. Add (unlike
// internal/mmds.Counter's Inc-only surface) is safe here since Client only
// ever runs in the conductor or proxy-master process against a real
// *metrics.M, never through the worker-side metrics pipe.
type Counter interface {
	Inc(name string)
	Add(name string, n int64)
}

type noopCounter struct{}

func (noopCounter) Inc(string)        {}
func (noopCounter) Add(string, int64) {}

// relayResultLabel is the bounded mmds_relay_requests_total{result} value
// for a completed Fetch — every distinct value is an enum literal, never
// derived from caller/upstream-controlled data, since only bounded enums
// may be used as metric labels. Mirrors internal/mmds.relayResultLabel's
// bucketing.
func relayResultLabel(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "ok"
	case status == http.StatusTooManyRequests:
		return "rate_limited"
	case status == http.StatusGatewayTimeout:
		return "timeout"
	case status >= 400 && status < 500:
		return "upstream_4xx"
	case status >= 500:
		return "upstream_5xx_or_blocked"
	default:
		return "other"
	}
}

// Client performs SSRF-hardened relay fetches, rate-limited and
// concurrency-bounded per (sandbox_id,name) key. Safe for concurrent use;
// shared across every endpoint on a node — internal mode runs the same
// registry and handlers in conductor, and this type is also what the
// proxy-master endpoint table reuses for external mode.
type Client struct {
	cfg Config

	// resolvePin resolves host to exactly one validated, pinned IP: reject
	// IP literals/userinfo/query/fragment/malformed hosts up front, then
	// validate every DNS answer and reject the whole resolution if any
	// answer is disallowed. Defaults to
	// defaultResolvePin. Tests override this directly to bypass real
	// DNS/SSRF validation entirely and point at an httptest server's real
	// loopback address, exercising the rest of the pipeline (dial, peer
	// verification, TLS, header injection, redirect rejection, size
	// bounds) against unmodified production code.
	resolvePin func(ctx context.Context, host string) (net.IP, error)

	// lookupIPAddr is defaultResolvePin's DNS step, factored out so tests
	// can exercise defaultResolvePin's real validate-every-answer logic
	// (including disallowedIP) with a fake DNS answer, without needing
	// real network access or a resolvePin override that skips validation
	// altogether. Defaults to net.DefaultResolver.LookupIPAddr.
	lookupIPAddr func(ctx context.Context, host string) ([]net.IPAddr, error)

	mu       sync.Mutex
	limiters map[string]*keyLimiter

	mx Counter
}

// insecureSkipVerifyForTests exists solely so client_test.go can exercise
// Fetch's full request pipeline against an httptest.NewTLSServer's
// self-signed certificate. Never set outside tests.
var insecureSkipVerifyForTests = false

// New builds a Client bounded by cfg. mx may be nil (metrics off).
func New(cfg Config, mx Counter) *Client {
	if mx == nil {
		mx = noopCounter{}
	}
	c := &Client{cfg: cfg, limiters: map[string]*keyLimiter{}, lookupIPAddr: net.DefaultResolver.LookupIPAddr, mx: mx}
	c.resolvePin = c.defaultResolvePin
	return c
}

func (c *Client) limiterFor(key string) *keyLimiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.limiters[key]
	if !ok {
		l = newKeyLimiter(c.cfg.MaxRequestsPerSecond, c.cfg.MaxInflightPerKey)
		c.limiters[key] = l
	}
	return l
}

// Fetch performs one SSRF-hardened GET to rawURL, injecting headerValue
// under headerName only after URL/DNS/peer/header-name validation succeeds.
// key scopes rate limiting and inflight concurrency — the caller passes
// sandboxID+"\x00"+name. ctx additionally bounds the request; the caller
// cancels it to abort an in-flight fetch, e.g. on auth revocation/rotation
// (cancellation on auth revision changes).
func (c *Client) Fetch(ctx context.Context, key, rawURL, headerName, headerValue string) (result Result) {
	start := time.Now()
	defer func() {
		c.mx.Inc(`mmds_relay_requests_total{result="` + relayResultLabel(result.Status) + `"}`)
		// metrics.M has no histogram/float support, so the sum is tracked in
		// milliseconds (mmds_relay_latency_seconds_sum_ms), documented via
		// the name itself rather than the doc's literal seconds unit.
		c.mx.Add("mmds_relay_latency_seconds_sum_ms", time.Since(start).Milliseconds())
		c.mx.Inc("mmds_relay_latency_seconds_count")
	}()

	limiter := c.limiterFor(key)
	if !limiter.allowRate() {
		return Result{Status: http.StatusTooManyRequests}
	}
	release, ok := limiter.acquireInflight()
	if !ok {
		return Result{Status: http.StatusTooManyRequests}
	}
	defer release()

	if !validAuthHeaderName(headerName) {
		return Result{Status: http.StatusBadGateway}
	}

	timeout := c.cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Hostname() == "" {
		return Result{Status: http.StatusBadGateway}
	}
	host := u.Hostname()
	if net.ParseIP(host) != nil {
		return Result{Status: http.StatusBadGateway} // IP literals disallowed; only hostnames are relayable
	}
	port := u.Port()
	if port == "" {
		port = "443"
	} else if _, perr := strconv.ParseUint(port, 10, 16); perr != nil {
		return Result{Status: http.StatusBadGateway}
	}

	pinned, err := c.resolvePin(reqCtx, host)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return Result{Status: http.StatusGatewayTimeout}
		}
		return Result{Status: http.StatusBadGateway}
	}

	resp, err := c.doFetch(reqCtx, u, host, port, pinned, headerName, headerValue)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return Result{Status: http.StatusGatewayTimeout}
		}
		return Result{Status: http.StatusBadGateway}
	}
	return resp
}

func (c *Client) doFetch(ctx context.Context, u *url.URL, host, port string, pinned net.IP, headerName, headerValue string) (Result, error) {
	dialer := &net.Dialer{}
	pinnedAddr := net.JoinHostPort(pinned.String(), port)
	transport := &http.Transport{
		Proxy: nil, // never honor environment proxies
		DialContext: func(dctx context.Context, _, _ string) (net.Conn, error) {
			// Connect directly to the pinned IP, never re-resolving — dctx's
			// network/addr (derived from the request URL's host) are
			// ignored; the pinned literal is authoritative.
			conn, derr := dialer.DialContext(dctx, "tcp", pinnedAddr)
			if derr != nil {
				return nil, derr
			}
			// Verify the connected peer equals the pinned IP before this
			// connection is used to send anything (in particular, the auth
			// header) — a TOCTOU defense against resolution changing
			// between validation and connect.
			remoteHost, _, serr := net.SplitHostPort(conn.RemoteAddr().String())
			if serr != nil || net.ParseIP(remoteHost) == nil || !net.ParseIP(remoteHost).Equal(pinned) {
				conn.Close()
				return nil, errors.New("mmdsrelay: connected peer does not match the pinned IP")
			}
			return conn, nil
		},
		TLSClientConfig: &tls.Config{
			ServerName: host, // original hostname for SNI/certificate validation, never the pinned IP
			MinVersion: tls.VersionTLS12,
			// InsecureSkipVerify is only ever true in tests (against an
			// httptest.NewTLSServer's self-signed cert); production Fetch
			// always verifies against the system roots.
			InsecureSkipVerify: insecureSkipVerifyForTests,
		},
		DisableCompression: true, // gzip is negotiated and unwrapped by this package, not net/http
	}
	defer transport.CloseIdleConnections()

	httpClient := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("mmdsrelay: redirects are never followed")
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Result{}, err
	}
	req.Host = host // original hostname, never the pinned IP
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "gzip") // the only encoding this client advertises/accepts
	req.Header.Set(headerName, headerValue)   // injected last, only after every prior check passed

	resp, err := httpClient.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()

	// 1xx is handled internally by net/http (never surfaced to Do's caller
	// as a final response) and 2xx/4xx/5xx pass through below; only 3xx
	// remains to reject explicitly here (redirects are never followed, so a
	// 3xx status itself — as opposed to CheckRedirect firing — only occurs
	// for a redirect net/http doesn't automatically chase, e.g. a 304-style
	// response outside a conditional request).
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return Result{Status: http.StatusBadGateway}, nil
	}

	encoding := resp.Header.Get("Content-Encoding")
	if encoding != "" && encoding != "gzip" {
		return Result{Status: http.StatusBadGateway}, nil // unsupported encoding
	}

	rawLimit := c.cfg.MaxResponseBytes
	if rawLimit <= 0 {
		rawLimit = 65536
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, rawLimit+1))
	if err != nil {
		return Result{}, err
	}
	if int64(len(raw)) > rawLimit {
		return Result{Status: http.StatusBadGateway}, nil // oversize
	}

	body := raw
	if encoding == "gzip" {
		decLimit := c.cfg.MaxDecompressedBytes
		if decLimit <= 0 {
			decLimit = 262144
		}
		gr, gerr := gzip.NewReader(bytes.NewReader(raw))
		if gerr != nil {
			return Result{Status: http.StatusBadGateway}, nil // malformed gzip
		}
		body, err = io.ReadAll(io.LimitReader(gr, decLimit+1))
		gr.Close()
		if err != nil {
			return Result{Status: http.StatusBadGateway}, nil
		}
		if int64(len(body)) > decLimit {
			return Result{Status: http.StatusBadGateway}, nil // decompressed oversize
		}
	}

	contentType := ""
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		if _, _, perr := mime.ParseMediaType(ct); perr == nil {
			contentType = ct
		}
		// An unparseable Content-Type is dropped (empty), not treated as a
		// fatal/malformed response — the body itself is still valid.
	}

	return Result{Status: resp.StatusCode, ContentType: contentType, Body: body}, nil
}

// defaultResolvePin resolves host, rejects the whole resolution if any
// answer is disallowed, bounds the answer count to cfg.MaxDNSAnswers, and
// returns exactly one validated IP to pin the connection to.
func (c *Client) defaultResolvePin(ctx context.Context, host string) (net.IP, error) {
	addrs, err := c.lookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errors.New("mmdsrelay: no DNS answers")
	}
	maxAnswers := c.cfg.MaxDNSAnswers
	if maxAnswers <= 0 {
		maxAnswers = 16
	}
	if len(addrs) > maxAnswers {
		return nil, errors.New("mmdsrelay: too many DNS answers")
	}
	for _, a := range addrs {
		if reason, blocked := disallowedIP(a.IP); blocked {
			c.mx.Inc(`mmds_relay_blocked_total{reason="` + reason + `"}`)
			return nil, errors.New("mmdsrelay: a DNS answer resolves to a disallowed address")
		}
	}
	return addrs[0].IP, nil
}
