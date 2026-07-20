package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/net/http/httpguts"
)

// This file adds CONNECT tunneling to the data plane: a client opens a raw TCP
// stream to a sandbox port. Per the data-plane model the CONNECT target HOST is
// ignored (it is uniformly the sandbox's floating IP) and only the PORT is honored;
// the sandbox id comes from E2b-Sandbox-Id (or the authority label). The route is
// resolved + access-token-checked exactly like a forwarded request, then the client
// connection is spliced to the backend. The same Tunnel primitive serves both the
// proxy's direct ingress and the external-mode proxyForwarder's CONNECT relay.

// directDialRoute opens a connection to a resolved route's backend in the
// process's current network namespace.
func directDialRoute(ctx context.Context, r Route) (net.Conn, error) {
	d := net.Dialer{}
	switch r.Kind {
	case KindUDS:
		return d.DialContext(ctx, "unix", r.UDS)
	case KindTCP:
		return d.DialContext(ctx, "tcp", r.Addr)
	default:
		return nil, fmt.Errorf("proxy: no dialable route in context")
	}
}

// serveConnect handles a CONNECT request: resolve the sandbox + port, auth, dial the
// backend, then splice. Mirrors ServeHTTP's classification (404/501/401) so CONNECT
// and forwarded requests behave identically.
func (p *Proxy) serveConnect(w http.ResponseWriter, r *http.Request) {
	sid, port, ok := parseConnect(r)
	if !ok {
		p.mx.Inc(`data_requests_total{result="badrequest"}`)
		writeProxyError(w, http.StatusBadRequest, "bad connect target", ProxyErrorBadRequest)
		return
	}
	request, err := RouteRequestFromHTTP(r, sid, port)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="badrequest"}`)
		writeProxyError(w, http.StatusBadRequest, err.Error(), ProxyErrorBadRequest)
		return
	}
	route, err := p.router.Route(r.Context(), request)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusBadGateway, "routing error", ProxyErrorRouteError)
		return
	}
	switch route.Kind {
	case KindNotFound:
		p.mx.Inc(`data_requests_total{result="notfound"}`)
		writeProxyError(w, http.StatusNotFound, "sandbox not found", ProxyErrorNotFound)
		return
	case KindDeny:
		p.mx.Inc(`data_requests_total{result="denied"}`)
		writeProxyError(w, http.StatusNotImplemented, "data plane not available on this sandbox", ProxyErrorDenied)
		return
	case KindWrongNodeEpoch:
		p.mx.Inc(`data_requests_total{result="wrong_node_epoch"}`)
		writeProxyError(w, http.StatusConflict, "wrong node epoch", ProxyErrorWrongNodeEpoch)
		return
	case KindWrongBinding:
		p.mx.Inc(`data_requests_total{result="wrong_binding"}`)
		writeProxyError(w, http.StatusConflict, "wrong execution binding", ProxyErrorWrongBinding)
		return
	case KindRouteInactive:
		p.mx.Inc(`data_requests_total{result="route_inactive"}`)
		writeProxyError(w, http.StatusConflict, "route inactive", ProxyErrorRouteInactive)
		return
	case KindUDS, KindTCP:
	default:
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusBadGateway, "routing error", ProxyErrorRouteError)
		return
	}
	if !p.authorized(r, route, port) {
		p.mx.Inc(`data_requests_total{result="unauthorized"}`)
		writeProxyError(w, http.StatusUnauthorized, "invalid access token", ProxyErrorUnauthorized)
		return
	}
	backend, err := p.dial(r.Context(), route)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="upstream_error"}`)
		writeProxyError(w, http.StatusBadGateway, "upstream error", ProxyErrorUpstreamError)
		return
	}
	p.mx.Inc(`data_requests_total{result="ok"}`)
	Tunnel(w, r, backend)
}

// parseConnect resolves (sid, port) for a CONNECT: sid from E2b-Sandbox-Id (or the
// authority's <port>-<sid> label as a fallback), port from the CONNECT target
// authority — the target host itself is ignored (uniformly the sandbox's floating IP).
func parseConnect(r *http.Request) (sid string, port int, ok bool) {
	sid = r.Header.Get(HeaderSandboxID)
	if sid == "" {
		if s, _, parsed := ParseSandbox(r); parsed {
			sid = s
		}
	}
	if sid == "" {
		return "", 0, false
	}
	if hp := r.Header.Get(HeaderSandboxPort); hp != "" {
		p, err := strconv.Atoi(hp)
		if err == nil && p > 0 {
			return sid, p, true
		}
		return "", 0, false
	}
	target := r.URL.Host
	if target == "" {
		target = r.Host
	}
	_, ps, err := net.SplitHostPort(target)
	if err != nil {
		return "", 0, false
	}
	port, err = strconv.Atoi(ps)
	if err != nil || port <= 0 {
		return "", 0, false
	}
	return sid, port, true
}

// WriteSandboxConnect issues the node/proxy-worker data-plane CONNECT handshake.
// The CONNECT authority is deliberately generic: sandbox identity and port are
// carried as explicit headers, so intermediates do not parse route-key or host
// labels and the backend connection is bound to exactly one sandbox port.
type SandboxConnectRequest struct {
	RouteRequest
	AccessToken string
}

func WriteSandboxConnect(w io.Writer, request SandboxConnectRequest) error {
	if request.SandboxID == "" || request.Port <= 0 {
		return fmt.Errorf("proxy: sandbox id and port are required for CONNECT")
	}
	if request.HasExecutionFence() && (request.ExpectedNodeID == "" || request.ExpectedNodeEpoch == 0 ||
		request.ExpectedRegistryGeneration == "" || request.ExpectedBindingDigest == "") {
		return fmt.Errorf("proxy: incomplete execution fence")
	}
	for name, value := range map[string]string{
		HeaderSandboxID:          request.SandboxID,
		HeaderAccessToken:        request.AccessToken,
		HeaderNodeID:             request.ExpectedNodeID,
		HeaderRegistryGeneration: request.ExpectedRegistryGeneration,
		HeaderBindingDigest:      request.ExpectedBindingDigest,
	} {
		if value != "" && (!httpguts.ValidHeaderFieldValue(value) || strings.TrimSpace(value) != value) {
			return fmt.Errorf("proxy: invalid %s header value", name)
		}
	}
	target := fmt.Sprintf("sandbox:%d", request.Port)
	var b strings.Builder
	fmt.Fprintf(&b, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n", target, target)
	fmt.Fprintf(&b, "%s: %s\r\n%s: %d\r\n", HeaderSandboxID, request.SandboxID, HeaderSandboxPort, request.Port)
	if request.AccessToken != "" {
		fmt.Fprintf(&b, "%s: %s\r\n", HeaderAccessToken, request.AccessToken)
	}
	if request.HasExecutionFence() {
		fmt.Fprintf(&b, "%s: %s\r\n", HeaderNodeID, request.ExpectedNodeID)
		fmt.Fprintf(&b, "%s: %d\r\n", HeaderNodeEpoch, request.ExpectedNodeEpoch)
		fmt.Fprintf(&b, "%s: %s\r\n", HeaderRegistryGeneration, request.ExpectedRegistryGeneration)
		fmt.Fprintf(&b, "%s: %s\r\n", HeaderBindingDigest, request.ExpectedBindingDigest)
	}
	b.WriteString("\r\n")
	_, err := io.WriteString(w, b.String())
	return err
}

// DialSandboxConnect dials addr and performs WriteSandboxConnect. The caller owns
// conn and must close it unless it passes the connection to Tunnel/TunnelBuffered.
func DialSandboxConnect(ctx context.Context, network, addr string, request SandboxConnectRequest) (net.Conn, *bufio.Reader, *http.Response, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := WriteSandboxConnect(conn, request); err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	return conn, br, resp, nil
}

// ForwardHTTPOnce sends r through one already-connected backend connection and
// reads exactly one response. It does not retain or pool the connection.
func ForwardHTTPOnce(r *http.Request, backend net.Conn, br *bufio.Reader, mutate func(*http.Request)) (*http.Response, error) {
	if br == nil {
		br = bufio.NewReader(backend)
	}
	out := r.Clone(r.Context())
	out.RequestURI = ""
	if out.URL == nil {
		out.URL = &url.URL{}
	} else {
		u := *out.URL
		out.URL = &u
	}
	out.URL.Scheme = "http"
	if out.Host != "" {
		out.URL.Host = out.Host
	} else if out.URL.Host == "" {
		out.URL.Host = "sandbox"
	}
	out.Close = true
	removeHopHeaders(out.Header)
	if mutate != nil {
		mutate(out)
	}
	if err := out.Write(backend); err != nil {
		return nil, err
	}
	return http.ReadResponse(br, out)
}

func removeHopHeaders(h http.Header) {
	for _, k := range []string{
		"Connection",
		"Proxy-Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
		HeaderNodeID,
		HeaderNodeEpoch,
		HeaderRegistryGeneration,
		HeaderBindingDigest,
	} {
		h.Del(k)
	}
}

// Tunnel splices the client connection (the CONNECT request) to backend,
// bidirectionally, for both HTTP/1.1 (Hijack + "200 Connection established") and
// HTTP/2 (200 response + request/response stream copy). It closes backend on return.
// Exported so the external-mode proxyForwarder can reuse it when relaying a CONNECT to a
// proxy worker.
func Tunnel(w http.ResponseWriter, r *http.Request, backend net.Conn) {
	defer backend.Close()

	if r.ProtoMajor == 2 {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		done := make(chan struct{}, 2)
		go func() { _, _ = io.Copy(backend, r.Body); done <- struct{}{} }()         // client -> backend
		go func() { _, _ = io.Copy(flushWriter{w}, backend); done <- struct{}{} }() // backend -> client
		<-done
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "connect unsupported", http.StatusInternalServerError)
		return
	}
	client, _, err := hj.Hijack()
	if err != nil {
		return
	}
	defer client.Close()
	if _, err := io.WriteString(client, "HTTP/1.1 200 Connection established\r\n\r\n"); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(backend, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, backend); done <- struct{}{} }()
	<-done
}

func TunnelBuffered(w http.ResponseWriter, r *http.Request, backend net.Conn, br *bufio.Reader) {
	Tunnel(w, r, &bufferedConn{Conn: backend, r: br})
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// flushWriter flushes after each write so the HTTP/2 backend->client tunnel half
// streams promptly instead of buffering.
type flushWriter struct{ w io.Writer }

func (fw flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	if f, ok := fw.w.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}
