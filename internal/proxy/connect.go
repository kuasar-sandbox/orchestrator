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
	"sync"
)

// This file adds CONNECT tunneling to the data plane. A legacy request selects a
// raw sandbox port; an explicit E2b-Sandbox-Service selects a logical backend and
// may carry an optional meaningful port. The sandbox id comes from
// E2b-Sandbox-Id or a legacy authority label. The final node resolves the canonical
// target, checks its access token, and splices the client to the selected backend.
// The same Tunnel primitive serves direct ingress and CONNECT relays.

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

// serveConnect resolves the sandbox + canonical target, authenticates the selected
// backend, then splices it. Recognized services unsupported by the local profile
// return 501; malformed or unknown services are rejected before route lookup.
func (p *Proxy) serveConnect(w http.ResponseWriter, r *http.Request) {
	sid, target, ok := ParseConnect(r)
	if !ok {
		p.mx.Inc(`data_requests_total{result="badrequest"}`)
		writeProxyError(w, http.StatusBadRequest, "bad connect target", ProxyErrorBadRequest)
		return
	}
	if target.Service == ConnectServiceExec {
		p.serveExecConnect(w, r, sid)
		return
	}
	route, flow, _, ok := p.admitRoute(w, r, sid, target)
	if !ok {
		return
	}
	defer flow.Close()
	backend, err := p.dial(r.Context(), route)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="upstream_error"}`)
		writeProxyError(w, http.StatusBadGateway, "upstream error", ProxyErrorUpstreamError)
		return
	}
	backend = flow.AttachBackend(backend)
	// H2 stream cancellation closes its backend independently of traffic limits.
	// Do not apply this to an H1 hijack: request EOF may be a valid half-close.
	if r.ProtoMajor == 2 {
		stopContextClose := context.AfterFunc(r.Context(), func() { _ = backend.Close() })
		defer stopContextClose()
	}
	p.mx.Inc(`data_requests_total{result="ok"}`)
	Tunnel(w, r, backend)
}

// ParseConnect resolves the sandbox identity and canonical CONNECT target. An
// explicit logical service ignores the CONNECT authority port, which is only a
// transport placeholder; a port explicitly carried by E2b-Sandbox-Port or the
// legacy <port>-<sid> host is retained for second-hop forwarding. Legacy and
// explicit forward targets also accept the authority port as an existing source.
// Conflicting identity or meaningful-port sources are rejected.
func ParseConnect(r *http.Request) (sid string, target ConnectTarget, ok bool) {
	if r == nil || r.Method != http.MethodConnect {
		return "", ConnectTarget{}, false
	}
	service, explicit, ok := parseConnectService(r.Header)
	if !ok {
		return "", ConnectTarget{}, false
	}
	target.Service = service

	addSID := func(value string) bool {
		if value == "" {
			return false
		}
		if sid != "" && sid != value {
			return false
		}
		sid = value
		return true
	}
	portSet := false
	addPort := func(value int) bool {
		if !validPort(value) {
			return false
		}
		if portSet && target.Port != value {
			return false
		}
		target.Port = value
		portSet = true
		return true
	}

	if values, present := headerValues(r.Header, HeaderSandboxID); present {
		for _, value := range values {
			if !addSID(value) {
				return "", ConnectTarget{}, false
			}
		}
	}
	if values, present := headerValues(r.Header, HeaderSandboxPort); present {
		for _, value := range values {
			port, err := strconv.Atoi(value)
			if err != nil || !addPort(port) {
				return "", ConnectTarget{}, false
			}
		}
	}
	if hostSID, hostPort, parsed := parseSandboxHost(r.Host); parsed {
		if !addSID(hostSID) || !addPort(hostPort) {
			return "", ConnectTarget{}, false
		}
	}
	if sid == "" {
		return "", ConnectTarget{}, false
	}

	if !explicit || service == ConnectServiceForward {
		if authorityPort, parsed := parseConnectAuthorityPort(r); parsed {
			if !addPort(authorityPort) {
				return "", ConnectTarget{}, false
			}
		}
		if !portSet {
			return "", ConnectTarget{}, false
		}
	}
	return sid, target, true
}

func parseConnectService(header http.Header) (service ConnectService, explicit bool, ok bool) {
	values, present := headerValues(header, HeaderSandboxService)
	if !present {
		return ConnectServiceLegacy, false, true
	}
	if len(values) != 1 || values[0] == "" {
		return "", true, false
	}
	service = ConnectService(values[0])
	switch service {
	case ConnectServiceForward, ConnectServiceE2BEnvd, ConnectServiceE2BInterpreter, ConnectServiceExec:
		return service, true, true
	default:
		return "", true, false
	}
}

func headerValues(header http.Header, name string) ([]string, bool) {
	values, present := header[http.CanonicalHeaderKey(name)]
	return values, present
}

func parseConnectAuthorityPort(r *http.Request) (int, bool) {
	authority := r.URL.Host
	if authority == "" {
		authority = r.Host
	}
	_, portString, err := net.SplitHostPort(authority)
	if err != nil {
		return 0, false
	}
	port, err := strconv.Atoi(portString)
	if err != nil || !validPort(port) {
		return 0, false
	}
	return port, true
}

func validPort(port int) bool { return port > 0 && port <= 65535 }

// WriteSandboxConnect issues a node/proxy-worker data-plane CONNECT handshake.
// Logical services use a generic authority and carry an optional meaningful port
// only in E2b-Sandbox-Port; the generic 443 is never synthesized as a port Header.
func WriteSandboxConnect(w io.Writer, sid string, target ConnectTarget, token string) error {
	if sid == "" || !validConnectTarget(target) {
		return fmt.Errorf("proxy: valid sandbox id and CONNECT target are required")
	}
	authorityPort := target.Port
	if target.Service != ConnectServiceLegacy && target.Service != ConnectServiceForward {
		authorityPort = 443
	}
	authority := fmt.Sprintf("sandbox:%d", authorityPort)
	var b strings.Builder
	fmt.Fprintf(&b, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n", authority, authority)
	fmt.Fprintf(&b, "%s: %s\r\n", HeaderSandboxID, sid)
	if target.Service != ConnectServiceLegacy {
		fmt.Fprintf(&b, "%s: %s\r\n", HeaderSandboxService, target.Service)
	}
	if target.Port > 0 {
		fmt.Fprintf(&b, "%s: %d\r\n", HeaderSandboxPort, target.Port)
	}
	if token != "" {
		fmt.Fprintf(&b, "%s: %s\r\n", HeaderAccessToken, token)
	}
	b.WriteString("\r\n")
	_, err := io.WriteString(w, b.String())
	return err
}

func validConnectTarget(target ConnectTarget) bool {
	if target.Port < 0 || target.Port > 65535 {
		return false
	}
	switch target.Service {
	case ConnectServiceLegacy, ConnectServiceForward:
		return validPort(target.Port)
	case ConnectServiceE2BEnvd, ConnectServiceE2BInterpreter, ConnectServiceExec:
		return true
	default:
		return false
	}
}

// DialSandboxConnect dials addr and performs WriteSandboxConnect. The caller owns
// conn and must close it unless it passes the connection to Tunnel/TunnelBuffered.
func DialSandboxConnect(ctx context.Context, network, addr, sid string, target ConnectTarget, token string) (net.Conn, *bufio.Reader, *http.Response, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := WriteSandboxConnect(conn, sid, target, token); err != nil {
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
	out := cloneForwardHTTPRequest(r)
	normalizeForwardHTTPRequest(out)
	if mutate != nil {
		mutate(out)
	}
	return writeForwardHTTPRequest(out, backend, br)
}

func cloneForwardHTTPRequest(r *http.Request) *http.Request {
	out := r.Clone(r.Context())
	if out.URL == nil {
		out.URL = &url.URL{}
	} else {
		u := *out.URL
		out.URL = &u
	}
	return out
}

func forwardClonedHTTPOnce(out *http.Request, backend net.Conn, br *bufio.Reader) (*http.Response, error) {
	normalizeForwardHTTPRequest(out)
	return writeForwardHTTPRequest(out, backend, br)
}

func normalizeForwardHTTPRequest(out *http.Request) {
	out.RequestURI = ""
	out.URL.Scheme = "http"
	if out.Host != "" {
		out.URL.Host = out.Host
	} else if out.URL.Host == "" {
		out.URL.Host = "sandbox"
	}
	out.Close = true
	removeHopHeaders(out.Header)
}

func writeForwardHTTPRequest(out *http.Request, backend net.Conn, br *bufio.Reader) (*http.Response, error) {
	if br == nil {
		br = bufio.NewReader(backend)
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
	} {
		h.Del(k)
	}
}

// h1ConnectStream keeps both the raw hijacked connection and net/http's
// buffered reader. Bytes read with the CONNECT headers remain visible to the
// tunnel, while writes and half-close operate on the connection itself.
type h1ConnectStream struct {
	net.Conn
	reader *bufio.Reader
}

func (s *h1ConnectStream) Read(p []byte) (int, error) { return s.reader.Read(p) }

func (s *h1ConnectStream) CloseWrite() error {
	if closer, ok := s.Conn.(interface{ CloseWrite() error }); ok {
		return closer.CloseWrite()
	}
	return nil
}

// h2ConnectStream maps an HTTP/2 CONNECT request/response pair to a duplex
// stream. Closing its write direction closes the request body so backend EOF can
// unblock a pending request-body read; handler return ends the response stream.
type h2ConnectStream struct {
	reader    io.ReadCloser
	writer    io.Writer
	flusher   http.Flusher
	closeOnce sync.Once
	closeErr  error
}

func (s *h2ConnectStream) Read(p []byte) (int, error) { return s.reader.Read(p) }

func (s *h2ConnectStream) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	s.flusher.Flush()
	return n, err
}

func (s *h2ConnectStream) CloseWrite() error { return s.closeReader() }
func (s *h2ConnectStream) Close() error      { return s.closeReader() }

func (s *h2ConnectStream) closeReader() error {
	s.closeOnce.Do(func() { s.closeErr = s.reader.Close() })
	return s.closeErr
}

var _ io.ReadWriteCloser = (*h1ConnectStream)(nil)
var _ interface{ CloseWrite() error } = (*h1ConnectStream)(nil)
var _ io.ReadWriteCloser = (*h2ConnectStream)(nil)
var _ interface{ CloseWrite() error } = (*h2ConnectStream)(nil)

// AcceptConnectStream flushes CONNECT 200 and returns the client-facing duplex
// stream without dialing an upstream. HTTP/1 buffered bytes are preserved and
// HTTP/2 writes are flushed. The caller owns the returned stream.
func AcceptConnectStream(w http.ResponseWriter, r *http.Request) (io.ReadWriteCloser, error) {
	if r.ProtoMajor == 2 {
		flusher, ok := w.(http.Flusher)
		if !ok {
			return nil, fmt.Errorf("proxy: CONNECT response does not support flush")
		}
		body := r.Body
		if body == nil {
			body = http.NoBody
		}
		downstream := &h2ConnectStream{reader: body, writer: w, flusher: flusher}
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		return downstream, nil
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("proxy: CONNECT response does not support hijack")
	}
	client, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, fmt.Errorf("proxy: hijack CONNECT: %w", err)
	}
	downstream := &h1ConnectStream{Conn: client, reader: rw.Reader}
	if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = downstream.Close()
		return nil, fmt.Errorf("proxy: write CONNECT response: %w", err)
	}
	if err := rw.Flush(); err != nil {
		_ = downstream.Close()
		return nil, fmt.Errorf("proxy: flush CONNECT response: %w", err)
	}
	return downstream, nil
}

// BufferedConnectStream exposes bytes already buffered while reading an
// upstream CONNECT response and preserves the connection's half-close support.
func BufferedConnectStream(conn net.Conn, reader *bufio.Reader) net.Conn {
	if reader == nil {
		reader = bufio.NewReader(conn)
	}
	return &bufferedConn{Conn: conn, r: reader}
}

// Tunnel splices the client CONNECT stream to backend for HTTP/1.1 and HTTP/2.
// A clean EOF half-closes the destination and both pumps are drained; an I/O
// error closes both streams to unblock the peer pump. Tunnel owns and closes the
// backend. Exported so chained CONNECT forwarders can use the same relay.
func Tunnel(w http.ResponseWriter, r *http.Request, backend net.Conn) {
	if r.ProtoMajor == 2 {
		flusher, ok := w.(http.Flusher)
		if !ok {
			_ = backend.Close()
			http.Error(w, "connect unsupported", http.StatusInternalServerError)
			return
		}
		body := r.Body
		if body == nil {
			body = http.NoBody
		}
		downstream := &h2ConnectStream{reader: body, writer: w, flusher: flusher}
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		relayConnectStreams(downstream, backend)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = backend.Close()
		http.Error(w, "connect unsupported", http.StatusInternalServerError)
		return
	}
	client, rw, err := hj.Hijack()
	if err != nil {
		_ = backend.Close()
		return
	}
	downstream := &h1ConnectStream{Conn: client, reader: rw.Reader}
	if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = downstream.Close()
		_ = backend.Close()
		return
	}
	if err := rw.Flush(); err != nil {
		_ = downstream.Close()
		_ = backend.Close()
		return
	}
	relayConnectStreams(downstream, backend)
}

func TunnelBuffered(w http.ResponseWriter, r *http.Request, backend net.Conn, br *bufio.Reader) {
	Tunnel(w, r, &bufferedConn{Conn: backend, r: br})
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func (c *bufferedConn) CloseWrite() error {
	if closer, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return closer.CloseWrite()
	}
	return nil
}

func relayConnectStreams(left, right io.ReadWriteCloser) {
	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			_ = left.Close()
			_ = right.Close()
		})
	}
	defer closeBoth()

	type result struct{ err error }
	results := make(chan result, 2)
	pump := func(dst io.Writer, src io.Reader) {
		_, err := io.Copy(dst, src)
		if err == nil {
			if closer, ok := dst.(interface{ CloseWrite() error }); ok {
				err = closer.CloseWrite()
			}
		}
		results <- result{err: err}
	}
	go pump(right, left)
	go pump(left, right)
	for range 2 {
		if result := <-results; result.err != nil {
			closeBoth()
		}
	}
}

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
