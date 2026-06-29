package membergroup

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

const (
	PacketPath = "/internal/memberlist/packet"
	StreamPath = "/internal/memberlist/stream"
)

type Resolver func(name, addr string) (string, bool)

type Hub struct {
	mu         sync.RWMutex
	transports map[string]*HTTPTransport
}

func NewHub() *Hub {
	return &Hub{transports: map[string]*HTTPTransport{}}
}

func (h *Hub) Register(t *HTTPTransport) {
	if h == nil || t == nil || t.label == "" {
		return
	}
	h.mu.Lock()
	h.transports[t.label] = t
	h.mu.Unlock()
}

func (h *Hub) Unregister(label string, t *HTTPTransport) {
	if h == nil || label == "" {
		return
	}
	h.mu.Lock()
	if cur := h.transports[label]; cur == t {
		delete(h.transports, label)
	}
	h.mu.Unlock()
}

func (h *Hub) Mount(mux *http.ServeMux) {
	mux.HandleFunc(PacketPath, h.servePacket)
	mux.HandleFunc(StreamPath, h.serveStream)
}

func (h *Hub) transport(label string) *HTTPTransport {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.transports[label]
}

func (h *Hub) servePacket(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	label := req.URL.Query().Get("label")
	t := h.transport(label)
	if t == nil {
		http.Error(w, "unknown memberlist label", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pkt := &memberlist.Packet{Buf: body, From: nameAddr(req.URL.Query().Get("from")), Timestamp: time.Now()}
	select {
	case t.packetCh <- pkt:
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "memberlist packet queue full", http.StatusServiceUnavailable)
	}
}

func (h *Hub) serveStream(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodConnect {
		h.serveConnectStream(w, req)
		return
	}
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	label := req.URL.Query().Get("label")
	t := h.transport(label)
	if t == nil {
		http.Error(w, "unknown memberlist label", http.StatusNotFound)
		return
	}
	rc := http.NewResponseController(w)
	_ = rc.EnableFullDuplex()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_ = rc.Flush()
	conn := newServerConn(req.Context(), req.Body, w, rc, req.URL.Query().Get("from"))
	select {
	case t.streamCh <- conn:
	case <-req.Context().Done():
		_ = conn.Close()
		return
	case <-t.done:
		_ = conn.Close()
		return
	}
	select {
	case <-conn.done:
	case <-req.Context().Done():
		_ = conn.Close()
	case <-t.done:
		_ = conn.Close()
	}
}

func (h *Hub) serveConnectStream(w http.ResponseWriter, req *http.Request) {
	label := req.URL.Query().Get("label")
	t := h.transport(label)
	if t == nil {
		http.Error(w, "unknown memberlist label", http.StatusNotFound)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "memberlist stream requires hijackable http/1 connection", http.StatusInternalServerError)
		return
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return
	}
	if _, err := rw.WriteString("HTTP/1.1 200 OK\r\n\r\n"); err != nil {
		_ = conn.Close()
		return
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return
	}
	select {
	case t.streamCh <- conn:
	case <-req.Context().Done():
		_ = conn.Close()
	case <-t.done:
		_ = conn.Close()
	}
}

type HTTPTransport struct {
	label    string
	selfName string
	resolve  Resolver
	client   *http.Client
	packetCh chan *memberlist.Packet
	streamCh chan net.Conn
	done     chan struct{}
	once     sync.Once
}

func NewHTTPTransport(label, selfName string, resolve Resolver, tlsCfg *tls.Config) *HTTPTransport {
	return &HTTPTransport{
		label: label, selfName: selfName, resolve: resolve,
		client:   newStreamClient(tlsCfg),
		packetCh: make(chan *memberlist.Packet, 1024),
		streamCh: make(chan net.Conn, 128),
		done:     make(chan struct{}),
	}
}

func newStreamClient(tlsCfg *tls.Config) *http.Client {
	tr := &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: true}
	return &http.Client{Transport: tr}
}

func (t *HTTPTransport) FinalAdvertiseAddr(_ string, _ int) (net.IP, int, error) {
	return net.IPv4(127, 0, 0, 1), 1, nil
}

func (t *HTTPTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	return t.WriteToAddress(b, memberlist.Address{Addr: addr})
}

func (t *HTTPTransport) WriteToAddress(b []byte, addr memberlist.Address) (time.Time, error) {
	base, err := t.resolveBase(addr)
	if err != nil {
		return time.Time{}, err
	}
	u := memberURL(base, PacketPath, t.label, t.selfName)
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(b))
	if err != nil {
		return time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := t.client.Do(req)
	now := time.Now()
	if err != nil {
		return now, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return now, fmt.Errorf("memberlist packet %s: %s", addr.Name, resp.Status)
	}
	return now, nil
}

func (t *HTTPTransport) PacketCh() <-chan *memberlist.Packet { return t.packetCh }

func (t *HTTPTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return t.DialAddressTimeout(memberlist.Address{Addr: addr}, timeout)
}

func (t *HTTPTransport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	base, err := t.resolveBase(addr)
	if err != nil {
		return nil, err
	}
	if conn, err := t.dialConnect(base, addr, timeout); err == nil {
		return conn, nil
	} else if strings.HasPrefix(base, "http://") {
		return nil, err
	}
	pr, pw := io.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, memberURL(base, StreamPath, t.label, t.selfName), pr)
	if err != nil {
		cancel()
		_ = pr.Close()
		_ = pw.Close()
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := t.client.Do(req)
	if err != nil {
		cancel()
		_ = pr.Close()
		_ = pw.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		_ = resp.Body.Close()
		_ = pr.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("memberlist stream %s: %s", addr.Name, resp.Status)
	}
	return newClientConn(resp.Body, pw, cancel, t.selfName, addr.Name), nil
}

func (t *HTTPTransport) dialConnect(base string, addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" {
		return nil, fmt.Errorf("memberlist connect stream only supports http")
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", u.Host)
	if err != nil {
		return nil, err
	}
	target := memberURL(base, StreamPath, t.label, t.selfName)
	tu, _ := url.Parse(target)
	host := tu.Host
	if host == "" {
		host = u.Host
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %s?%s HTTP/1.1\r\nHost: %s\r\n\r\n", StreamPath, tu.RawQuery, host); err != nil {
		_ = conn.Close()
		return nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("memberlist stream %s: %s", addr.Name, resp.Status)
	}
	return &bufferedConn{Conn: conn, r: br}, nil
}

func (t *HTTPTransport) StreamCh() <-chan net.Conn { return t.streamCh }

func (t *HTTPTransport) Shutdown() error {
	t.once.Do(func() { close(t.done) })
	return nil
}

func (t *HTTPTransport) resolveBase(addr memberlist.Address) (string, error) {
	if t.resolve == nil {
		return "", fmt.Errorf("memberlist: no resolver for %s", addr.String())
	}
	base, ok := t.resolve(addr.Name, addr.Addr)
	if !ok || base == "" {
		return "", fmt.Errorf("memberlist: cannot resolve %s", addr.String())
	}
	return strings.TrimRight(base, "/"), nil
}

func memberURL(base, path, label, from string) string {
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "http://" + strings.TrimRight(base, "/") + path + "?label=" + url.QueryEscape(label) + "&from=" + url.QueryEscape(from)
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	q := u.Query()
	q.Set("label", label)
	q.Set("from", from)
	u.RawQuery = q.Encode()
	return u.String()
}

type nameAddr string

func (a nameAddr) Network() string { return "memberlist-http" }
func (a nameAddr) String() string  { return string(a) }
