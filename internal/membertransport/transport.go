// Package membertransport provides the HTTP transport used by registry/scaler
// membership failure-detection runtimes. It exposes packet and stream channels
// over the shared control-plane mux so deployments do not need UDP reachability.
package membertransport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	PathPrefix  = "/internal/memberlist"
	PacketPath  = PathPrefix + "/packet/"
	StreamPath  = PathPrefix + "/stream/"
	HeaderFrom  = "X-Kuasar-Memberlist-From"
	maxPacketSz = 8 << 20
)

var (
	ErrClosed       = errors.New("membertransport: closed")
	ErrUnknownLabel = errors.New("membertransport: unknown label")
)

type Packet struct {
	Buf       []byte
	From      string
	Timestamp time.Time
}

type Resolver interface {
	ResolveMember(addr string) (baseURL string, ok bool)
}

type StaticResolver map[string]string

func (r StaticResolver) ResolveMember(addr string) (string, bool) {
	v, ok := r[addr]
	return v, ok
}

type Mux struct {
	mu         sync.RWMutex
	transports map[string]*Transport
}

func NewMux() *Mux {
	return &Mux{transports: map[string]*Transport{}}
}

func (m *Mux) Register(t *Transport) error {
	if t == nil || t.Label == "" {
		return fmt.Errorf("membertransport: empty transport label")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if old := m.transports[t.Label]; old != nil {
		return nil
	}
	m.transports[t.Label] = t
	return nil
}

func (m *Mux) Unregister(label string) {
	m.mu.Lock()
	delete(m.transports, label)
	m.mu.Unlock()
}

func (m *Mux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case strings.HasPrefix(req.URL.Path, PacketPath):
		m.servePacket(w, req, strings.TrimPrefix(req.URL.Path, PacketPath))
	case strings.HasPrefix(req.URL.Path, StreamPath):
		m.serveStream(w, req, strings.TrimPrefix(req.URL.Path, StreamPath))
	default:
		http.NotFound(w, req)
	}
}

func (m *Mux) transport(labelPath string) (*Transport, string, bool) {
	label, err := url.PathUnescape(path.Clean("/" + labelPath)[1:])
	if err != nil || label == "" || label == "." {
		return nil, "", false
	}
	m.mu.RLock()
	t := m.transports[label]
	m.mu.RUnlock()
	return t, label, t != nil
}

func (m *Mux) servePacket(w http.ResponseWriter, req *http.Request, labelPath string) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	t, label, ok := m.transport(labelPath)
	if !ok {
		http.Error(w, fmt.Sprintf("%s: %s", ErrUnknownLabel, label), http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, maxPacketSz+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) > maxPacketSz {
		http.Error(w, "packet too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := t.deliverPacket(Packet{Buf: body, From: req.Header.Get(HeaderFrom), Timestamp: time.Now()}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *Mux) serveStream(w http.ResponseWriter, req *http.Request, labelPath string) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	t, label, ok := m.transport(labelPath)
	if !ok {
		http.Error(w, fmt.Sprintf("%s: %s", ErrUnknownLabel, label), http.StatusNotFound)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream needs a flushable writer", http.StatusInternalServerError)
		return
	}
	server, client := net.Pipe()
	if err := t.deliverStream(server); err != nil {
		_ = server.Close()
		_ = client.Close()
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(client, req.Body)
		_ = client.Close()
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(flushWriter{w: w, flush: flusher.Flush}, client)
		_ = client.Close()
		done <- struct{}{}
	}()
	select {
	case <-req.Context().Done():
	case <-done:
	}
}

func (m *Mux) OpenLocalStream(label string) (net.Conn, error) {
	m.mu.RLock()
	t := m.transports[label]
	m.mu.RUnlock()
	if t == nil {
		return nil, ErrUnknownLabel
	}
	server, client := net.Pipe()
	if err := t.deliverStream(server); err != nil {
		_ = server.Close()
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

type Transport struct {
	Label    string
	Self     string
	Resolver Resolver
	Client   *http.Client

	packetCh chan *Packet
	streamCh chan net.Conn
	done     chan struct{}
	once     sync.Once
}

func New(label, self string, resolver Resolver, client *http.Client) *Transport {
	if client == nil {
		client = http.DefaultClient
	}
	return &Transport{
		Label:    label,
		Self:     self,
		Resolver: resolver,
		Client:   client,
		packetCh: make(chan *Packet, 1024),
		streamCh: make(chan net.Conn, 128),
		done:     make(chan struct{}),
	}
}

func (t *Transport) PacketCh() <-chan *Packet { return t.packetCh }

func (t *Transport) StreamCh() <-chan net.Conn { return t.streamCh }

func (t *Transport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil, 0, fmt.Errorf("membertransport: invalid advertise ip %q", ip)
	}
	return parsed, port, nil
}

func (t *Transport) WriteTo(buf []byte, addr string) (time.Time, error) {
	base, err := t.resolve(addr)
	if err != nil {
		return time.Now(), err
	}
	u := strings.TrimRight(base, "/") + PacketPath + url.PathEscape(t.Label)
	now := time.Now()
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(buf))
	if err != nil {
		return now, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(HeaderFrom, t.Self)
	resp, err := t.Client.Do(req)
	if err != nil {
		return now, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return now, fmt.Errorf("membertransport: packet %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return now, nil
}

func (t *Transport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	base, err := t.resolve(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	pr, pw := io.Pipe()
	u := strings.TrimRight(base, "/") + StreamPath + url.PathEscape(t.Label)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, pr)
	if err != nil {
		cancel()
		_ = pr.Close()
		_ = pw.Close()
		return nil, err
	}
	req.Header.Set(HeaderFrom, t.Self)
	respCh := make(chan dialResult, 1)
	go func() {
		resp, err := t.Client.Do(req)
		respCh <- dialResult{resp: resp, err: err}
	}()
	select {
	case <-ctx.Done():
		cancel()
		_ = pr.Close()
		_ = pw.Close()
		return nil, ctx.Err()
	case res := <-respCh:
		if res.err != nil {
			cancel()
			_ = pr.Close()
			_ = pw.Close()
			return nil, res.err
		}
		if res.resp.StatusCode/100 != 2 {
			body, _ := io.ReadAll(io.LimitReader(res.resp.Body, 4096))
			_ = res.resp.Body.Close()
			cancel()
			_ = pr.Close()
			_ = pw.Close()
			return nil, fmt.Errorf("membertransport: stream %s: %s", res.resp.Status, strings.TrimSpace(string(body)))
		}
		return &httpConn{r: res.resp.Body, w: pw, cancel: cancel, local: t.Self, remote: addr}, nil
	}
}

func (t *Transport) Shutdown() error {
	t.once.Do(func() { close(t.done) })
	return nil
}

func (t *Transport) resolve(addr string) (string, error) {
	select {
	case <-t.done:
		return "", ErrClosed
	default:
	}
	if t.Resolver == nil {
		return "", fmt.Errorf("membertransport: resolver is nil")
	}
	base, ok := t.Resolver.ResolveMember(addr)
	if !ok || base == "" {
		return "", fmt.Errorf("membertransport: no endpoint for %q", addr)
	}
	return base, nil
}

func (t *Transport) deliverPacket(pkt Packet) error {
	select {
	case <-t.done:
		return ErrClosed
	case t.packetCh <- &pkt:
		return nil
	default:
		return fmt.Errorf("membertransport: packet channel full")
	}
}

func (t *Transport) deliverStream(conn net.Conn) error {
	select {
	case <-t.done:
		return ErrClosed
	case t.streamCh <- conn:
		return nil
	default:
		return fmt.Errorf("membertransport: stream channel full")
	}
}

type dialResult struct {
	resp *http.Response
	err  error
}

type flushWriter struct {
	w     io.Writer
	flush func()
}

func (w flushWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.flush()
	return n, err
}

type httpConn struct {
	r      io.ReadCloser
	w      *io.PipeWriter
	cancel context.CancelFunc
	local  string
	remote string
}

func (c *httpConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *httpConn) Write(p []byte) (int, error) { return c.w.Write(p) }

func (c *httpConn) Close() error {
	c.cancel()
	err1 := c.w.Close()
	err2 := c.r.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (c *httpConn) LocalAddr() net.Addr              { return stringAddr(c.local) }
func (c *httpConn) RemoteAddr() net.Addr             { return stringAddr(c.remote) }
func (c *httpConn) SetDeadline(time.Time) error      { return nil }
func (c *httpConn) SetReadDeadline(time.Time) error  { return nil }
func (c *httpConn) SetWriteDeadline(time.Time) error { return nil }

type stringAddr string

func (a stringAddr) Network() string { return "member-http" }
func (a stringAddr) String() string  { return string(a) }
