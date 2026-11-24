package registry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func awaitNodeLinkChannel(t *testing.T, r *Registry) *nodeChannel {
	t.Helper()
	deadline := time.After(3 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if conn, ok := r.node("n1"); ok {
			return conn.(*nodeChannel)
		}
		select {
		case <-deadline:
			t.Fatal("node-link not registered")
		case <-tick.C:
		}
	}
}

func awaitNodeLinkEvent(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestNodeLinkTeardownJoinsWriterAndFencesStaleSend(t *testing.T) {
	r := testReg(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	body, peer := io.Pipe()
	defer peer.Close()
	defer body.Close()
	var output bytes.Buffer
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	flushes := 0
	flush := func() {
		flushes++
		if flushes == 2 { // The first flush is the Hello.
			close(entered)
			<-release
		}
	}
	go func() {
		defer close(done)
		if err := r.serveNodeLinkLocal(ctx, &output, flush, body, &routesync.NodeRegister{
			NodeID: "n1", GroupSupportVersion: routesync.GroupSupportVersion,
		}); err != nil {
			t.Error(err)
		}
	}()
	conn := awaitNodeLinkChannel(t, r)
	sent := make(chan error, 1)
	go func() {
		sent <- conn.send(&routesync.Command{CmdID: "in-flight", Kind: routesync.CmdDelete})
	}()
	awaitNodeLinkEvent(t, entered, "in-flight Flush")
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for !conn.closing.Load() {
		select {
		case <-done:
			t.Fatal("handler returned while its response writer was still in Flush")
		case <-deadline:
			t.Fatal("handler did not fence the closing channel")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	// An old channel reference must fail even while teardown is joining the
	// admitted writer. It must not queue another write behind that writer.
	if err := conn.send(&routesync.Command{CmdID: "stale", Kind: routesync.CmdDelete}); !errors.Is(err, ErrNodeGone) {
		t.Fatalf("stale send = %v, want ErrNodeGone", err)
	}
	select {
	case <-done:
		t.Fatal("handler returned before the admitted Flush completed")
	default:
	}
	unblock()
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("admitted writer did not finish")
	}
	awaitNodeLinkEvent(t, done, "handler return")
	before := output.Len()
	if err := conn.send(&routesync.Command{CmdID: "late", Kind: routesync.CmdDelete}); !errors.Is(err, ErrNodeGone) || output.Len() != before || flushes != 2 {
		t.Fatalf("send after handler return: %v, bytes=%d -> %d, flushes=%d", err, before, output.Len(), flushes)
	}
}

type nodeLinkFlowWriter struct {
	http.ResponseWriter
	entered chan struct{}
	once    sync.Once
}

func (w *nodeLinkFlowWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *nodeLinkFlowWriter) Flush()                      { w.ResponseWriter.(http.Flusher).Flush() }
func (w *nodeLinkFlowWriter) Write(p []byte) (int, error) {
	if len(p) > 32*1024 {
		w.once.Do(func() { close(w.entered) })
	}
	return w.ResponseWriter.Write(p)
}

// A request half-close is not necessarily a canceled HTTP/2 stream. With a
// tiny receive window and an unread response, merely acquiring the send mutex
// at teardown would deadlock the handler on the admitted writer.
func TestNodeLinkTeardownInterruptsFlowControlledWriter(t *testing.T) {
	r := testReg(t)
	entered, done := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(done)
		r.ServeNodeLink(&nodeLinkFlowWriter{ResponseWriter: w, entered: entered}, req)
	}), &http2.Server{}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	transport, err := http2.ConfigureTransports(&http.Transport{HTTP2: &http.HTTP2Config{MaxReceiveBufferPerStream: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	transport.AllowHTTP = true
	client, err := transport.NewClientConn(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	body, peer := io.Pipe()
	defer peer.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+routesync.NodeLinkPath, body)
	if err != nil {
		t.Fatal(err)
	}
	registered := make(chan error, 1)
	go func() {
		registered <- routesync.WriteMsg(peer, &routesync.Msg{Type: routesync.TypeNodeRegister, NodeReg: &routesync.NodeRegister{
			NodeID: "n1", GroupSupportVersion: routesync.GroupSupportVersion,
		}})
	}()
	resp, err := client.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if err := <-registered; err != nil {
		t.Fatal(err)
	}
	hello, err := routesync.ReadMsg(resp.Body)
	if err != nil || hello.Type != routesync.TypeHello {
		t.Fatalf("hello: %+v %v", hello, err)
	}
	conn := awaitNodeLinkChannel(t, r)
	sent := make(chan error, 1)
	go func() {
		sent <- conn.send(&routesync.Command{CmdID: "blocked", Kind: routesync.CmdCreate, Config: map[string]string{"payload": strings.Repeat("x", 512*1024)}})
	}()
	awaitNodeLinkEvent(t, entered, "flow-controlled write")
	select {
	case err := <-sent:
		t.Fatalf("write unexpectedly completed without a receiver: %v", err)
	default:
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	awaitNodeLinkEvent(t, done, "handler return without reading response")
	select {
	case err := <-sent:
		if err == nil || errors.Is(err, ErrNodeGone) {
			t.Fatalf("in-flight write outcome = %v, want transport failure", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight writer did not end")
	}
	if err := conn.send(&routesync.Command{CmdID: "stale", Kind: routesync.CmdDelete}); !errors.Is(err, ErrNodeGone) {
		t.Fatalf("stale send = %v, want ErrNodeGone", err)
	}
}
