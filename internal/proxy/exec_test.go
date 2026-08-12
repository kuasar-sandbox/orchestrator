package proxy_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

const execTestServiceSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type execTestRouter struct {
	identity       proxy.ExecIdentity
	found          bool
	lookupErr      error
	activateResult proxy.ExecIdentity
	activateFound  bool
	activateErr    error
	lookupCalls    atomic.Int32
	activateCalls  atomic.Int32
	routeCalls     atomic.Int32
	activateCtx    chan context.Context
}

func (r *execTestRouter) LookupRoute(context.Context, string, proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	r.routeCalls.Add(1)
	return proxy.RouteBinding{Kind: proxy.KindDeny}, true, nil
}

func (r *execTestRouter) ActivateRoute(context.Context, proxy.RouteBinding) (proxy.Route, bool, error) {
	r.routeCalls.Add(1)
	return proxy.Route{}, false, nil
}

func (r *execTestRouter) LookupExec(context.Context, string) (proxy.ExecIdentity, bool, error) {
	r.lookupCalls.Add(1)
	return r.identity, r.found, r.lookupErr
}

func (r *execTestRouter) ActivateExec(ctx context.Context, _ string, expected proxy.ExecIdentity) (proxy.ExecIdentity, bool, error) {
	r.activateCalls.Add(1)
	if r.activateCtx != nil {
		select {
		case r.activateCtx <- ctx:
		default:
		}
	}
	if expected != r.identity {
		return proxy.ExecIdentity{}, false, nil
	}
	return r.activateResult, r.activateFound, r.activateErr
}

func TestExecRejectsNonConnectAndInvalidTokenWithoutLifecycleSideEffects(t *testing.T) {
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		AuthSandboxID: "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	var dials atomic.Int32
	px := proxy.NewWithDialer(router, func() string { return "off" }, discardExecLogger(), nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, fmt.Errorf("unexpected dial")
		}, t.TempDir())

	t.Run("ordinary HTTP", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "http://sandbox/exec", nil)
		req.Header.Set(proxy.HeaderSandboxID, "node-s1")
		req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
		resp := httptest.NewRecorder()
		px.ServeHTTP(resp, req)
		if resp.Code != http.StatusMethodNotAllowed || resp.Header().Get("Allow") != http.MethodConnect {
			t.Fatalf("ordinary exec response = %d Allow=%q, want 405 Allow=CONNECT", resp.Code, resp.Header().Get("Allow"))
		}
		if router.lookupCalls.Load() != 0 || router.activateCalls.Load() != 0 || router.routeCalls.Load() != 0 || dials.Load() != 0 {
			t.Fatalf("ordinary exec caused side effects: lookup=%d activate=%d route=%d dial=%d",
				router.lookupCalls.Load(), router.activateCalls.Load(), router.routeCalls.Load(), dials.Load())
		}
	})

	t.Run("invalid KAT ignores auth off mode", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", nil)
		req.Host = "sandbox:443"
		req.Header.Set(proxy.HeaderSandboxID, "node-s1")
		req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
		req.Header.Set(proxy.HeaderAccessToken, "not-a-kat")
		resp := httptest.NewRecorder()
		px.ServeHTTP(resp, req)
		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("invalid KAT response = %d, want 401", resp.Code)
		}
		if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != 0 || router.routeCalls.Load() != 0 || dials.Load() != 0 {
			t.Fatalf("invalid KAT caused lifecycle side effects: lookup=%d activate=%d route=%d dial=%d",
				router.lookupCalls.Load(), router.activateCalls.Load(), router.routeCalls.Load(), dials.Load())
		}
	})
}

func TestExecRejectsForwardTokenAndChangedIdentityBeforeDial(t *testing.T) {
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		AuthSandboxID: "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	forwardToken, err := keys.MintForwardAccessToken(execTestServiceSecret, identity.AuthSandboxID)
	if err != nil {
		t.Fatal(err)
	}
	validToken, err := keys.MintExecAccessToken(execTestServiceSecret, identity.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name             string
		token            string
		activateIdentity proxy.ExecIdentity
		wantActivations  int32
	}{
		{name: "forward audience", token: forwardToken, activateIdentity: identity, wantActivations: 0},
		{name: "identity changed after activation", token: validToken, activateIdentity: proxy.ExecIdentity{
			NodeSandboxID: "node-s1", AuthSandboxID: "other", ServiceSecret: execTestServiceSecret,
		}, wantActivations: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := &execTestRouter{
				identity: identity, found: true,
				activateResult: test.activateIdentity, activateFound: true,
			}
			var dials atomic.Int32
			px := proxy.NewWithDialer(router, func() string { return "off" }, discardExecLogger(), nil,
				func(context.Context, proxy.Route) (net.Conn, error) {
					dials.Add(1)
					return nil, fmt.Errorf("unexpected dial")
				}, t.TempDir())
			req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", nil)
			req.Host = "sandbox:443"
			req.Header.Set(proxy.HeaderSandboxID, "node-s1")
			req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
			req.Header.Set(proxy.HeaderAccessToken, test.token)
			resp := httptest.NewRecorder()
			px.ServeHTTP(resp, req)
			if resp.Code != http.StatusUnauthorized {
				t.Fatalf("response = %d, want 401", resp.Code)
			}
			if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != test.wantActivations || router.routeCalls.Load() != 0 || dials.Load() != 0 {
				t.Fatalf("calls lookup=%d activate=%d route=%d dial=%d",
					router.lookupCalls.Load(), router.activateCalls.Load(), router.routeCalls.Load(), dials.Load())
			}
		})
	}
}

func TestExecActivationFailureReturnsServiceUnavailableBeforeDial(t *testing.T) {
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		AuthSandboxID: "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}
	router := &execTestRouter{
		identity: identity, found: true,
		activateErr: fmt.Errorf("activation timed out"),
	}
	var dials atomic.Int32
	px := proxy.NewWithDialer(router, func() string { return "off" }, discardExecLogger(), nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, fmt.Errorf("unexpected dial")
		}, t.TempDir())
	req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", nil)
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	req.Header.Set(proxy.HeaderAccessToken, token)
	resp := httptest.NewRecorder()
	px.ServeHTTP(resp, req)
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("activation failure response = %d, want 503", resp.Code)
	}
	if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != 1 || dials.Load() != 0 {
		t.Fatalf("calls lookup=%d activate=%d dial=%d",
			router.lookupCalls.Load(), router.activateCalls.Load(), dials.Load())
	}
}

func TestExecH1PreservesBufferedInputAndHalfCloseTail(t *testing.T) {
	runRoot := t.TempDir()
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		AuthSandboxID: "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	backend, backendDone := startExecBackend(t, runRoot, identity.NodeSandboxID, []byte("exec-response-tail"))
	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
		activateCtx: make(chan context.Context, 1),
	}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil, nil, runRoot)
	ts := httptest.NewServer(px)
	defer ts.Close()

	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}
	frame := execTestFrame(` {"type":"exec_request","exec":{"argv":["true"]}} `)
	clientTail := []byte("buffered-mux-tail")
	requestBytes := append(append([]byte(nil), frame...), clientTail...)

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tcpConn := conn.(*net.TCPConn)
	defer tcpConn.Close()
	_ = tcpConn.SetDeadline(time.Now().Add(5 * time.Second))
	var request bytes.Buffer
	fmt.Fprintf(&request, "CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\n%s: %s\r\n%s: exec\r\n%s: 8123\r\n%s: %s\r\n\r\n",
		proxy.HeaderSandboxID, identity.NodeSandboxID,
		proxy.HeaderSandboxService,
		proxy.HeaderSandboxPort,
		proxy.HeaderAccessToken, token)
	request.Write(requestBytes)
	if _, err := tcpConn.Write(request.Bytes()); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(tcpConn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want 200", resp.StatusCode)
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	// The server request context is canceled by this TCP read EOF even after
	// Hijack. The exec relay must still drain the reverse direction.
	requestCtx := <-router.activateCtx
	select {
	case <-requestCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("net/http request context did not observe client write-side EOF")
	}
	defer resp.Body.Close()
	gotResponse, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	gotRequest := <-backend
	backendErr := <-backendDone
	if got := string(gotResponse); got != "exec-response-tail" {
		t.Errorf("response tail = %q", got)
	}
	if !bytes.Equal(gotRequest, requestBytes) {
		t.Fatalf("backend request changed:\n got %q\nwant %q", gotRequest, requestBytes)
	}
	if backendErr != nil {
		t.Fatal(backendErr)
	}
	if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != 1 || router.routeCalls.Load() != 0 {
		t.Fatalf("calls lookup=%d activate=%d route=%d", router.lookupCalls.Load(), router.activateCalls.Load(), router.routeCalls.Load())
	}
}

func TestExecH2StreamsRequestAndFlushesTrailingResponse(t *testing.T) {
	runRoot := t.TempDir()
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-h2",
		AuthSandboxID: "stable-h2",
		ServiceSecret: execTestServiceSecret,
	}
	backend, backendDone := startExecBackend(t, runRoot, identity.NodeSandboxID, []byte("h2-response-tail"))
	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil, nil, runRoot)
	ts := httptest.NewUnstartedServer(px)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}
	frame := execTestFrame(`{"type":"exec_request"}`)
	requestBytes := append(append([]byte(nil), frame...), []byte("h2-mux-tail")...)
	bodyReader, bodyWriter := io.Pipe()
	req, err := http.NewRequest(http.MethodConnect, ts.URL, bodyReader)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	req.Header.Set(proxy.HeaderAccessToken, token)
	writeDone := make(chan error, 1)
	go func() {
		_, err := bodyWriter.Write(requestBytes)
		if closeErr := bodyWriter.Close(); err == nil {
			err = closeErr
		}
		writeDone <- err
	}()
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
		t.Fatalf("response = %s %d, want HTTP/2 200", resp.Proto, resp.StatusCode)
	}
	gotResponse, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(gotResponse); got != "h2-response-tail" {
		t.Fatalf("response tail = %q", got)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if gotRequest := <-backend; !bytes.Equal(gotRequest, requestBytes) {
		t.Fatalf("backend request changed:\n got %q\nwant %q", gotRequest, requestBytes)
	}
	if err := <-backendDone; err != nil {
		t.Fatal(err)
	}
}

func TestExecGateRejectsNonExecFirstFrameAfterConnect200(t *testing.T) {
	runRoot := t.TempDir()
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-gate",
		AuthSandboxID: "stable-gate",
		ServiceSecret: execTestServiceSecret,
	}
	dir := filepath.Join(runRoot, identity.NodeSandboxID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "ctl.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	backendBytes := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			backendBytes <- []byte("accept-error")
			return
		}
		defer conn.Close()
		got, _ := io.ReadAll(conn)
		backendBytes <- got
	}()

	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil, nil, runRoot)
	ts := httptest.NewServer(px)
	defer ts.Close()
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tcpConn := conn.(*net.TCPConn)
	defer tcpConn.Close()
	_ = tcpConn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(tcpConn, "CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\n%s: %s\r\n%s: exec\r\n%s: %s\r\n\r\n",
		proxy.HeaderSandboxID, identity.NodeSandboxID,
		proxy.HeaderSandboxService,
		proxy.HeaderAccessToken, token)
	reader := bufio.NewReader(tcpConn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want 200", resp.StatusCode)
	}
	if _, err := tcpConn.Write(execTestFrame(`{"type":"snapshot_request"}`)); err != nil {
		t.Fatal(err)
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if got := <-backendBytes; len(got) != 0 {
		t.Fatalf("gate forwarded non-exec first frame: %q", got)
	}
}

func TestExecH2ContextCancellationClosesCtlStream(t *testing.T) {
	runRoot := t.TempDir()
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-cancel",
		AuthSandboxID: "stable-cancel",
		ServiceSecret: execTestServiceSecret,
	}
	dir := filepath.Join(runRoot, identity.NodeSandboxID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "ctl.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	frame := execTestFrame(`{"type":"exec_request"}`)
	frameSeen := make(chan struct{})
	backendDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close()
		got := make([]byte, len(frame))
		if _, err := io.ReadFull(conn, got); err != nil {
			backendDone <- err
			return
		}
		if !bytes.Equal(got, frame) {
			backendDone <- fmt.Errorf("gate changed first frame")
			return
		}
		close(frameSeen)
		_, err = io.Copy(io.Discard, conn)
		backendDone <- err
	}()

	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil, nil, runRoot)
	ts := httptest.NewUnstartedServer(px)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}

	bodyReader, bodyWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodConnect, ts.URL, bodyReader)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	req.Header.Set(proxy.HeaderAccessToken, token)
	writeDone := make(chan error, 1)
	go func() {
		_, err := bodyWriter.Write(frame)
		writeDone <- err
	}()
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
		t.Fatalf("response = %s %d, want HTTP/2 200", resp.Proto, resp.StatusCode)
	}
	select {
	case <-frameSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("ctl backend did not receive accepted exec frame")
	}
	cancel()
	_ = bodyWriter.Close()
	_ = resp.Body.Close()
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-backendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request context cancellation did not close ctl stream")
	}
}

func TestExecH1FullClientCloseTerminatesHandlerAndCtlStream(t *testing.T) {
	runRoot := t.TempDir()
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-close",
		AuthSandboxID: "stable-close",
		ServiceSecret: execTestServiceSecret,
	}
	dir := filepath.Join(runRoot, identity.NodeSandboxID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "ctl.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	frame := execTestFrame(`{"type":"exec_request"}`)
	frameSeen := make(chan struct{})
	backendDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close()
		got := make([]byte, len(frame))
		if _, err := io.ReadFull(conn, got); err != nil {
			backendDone <- err
			return
		}
		if !bytes.Equal(got, frame) {
			backendDone <- fmt.Errorf("gate changed first frame")
			return
		}
		close(frameSeen)
		_, err = io.Copy(io.Discard, conn)
		backendDone <- err
	}()

	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil, nil, runRoot)
	handlerDone := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		px.ServeHTTP(w, r)
	}))
	defer ts.Close()
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tcpConn := conn.(*net.TCPConn)
	_ = tcpConn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(tcpConn, "CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\n%s: %s\r\n%s: exec\r\n%s: %s\r\n\r\n",
		proxy.HeaderSandboxID, identity.NodeSandboxID,
		proxy.HeaderSandboxService,
		proxy.HeaderAccessToken, token)
	resp, err := http.ReadResponse(bufio.NewReader(tcpConn), &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want 200", resp.StatusCode)
	}
	if _, err := tcpConn.Write(frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-frameSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("ctl backend did not receive accepted exec frame")
	}
	if err := tcpConn.SetLinger(0); err != nil {
		t.Fatal(err)
	}
	if err := tcpConn.Close(); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	select {
	case err := <-backendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("full client close did not close ctl stream")
	}
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("full client close left exec handler running")
	}
}

func startExecBackend(t *testing.T, runRoot, sid string, response []byte) (<-chan []byte, <-chan error) {
	t.Helper()
	dir := filepath.Join(runRoot, sid)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "ctl.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	request := make(chan []byte, 1)
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		got, err := io.ReadAll(conn)
		if err != nil {
			done <- err
			return
		}
		request <- got
		if _, err := conn.Write(response); err != nil {
			done <- err
			return
		}
		if closer, ok := conn.(interface{ CloseWrite() error }); ok {
			err = closer.CloseWrite()
		}
		done <- err
	}()
	return request, done
}

func execTestFrame(payload string) []byte {
	frame := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	return frame
}

func discardExecLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestExecUnavailableWithoutTrustedRunRoot(t *testing.T) {
	identity := proxy.ExecIdentity{NodeSandboxID: "node-s1", AuthSandboxID: "stable-s1", ServiceSecret: execTestServiceSecret}
	router := &execTestRouter{identity: identity, found: true, activateResult: identity, activateFound: true}
	px := proxy.New(router, func() string { return "enforce" }, discardExecLogger(), nil)
	req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", strings.NewReader("unused"))
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	resp := httptest.NewRecorder()
	px.ServeHTTP(resp, req)
	if resp.Code != http.StatusNotImplemented || router.lookupCalls.Load() != 0 || router.activateCalls.Load() != 0 {
		t.Fatalf("response=%d lookup=%d activate=%d, want side-effect-free 501", resp.Code, router.lookupCalls.Load(), router.activateCalls.Load())
	}
}
