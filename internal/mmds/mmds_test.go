package mmds

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeSource struct {
	fip         map[string]string
	info        map[string][2]string
	routes      map[string]map[string]MMDSRoute
	routeErr    error
	incarnation map[string]string
	unavailable bool
}

func (f fakeSource) MMDSAvailable() bool { return !f.unavailable }

func (f fakeSource) ByFloatingIP(ip string) (string, bool) { sid, ok := f.fip[ip]; return sid, ok }
func (f fakeSource) SandboxInfo(sid string) (string, string, bool) {
	v, ok := f.info[sid]
	return v[0], v[1], ok
}

// MmdsSecret returns a deterministic per-sandbox secret for known sandboxes (a
// stand-in for keys.MmdsSecret), and ok=false for unknown ids so a forged token's
// sid fails to verify.
func (f fakeSource) MmdsSecret(sid string) ([]byte, bool) {
	if _, ok := f.info[sid]; !ok {
		return nil, false
	}
	return []byte("secret-for-" + sid), true
}

func (f fakeSource) Incarnation(sid string) (string, bool) {
	if f.incarnation != nil {
		value, ok := f.incarnation[sid]
		return value, ok
	}
	if _, ok := f.info[sid]; !ok {
		return "", false
	}
	return "run-for-" + sid, true
}

func (f fakeSource) MMDSRoute(_ context.Context, sid, path string) (MMDSRoute, bool, error) {
	if f.routeErr != nil {
		return MMDSRoute{}, false, f.routeErr
	}
	route, ok := f.routes[sid][path]
	return route, ok, nil
}

func TestPutGetFlow(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()

	// PUT from the sandbox's floating IP -> a session token.
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "100.100.96.5:34567"
	req.Header.Set("X-metadata-token-ttl-seconds", "60")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || w.Body.Len() == 0 {
		t.Fatalf("PUT: code=%d body=%q", w.Code, w.Body.String())
	}
	token := w.Body.String()

	// GET with the token from the same source IP -> the sandbox metadata.
	req = httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("GET: code=%d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{`"instanceID":"sbx-1"`, `"envID":"tmpl-1"`, `"accessTokenHash":"` + HashToken("tok-abc") + `"`} {
		if !strings.Contains(body, want) {
			t.Errorf("GET body missing %q: %s", want, body)
		}
	}

	// A forged/tampered token -> 401 (unforgeable without the per-sandbox secret).
	req = httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", "sbx-evil.deadbeef")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("GET forged token: code=%d (want 401)", w.Code)
	}

	// PUT from an unregistered floating IP -> 503 after the park (envd's poll retries).
	req = httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "100.100.96.9:1"
	req.Header.Set("X-metadata-token-ttl-seconds", "60")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("PUT unknown ip: code=%d (want 503)", w.Code)
	}
}

func TestHashToken(t *testing.T) {
	h := HashToken("hello")
	if len(h) != 128 { // keys.HashAccessTokenBytes = hex(sha512) = 64 bytes = 128 hex chars
		t.Fatalf("hash not 128-char hex sha512: %q (len %d)", h, len(h))
	}
	if h != HashToken("hello") {
		t.Fatal("hash not deterministic")
	}
	if HashToken("a") == HashToken("b") {
		t.Fatal("distinct tokens hashed equal")
	}
}

// These tests pin the scoped peer-close wait inside writeResponse: it parks
// only on fully length-delimited HTTP/1.1 Connection: close business
// responses, a slow reader never loses body bytes, every other shape keeps
// net/http's normal close semantics, and write-path errors surface without
// waiting.

// Slow-reader coverage keeps reading this body after peerCloseGrace expires.
var testBody64KiB = bytes.Repeat([]byte("A-b."), 1<<14)

func testSource() fakeSource {
	return fakeSource{
		fip:  map[string]string{"127.0.0.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {
				"/large": {Type: "static", ContentType: "application/octet-stream", Body: testBody64KiB},
				"/empty": {Type: "static", ContentType: "text/plain"},
			},
		},
	}
}

// newTestServer serves the full MMDS handler chain on real TCP.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(New(testSource(), time.Second, nil).Handler())
	server.Config.MaxHeaderBytes = maxHeaderBytes
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func mintLiveToken(t *testing.T, url string) string {
	t.Helper()
	put, err := http.NewRequest(http.MethodPut, url+"/latest/api/token", nil)
	if err != nil {
		t.Fatal(err)
	}
	put.Header.Set("X-metadata-token-ttl-seconds", "60")
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport, Timeout: 2 * time.Second}).Do(put)
	if err != nil {
		t.Fatal(err)
	}
	token, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("PUT token status=%d body=%q err=%v", response.StatusCode, token, err)
	}
	return string(token)
}

// dialRequest opens a raw connection, writes a complete request, and returns
// the connection and a buffered reader for it.
func dialRequest(t *testing.T, address, request string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReader(conn)
}

// --- writeResponse unit coverage: applicability and write-path errors ---

// flushStubWriter records the framed write; FlushError mirrors the optional
// interface net/http's ResponseController uses to surface flush errors, and
// the knobs simulate write, short-write, and flush failures.
type flushStubWriter struct {
	header   http.Header
	status   int
	writeErr error
	shortBy  int
	flushErr error
	written  int
	flushes  int
}

func newFlushStub() *flushStubWriter { return &flushStubWriter{header: make(http.Header)} }

func (w *flushStubWriter) Header() http.Header    { return w.header }
func (w *flushStubWriter) WriteHeader(status int) { w.status = status }
func (w *flushStubWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	w.written += len(p)
	return len(p) - w.shortBy, nil
}
func (w *flushStubWriter) FlushError() error {
	w.flushes++
	return w.flushErr
}

// bareStubWriter has no Flush method at all.
type bareStubWriter struct {
	header  http.Header
	status  int
	written int
}

func (w *bareStubWriter) Header() http.Header    { return w.header }
func (w *bareStubWriter) WriteHeader(status int) { w.status = status }
func (w *bareStubWriter) Write(p []byte) (int, error) {
	w.written += len(p)
	return len(p), nil
}

func applicabilityRequest(t *testing.T, mutate func(*http.Request)) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	if mutate != nil {
		mutate(request)
	}
	return request
}

func TestWriteResponsePeerCloseApplicability(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*http.Request)
		status   int
		body     []byte
		wantWait bool
	}{
		{
			name:     "http/1.1 connection close waits",
			mutate:   func(r *http.Request) { r.Close = true },
			body:     []byte("metadata"),
			wantWait: true,
		},
		{
			name: "keep-alive skips the wait",
			body: []byte("metadata"),
		},
		{
			name:   "http/1.0 skips the wait",
			mutate: func(r *http.Request) { r.Close = true; r.ProtoMinor = 0 },
			body:   []byte("metadata"),
		},
		{
			name: "bodied request skips the wait",
			mutate: func(r *http.Request) {
				r.Close = true
				r.ContentLength = 64
			},
			body: []byte("metadata"),
		},
		{
			name: "transfer-encoded request skips the wait",
			mutate: func(r *http.Request) {
				r.Close = true
				r.TransferEncoding = []string{"chunked"}
			},
			body: []byte("metadata"),
		},
		{
			name:   "bodiless status skips the wait",
			mutate: func(r *http.Request) { r.Close = true },
			status: http.StatusNoContent,
		},
		{
			name:     "empty body with legal zero length still waits",
			mutate:   func(r *http.Request) { r.Close = true },
			body:     nil,
			wantWait: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writer := newFlushStub()
			status := tc.status
			if status == 0 {
				status = http.StatusOK
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			request := applicabilityRequest(t, tc.mutate).WithContext(ctx)
			if err := writeResponse(writer, request, status, "text/plain", tc.body); err != nil {
				t.Fatalf("writeResponse error = %v", err)
			}
			if got := writer.flushes != 0; got != tc.wantWait {
				t.Fatalf("flushed = %v, want %v", got, tc.wantWait)
			}
		})
	}
}

func TestWriteResponseWriterFailures(t *testing.T) {
	cases := []struct {
		name    string
		writer  http.ResponseWriter
		wantErr error
	}{
		{
			name:    "write error",
			writer:  &flushStubWriter{header: make(http.Header), writeErr: io.ErrClosedPipe},
			wantErr: io.ErrClosedPipe,
		},
		{
			name:    "short write",
			writer:  &flushStubWriter{header: make(http.Header), shortBy: 1},
			wantErr: io.ErrShortWrite,
		},
		{
			name:   "no flusher",
			writer: &bareStubWriter{header: make(http.Header)},
		},
		{
			name:    "flush error",
			writer:  &flushStubWriter{header: make(http.Header), flushErr: io.ErrClosedPipe},
			wantErr: io.ErrClosedPipe,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := applicabilityRequest(t, func(r *http.Request) { r.Close = true })
			started := time.Now()
			err := writeResponse(tc.writer, request, http.StatusOK, "text/plain", []byte("metadata"))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("writeResponse error = %v, want %v", err, tc.wantErr)
			}
			if elapsed := time.Since(started); elapsed >= peerCloseGrace/2 {
				t.Fatalf("writeResponse took %v after a write failure, want no wait", elapsed)
			}
		})
	}
}

func TestClientCloseWakesServerWait(t *testing.T) {
	server := httptest.NewUnstartedServer(New(testSource(), time.Second, nil).Handler())
	serverClosed := make(chan time.Time, 64)
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			serverClosed <- time.Now()
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}

	exchange := func(request *http.Request, what string, attempt int) []byte {
		t.Helper()
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		status, contentLength := response.StatusCode, response.ContentLength
		clientDone := time.Now()
		_ = response.Body.Close() // tears the keep-alives-disabled connection down
		if err != nil || status != http.StatusOK {
			t.Fatalf("%s attempt %d: status=%d body=%q err=%v", what, attempt, status, body, err)
		}
		if contentLength != int64(len(body)) {
			t.Fatalf("%s attempt %d: Content-Length=%d, body=%d", what, attempt, contentLength, len(body))
		}
		select {
		case ended := <-serverClosed:
			if elapsed := ended.Sub(clientDone); elapsed >= peerCloseGrace/2 {
				t.Fatalf("%s attempt %d: server closed %v after the client finished; the client close did not wake the wait", what, attempt, elapsed)
			}
		case <-time.After(2 * peerCloseGrace):
			t.Fatalf("%s attempt %d: server never closed the connection", what, attempt)
		}
		return body
	}

	for attempt := 0; attempt < 2; attempt++ {
		put, err := http.NewRequest(http.MethodPut, server.URL+"/latest/api/token", nil)
		if err != nil {
			t.Fatal(err)
		}
		put.Header.Set("X-metadata-token-ttl-seconds", "60")
		token := exchange(put, "PUT", attempt)

		get, err := http.NewRequest(http.MethodGet, server.URL+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		get.Header.Set("X-metadata-token", string(token))
		if body := exchange(get, "GET", attempt); len(body) == 0 {
			t.Fatal("empty root document")
		}
	}
}

// Wait for the server's close before consuming the body: this covers both
// the bounded grace and a reader that outlives it, without scheduling sleeps.
func TestPeerCloseGracePreservesUnreadBody(t *testing.T) {
	server := httptest.NewUnstartedServer(New(testSource(), time.Second, nil).Handler())
	closed := make(chan time.Time, 2)
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- time.Now()
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	token := mintLiveToken(t, server.URL)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("token connection did not close")
	}

	start := time.Now()
	_, reader := dialRequest(t, strings.TrimPrefix(server.URL, "http://"),
		"GET /large HTTP/1.1\r\nHost: test\r\nConnection: close\r\nX-metadata-token: "+token+"\r\n\r\n")
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ended := <-closed:
		if elapsed := ended.Sub(start); elapsed < peerCloseGrace {
			t.Fatalf("server closed after %v, before grace expired", elapsed)
		}
	case <-time.After(2 * peerCloseGrace):
		t.Fatal("server did not close after the bounded grace")
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || !bytes.Equal(body, testBody64KiB) {
		t.Fatalf("body after server close: %d bytes, err=%v; want full %d bytes", len(body), err, len(testBody64KiB))
	}
	if response.ContentLength != int64(len(body)) {
		t.Fatalf("Content-Length=%d, body=%d", response.ContentLength, len(body))
	}
}

func TestKeepAliveConnectionIsReused(t *testing.T) {
	server := newTestServer(t)
	token := mintLiveToken(t, server.URL)
	request := "GET / HTTP/1.1\r\nHost: test\r\nX-metadata-token: " + token + "\r\n\r\n"
	conn, reader := dialRequest(t, strings.TrimPrefix(server.URL, "http://"), request)

	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			if _, err := io.WriteString(conn, request); err != nil {
				t.Fatal(err)
			}
		}
		start := time.Now()
		response, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err)
		}
		if response.Close {
			t.Fatalf("request %d: server closed a keep-alive connection", attempt)
		}
		if elapsed := time.Since(start); elapsed >= peerCloseGrace/2 {
			t.Fatalf("keep-alive request %d took %v, want no wait", attempt, elapsed)
		}
		if len(body) == 0 {
			t.Fatal("empty root document")
		}
	}
}

func TestPipelinedRequestAfterCloseIsNotServed(t *testing.T) {
	server := newTestServer(t)
	token := mintLiveToken(t, server.URL)

	request := "GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\nX-metadata-token: " + token + "\r\n\r\n" +
		"GET /large HTTP/1.1\r\nHost: test\r\nX-metadata-token: " + token + "\r\n\r\n"
	start := time.Now()
	_, reader := dialRequest(t, strings.TrimPrefix(server.URL, "http://"), request)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatalf("first response body: %v", err)
	}
	_ = response.Body.Close()
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read to EOF failed: %v", err)
	}
	if bytes.Contains(rest, []byte("HTTP/1.1 ")) {
		t.Fatalf("pipelined request was answered: %q", rest)
	}
	if elapsed := time.Since(start); elapsed > 3*peerCloseGrace {
		t.Fatalf("exchange took %v, the server waited for the pipelined request", elapsed)
	}
}

// Observe handler completion separately from the production Serve lifecycle.
func TestCancellation(t *testing.T) {
	t.Run("parked handlers exit", testCancellationStopsParkedHandlers)
	t.Run("production Serve closes connections", testServeCancellation)
}

func testCancellationStopsParkedHandlers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const connections = 4
	handlerDone := make(chan struct{}, connections)
	inner := New(testSource(), time.Second, nil).Handler()
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			inner.ServeHTTP(w, r)
			if r.Method == http.MethodGet {
				handlerDone <- struct{}{}
			}
		}),
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(listener) }()
	t.Cleanup(func() {
		_ = srv.Close()
		<-serveDone
	})

	address := listener.Addr().String()
	token := mintLiveToken(t, "http://"+address)

	clients := make([]net.Conn, 0, connections)
	defer func() {
		cancel()
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	for range connections {
		conn, reader := dialRequest(t, address, "GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\nX-metadata-token: "+token+"\r\n\r\n")
		clients = append(clients, conn)
		// The framed response proves each wait is armed before cancellation.
		response, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(response.Body); err != nil {
			t.Fatal(err)
		}
	}

	cancel()
	within := time.After(peerCloseGrace / 2)
	for range connections {
		select {
		case <-handlerDone:
		case <-within:
			t.Fatal("parked handlers still running after Serve cancellation")
		}
	}
}

func testServeCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(testSource(), time.Second, nil).Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
	})
	address := listener.Addr().String()
	token := mintLiveToken(t, "http://"+address)
	_, reader := dialRequest(t, address, "GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\nX-metadata-token: "+token+"\r\n\r\n")
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	cancel()
	start := time.Now()
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Fatalf("read after cancellation = %v, want EOF", err)
	}
	if elapsed := time.Since(start); elapsed >= peerCloseGrace/2 {
		t.Fatalf("connection remained open for %v after cancellation", elapsed)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(peerCloseGrace / 2):
		t.Fatal("Serve did not exit after cancellation")
	}
}
