package mmds

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// These tests pin the HTTP-layer form of the MMDS deferClose fix
// (waitForClientClose): a Connection: close exchange parks until the client
// closes (bounded by clientCloseGrace), the grace is skipped for keep-alive
// requests, and both client close and server shutdown wake the park promptly
// so the grace never accumulates.

// newCloseWaitServer serves waitForClientClose(inner) on a real http.Server
// and reports how long the middleware itself ran per request.
func newCloseWaitServer(t *testing.T, inner http.Handler) (*httptest.Server, <-chan time.Duration) {
	t.Helper()
	returned := make(chan time.Duration, 8)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		waitForClientClose(inner).ServeHTTP(w, r)
		returned <- time.Since(start)
	}))
	server.Start()
	t.Cleanup(server.Close)
	return server, returned
}

func waitDuration(t *testing.T, ch <-chan time.Duration, failure string) time.Duration {
	t.Helper()
	select {
	case elapsed := <-ch:
		return elapsed
	case <-time.After(2 * clientCloseGrace):
		t.Fatal(failure)
		return 0
	}
}

var closeWaitBody = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", "8")
	_, _ = w.Write([]byte("metadata"))
})

func TestWaitForClientCloseWakesWhenClientCloses(t *testing.T) {
	server, returned := newCloseWaitServer(t, closeWaitBody)
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}

	started := time.Now()
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(body) != "metadata" {
		t.Fatalf("body = %q", body)
	}
	if elapsed := time.Since(started); elapsed >= clientCloseGrace/2 {
		t.Fatalf("request took %v, want client-close path below %v", elapsed, clientCloseGrace/2)
	}
	// Closing resp.Body with keep-alives disabled tears the client connection
	// down; the parked middleware must observe that via the request context.
	if elapsed := waitDuration(t, returned, "middleware did not observe client close"); elapsed >= clientCloseGrace/2 {
		t.Fatalf("middleware ran %v, want context wake below %v", elapsed, clientCloseGrace/2)
	}
}

func TestWaitForClientCloseFallsBackToBoundedGrace(t *testing.T) {
	server, returned := newCloseWaitServer(t, closeWaitBody)

	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if body, err := io.ReadAll(response.Body); err != nil || string(body) != "metadata" {
		t.Fatalf("body read = %q, err=%v", body, err)
	}
	// Hold the socket open: only the grace timer can end the park.
	if elapsed := waitDuration(t, returned, "middleware never returned"); elapsed < clientCloseGrace/2 || elapsed > 2*clientCloseGrace {
		t.Fatalf("middleware ran %v, want the bounded fallback around %v", elapsed, clientCloseGrace)
	}
}

func TestWaitForClientCloseSkipsKeepAlive(t *testing.T) {
	server, _ := newCloseWaitServer(t, closeWaitBody)
	client := &http.Client{Timeout: time.Second}

	for request := 0; request < 2; request++ {
		start := time.Now()
		resp, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err)
		}
		if string(body) != "metadata" {
			t.Fatalf("body = %q", body)
		}
		if resp.Close {
			t.Fatalf("request %d: server closed a keep-alive connection", request)
		}
		if elapsed := time.Since(start); elapsed >= clientCloseGrace/2 {
			t.Fatalf("keep-alive request %d took %v, want no park below %v", request, elapsed, clientCloseGrace/2)
		}
	}
}

func TestWaitForClientCloseWakesOnServerClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	returned := make(chan time.Duration, 1)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		waitForClientClose(closeWaitBody).ServeHTTP(w, r)
		returned <- time.Since(start)
	})}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(listener) }()
	t.Cleanup(func() {
		_ = srv.Close()
		<-serveDone
	})

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	// The framed response proves the park is armed; Close cancels in-flight
	// request contexts, so the parked middleware must not pay the grace.
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := waitDuration(t, returned, "middleware did not wake on server close"); elapsed >= clientCloseGrace/2 {
		t.Fatalf("middleware ran %v, want shutdown wake below %v", elapsed, clientCloseGrace/2)
	}
}

func TestServeCancellationClosesPendingWaitsPromptly(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- New(fakeSource{unavailable: true}, time.Second, nil).Serve(ctx, listener) }()

	const connections = 4
	clients := make([]net.Conn, 0, connections)
	defer func() {
		cancel()
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	for range connections {
		client, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, client)
		if _, err := io.WriteString(client, "GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		// The source is unavailable, so every exchange is a framed 503 that
		// still parks; reading the full response proves the park is armed.
		response, err := http.ReadResponse(bufio.NewReader(client), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(response.Body); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
		}
	}

	cancel()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(clientCloseGrace / 2):
		t.Fatal("Serve cancellation accumulated per-connection client-close grace")
	}
}

func TestHandlerEnvdStyleCloseExchangesReturnPromptly(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"127.0.0.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	server := httptest.NewServer(New(src, time.Second, nil).Handler())
	defer server.Close()
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}

	for attempt := 0; attempt < 10; attempt++ {
		put, err := http.NewRequest(http.MethodPut, server.URL+"/latest/api/token", nil)
		if err != nil {
			t.Fatal(err)
		}
		put.Header.Set("X-metadata-token-ttl-seconds", "60")
		start := time.Now()
		response, err := client.Do(put)
		if err != nil {
			t.Fatal(err)
		}
		token, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("PUT status=%d body=%q err=%v", response.StatusCode, token, err)
		}
		if response.ContentLength != int64(len(token)) {
			t.Fatalf("PUT Content-Length=%d, body=%d", response.ContentLength, len(token))
		}
		if elapsed := time.Since(start); elapsed >= clientCloseGrace/2 {
			t.Fatalf("PUT attempt %d took %v; client close did not wake the park", attempt, elapsed)
		}

		get, err := http.NewRequest(http.MethodGet, server.URL+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		get.Header.Set("X-metadata-token", string(token))
		start = time.Now()
		response, err = client.Do(get)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("GET status=%d body=%q err=%v", response.StatusCode, body, err)
		}
		if response.ContentLength != int64(len(body)) {
			t.Fatalf("GET Content-Length=%d, body=%d", response.ContentLength, len(body))
		}
		if elapsed := time.Since(start); elapsed >= clientCloseGrace/2 {
			t.Fatalf("GET attempt %d took %v; client close did not wake the park", attempt, elapsed)
		}
	}
}
