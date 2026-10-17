package proxydrain

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

func testServer(t *testing.T, handler http.Handler) (*Server, string, <-chan error) {
	return testServerTLS(t, handler, nil)
}

func testServerTLS(t *testing.T, handler http.Handler, tlsConfig *tls.Config) (*Server, string, <-chan error) {
	t.Helper()
	ready := make(chan struct{})
	server, err := New(context.Background(), handler, tlsConfig, func() { close(ready) })
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() { server.ForceClose() })
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("accept loop not entered")
	}
	scheme := "http://"
	if tlsConfig != nil {
		scheme = "https://"
	}
	return server, scheme + listener.Addr().String(), serveDone
}

// Use exactly one physical connection. In particular, ClientConn.RoundTrip
// cannot silently retry a GOAWAY on a newly dialed HTTP/2 connection.
func TestDrainRejectsNewBusinessOnExistingConnection(t *testing.T) {
	for _, protocol := range []string{"http1", "http1tls", "h2c", "h2"} {
		t.Run(protocol, func(t *testing.T) {
			http1 := protocol == "http1" || protocol == "http1tls"
			var tlsConfig *tls.Config
			if protocol == "h2" || protocol == "http1tls" {
				certificate := httptest.NewTLSServer(nil)
				tlsConfig = certificate.TLS.Clone()
				certificate.Close()
			}
			release := make(chan struct{})
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			defer finish()
			var newHandlers atomic.Int32
			payload := strings.Repeat("finite-response-", 4096)
			server, address, _ := testServerTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/held" {
					newHandlers.Add(1)
					w.WriteHeader(http.StatusOK)
					return
				}
				io.WriteString(w, "begin:")
				http.NewResponseController(w).Flush()
				<-release
				io.WriteString(w, payload)
			}), tlsConfig)
			request, _ := http.NewRequest(http.MethodGet, address+"/held", nil)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			var conn net.Conn
			var err error
			if tlsConfig != nil {
				alpn := "h2"
				if http1 {
					alpn = "http/1.1"
				}
				conn, err = tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", request.URL.Host, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{alpn}})
			} else {
				conn, err = net.DialTimeout("tcp", request.URL.Host, time.Second)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(4 * time.Second))
			var first *http.Response
			var reader *bufio.Reader
			var client *http2.ClientConn
			if http1 {
				reader = bufio.NewReader(conn)
				if err := request.Write(conn); err != nil {
					t.Fatal(err)
				}
				first, err = http.ReadResponse(reader, request)
			} else {
				transport := &http2.Transport{}
				client, err = transport.NewClientConn(conn)
				if err == nil {
					first, err = client.RoundTrip(request.WithContext(ctx))
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			defer first.Body.Close()
			done := make(chan error, 1)
			go func() { done <- server.Drain(ctx) }()
			waitDraining(t, server)
			second, _ := http.NewRequestWithContext(ctx, http.MethodGet, address+"/new", nil)
			var response *http.Response
			if http1 {
				// Pipeline behind the still-running response, on the original
				// keep-alive connection, after the admission barrier closed.
				if err := second.Write(conn); err != nil {
					t.Fatal(err)
				}
			} else {
				response, err = client.RoundTrip(second)
				if err == nil && response.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("new stream admitted: %s", response.Status)
				}
				if response != nil {
					response.Body.Close()
				}
			}
			finish()
			body, err := io.ReadAll(first.Body)
			if err != nil || string(body) != "begin:"+payload {
				t.Fatalf("original response incomplete: bytes=%d err=%v", len(body), err)
			}
			if http1 {
				response, err = http.ReadResponse(reader, second)
				if err == nil && response.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("new request admitted: %s", response.Status)
				}
				if response != nil {
					response.Body.Close()
				}
				// Complete the TCP handshake after observing the server's FIN,
				// as http.Transport does when a connection stops accepting work.
				conn.Close()
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if got := newHandlers.Load(); got != 0 {
				t.Fatalf("new business entered %d handlers during drain", got)
			}
		})
	}
}

func TestHTTP1CloseHandshakeSharesDrainDeadlineAndCanBeForced(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "force"}[force], func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			defer finish()
			server, address, _ := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				io.WriteString(w, "begin:")
				http.NewResponseController(w).Flush()
				<-release
				io.WriteString(w, "complete")
			}))
			request, _ := http.NewRequest(http.MethodGet, address, nil)
			conn, err := net.DialTimeout("tcp", request.URL.Host, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			if err := request.Write(conn); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			response, err := http.ReadResponse(reader, request)
			if err != nil {
				t.Fatal(err)
			}
			budget := time.Second
			if force {
				budget = 10 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			go server.Drain(ctx)
			waitDraining(t, server)
			finish()
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || string(body) != "begin:complete" {
				t.Fatalf("finite response truncated: %q %v", body, err)
			}
			if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
				t.Fatalf("server did not finish writes: %v", err)
			}
			// Deliberately retain our write side after receiving FIN. This is
			// transport cleanup only: the response above must still be complete.
			if force {
				done := make(chan struct{})
				go func() { server.ForceClose(); close(done) }()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("ForceClose blocked behind the HTTP server lock")
				}
			}
			wait, stopWait := context.WithTimeout(context.Background(), 2*time.Second)
			defer stopWait()
			err = server.Drain(wait)
			if !errors.Is(err, ErrForcedClose) {
				t.Fatalf("interrupted close handshake was not classified as forced: %v", err)
			}
			if errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrForcedClose) {
				t.Fatalf("drain did not finish within its original budget: %v", err)
			}
		})
	}
}

func waitDraining(t *testing.T, s *Server) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		s.mu.Lock()
		draining, changed := s.draining, s.changed
		s.mu.Unlock()
		if draining {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatal("drain did not start")
		}
	}
}

func TestDrainWaitsForCompleteStreamingResponse(t *testing.T) {
	for _, protocol := range []string{"http1", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			payload := strings.Repeat("complete-response-", 4096)
			server, address, serveDone := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, "start:")
				http.NewResponseController(w).Flush()
				close(entered)
				<-release
				io.WriteString(w, payload)
			}))
			var transport http.RoundTripper
			if protocol == "h2c" {
				transport = &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}}
			} else {
				transport = &http.Transport{DisableKeepAlives: true}
			}
			client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
			response, err := client.Get(address)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			<-entered
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			drained := make(chan error, 1)
			go func() { drained <- server.Drain(ctx) }()
			waitDraining(t, server)
			select {
			case err := <-serveDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("Serve did not stop accepting")
			}
			select {
			case err := <-drained:
				t.Fatalf("drain finished before response: %v", err)
			default:
			}
			close(release)
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != "start:"+payload {
				t.Fatalf("response truncated: %d bytes", len(body))
			}
			if err := <-drained; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDrainWaitsForHijackedTunnelAndPreservesSharedListener(t *testing.T) {
	entered := make(chan struct{})
	server, err := New(context.Background(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		io.WriteString(rw, "HTTP/1.1 200 Connection Established\r\n\r\n")
		rw.Flush()
		close(entered)
		io.Copy(conn, conn)
	}), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	file, err := owner.File()
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	copy, err := net.FileListener(file)
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(copy)
	defer server.ForceClose()
	conn, err := net.Dial("tcp", owner.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	io.WriteString(conn, "CONNECT example:80 HTTP/1.1\r\nHost: example\r\n\r\n")
	reader := bufio.NewReader(conn)
	if _, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect}); err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Drain(ctx) }()
	waitDraining(t, server)
	io.WriteString(conn, "still-alive")
	buffer := make([]byte, len("still-alive"))
	if _, err := io.ReadFull(reader, buffer); err != nil {
		t.Fatal(err)
	}
	if string(buffer) != "still-alive" {
		t.Fatal("tunnel corrupted")
	}
	select {
	case err := <-done:
		t.Fatalf("hijack escaped drain: %v", err)
	default:
	}
	conn.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Closing a process-local copy must not shutdown the shared socket.
	client, err := net.Dial("tcp", owner.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	owner.SetDeadline(time.Now().Add(time.Second))
	accepted, err := owner.Accept()
	if err != nil {
		t.Fatal(err)
	}
	accepted.Close()
}

func TestForcedCloseAndCallerCancellationDoNotResetDrain(t *testing.T) {
	entered := make(chan struct{})
	server, address, _ := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	requestDone := make(chan error, 1)
	go func() {
		response, err := (&http.Client{Timeout: 3 * time.Second}).Get(address)
		if response != nil {
			response.Body.Close()
		}
		requestDone <- err
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	first := make(chan error, 1)
	go func() { first <- server.Drain(ctx) }()
	waitDraining(t, server)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller result=%v", err)
	}
	start := time.Now()
	if err := server.Drain(context.Background()); !errors.Is(err, ErrForcedClose) {
		t.Fatalf("final result=%v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("drain deadline was extended")
	}
	if err := <-requestDone; err == nil {
		t.Fatal("unanswered request unexpectedly succeeded")
	}
}
