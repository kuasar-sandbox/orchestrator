package proxydrain

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type heldAcceptListener struct {
	net.Listener
	entered chan struct{}
	release chan struct{}
}

func (l *heldAcceptListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		close(l.entered)
		<-l.release
	}
	return conn, err
}

func TestForceCloseRejectsConnectionAcceptedAfterSnapshot(t *testing.T) {
	var admitted atomic.Int32
	server, err := New(context.Background(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		admitted.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	held := &heldAcceptListener{Listener: listener, entered: make(chan struct{}), release: make(chan struct{})}
	served := make(chan error, 1)
	go func() { served <- server.Serve(held) }()
	client, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(client, "GET / HTTP/1.1\r\nHost: proxy\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("accept did not reach publication barrier")
	}
	forced := make(chan struct{})
	go func() { server.ForceClose(); close(forced) }()
	waitDraining(t, server) // ForceClose takes its snapshot under this same lock.
	close(held.release)
	select {
	case <-forced:
	case <-time.After(time.Second):
		t.Fatal("force close did not finish")
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("late connection was not closed: %v", err)
	}
	server.mu.Lock()
	remaining := len(server.connections)
	server.mu.Unlock()
	if remaining != 0 || admitted.Load() != 0 {
		t.Fatalf("late connection escaped force close: connections=%d handlers=%d", remaining, admitted.Load())
	}
}

// Hold Serve between Accept and publishing the connection to net/http. This
// reproduces the boundary that previously discarded an accepted first request.
func TestDrainCompletesAlreadyAcceptedFirstRequest(t *testing.T) {
	server, err := New(context.Background(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "complete") }), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	previous := server.http.ConnContext
	server.http.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		close(entered)
		<-release
		return previous(ctx, conn)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 2 * time.Second}
	result := make(chan error, 1)
	go func() {
		response, err := client.Get("http://" + listener.Addr().String())
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 || string(body) != "complete" {
				err = io.ErrUnexpectedEOF
			} else {
				err = readErr
			}
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no accepted connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- server.Drain(ctx) }()
	for {
		server.mu.Lock()
		stopping, changed := server.draining, server.changed
		server.mu.Unlock()
		if stopping {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("accepted request lost: %v", err)
	}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}
