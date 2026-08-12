package proxy_test

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

func TestTunnelBufferedPreservesBothReadersAndHalfCloseTail(t *testing.T) {
	worker, master := newTrafficHarness(t)
	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backendListener.Close()
	prefetched := []byte("backend-prefetched/")
	tail := []byte("backend-tail")
	backendInput := make(chan []byte, 1)
	backendDone := make(chan error, 1)
	go func() {
		conn, err := backendListener.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close()
		if _, err := conn.Write(prefetched); err != nil {
			backendDone <- err
			return
		}
		got, err := io.ReadAll(conn)
		if err != nil {
			backendDone <- err
			return
		}
		backendInput <- got
		if _, err := conn.Write(tail); err != nil {
			backendDone <- err
			return
		}
		if closer, ok := conn.(interface{ CloseWrite() error }); ok {
			err = closer.CloseWrite()
		}
		backendDone <- err
	}()

	handlerDone := make(chan struct{})
	attached := make(chan struct{})
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		backend, err := net.Dial("tcp", backendListener.Addr().String())
		if err != nil {
			http.Error(w, "dial failed", http.StatusBadGateway)
			return
		}
		reader := bufio.NewReader(backend)
		if _, err := reader.Peek(len(prefetched)); err != nil {
			_ = backend.Close()
			http.Error(w, "prefetch failed", http.StatusBadGateway)
			return
		}
		flow := worker.BeginParking("node-s1", proxy.ConnectServiceForward)
		backend = flow.AttachBackend(backend)
		defer flow.Close()
		close(attached)
		proxy.TunnelBuffered(w, r, backend, reader)
	}))
	defer front.Close()

	conn, err := net.Dial("tcp", front.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tcpConn := conn.(*net.TCPConn)
	defer tcpConn.Close()
	_ = tcpConn.SetDeadline(time.Now().Add(5 * time.Second))
	clientInput := []byte("client-buffered-frame/mux-tail")
	request := append([]byte("CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\n\r\n"), clientInput...)
	if _, err := tcpConn.Write(request); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tcpConn), &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want 200", resp.StatusCode)
	}
	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Fatal("H1 backend was not attached")
	}
	waitInflight(t, master, 0, 1)
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if want := append(append([]byte(nil), prefetched...), tail...); !bytes.Equal(output, want) {
		t.Fatalf("tunnel output = %q, want %q", output, want)
	}
	if got := <-backendInput; !bytes.Equal(got, clientInput) {
		t.Fatalf("backend input = %q, want %q", got, clientInput)
	}
	if err := <-backendDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("half-closed tunnel handler did not finish")
	}
	waitInflight(t, master, 0, 0)
}

func TestTunnelH2FlushesTailAfterRequestEOF(t *testing.T) {
	worker, master := newTrafficHarness(t)
	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backendListener.Close()
	backendInput := make(chan []byte, 1)
	backendDone := make(chan error, 1)
	go func() {
		conn, err := backendListener.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close()
		got, err := io.ReadAll(conn)
		if err != nil {
			backendDone <- err
			return
		}
		backendInput <- got
		_, err = io.WriteString(conn, "h2-response-tail")
		if err == nil {
			if closer, ok := conn.(interface{ CloseWrite() error }); ok {
				err = closer.CloseWrite()
			}
		}
		backendDone <- err
	}()

	attached := make(chan struct{})
	front := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend, err := net.Dial("tcp", backendListener.Addr().String())
		if err != nil {
			http.Error(w, "dial failed", http.StatusBadGateway)
			return
		}
		flow := worker.BeginParking("node-s1", proxy.ConnectServiceForward)
		backend = flow.AttachBackend(backend)
		defer flow.Close()
		close(attached)
		proxy.Tunnel(w, r, backend)
	}))
	front.EnableHTTP2 = true
	front.StartTLS()
	defer front.Close()
	bodyReader, bodyWriter := io.Pipe()
	req, err := http.NewRequest(http.MethodConnect, front.URL, bodyReader)
	if err != nil {
		t.Fatal(err)
	}
	clientInput := []byte("h2-client-frame/mux-tail")
	writeDone := make(chan error, 1)
	releaseEOF := make(chan struct{})
	go func() {
		_, err := bodyWriter.Write(clientInput)
		<-releaseEOF
		if closeErr := bodyWriter.Close(); err == nil {
			err = closeErr
		}
		writeDone <- err
	}()
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
		t.Fatalf("response = %s %d, want HTTP/2 200", resp.Proto, resp.StatusCode)
	}
	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Fatal("H2 backend was not attached")
	}
	waitInflight(t, master, 0, 1)
	close(releaseEOF)
	output, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != "h2-response-tail" {
		t.Fatalf("response tail = %q", output)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if got := <-backendInput; !bytes.Equal(got, clientInput) {
		t.Fatalf("backend input = %q, want %q", got, clientInput)
	}
	if err := <-backendDone; err != nil {
		t.Fatal(err)
	}
	waitInflight(t, master, 0, 0)
}

func TestTunnelH2CancellationClosesBackendAndHandler(t *testing.T) {
	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backendListener.Close()
	inputSeen := make(chan struct{})
	backendDone := make(chan error, 1)
	go func() {
		conn, err := backendListener.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close()
		one := make([]byte, 1)
		if _, err := io.ReadFull(conn, one); err != nil {
			backendDone <- err
			return
		}
		close(inputSeen)
		_, err = io.Copy(io.Discard, conn)
		backendDone <- err
	}()
	handlerDone := make(chan struct{})
	front := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		backend, err := net.Dial("tcp", backendListener.Addr().String())
		if err != nil {
			http.Error(w, "dial failed", http.StatusBadGateway)
			return
		}
		proxy.Tunnel(w, r, backend)
	}))
	front.EnableHTTP2 = true
	front.StartTLS()
	defer front.Close()
	bodyReader, bodyWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodConnect, front.URL, bodyReader)
	if err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := bodyWriter.Write([]byte("x"))
		writeDone <- err
	}()
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
		t.Fatalf("response = %s %d, want HTTP/2 200", resp.Proto, resp.StatusCode)
	}
	select {
	case <-inputSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("backend did not receive request data")
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
		t.Fatal("HTTP/2 cancellation did not close backend")
	}
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP/2 cancellation left tunnel handler running")
	}
}
