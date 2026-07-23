package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSandboxConnectHandshakeContextIsBounded(t *testing.T) {
	started := time.Now()
	ctx, cancel := sandboxConnectHandshakeContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("CONNECT handshake context has no deadline")
	}
	remaining := deadline.Sub(started)
	if remaining <= 0 || remaining > sandboxConnectHandshakeTimeout+100*time.Millisecond {
		t.Fatalf("CONNECT handshake deadline = %s", remaining)
	}

	parent, parentCancel := context.WithTimeout(context.Background(), time.Second)
	defer parentCancel()
	child, childCancel := sandboxConnectHandshakeContext(parent)
	defer childCancel()
	parentDeadline, _ := parent.Deadline()
	childDeadline, _ := child.Deadline()
	if !childDeadline.Equal(parentDeadline) {
		t.Fatalf("CONNECT handshake extended parent deadline: parent=%s child=%s", parentDeadline, childDeadline)
	}
}

func TestForwardHTTPOnceInterruptsBlockedRequestWrite(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, "http://sandbox/", nil).WithContext(ctx)

	started := time.Now()
	_, err := ForwardHTTPOnce(request, client, nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked request write error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked request write ignored cancellation for %s", elapsed)
	}
}

func TestForwardHTTPOnceInterruptsBlockedResponseHeader(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		defer server.Close()
		_, _ = http.ReadRequest(bufio.NewReader(server))
		_, _ = io.Copy(io.Discard, server)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "http://sandbox/", nil).WithContext(ctx)

	started := time.Now()
	_, err := ForwardHTTPOnce(request, client, nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked response header error = %v", err)
	}
	_ = client.Close()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked response header ignored cancellation for %s", elapsed)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("canceled response header read did not release the backend")
	}
}
