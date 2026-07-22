package proxy

import (
	"context"
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
