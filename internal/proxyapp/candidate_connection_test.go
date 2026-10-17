package proxyapp

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestCandidateConnectionDescriptorOwnership(t *testing.T) {
	parent, child := terminalErrorWires(t)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	count := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	before := count()
	for _, kind := range []string{"snapshot", "config_connected"} {
		if err := parent.send(serviceMessage{Kind: kind, Request: 7, file: reader}); err != nil {
			t.Fatal(err)
		}
		message, err := child.read()
		if kind == "snapshot" {
			if err == nil {
				t.Fatal("unexpected descriptor accepted")
			}
		} else {
			if err != nil || message.file == nil || message.Request != 7 {
				t.Fatalf("valid transfer: %+v %v", message, err)
			}
			flags, err := unix.FcntlInt(message.file.Fd(), unix.F_GETFD, 0)
			if err != nil || flags&unix.FD_CLOEXEC == 0 {
				t.Fatalf("received descriptor inherits unexpectedly: %d %v", flags, err)
			}
			message.file.Close()
		}
		if count() != before {
			t.Fatal("received descriptor leaked")
		}
	}
	if _, err := writer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var body [1]byte
	if _, err := io.ReadFull(reader, body[:]); err != nil || body[0] != 'x' {
		t.Fatalf("sender descriptor was closed: %q %v", body, err)
	}
}

func TestCandidateConnectionClosesLateReply(t *testing.T) {
	parent, child := terminalErrorWires(t)
	service := NewService()
	service.parent = child
	service.configReplies = make(chan serviceMessage, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := service.dialCandidateConfig(ctx); done <- err }()
	first, err := parent.read()
	if err != nil || first.Kind != "connect_config" {
		t.Fatalf("connect request: %+v %v", first, err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	late, err := os.Open("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	service.configReplies <- serviceMessage{Kind: "config_connected", Request: first.Request, file: late}
	listener, err := net.Listen("unix", t.TempDir()+"/config.sock")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	conn, err := net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	file, err := conn.(*net.UnixConn).File()
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		connected, err := service.dialCandidateConfig(ctx)
		if err == nil {
			_, err = connected.Write([]byte("connected"))
			connected.Close()
		}
		done <- err
	}()
	second, err := parent.read()
	if err != nil || second.Request == first.Request {
		t.Fatalf("request identity reused: %+v %v", second, err)
	}
	service.configReplies <- serviceMessage{Kind: "config_connected", Request: second.Request, file: file}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := late.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("late descriptor not closed: %v", err)
	}
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("transferred file handle not released: %v", err)
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	var body [9]byte
	if _, err := io.ReadFull(peer, body[:]); err != nil || string(body[:]) != "connected" {
		t.Fatalf("returned connection: %q %v", body, err)
	}
}
