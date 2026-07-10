package vswitch

import (
	"bufio"
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAttachUsesTapFDSocketPrepare(t *testing.T) {
	sock, reqCh, closeFn := serveTapFDOnce(t, "TAPFD/1 OK port=12 floating_ip=100.100.96.12 mac=02:00:00:00:80:0c ip=169.254.0.21 mode=tap\n")
	defer closeFn()
	c := New("connector-ctl", "sw0", WithTapFDSocket(sock))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	port, err := c.Attach(ctx, AttachReq{
		InnerIP:          "169.254.0.21",
		TransitGatewayIP: "192.0.2.1",
		TransitGeneveVNI: 4242,
		TransitMAC:       "02:00:00:00:00:09",
	})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if got, want := <-reqCh, "TAPFD/1 PREPARE VSWITCH=sw0 INNER_IP=169.254.0.21 TRANSIT_GATEWAY_IP=192.0.2.1 TRANSIT_GENEVE_VNI=4242 TRANSIT_MAC=02:00:00:00:00:09\n"; got != want {
		t.Fatalf("request = %q, want %q", got, want)
	}
	if port.Port != "12" || port.FloatingIP != "100.100.96.12" ||
		port.MAC != "02:00:00:00:80:0c" || port.InnerIP != "169.254.0.21" {
		t.Fatalf("port = %+v", port)
	}
}

func TestDetachUsesTapFDSocketRelease(t *testing.T) {
	sock, reqCh, closeFn := serveTapFDOnce(t, "TAPFD/1 OK port=12 released=1\n")
	defer closeFn()
	c := New("connector-ctl", "sw0", WithTapFDSocket(sock))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Detach(ctx, "12"); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if got, want := <-reqCh, "TAPFD/1 RELEASE VSWITCH=sw0 PORT=12\n"; got != want {
		t.Fatalf("request = %q, want %q", got, want)
	}
}

func TestTapFDSocketErrorResponse(t *testing.T) {
	sock, _, closeFn := serveTapFDOnce(t, "TAPFD/1 ERR code=PORT_UNAVAILABLE message=no_free_slots\n")
	defer closeFn()
	c := New("connector-ctl", "sw0", WithTapFDSocket(sock))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.Attach(ctx, AttachReq{InnerIP: "169.254.0.21"})
	if err == nil || !strings.Contains(err.Error(), "PORT_UNAVAILABLE") {
		t.Fatalf("Attach error = %v, want PORT_UNAVAILABLE", err)
	}
}

func serveTapFDOnce(t *testing.T, response string) (path string, reqCh <-chan string, closeFn func()) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "tapfd.sock")
	addr, err := net.ResolveUnixAddr("unix", path)
	if err != nil {
		t.Fatalf("ResolveUnixAddr: %v", err)
	}
	ln, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatalf("ListenUnix: %v", err)
	}
	ch := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.AcceptUnix()
		if err != nil {
			return
		}
		defer conn.Close()
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err == nil {
			ch <- line
		}
		_, _ = conn.Write([]byte(response))
	}()
	return path, ch, func() {
		_ = ln.Close()
		<-done
	}
}
