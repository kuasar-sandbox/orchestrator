package vswitch

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
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

func TestGenerationBitsReadsStatus(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "connector-ctl")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf '%s\n' '{\"generation_bits\":4}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := New(bin, "sw0").GenerationBits(context.Background())
	if err != nil || got != 4 {
		t.Fatalf("GenerationBits=%d err=%v", got, err)
	}
}

func TestGenerationBitsUsesTapFDInfo(t *testing.T) {
	sock, reqCh, closeFn := serveTapFDOnce(t, "TAPFD/1 OK generation_bits=4\n")
	defer closeFn()
	got, err := New("missing-connector-ctl", "sw0", WithTapFDSocket(sock)).GenerationBits(context.Background())
	if err != nil || got != 4 {
		t.Fatalf("GenerationBits=%d err=%v", got, err)
	}
	if req := <-reqCh; req != "TAPFD/1 INFO VSWITCH=sw0\n" {
		t.Fatalf("request=%q", req)
	}
}

func TestGenerationBitsOldTapFDProviderFallsBackToZero(t *testing.T) {
	sock, _, closeFn := serveTapFDOnce(t, "TAPFD/1 ERR code=BAD_REQUEST message=unsupported_request_op_INFO\n")
	defer closeFn()
	got, err := New("missing-connector-ctl", "sw0", WithTapFDSocket(sock)).GenerationBits(context.Background())
	if err != nil || got != 0 {
		t.Fatalf("GenerationBits=%d err=%v", got, err)
	}
}

func TestAttachUsesTapFDGeneration(t *testing.T) {
	sock, reqCh, closeFn := serveTapFDOnce(t, "TAPFD/1 OK port=12 generation=3 floating_ip=100.100.48.11 mac=02:00:00:00:80:0c ip=169.254.0.21 mode=tap\n")
	defer closeFn()
	port, err := New("connector-ctl", "sw0", WithTapFDSocket(sock)).Attach(context.Background(), AttachReq{Generation: 3, InnerIP: "169.254.0.21"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := <-reqCh, "TAPFD/1 PREPARE VSWITCH=sw0 INNER_IP=169.254.0.21 GENERATION=3\n"; got != want {
		t.Fatalf("request=%q want=%q", got, want)
	}
	if port.Generation != 3 {
		t.Fatalf("generation=%d", port.Generation)
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

func TestDetachRecognizesExactAlreadyReleasedResponse(t *testing.T) {
	sock, _, closeFn := serveTapFDOnce(t, "TAPFD/1 ERR code=PORT_UNAVAILABLE message=port_12:_port_not_attached\n")
	defer closeFn()
	c := New("connector-ctl", "sw0", WithTapFDSocket(sock))

	err := c.Detach(context.Background(), "12")
	if !errors.Is(err, ErrPortNotAttached) {
		t.Fatalf("Detach error = %v, want ErrPortNotAttached", err)
	}
}

func TestDetachDoesNotBroadenPortUnavailable(t *testing.T) {
	sock, _, closeFn := serveTapFDOnce(t, "TAPFD/1 ERR code=PORT_UNAVAILABLE message=port_12:_port_not_provisioned\n")
	defer closeFn()
	c := New("connector-ctl", "sw0", WithTapFDSocket(sock))

	err := c.Detach(context.Background(), "12")
	if err == nil || errors.Is(err, ErrPortNotAttached) {
		t.Fatalf("Detach error = %v, want ordinary provider failure", err)
	}
}

func TestCLIPortNotAttachedRequiresExactPortError(t *testing.T) {
	for _, tt := range []struct {
		name   string
		stderr string
		port   string
		want   bool
	}{
		{name: "cobra", stderr: "Error: port 12: port not attached\n", port: "12", want: true},
		{name: "main echo", stderr: "port 12: port not attached\n", port: "012", want: true},
		{name: "other port", stderr: "Error: port 13: port not attached\n", port: "12"},
		{name: "other unavailable", stderr: "Error: port 12: port not provisioned\n", port: "12"},
		{name: "embedded", stderr: "failure: port 12: port not attached\n", port: "12"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := cliPortNotAttached(tt.stderr, tt.port); got != tt.want {
				t.Fatalf("cliPortNotAttached(%q, %q) = %t, want %t", tt.stderr, tt.port, got, tt.want)
			}
		})
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

func TestTapFDSocketRejectsZeroPort(t *testing.T) {
	sock, _, closeFn := serveTapFDOnce(t, "TAPFD/1 OK port=0 mode=tap\n")
	defer closeFn()
	c := New("connector-ctl", "sw0", WithTapFDSocket(sock))
	if _, err := c.Attach(context.Background(), AttachReq{InnerIP: "169.254.0.21"}); err == nil {
		t.Fatal("Attach accepted port zero")
	}
	if err := c.Detach(context.Background(), "0"); err == nil {
		t.Fatal("Detach accepted port zero")
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
