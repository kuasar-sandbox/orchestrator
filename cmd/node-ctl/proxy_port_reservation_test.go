package main

import (
	"errors"
	"net"
	"syscall"
	"testing"
)

func TestProxyPortReservationBlocksUnrelatedSourceBinding(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp4", reserveLoopbackAddress(t))
	if err != nil {
		t.Fatal(err)
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, syscall.IPPROTO_TCP)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	// Model an unrelated outgoing connection claiming the port while the
	// master is down. The old listen-and-close helper permits this bind.
	err = syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}, Port: addr.Port})
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("unrelated source bind bypassed reservation: %v", err)
	}
}

func TestProxyPortReservationDoesNotMaskLeakedListener(t *testing.T) {
	addr := reserveLoopbackAddress(t)
	for attempt := 0; attempt < 20; attempt++ {
		listener, err := net.Listen("tcp4", addr)
		if err != nil {
			t.Fatal(err)
		}
		// Duplicate the descriptor like the real master does for a worker.
		file, err := listener.(*net.TCPListener).File()
		if err != nil {
			_ = listener.Close()
			t.Fatal(err)
		}
		if err := listener.Close(); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		probeErr := assertAddressReusable(addr)
		closeErr := file.Close()
		if !errors.Is(probeErr, syscall.EADDRINUSE) {
			t.Fatalf("attempt %d: live inherited listener was not detected: %v", attempt, probeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if err := assertAddressReusable(addr); err != nil {
			t.Fatalf("attempt %d: released listener remains unavailable: %v", attempt, err)
		}
	}
}
