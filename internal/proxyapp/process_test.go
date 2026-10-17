package proxyapp

import (
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

func TestListenerFilePreservesSharedNonblockingMode(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	raw, err := listener.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		var flags int
		var inner error
		if err := raw.Control(func(fd uintptr) { flags, inner = unix.FcntlInt(fd, unix.F_GETFL, 0) }); err != nil {
			t.Fatal(err)
		}
		if inner != nil || flags&unix.O_NONBLOCK == 0 {
			t.Fatalf("shared listener became blocking: flags=%#x err=%v", flags, inner)
		}
	}
	check()
	file, err := listenerFile(listener)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	_ = file.Fd() // os/exec obtains this descriptor for ExtraFiles.
	check()
}

func TestSocketPairIsCloseOnExec(t *testing.T) {
	master, worker, err := newSocketPair("test")
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer worker.Close()
	for _, file := range []uintptr{master.Fd(), worker.Fd()} {
		flags, err := unix.FcntlInt(file, unix.F_GETFD, 0)
		if err != nil {
			t.Fatal(err)
		}
		if flags&unix.FD_CLOEXEC == 0 {
			t.Fatalf("descriptor %d is not close-on-exec", file)
		}
	}
}
