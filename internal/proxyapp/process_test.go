package proxyapp

import (
	"testing"

	"golang.org/x/sys/unix"
)

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
