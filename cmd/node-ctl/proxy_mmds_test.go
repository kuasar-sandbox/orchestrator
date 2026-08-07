package main

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestNewSocketpairIsCloseOnExec(t *testing.T) {
	master, worker, err := newSocketpair()
	if err != nil {
		t.Fatalf("newSocketpair: %v", err)
	}
	defer master.Close()
	defer worker.Close()

	for _, file := range []*os.File{master, worker} {
		flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0)
		if err != nil {
			t.Fatalf("get descriptor flags: %v", err)
		}
		if flags&unix.FD_CLOEXEC == 0 {
			t.Fatalf("descriptor %d is not close-on-exec", file.Fd())
		}
	}
}
