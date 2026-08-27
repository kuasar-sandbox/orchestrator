package proxyshm

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestUpdatesCloseStopsReaderAndClosesDescriptor(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer write.Close()
	descriptor, err := unix.Dup(int(read.Fd()))
	if err != nil {
		read.Close()
		t.Fatal(err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	updates, err := NewUpdatesFromFD(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := write.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !updates.Wait(ctx, time.Now().Add(time.Second), nil) {
		t.Fatal("notification was not observed")
	}
	closed := make(chan error, 1)
	go func() { closed <- updates.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Updates.Close did not stop its reader")
	}
	if err := updates.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(descriptor), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("descriptor remains open: %v", err)
	}
}
