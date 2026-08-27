package proxyshm

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"runtime"
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
	if !updates.Wait(ctx, time.Now().Add(time.Second), updates.hasRevision) {
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

func TestUpdatesFileOwnershipSurvivesGC(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer write.Close()
	updates, err := NewUpdatesFromFile(read)
	if err != nil {
		t.Fatal(err)
	}
	defer updates.Close()
	read = nil
	runtime.GC()
	if _, err := write.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !updates.Wait(ctx, time.Now().Add(time.Second), updates.hasRevision) {
		t.Fatal("notification was not observed after GC")
	}
}

func (u *Updates) hasRevision() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.rev > 0
}

func TestWakeWriterFileOwnershipSurvivesGC(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	wakes := NewWakeWriterFromFile(write)
	defer wakes.Close()
	write = nil
	runtime.GC()
	wakes.Wake("sandbox")
	if err := read.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var size [4]byte
	if _, err := io.ReadFull(read, size[:]); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, binary.LittleEndian.Uint32(size[:]))
	if _, err := io.ReadFull(read, payload); err != nil {
		t.Fatal(err)
	}
	if got, want := string(payload), "sandbox"; got != want {
		t.Fatalf("wake payload = %q, want %q", got, want)
	}
}
