package proxyapp

import (
	"errors"
	"net"
	"os"
	"testing"
)

func TestPreparedWorkerCloseAfterDescriptorOwnersStopped(t *testing.T) {
	for _, alreadyClosed := range []bool{false, true} {
		file, err := os.CreateTemp(t.TempDir(), "wake")
		if err != nil {
			t.Fatal(err)
		}
		peer, conn := net.Pipe()
		defer peer.Close()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if alreadyClosed {
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if err := ln.Close(); err != nil {
				t.Fatal(err)
			}
		}
		worker := &PreparedWorker{wakeFile: file, statsConn: conn, data: ln}
		for n := 0; n < 2; n++ {
			if err := worker.Close(); err != nil {
				t.Fatalf("alreadyClosed=%v Close[%d]=%v", alreadyClosed, n, err)
			}
		}
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("file remains open: %v", err)
		}
		if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("listener remains open: %v", err)
		}
	}
}

type failingCloseListener struct {
	net.Listener
	err error
}

func (l failingCloseListener) Close() error { return l.err }

func TestPreparedWorkerClosePreservesUnexpectedErrors(t *testing.T) {
	want := errors.New("unexpected close failure")
	worker := &PreparedWorker{data: failingCloseListener{err: want}}
	for n := 0; n < 2; n++ {
		if err := worker.Close(); !errors.Is(err, want) {
			t.Fatalf("Close[%d]=%v want=%v", n, err, want)
		}
	}
}
