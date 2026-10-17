package proxyapp

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func terminalErrorWires(t *testing.T) (*serviceWire, *serviceWire) {
	t.Helper()
	a, b, err := newServicePair()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer b.Close()
	parent, err := serviceConnection(a, "boot", "operation")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { parent.conn.Close() })
	child, err := serviceConnection(b, "boot", "operation")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { child.conn.Close() })
	return parent, child
}

func TestCandidateTerminalErrorSurvivesQueuedControl(t *testing.T) {
	for _, drained := range []bool{false, true} {
		name := "preparation"
		if drained {
			name = "ended-business"
		}
		t.Run(name, func(t *testing.T) {
			parent, peer := terminalErrorWires(t)
			candidate := NewService()
			candidate.timeout = time.Second
			candidate.parent = peer
			if drained {
				candidate.local = NewInstance("candidate", "boot", "operation", 0)
				candidate.local.deadline = time.Now().Add(time.Second)
				// The runner's result has already been consumed. A second receive
				// would deadlock just as in the real localDone failure path.
				candidate.localDone = make(chan error)
			}
			for _, kind := range []string{"error_ack", "drain", "detach", "resume"} {
				if err := parent.send(serviceMessage{Kind: kind, Error: "stale error", Deadline: time.Now().Add(time.Second)}); err != nil {
					t.Fatal(err)
				}
			}
			messages, connectionError := make(chan serviceMessage, 1), make(chan error, 1)
			readerDone, stopReader := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(readerDone)
				for {
					message, err := peer.read()
					if err != nil {
						connectionError <- err
						return
					}
					select {
					case messages <- message:
					case <-stopReader:
						return
					}
				}
			}()
			defer func() { close(stopReader); peer.conn.Close(); <-readerDone }()
			coordinator := NewService()
			child := &serviceChild{wire: parent, changed: make(chan struct{})}
			coordinator.readChild(child)
			defer func() { parent.conn.Close(); <-child.readerCompletion() }()
			cause := errors.New("exact terminal initialization or worker error")
			if err := candidate.finishCandidateError(cause, messages, connectionError); err != cause {
				t.Fatalf("terminal handoff failed: %v", err)
			}
			peer.conn.Close()
			<-child.readerCompletion()
			if err := child.failed(); err == nil || !strings.Contains(err.Error(), cause.Error()) {
				t.Fatalf("coordinator lost original terminal error: %v", err)
			}
		})
	}
}

func TestCandidateTerminalErrorKeepsDeadlineAndCause(t *testing.T) {
	for _, disconnected := range []bool{false, true} {
		name := "missing-ack"
		if disconnected {
			name = "disconnected"
		}
		t.Run(name, func(t *testing.T) {
			parent, peer := terminalErrorWires(t)
			candidate := NewService()
			candidate.timeout = time.Second
			candidate.parent = peer
			candidate.local = NewInstance("candidate", "boot", "operation", 0)
			candidate.local.deadline = time.Now().Add(20 * time.Millisecond)
			candidate.localDone = make(chan error)
			connectionError := make(chan error, 1)
			if disconnected {
				parent.conn.Close()
			}
			cause := errors.New("original failure")
			start := time.Now()
			err := candidate.finishCandidateError(cause, make(chan serviceMessage), connectionError)
			if !errors.Is(err, cause) || err == cause || time.Since(start) > time.Second {
				t.Fatalf("error/stop budget changed: %v elapsed=%s", err, time.Since(start))
			}
		})
	}
}
