package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"golang.org/x/sys/unix"
)

func TestCheckpointCleanupDeferredResultRequiresPureMemoryReaderConflict(t *testing.T) {
	busy := fmt.Errorf("reader fence: %w", artifact.ErrCheckpointMemoryReadersBusy)
	closeFailure := errors.New("storage close failed")
	for _, test := range []struct {
		name     string
		cleanup  error
		close    error
		inspect  bool
		deferred bool
	}{
		{name: "memory reader", cleanup: busy, deferred: true},
		{name: "close failure", cleanup: busy, close: closeFailure},
		{name: "inspection", cleanup: busy, inspect: true},
		{name: "unclassified busy", cleanup: unix.EWOULDBLOCK},
		{name: "storage failure", cleanup: closeFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := checkpointCleanupResult(test.cleanup, test.close, test.inspect)
			if got := errors.Is(err, errCheckpointCleanupDeferred); got != test.deferred {
				t.Fatalf("deferred=%v want %v: %v", got, test.deferred, err)
			}
			if test.close != nil && !errors.Is(err, test.close) {
				t.Fatalf("deferred result hid close failure: %v", err)
			}
		})
	}
}
