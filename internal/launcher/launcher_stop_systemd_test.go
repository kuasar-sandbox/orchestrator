package launcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This opt-in regression exercises real coalesced systemd jobs. It installs
// only uniquely named temporary fixture units and removes them before return.
func TestSystemdConcurrentStops(t *testing.T) {
	if os.Getenv("REQUIRE_LAUNCHER_SYSTEMD") != "1" {
		t.Skip("requires explicit privileged systemd fixture")
	}
	if os.Geteuid() != 0 {
		t.Fatal("systemd fixture requires root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := NewSystemd(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	prefix := fmt.Sprintf("kuasar-stop-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	units := []string{prefix + "-blocked.service", prefix + "-independent.service"}
	var files []string
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, unit := range units {
			if err := s.Stop(cleanup, unit); err != nil {
				t.Error(err)
			}
			_ = s.ResetFailed(cleanup, unit)
		}
		for _, path := range files {
			if err := os.Remove(path); err != nil {
				t.Error(err)
			}
		}
		if err := s.Reload(cleanup); err != nil {
			t.Error(err)
		}
	}()
	for i, unit := range units {
		path := filepath.Join("/run/systemd/system", unit)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, path)
		content := "[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/usr/bin/true\nTimeoutStopSec=5\n"
		if i == 0 {
			content += "ExecStop=/usr/bin/sleep 2\n"
		}
		_, writeErr := f.WriteString(content)
		closeErr := f.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	for _, unit := range units {
		if err := s.Start(ctx, unit); err != nil {
			t.Fatal(err)
		}
	}
	stopCtx, cancelStops := context.WithTimeout(ctx, 6*time.Second)
	defer cancelStops()
	results := make(chan error, 8)
	go func() { results <- s.Stop(stopCtx, units[0]) }()
	// Observe the first stop before adding waiters, rather than guessing when
	// its D-Bus method has reached the manager.
	for {
		current, err := s.List(ctx, units[0])
		if err != nil {
			t.Fatal(err)
		}
		if len(current) == 1 && current[0].ActiveState == "deactivating" {
			break
		}
		if stopCtx.Err() != nil {
			t.Fatal("first stop never became active")
		}
		time.Sleep(time.Millisecond)
	}
	for range 7 {
		go func() { results <- s.Stop(stopCtx, units[0]) }()
	}
	cancelled, cancelWaiter := context.WithCancel(ctx)
	cancelWaiter()
	if err := s.Stop(cancelled, units[0]); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled waiter = %v", err)
	}
	if err := s.Stop(ctx, units[1]); err != nil {
		t.Fatal(err)
	}
	current, err := s.List(ctx, units[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 1 || current[0].ActiveState != "deactivating" {
		t.Error("independent unit stop waited for the blocked unit")
	}
	for range 8 {
		if err := <-results; err != nil {
			t.Errorf("coalesced stop lost completion: %v", err)
		}
	}
	current, err = s.List(ctx, units[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, unit := range current {
		if !strings.EqualFold(unit.ActiveState, "inactive") {
			t.Errorf("unit still live: %+v", unit)
		}
	}
}
