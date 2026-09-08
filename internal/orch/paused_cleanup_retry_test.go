package orch

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// The first attempt holds the lifecycle lock while the second starts. After
// the first clears RunDir, the second must reload ownership under that lock;
// it must not retry with a snapshot captured before the competing attempt.
func TestPausedCleanupRetryReloadsAfterCompetingAttempt(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "paused-competing-cleanup")
	fixture.sb.State = types.StatePaused
	fixture.sb.RunID, fixture.sb.VswitchPort, fixture.sb.FloatingIP = "", "", ""
	fixture.sb.ResumeSource = types.ResumeSource{
		Kind: types.ResumeSourceSnapshot,
		Ref:  "manifest://" + strings.Repeat("c", 64),
	}
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	stale, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || stale == nil {
		t.Fatalf("read initial owner: %+v, %v", stale, err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var calls atomic.Int32
	fixture.o.removeSandboxRunDir = func(path string) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return os.RemoveAll(path)
	}
	var workers sync.WaitGroup
	// Join test-owned work before fixture cleanup closes the store/directories,
	// including assertion-failure paths while the first attempt is blocked.
	t.Cleanup(func() {
		unblock()
		workers.Wait()
	})
	done := make(chan error, 2)
	start := func() {
		workers.Add(1)
		go func() {
			defer workers.Done()
			done <- fixture.o.finalizePausedCleanupOnce(fixture.sb.ID)
		}()
	}
	start()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first cleanup did not reach RunDir removal")
	}
	start()
	unblock()
	for n := 0; n < 2; n++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("serialized cleanup %d: %v", n, err)
			}
		case <-time.After(time.Second):
			t.Fatal("serialized cleanup did not finish")
		}
	}
	workers.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("RunDir removals=%d, want one after ownership reload", got)
	}
	stored, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.RunDir != "" || stored.BaseDir != fixture.sb.BaseDir || stored.ResumeSource != fixture.sb.ResumeSource {
		t.Fatalf("cleaned owner: %+v, %v", stored, err)
	}
	if _, err := os.Stat(fixture.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RunDir remains: %v", err)
	}
	if _, err := os.Stat(fixture.sb.BaseDir); err != nil {
		t.Fatalf("BaseDir/checkpoint owner removed: %v", err)
	}

	// Pin the original failure's cause without weakening the ownership CAS:
	// the old direct helper invocation still rejects the now-stale snapshot.
	if err := fixture.o.cleanupPausedOwnership(context.Background(), stale); err == nil || !strings.Contains(err.Error(), "paused RunDir ownership changed") {
		t.Fatalf("stale cleanup did not preserve ownership fence: %v", err)
	}
	if err := fixture.o.finalizePausedCleanupOnce(fixture.sb.ID); err != nil {
		t.Fatalf("fresh serialized retry after completed cleanup: %v", err)
	}
}
