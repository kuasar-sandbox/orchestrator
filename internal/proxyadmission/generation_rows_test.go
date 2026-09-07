//go:build linux

package proxyadmission

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
)

func TestMasterRetiresSlotWithoutWaitingForLiveWriter(t *testing.T) {
	for _, afterAdd := range []bool{false, true} {
		t.Run(fmt.Sprint(afterAdd), func(t *testing.T) {
			master, workers := newTestArena(t, 1, 2)
			old := applyTestBinding(t, master, "old-sid", "old-identity", config.MaxInflight{Total: 1})
			paused, resume := make(chan struct{}), make(chan struct{})
			hook := func() { close(paused); <-resume }
			if afterAdd {
				workers[0].afterAdd = hook
			} else {
				workers[0].afterScan = hook
			}
			result := make(chan error, 1)
			go func() {
				lease, err := workers[0].TryAcquire(old, ServiceForward)
				if lease != nil {
					lease.Release()
				}
				result <- err
			}()
			<-paused
			applied := make(chan Binding, 1)
			go func() {
				master.Delete("old-sid")
				update, err := master.PrepareUpsert("new-sid", "new-identity", config.MaxInflight{Total: 1})
				if err != nil {
					t.Error(err)
					applied <- Binding{}
					return
				}
				next := update.Binding()
				update.Commit()
				applied <- next
			}()
			var next Binding
			select {
			case next = <-applied:
			case <-time.After(time.Second):
				close(resume)
				t.Fatal("master route apply waits for a live worker")
			}
			if next.Slot != old.Slot || next.Generation == old.Generation {
				close(resume)
				t.Fatal("test did not reuse the slot across identities")
			}
			current, err := workers[1].TryAcquire(next, ServiceExec)
			if err != nil {
				close(resume)
				t.Fatal(err)
			}
			close(resume)
			if err := <-result; !errors.Is(err, ErrStaleBinding) {
				t.Fatalf("old writer result=%v", err)
			}
			workers[0].afterScan = nil
			workers[0].afterAdd = nil
			if _, err := workers[0].TryAcquire(next, ServiceExec); !errors.Is(err, ErrLimitReached) {
				t.Fatalf("old writer changed new count: %v", err)
			}
			current.Release()
		})
	}
}

func TestRowInitializationAndOldReleaseShareSlotLock(t *testing.T) {
	master, workers := newTestArena(t, 1, 1)
	for iteration := 0; iteration < 100; iteration++ {
		old := applyTestBinding(t, master, "old", "old", config.MaxInflight{Total: 1})
		lease, err := workers[0].TryAcquire(old, ServiceForward)
		if err != nil {
			t.Fatal(err)
		}
		master.Delete("old")
		next := applyTestBinding(t, master, "new", "new", config.MaxInflight{Total: 1})
		if old.Slot != next.Slot {
			t.Fatal("slot not reused")
		}
		var current *Lease
		var acquireErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); lease.Release() }()
		go func() { defer wg.Done(); current, acquireErr = workers[0].TryAcquire(next, ServiceExec) }()
		wg.Wait()
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		if _, err := workers[0].TryAcquire(next, ServiceExec); !errors.Is(err, ErrLimitReached) {
			t.Fatalf("late release decremented replacement: %v", err)
		}
		current.Release()
		master.Delete("new")
	}
}

func TestReplacementRollbackKeepsOriginalOutstandingRows(t *testing.T) {
	master, workers := newTestArena(t, 1, 2)
	old := applyTestBinding(t, master, "sid", "old", config.MaxInflight{Total: 1})
	held, err := workers[0].TryAcquire(old, ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	update, err := master.PrepareUpsert("sid", "replacement", config.MaxInflight{Total: 1})
	if err != nil {
		t.Fatal(err)
	}
	unpublished := update.Binding()
	if workers[0].Valid(old) {
		t.Fatal("old binding not revoked during replacement")
	}
	update.Rollback()
	if workers[0].Valid(unpublished) || !workers[0].Valid(old) {
		t.Fatal("rollback generation liveness incorrect")
	}
	if _, err := workers[1].TryAcquire(old, ServiceExec); !errors.Is(err, ErrLimitReached) {
		t.Fatalf("rollback lost existing count: %v", err)
	}
	held.Release()
	master.BeginSync()
	replay := applyTestBinding(t, master, "sid", "old", config.MaxInflight{Total: 1})
	master.Bookmark()
	if replay != old {
		t.Fatal("unchanged replay reset generation")
	}
}

func TestMasterAndSurvivorProgressWithStoppedChild(t *testing.T) {
	const childEnv = "KUASAR_TEST_GENERATION_STOP_CHILD"
	if os.Getenv(childEnv) == "1" {
		epoch, _ := strconv.ParseUint(os.Getenv("KUASAR_TEST_GENERATION_EPOCH"), 10, 64)
		generation, _ := strconv.ParseUint(os.Getenv("KUASAR_TEST_GENERATION_BINDING"), 10, 64)
		slot, _ := strconv.ParseUint(os.Getenv("KUASAR_TEST_GENERATION_SLOT"), 10, 32)
		worker, err := OpenWorker(os.NewFile(3, "arena"), 2, 2, 0, epoch)
		if err != nil {
			t.Fatal(err)
		}
		ready := os.NewFile(4, "ready")
		worker.afterAdd = func() {
			if _, err := ready.Write([]byte{1}); err != nil {
				panic(err)
			}
			_ = ready.Close()
			// Stop the entire child while its process-local slot mutex is held.
			if err := syscall.Kill(os.Getpid(), syscall.SIGSTOP); err != nil {
				panic(err)
			}
		}
		_, err = worker.TryAcquire(Binding{Slot: uint32(slot), Generation: generation, Limits: config.MaxInflight{Total: 1}}, ServiceForward)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	master, err := NewMaster(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	if err := master.BeginWorker(1, 2); err != nil {
		t.Fatal(err)
	}
	f, err := master.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	survivor, err := OpenWorker(f, 2, 2, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer survivor.Close()
	for iteration := 0; iteration < 3; iteration++ {
		epoch := uint64(10 + iteration)
		if err := master.BeginWorker(0, epoch); err != nil {
			t.Fatal(err)
		}
		old := applyTestBinding(t, master, "old", "old", config.MaxInflight{Total: 1})
		file, err := master.DupFile()
		if err != nil {
			t.Fatal(err)
		}
		readyR, readyW, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMasterAndSurvivorProgressWithStoppedChild$", "-test.count=1")
		command.ExtraFiles = []*os.File{file, readyW}
		command.Env = append(os.Environ(), childEnv+"=1", "KUASAR_TEST_GENERATION_EPOCH="+strconv.FormatUint(epoch, 10), "KUASAR_TEST_GENERATION_BINDING="+strconv.FormatUint(old.Generation, 10), "KUASAR_TEST_GENERATION_SLOT="+strconv.FormatUint(uint64(old.Slot), 10))
		var output bytes.Buffer
		command.Stdout = &output
		command.Stderr = &output
		if err := command.Start(); err != nil {
			cancel()
			t.Fatal(err)
		}
		_ = file.Close()
		_ = readyW.Close()
		func() {
			defer cancel()
			defer func() { _ = command.Process.Kill(); _ = command.Wait(); _ = readyR.Close() }()
			if err := readyR.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			var b [1]byte
			if _, err := io.ReadFull(readyR, b[:]); err != nil {
				t.Fatalf("child readiness: %v", err)
			}
			// Observe STOP without reaping the child; Wait remains the supervisor's
			// responsibility after Kill. This is a real process-shared mmap test.
			var status syscall.WaitStatus
			if _, err := syscall.Wait4(command.Process.Pid, &status, syscall.WUNTRACED, nil); err != nil || !status.Stopped() {
				t.Fatalf("child not stopped: %v status=%v", err, status)
			}
			if _, err := survivor.TryAcquire(old, ServiceForward); !errors.Is(err, ErrLimitReached) {
				t.Fatalf("missing stopped child's count: %v", err)
			}
			applied := make(chan Binding, 1)
			go func() {
				master.Delete("old")
				update, err := master.PrepareUpsert("new", "new", config.MaxInflight{Total: 1})
				if err != nil {
					t.Error(err)
					applied <- Binding{}
					return
				}
				next := update.Binding()
				update.Commit()
				applied <- next
			}()
			var next Binding
			select {
			case next = <-applied:
			case <-time.After(time.Second):
				t.Fatal("master blocked on stopped child")
			}
			lease, err := survivor.TryAcquire(next, ServiceForward)
			if err != nil {
				t.Fatal(err)
			}
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := command.Wait(); err == nil {
				t.Fatal("killed child exited successfully")
			}
			if err := master.ClearWorker(0, epoch); err != nil {
				t.Fatal(err)
			}
			if _, err := survivor.TryAcquire(next, ServiceForward); !errors.Is(err, ErrLimitReached) {
				t.Fatalf("cleanup changed survivor count: %v", err)
			}
			lease.Release()
			master.Delete("new")
		}()
	}
}

func TestConcurrentMixedServicesMaintainBound(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			master, workers := newTestArena(t, 1, n)
			binding := applyTestBinding(t, master, "sid", "sid", config.MaxInflight{Total: 9})
			var active, peak atomic.Int64
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					for j := 0; j < 2000; j++ {
						lease, err := workers[i].TryAcquire(binding, Service((i+j)%serviceCount))
						if errors.Is(err, ErrLimitReached) {
							continue
						}
						if err != nil {
							t.Error(err)
							return
						}
						current := active.Add(1)
						for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
						}
						if current > int64(9+n-1) {
							t.Errorf("active=%d exceeds %d", current, 9+n-1)
						}
						active.Add(-1)
						lease.Release()
					}
				}(i)
			}
			wg.Wait()
			for i := range workers {
				row := master.row(binding.Slot, i)
				for service := range row.Counters {
					if atomic.LoadUint64(&row.Counters[service]) != 0 {
						t.Fatal("counter leaked")
					}
				}
			}
		})
	}
}
