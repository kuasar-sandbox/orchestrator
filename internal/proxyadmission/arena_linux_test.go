//go:build linux

package proxyadmission

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
)

func newTestArena(t *testing.T, routeCapacity, workers int) (*Master, []*Worker) {
	t.Helper()
	master, err := NewMaster(routeCapacity, workers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = master.Close() })
	views := make([]*Worker, workers)
	for index := range views {
		epoch := uint64(index + 1)
		if err := master.BeginWorker(index, epoch); err != nil {
			t.Fatal(err)
		}
		file, err := master.DupFile()
		if err != nil {
			t.Fatal(err)
		}
		views[index], err = OpenWorker(file, routeCapacity, workers, index, epoch)
		if err != nil {
			t.Fatal(err)
		}
		worker := views[index]
		t.Cleanup(func() { _ = worker.Close() })
	}
	return master, views
}

func applyTestBinding(t *testing.T, master *Master, sid, identity string, limits config.MaxInflight) Binding {
	t.Helper()
	update, err := master.PrepareUpsert(sid, identity, limits)
	if err != nil {
		t.Fatal(err)
	}
	binding := update.Binding()
	update.Commit()
	return binding
}

func TestArenaLayoutAndValidation(t *testing.T) {
	if !(Binding{}).Unlimited() {
		t.Fatal("zero binding is not the unlimited fast path")
	}
	if (Binding{Generation: 1}).Unlimited() {
		t.Fatal("generation without a slot bypassed admission")
	}
	report, err := Report(65536, 2)
	if err != nil {
		t.Fatal(err)
	}
	if report.CounterBytes != 65536*2*4*8 || report.MappedBytes <= report.CounterBytes || report.ScratchBytes == 0 {
		t.Fatalf("memory report = %+v", report)
	}
	if report.MappedBytes != 8388800 {
		t.Fatalf("default two-worker mmap bytes = %d, want 8388800", report.MappedBytes)
	}
	t.Logf("default arena: counters=%d headers=%d guards=%d scratch=%d mapped=%d",
		report.CounterBytes, report.HeaderBytes, report.GuardBytes, report.ScratchBytes, report.MappedBytes)
	if _, err := Size(0, 2); err == nil {
		t.Fatal("zero route capacity accepted")
	}
	if _, err := Size(1, 0); err == nil {
		t.Fatal("zero workers accepted")
	}
	if _, err := Size(1, int(math.MaxUint32)); err == nil {
		t.Fatal("entry stride wider than its header field was accepted")
	}
	if _, err := OpenWorker(nil, 1, 1, 0, 1); err == nil {
		t.Fatal("missing descriptor accepted")
	}
	closed, err := os.CreateTemp(t.TempDir(), "admission-closed")
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()
	if _, err := OpenWorker(closed, 1, 1, 0, 1); err == nil {
		t.Fatal("closed descriptor accepted")
	}

	master, _ := newTestArena(t, 2, 1)
	file, err := master.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorker(file, 2, 1, 1, 9); err == nil {
		t.Fatal("out-of-range worker index accepted")
	}
	master.header.Version++
	file, err = master.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorker(file, 2, 1, 0, 9); err == nil {
		t.Fatal("arena version mismatch accepted")
	}
	master.header.Version = ArenaVersion
	master.header.EntryStride++
	file, err = master.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorker(file, 2, 1, 0, 9); err == nil {
		t.Fatal("invalid arena stride accepted")
	}
	master.header.EntryStride--
	master.header.Workers++
	file, err = master.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorker(file, 2, 1, 0, 9); err == nil {
		t.Fatal("invalid arena worker count accepted")
	}
	master.header.Workers--
	file, err = master.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorker(file, 2, 2, 0, 9); err == nil {
		t.Fatal("worker count mismatch accepted")
	}

	truncated, err := os.CreateTemp(t.TempDir(), "admission-truncated")
	if err != nil {
		t.Fatal(err)
	}
	if err := truncated.Truncate(8); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorker(truncated, 2, 1, 0, 9); err == nil {
		t.Fatal("truncated mmap accepted")
	}
}

func TestDeterministicBoundMPlusNMinusOne(t *testing.T) {
	for policy, limits := range map[string]config.MaxInflight{
		"total":             {Total: 9},
		"service":           {Forward: 9},
		"total-and-service": {Total: 9, Forward: 9},
	} {
		t.Run(policy, func(t *testing.T) {
			for _, workers := range []int{1, 2, 4, 8} {
				t.Run(strconv.Itoa(workers), func(t *testing.T) {
					const limit = 9
					master, views := newTestArena(t, 4, workers)
					binding := applyTestBinding(t, master, "sid", "identity", limits)
					leases := make([]*Lease, 0, limit+workers)
					for index := 0; index < limit-1; index++ {
						lease, err := views[0].TryAcquire(binding, ServiceForward)
						if err != nil {
							t.Fatalf("seed acquire %d: %v", index, err)
						}
						leases = append(leases, lease)
					}

					scanned := make(chan struct{}, workers)
					releaseScan := make(chan struct{})
					for _, worker := range views {
						worker.afterScan = func() {
							scanned <- struct{}{}
							<-releaseScan
						}
					}
					type result struct {
						lease *Lease
						err   error
					}
					results := make(chan result, workers)
					for _, worker := range views {
						go func(worker *Worker) {
							lease, err := worker.TryAcquire(binding, ServiceForward)
							results <- result{lease: lease, err: err}
						}(worker)
					}
					for index := 0; index < workers; index++ {
						<-scanned
					}
					close(releaseScan)
					for index := 0; index < workers; index++ {
						result := <-results
						if result.err != nil {
							t.Fatalf("concurrent acquire: %v", result.err)
						}
						leases = append(leases, result.lease)
					}
					for _, worker := range views {
						worker.afterScan = nil
					}
					if got, want := len(leases), limit+workers-1; got != want {
						t.Fatalf("admitted = %d, want bound %d", got, want)
					}
					t.Logf("M=%d N=%d admitted=%d overshoot=%d bound=%d", limit, workers, len(leases), len(leases)-limit, limit+workers-1)
					for _, worker := range views {
						if _, err := worker.TryAcquire(binding, ServiceForward); !errors.Is(err, ErrLimitReached) {
							t.Fatalf("post-bound acquire error = %v", err)
						}
					}
					for _, lease := range leases {
						lease.Release()
					}
				})
			}
		})
	}
}

func TestSkewUsesWholeLimitAndDoesNotAllowNByM(t *testing.T) {
	master, workers := newTestArena(t, 4, 4)
	binding := applyTestBinding(t, master, "sid", "identity", config.MaxInflight{Total: 7})
	leasing := make([]*Lease, 0, 7)
	for index := 0; index < 7; index++ {
		lease, err := workers[0].TryAcquire(binding, ServiceForward)
		if err != nil {
			t.Fatalf("skew acquire %d: %v", index, err)
		}
		leasing = append(leasing, lease)
	}
	for _, worker := range workers {
		if _, err := worker.TryAcquire(binding, ServiceForward); !errors.Is(err, ErrLimitReached) {
			t.Fatalf("N*M behavior: %v", err)
		}
	}
	for _, lease := range leasing {
		lease.Release()
	}
}

func TestServiceAndTotalAreCheckedTogether(t *testing.T) {
	master, workers := newTestArena(t, 8, 2)
	binding := applyTestBinding(t, master, "sid", "identity", config.MaxInflight{Total: 3, Forward: 2, Exec: 3})
	forward1, err := workers[0].TryAcquire(binding, ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	forward2, err := workers[1].TryAcquire(binding, ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workers[0].TryAcquire(binding, ServiceForward); !errors.Is(err, ErrLimitReached) {
		t.Fatalf("forward service limit error = %v", err)
	}
	execLease, err := workers[0].TryAcquire(binding, ServiceExec)
	if err != nil {
		t.Fatalf("other service before total: %v", err)
	}
	if _, err := workers[1].TryAcquire(binding, ServiceExec); !errors.Is(err, ErrLimitReached) {
		t.Fatalf("total limit error = %v", err)
	}
	forward1.Release()
	forward2.Release()
	execLease.Release()

	other := applyTestBinding(t, master, "other", "other-identity", config.MaxInflight{Total: 1})
	lease, err := workers[1].TryAcquire(other, ServiceExec)
	if err != nil {
		t.Fatalf("other Sandbox shared limit: %v", err)
	}
	lease.Release()
}

func TestGenerationReuseAndOldReleaseCannotCorruptNewRoute(t *testing.T) {
	master, workers := newTestArena(t, 1, 1)
	old := applyTestBinding(t, master, "sid", "old-identity", config.MaxInflight{Total: 1})
	oldLease, err := workers[0].TryAcquire(old, ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	update, err := master.PrepareUpsert("sid", "new-identity", config.MaxInflight{Total: 1})
	if err != nil {
		t.Fatal(err)
	}
	newBinding := update.Binding()
	if newBinding.Generation == old.Generation {
		t.Fatal("identity replacement reused generation")
	}
	update.Commit()
	newLease, err := workers[0].TryAcquire(newBinding, ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	oldLease.Release()
	if _, err := workers[0].TryAcquire(newBinding, ServiceForward); !errors.Is(err, ErrLimitReached) {
		t.Fatalf("old release reduced new generation: %v", err)
	}
	newLease.Release()
}

func TestDeleteRecreateWithOpenLeaseDoesNotCorruptNewRoute(t *testing.T) {
	master, workers := newTestArena(t, 1, 1)
	old := applyTestBinding(t, master, "sid", "old-identity", config.MaxInflight{Total: 1})
	oldLease, err := workers[0].TryAcquire(old, ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	master.Delete("sid")
	if workers[0].Valid(old) {
		t.Fatal("deleted generation remained valid")
	}
	recreated := applyTestBinding(t, master, "sid", "new-identity", config.MaxInflight{Total: 1})
	if recreated.Generation == old.Generation {
		t.Fatal("recreated route reused generation")
	}
	newLease, err := workers[0].TryAcquire(recreated, ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	oldLease.Release()
	if _, err := workers[0].TryAcquire(recreated, ServiceForward); !errors.Is(err, ErrLimitReached) {
		t.Fatalf("old deleted-route release reduced recreated generation: %v", err)
	}
	newLease.Release()
}

func TestWorkerColumnClearsOnlyForReapedEpochAndReplacementReusesIndex(t *testing.T) {
	master, workers := newTestArena(t, 2, 2)
	binding := applyTestBinding(t, master, "sid", "identity", config.MaxInflight{Total: 1})
	lease, err := workers[0].TryAcquire(binding, ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	if err := master.ClearWorker(0, 99); err == nil {
		t.Fatal("wrong epoch cleared worker column")
	}
	if _, err := workers[1].TryAcquire(binding, ServiceForward); !errors.Is(err, ErrLimitReached) {
		t.Fatalf("column cleared before exact exit: %v", err)
	}
	if err := master.ClearWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	lease.Release() // the reaped process cannot do this in production; generation safety makes it harmless here.
	if err := master.BeginWorker(0, 10); err != nil {
		t.Fatal(err)
	}
	file, err := master.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := OpenWorker(file, 2, 2, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	replacementLease, err := replacement.TryAcquire(binding, ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	replacementLease.Release()
}

const admissionChildModeEnv = "KUASAR_TEST_PROXY_ADMISSION_CHILD_MODE"

func TestProcessSharedCountersAndReapedWorkerCleanup(t *testing.T) {
	if mode := os.Getenv(admissionChildModeEnv); mode != "" {
		runAdmissionChild(t, mode)
		return
	}
	master, err := NewMaster(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	if err := master.BeginWorker(1, 22); err != nil {
		t.Fatal(err)
	}
	file, err := master.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	survivor, err := OpenWorker(file, 2, 2, 1, 22)
	if err != nil {
		t.Fatal(err)
	}
	defer survivor.Close()
	binding := applyTestBinding(t, master, "sid", "identity", config.MaxInflight{Total: 1})

	for iteration, mode := range []string{"before", "after", "locked-before-add", "locked-after-add", "after"} {
		epoch := uint64(100 + iteration)
		if err := master.BeginWorker(0, epoch); err != nil {
			t.Fatal(err)
		}
		arenaFile, err := master.DupFile()
		if err != nil {
			t.Fatal(err)
		}
		readyRead, readyWrite, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		exitRead, exitWrite, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(os.Args[0], "-test.run=^TestProcessSharedCountersAndReapedWorkerCleanup$")
		command.Env = append(os.Environ(),
			admissionChildModeEnv+"="+mode,
			"KUASAR_TEST_PROXY_ADMISSION_EPOCH="+strconv.FormatUint(epoch, 10),
			"KUASAR_TEST_PROXY_ADMISSION_SLOT="+strconv.FormatUint(uint64(binding.Slot), 10),
			"KUASAR_TEST_PROXY_ADMISSION_GENERATION="+strconv.FormatUint(binding.Generation, 10),
		)
		command.ExtraFiles = []*os.File{arenaFile, readyWrite, exitRead}
		var output bytes.Buffer
		command.Stdout = &output
		command.Stderr = &output
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		_ = arenaFile.Close()
		_ = readyWrite.Close()
		_ = exitRead.Close()
		var ready [1]byte
		if _, err := io.ReadFull(readyRead, ready[:]); err != nil {
			t.Fatalf("child %s readiness: %v\n%s", mode, err, output.String())
		}
		_ = readyRead.Close()
		counted := mode == "after" || mode == "locked-after-add"
		if counted {
			if _, err := survivor.TryAcquire(binding, ServiceForward); !errors.Is(err, ErrLimitReached) {
				t.Fatalf("iteration %d did not observe child increment: %v", iteration, err)
			}
		} else {
			lease, err := survivor.TryAcquire(binding, ServiceForward)
			if err != nil {
				t.Fatalf("crash-before-increment blocked survivor: %v", err)
			}
			lease.Release()
		}
		if mode == "locked-before-add" || mode == "locked-after-add" {
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
		}
		_ = exitWrite.Close()
		if err := command.Wait(); err != nil {
			var exitErr *exec.ExitError
			if mode != "locked-before-add" && mode != "locked-after-add" || !errors.As(err, &exitErr) {
				t.Fatalf("child %s: %v\n%s", mode, err, output.String())
			}
		}
		if counted {
			// Process exit alone leaves a conservative stale-high absolute cell.
			if _, err := survivor.TryAcquire(binding, ServiceForward); !errors.Is(err, ErrLimitReached) {
				t.Fatalf("iteration %d stale-high disappeared before master cleanup: %v", iteration, err)
			}
		}
		clearResult := make(chan error, 1)
		go func() { clearResult <- master.ClearWorker(0, epoch) }()
		select {
		case err := <-clearResult:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("master cleanup blocked on a reaped worker's stale row guard")
		}
		lease, err := survivor.TryAcquire(binding, ServiceForward)
		if err != nil {
			t.Fatalf("iteration %d cleanup did not restore capacity: %v", iteration, err)
		}
		lease.Release()
	}
}

func runAdmissionChild(t *testing.T, mode string) {
	t.Helper()
	epoch, err := strconv.ParseUint(os.Getenv("KUASAR_TEST_PROXY_ADMISSION_EPOCH"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	slot, err := strconv.ParseUint(os.Getenv("KUASAR_TEST_PROXY_ADMISSION_SLOT"), 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := strconv.ParseUint(os.Getenv("KUASAR_TEST_PROXY_ADMISSION_GENERATION"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := OpenWorker(os.NewFile(3, "admission-child"), 2, 2, 0, epoch)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	ready := os.NewFile(4, "admission-ready")
	exit := os.NewFile(5, "admission-exit")
	signalAndWait := func() {
		if _, err := ready.Write([]byte{1}); err != nil {
			panic(err)
		}
		_ = ready.Close()
		var release [1]byte
		_, _ = exit.Read(release[:])
	}
	switch mode {
	case "locked-before-add":
		worker.afterScan = signalAndWait
	case "locked-after-add":
		worker.afterAdd = signalAndWait
	}
	if mode == "after" || mode == "locked-before-add" || mode == "locked-after-add" {
		if _, err := worker.TryAcquire(Binding{
			Slot: uint32(slot), Generation: generation, Limits: config.MaxInflight{Total: 1},
		}, ServiceForward); err != nil {
			t.Fatal(err)
		}
	} else if mode != "before" {
		t.Fatalf("unknown child mode %q", mode)
	}
	if mode != "locked-before-add" && mode != "locked-after-add" {
		if _, err := ready.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		_ = ready.Close()
		var release [1]byte
		_, _ = exit.Read(release[:])
	}
	_ = exit.Close()
}

func TestConcurrentAcquireRelease(t *testing.T) {
	master, workers := newTestArena(t, 2, 4)
	binding := applyTestBinding(t, master, "sid", "identity", config.MaxInflight{Total: 64})
	var wg sync.WaitGroup
	for index := 0; index < 1000; index++ {
		worker := workers[index%len(workers)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := worker.TryAcquire(binding, ServiceForward)
			if err == nil {
				lease.Release()
			} else if !errors.Is(err, ErrLimitReached) {
				t.Errorf("TryAcquire: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentReleaseAndAcquireWavesStayWithinBound(t *testing.T) {
	const limit = 9
	for _, workerCount := range []int{1, 2, 4, 8} {
		t.Run(strconv.Itoa(workerCount), func(t *testing.T) {
			master, workers := newTestArena(t, 2, workerCount)
			binding := applyTestBinding(t, master, "sid", "identity", config.MaxInflight{Total: limit})
			leases := make([]*Lease, 0, limit+workerCount-1)
			for index := 0; index < limit-1; index++ {
				lease, err := workers[index%workerCount].TryAcquire(binding, ServiceForward)
				if err != nil {
					t.Fatalf("seed acquire %d: %v", index, err)
				}
				leases = append(leases, lease)
			}

			observed := len(leases)
			for wave := 0; wave < 500; wave++ {
				releaseCount := wave%workerCount + 1
				if releaseCount > len(leases) {
					releaseCount = len(leases)
				}
				releasing := append([]*Lease(nil), leases[len(leases)-releaseCount:]...)
				leases = leases[:len(leases)-releaseCount]

				start := make(chan struct{})
				acquired := make(chan *Lease, workerCount)
				var wg sync.WaitGroup
				for _, lease := range releasing {
					wg.Add(1)
					go func(lease *Lease) {
						defer wg.Done()
						<-start
						lease.Release()
					}(lease)
				}
				for _, worker := range workers {
					wg.Add(1)
					go func(worker *Worker) {
						defer wg.Done()
						<-start
						lease, err := worker.TryAcquire(binding, ServiceForward)
						if err == nil {
							acquired <- lease
							return
						}
						if !errors.Is(err, ErrLimitReached) {
							t.Errorf("wave %d TryAcquire: %v", wave, err)
						}
					}(worker)
				}
				close(start)
				wg.Wait()
				close(acquired)
				for lease := range acquired {
					leases = append(leases, lease)
				}
				if len(leases) > observed {
					observed = len(leases)
				}
				if bound := limit + workerCount - 1; len(leases) > bound {
					t.Fatalf("wave %d admitted=%d exceeds M+N-1=%d", wave, len(leases), bound)
				}
			}
			t.Logf("concurrent release/acquire M=%d N=%d maximum=%d overshoot=%d", limit, workerCount, observed, observed-limit)
			for _, lease := range leases {
				lease.Release()
			}
		})
	}
}

func BenchmarkTryAcquireRelease(b *testing.B) {
	for _, workerCount := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workerCount), func(b *testing.B) {
			master, workers := newBenchmarkArena(b, 16, workerCount)
			binding := applyBenchmarkBinding(b, master, config.MaxInflight{Total: 1 << 30})
			latencies := make([]time.Duration, 0, min(b.N, 100000))
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				start := time.Now()
				lease, err := workers[0].TryAcquire(binding, ServiceForward)
				if err != nil {
					b.Fatal(err)
				}
				lease.Release()
				if len(latencies) < cap(latencies) {
					latencies = append(latencies, time.Since(start))
				}
			}
			b.StopTimer()
			reportLatencyPercentiles(b, latencies)
		})
	}
}

func BenchmarkSameSIDContention(b *testing.B) {
	for _, workerCount := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workerCount), func(b *testing.B) {
			master, workers := newBenchmarkArena(b, 16, workerCount)
			binding := applyBenchmarkBinding(b, master, config.MaxInflight{Total: 1 << 30})
			var next atomic.Uint64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				worker := workers[int(next.Add(1)-1)%len(workers)]
				for pb.Next() {
					lease, err := worker.TryAcquire(binding, ServiceForward)
					if err != nil {
						b.Fatal(err)
					}
					lease.Release()
				}
			})
		})
	}
}

func BenchmarkDifferentSIDParallel(b *testing.B) {
	for _, workerCount := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workerCount), func(b *testing.B) {
			const sidCount = 256
			master, workers := newBenchmarkArena(b, sidCount, workerCount)
			bindings := make([]Binding, sidCount)
			for index := range bindings {
				update, err := master.PrepareUpsert(
					fmt.Sprintf("sid-%d", index), fmt.Sprintf("identity-%d", index),
					config.MaxInflight{Total: 1 << 30},
				)
				if err != nil {
					b.Fatal(err)
				}
				bindings[index] = update.Binding()
				update.Commit()
			}
			var next atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := int(next.Add(1) - 1)
				worker := workers[id%len(workers)]
				binding := bindings[id%len(bindings)]
				for pb.Next() {
					lease, err := worker.TryAcquire(binding, ServiceForward)
					if err != nil {
						b.Fatal(err)
					}
					lease.Release()
				}
			})
		})
	}
}

func newBenchmarkArena(b *testing.B, capacity, workerCount int) (*Master, []*Worker) {
	b.Helper()
	master, err := NewMaster(capacity, workerCount)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = master.Close() })
	workers := make([]*Worker, workerCount)
	for index := range workers {
		epoch := uint64(index + 1)
		if err := master.BeginWorker(index, epoch); err != nil {
			b.Fatal(err)
		}
		file, err := master.DupFile()
		if err != nil {
			b.Fatal(err)
		}
		workers[index], err = OpenWorker(file, capacity, workerCount, index, epoch)
		if err != nil {
			b.Fatal(err)
		}
		worker := workers[index]
		b.Cleanup(func() { _ = worker.Close() })
	}
	return master, workers
}

func reportLatencyPercentiles(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	percentile := func(numerator int) float64 {
		index := (len(samples)*numerator + 99) / 100
		if index > 0 {
			index--
		}
		return float64(samples[index].Nanoseconds())
	}
	b.ReportMetric(percentile(50), "p50-ns")
	b.ReportMetric(percentile(95), "p95-ns")
	b.ReportMetric(percentile(99), "p99-ns")
}

func applyBenchmarkBinding(b *testing.B, master *Master, limits config.MaxInflight) Binding {
	b.Helper()
	update, err := master.PrepareUpsert("sid", "identity", limits)
	if err != nil {
		b.Fatal(err)
	}
	binding := update.Binding()
	update.Commit()
	return binding
}
