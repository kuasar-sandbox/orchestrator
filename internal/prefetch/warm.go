package prefetch

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"golang.org/x/sys/unix"
)

const (
	defaultWorkers = 4
	defaultQueue   = 128
	defaultChunk   = 1 << 20 // 1 MiB readahead stride
	barrier        = true    // MADV_POPULATE_READ residency barrier
)

// ErrUnavailable means this node cannot accept another prefetch request.
var ErrUnavailable = errors.New("prefetch: service unavailable")

type job struct {
	target target
	result Result
}

// warmer owns durable-in-process prefetch requests. Its worker context is
// intentionally independent from the HTTP request that admitted a job.
type warmer struct {
	res resolver
	log *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	queue  chan *job
	wg     sync.WaitGroup

	mu        sync.RWMutex
	requests  map[string]*job
	byRef     map[string]string
	closed    bool
	closeOnce sync.Once
}

func New(cfg config.CheckpointConfig, log *slog.Logger) Warmer {
	ctx, cancel := context.WithCancel(context.Background())
	w := &warmer{
		res:      resolver{cfg: cfg},
		log:      log,
		ctx:      ctx,
		cancel:   cancel,
		queue:    make(chan *job, defaultQueue),
		requests: make(map[string]*job),
		byRef:    make(map[string]string),
	}
	for range defaultWorkers {
		w.wg.Add(1)
		go w.worker()
	}
	return w
}

// Warm resolves synchronously, so malformed references and unavailable local
// storage are rejected before a request is accepted. The actual I/O is queued
// against the warmer's lifetime context, never the caller's HTTP context.
func (w *warmer) Warm(_ context.Context, req PrefetchReq) (Result, error) {
	t, err := w.res.resolve(context.Background(), req.Reference)
	if err != nil {
		return Result{}, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return Result{}, ErrUnavailable
	}
	if id, ok := w.byRef[t.refKey]; ok {
		return w.requests[id].result, nil
	}
	now := time.Now().UTC()
	j := &job{target: t, result: Result{RequestID: uuid.NewString(), State: "queued", CreatedAt: now}}
	w.requests[j.result.RequestID] = j
	w.byRef[t.refKey] = j.result.RequestID
	select {
	case w.queue <- j:
		w.log.Info("prefetch queued", "request_id", j.result.RequestID, "reference", req.Reference, "files", len(t.paths))
		return j.result, nil
	default:
		delete(w.requests, j.result.RequestID)
		delete(w.byRef, t.refKey)
		return Result{}, ErrUnavailable
	}
}

func (w *warmer) Status(requestID string) (Result, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	j, ok := w.requests[requestID]
	if !ok {
		return Result{}, false
	}
	return j.result, true
}

func (w *warmer) worker() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case j := <-w.queue:
			if j != nil {
				w.run(j)
			}
		}
	}
}

func (w *warmer) run(j *job) {
	now := time.Now().UTC()
	w.mu.Lock()
	j.result.State = "fetching"
	j.result.StartedAt = &now
	w.mu.Unlock()

	var firstErr error
	for _, p := range j.target.paths {
		if err := warmOne(w.ctx, p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	completed := time.Now().UTC()
	w.mu.Lock()
	j.result.CompletedAt = &completed
	if firstErr != nil {
		j.result.State = "failed"
		j.result.Error = firstErr.Error()
	} else {
		j.result.State = "fetched"
	}
	result := j.result
	w.mu.Unlock()
	if firstErr != nil {
		w.log.Warn("prefetch failed", "request_id", result.RequestID, "err", firstErr)
	} else {
		w.log.Info("prefetch done", "request_id", result.RequestID, "files", len(j.target.paths))
	}
}

func warmOne(ctx context.Context, path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return prefetchFile(ctx, int(f.Fd()), fi.Size())
}

func prefetchFile(ctx context.Context, fd int, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if size > 0 {
		if err := readaheadRange(ctx, fd, size); err != nil {
			return err
		}
	}
	if !barrier || size == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		return err
	}
	defer unix.Munmap(data)
	return unix.Madvise(data, unix.MADV_POPULATE_READ)
}

func readaheadRange(ctx context.Context, fd int, size int64) error {
	chunk := int64(defaultChunk)
	tail := chunk
	if tail > size {
		tail = size
	}
	if off := size - tail; off >= 0 {
		if err := readahead(fd, off, tail); err != nil {
			if err == unix.ENOSYS {
				return unix.Fadvise(fd, 0, size, unix.FADV_WILLNEED)
			}
			return err
		}
	}
	limit := size - tail
	for off := int64(0); off < limit; off += chunk {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := chunk
		if limit-off < n {
			n = limit - off
		}
		if err := readahead(fd, off, n); err != nil {
			if err == unix.ENOSYS {
				return unix.Fadvise(fd, 0, size, unix.FADV_WILLNEED)
			}
			return err
		}
	}
	return nil
}

func readahead(fd int, off, n int64) error {
	_, _, en := unix.RawSyscall(unix.SYS_READAHEAD, uintptr(fd), uintptr(off), uintptr(n))
	if en != 0 {
		return en
	}
	return nil
}

func (w *warmer) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
		w.cancel()
	})
	w.wg.Wait()
	return nil
}
