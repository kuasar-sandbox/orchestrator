package proxyshm

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// MasterView is the proxy master's routesync sink and wake source.
type MasterView struct {
	table     *Table
	wakes     *WakeQueue
	notify    *Broadcaster
	defaultPk time.Duration
	log       *slog.Logger
}

func NewMasterView(table *Table, defaultPark time.Duration, log *slog.Logger) *MasterView {
	if defaultPark <= 0 {
		defaultPark = 30 * time.Second
	}
	return &MasterView{
		table:     table,
		wakes:     NewWakeQueue(4096),
		notify:    NewBroadcaster(),
		defaultPk: defaultPark,
		log:       log,
	}
}

func (v *MasterView) BeginSync() {
	v.table.BeginSync()
	v.notify.Notify()
}

func (v *MasterView) ApplyUpsert(r routesync.RouteEntry) {
	if err := v.table.Upsert(r); err != nil {
		if v.log != nil {
			v.log.Warn("proxyshm: apply route", "sid", r.SandboxID, "err", err)
		}
		return
	}
	v.notify.Notify()
}

func (v *MasterView) ApplyDelete(delete routesync.RouteDelete) {
	if v.table.DeleteRoute(delete) {
		v.notify.Notify()
	}
}

func (v *MasterView) Bookmark(fullSync bool) {
	v.table.Bookmark(fullSync)
	v.notify.Notify()
}

func (v *MasterView) SetPolicy(p routesync.Policy) {
	if p.ParkTimeoutMS <= 0 {
		p.ParkTimeoutMS = int(v.defaultPk / time.Millisecond)
	}
	if err := v.table.SetPolicy(p); err != nil && v.log != nil {
		v.log.Warn("proxyshm: set policy", "err", err)
	}
	v.notify.Notify()
}

func (v *MasterView) NextWake(ctx context.Context) (routesync.RouteWake, bool) {
	return v.wakes.Next(ctx)
}

func (v *MasterView) Wake(wake routesync.RouteWake) {
	v.wakes.Enqueue(wake)
}

func (v *MasterView) RegisterNotifyWriter(f *os.File) func() {
	return v.notify.Add(f)
}

// WakeQueue deduplicates wake requests until routesync consumes them.
type WakeQueue struct {
	mu      sync.Mutex
	ch      chan routesync.RouteWake
	pending map[routesync.RouteWake]bool
}

func NewWakeQueue(size int) *WakeQueue {
	if size <= 0 {
		size = 1024
	}
	return &WakeQueue{ch: make(chan routesync.RouteWake, size), pending: map[routesync.RouteWake]bool{}}
}

func (q *WakeQueue) Enqueue(wake routesync.RouteWake) {
	if !validRouteWake(wake) {
		return
	}
	q.mu.Lock()
	if q.pending[wake] {
		q.mu.Unlock()
		return
	}
	q.pending[wake] = true
	q.mu.Unlock()
	select {
	case q.ch <- wake:
	default:
		q.mu.Lock()
		delete(q.pending, wake)
		q.mu.Unlock()
	}
}

func (q *WakeQueue) Next(ctx context.Context) (routesync.RouteWake, bool) {
	select {
	case wake := <-q.ch:
		q.mu.Lock()
		delete(q.pending, wake)
		q.mu.Unlock()
		return wake, true
	case <-ctx.Done():
		return routesync.RouteWake{}, false
	}
}

// Broadcaster wakes worker-local waiters after the master changes shared state.
type Broadcaster struct {
	mu    sync.Mutex
	files map[int]*os.File
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{files: map[int]*os.File{}}
}

func (b *Broadcaster) Add(f *os.File) func() {
	if f == nil {
		return func() {}
	}
	fd := int(f.Fd())
	_ = unix.SetNonblock(fd, true)
	b.mu.Lock()
	b.files[fd] = f
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		if b.files[fd] == f {
			delete(b.files, fd)
		}
		b.mu.Unlock()
		_ = f.Close()
	}
}

func (b *Broadcaster) Notify() {
	b.mu.Lock()
	files := make([]*os.File, 0, len(b.files))
	for _, f := range b.files {
		files = append(files, f)
	}
	b.mu.Unlock()
	for _, f := range files {
		_, err := unix.Write(int(f.Fd()), []byte{1})
		if err == nil || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			continue
		}
	}
}

// WorkerView is a read-only proxy.Router and MMDS source backed by shared memory.
type WorkerView struct {
	table       *Table
	updates     *Updates
	wake        func(routesync.RouteWake)
	defaultPark time.Duration
}

func NewWorkerView(table *Table, updates *Updates, wake func(routesync.RouteWake), defaultPark time.Duration) *WorkerView {
	if defaultPark <= 0 {
		defaultPark = 30 * time.Second
	}
	return &WorkerView{table: table, updates: updates, wake: wake, defaultPark: defaultPark}
}

func (v *WorkerView) Route(ctx context.Context, request proxy.RouteRequest) (proxy.Route, error) {
	if !v.waitSynced(ctx) {
		return proxy.Route{Kind: proxy.KindRouteInactive}, nil
	}
	r, ok := v.table.Lookup(request.SandboxID)
	if !ok {
		if request.HasExecutionFence() {
			return proxy.Route{Kind: proxy.KindWrongBinding}, nil
		}
		return proxy.Route{Kind: proxy.KindNotFound}, nil
	}
	if kind, failed := routeFenceFailure(request, r); failed {
		return proxy.Route{Kind: kind}, nil
	}
	if r.State == routesync.StateRunning {
		return routeForEntry(r, request.Port), nil
	}
	if r.State != routesync.StatePaused {
		return proxy.Route{Kind: proxy.KindRouteInactive}, nil
	}
	if v.wake != nil {
		v.wake(routesync.RouteWake{
			SandboxID:          r.SandboxID,
			NodeID:             r.NodeID,
			NodeEpoch:          r.NodeEpoch,
			RegistryGeneration: r.RegistryGeneration,
			BindingDigest:      r.BindingDigest,
		})
	}
	return v.waitForRoute(ctx, request)
}

func routeForEntry(r routesync.RouteEntry, port int) proxy.Route {
	return proxy.RouteForTarget(r.Profile, r.EnvdUDS, r.CiUDS, r.FloatingIP, r.AccessToken, port)
}

func routeFenceFailure(request proxy.RouteRequest, r routesync.RouteEntry) (proxy.Kind, bool) {
	managed := r.NodeID != "" || r.NodeEpoch != 0 || r.RegistryGeneration != "" || r.BindingDigest != ""
	return proxy.RouteFenceFailure(request, managed, r.NodeID, r.NodeEpoch, r.RegistryGeneration, r.BindingDigest)
}

func (v *WorkerView) ByFloatingIP(ip string) (string, bool) {
	return v.table.ByFloatingIP(ip)
}

func (v *WorkerView) SandboxInfo(sid string) (templateID, accessToken string, ok bool) {
	return v.table.SandboxInfo(sid)
}

func (v *WorkerView) MmdsSecret(sid string) ([]byte, bool) {
	return v.table.MmdsSecret(sid)
}

func (v *WorkerView) Policy() routesync.Policy {
	return v.table.Policy()
}

func (v *WorkerView) waitSynced(ctx context.Context) bool {
	deadline := time.Now().Add(v.parkTimeout())
	for {
		if v.table.Synced() {
			return true
		}
		if !v.waitChange(ctx, deadline, v.table.Rev()) {
			return false
		}
	}
}

func (v *WorkerView) waitForRoute(ctx context.Context, request proxy.RouteRequest) (proxy.Route, error) {
	deadline := time.Now().Add(v.parkTimeout())
	for {
		rev := v.table.Rev()
		r, ok := v.table.Lookup(request.SandboxID)
		if !ok {
			if request.HasExecutionFence() {
				return proxy.Route{Kind: proxy.KindWrongBinding}, nil
			}
			return proxy.Route{Kind: proxy.KindNotFound}, nil
		}
		if kind, failed := routeFenceFailure(request, r); failed {
			return proxy.Route{Kind: kind}, nil
		}
		if r.State == routesync.StateRunning {
			return routeForEntry(r, request.Port), nil
		}
		if r.State != routesync.StatePaused {
			return proxy.Route{Kind: proxy.KindRouteInactive}, nil
		}
		if !v.waitChange(ctx, deadline, rev) {
			// The deadline and an Upsert notification can become ready together.
			// Re-read once so a committed resume is not reported as inactive.
			latest, found := v.table.Lookup(request.SandboxID)
			if !found {
				if request.HasExecutionFence() {
					return proxy.Route{Kind: proxy.KindWrongBinding}, nil
				}
				return proxy.Route{Kind: proxy.KindNotFound}, nil
			}
			if kind, failed := routeFenceFailure(request, latest); failed {
				return proxy.Route{Kind: kind}, nil
			}
			if latest.State == routesync.StateRunning {
				return routeForEntry(latest, request.Port), nil
			}
			return proxy.Route{Kind: proxy.KindRouteInactive}, nil
		}
	}
}

func (v *WorkerView) parkTimeout() time.Duration {
	p := v.table.Policy()
	if p.ParkTimeoutMS > 0 {
		return time.Duration(p.ParkTimeoutMS) * time.Millisecond
	}
	return v.defaultPark
}

func (v *WorkerView) waitChange(ctx context.Context, deadline time.Time, rev uint64) bool {
	if time.Now().After(deadline) {
		return false
	}
	if v.table.Rev() != rev {
		return true
	}
	if v.updates != nil {
		return v.updates.Wait(ctx, deadline, func() bool { return v.table.Rev() != rev })
	}
	timer := time.NewTimer(minDuration(50*time.Millisecond, time.Until(deadline)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return v.table.Rev() != rev
	}
}

// Updates converts a notification pipe into local waitable revisions.
type Updates struct {
	mu  sync.Mutex
	rev uint64
	ch  chan struct{}
}

func NewUpdatesFromFD(fd int) *Updates {
	u := &Updates{ch: make(chan struct{})}
	if fd >= 0 {
		go u.readLoop(os.NewFile(uintptr(fd), "proxy-notify"))
	}
	return u
}

func (u *Updates) Wait(ctx context.Context, deadline time.Time, changed func() bool) bool {
	u.mu.Lock()
	ch := u.ch
	u.mu.Unlock()
	if changed != nil && changed() {
		return true
	}
	timer := time.NewTimer(minDuration(time.Until(deadline), 24*time.Hour))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	case <-ch:
		return true
	}
}

func (u *Updates) readLoop(f *os.File) {
	defer f.Close()
	buf := make([]byte, 256)
	for {
		if _, err := f.Read(buf); err != nil {
			return
		}
		u.bump()
	}
}

func (u *Updates) bump() {
	u.mu.Lock()
	close(u.ch)
	u.ch = make(chan struct{})
	u.rev++
	u.mu.Unlock()
}

// WakeWriter sends worker wake requests to the master over a pipe.
type WakeWriter struct {
	mu sync.Mutex
	f  *os.File
}

func NewWakeWriterFromFD(fd int) *WakeWriter {
	if fd < 0 {
		return nil
	}
	return &WakeWriter{f: os.NewFile(uintptr(fd), "proxy-wake")}
}

func (w *WakeWriter) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	return w.f.Close()
}

func (w *WakeWriter) Wake(wake routesync.RouteWake) {
	if w == nil || w.f == nil || !validRouteWake(wake) {
		return
	}
	b, err := json.Marshal(wake)
	if err != nil || len(b) > maxWakeFrame {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var lenbuf [4]byte
	binary.LittleEndian.PutUint32(lenbuf[:], uint32(len(b)))
	_, _ = w.f.Write(lenbuf[:])
	_, _ = w.f.Write(b)
}

// ReadWakeLoop drains a worker wake pipe and enqueues requests on master.
func ReadWakeLoop(ctx context.Context, f *os.File, wake func(routesync.RouteWake)) {
	defer f.Close()
	for ctx.Err() == nil {
		var lenbuf [4]byte
		if _, err := io.ReadFull(f, lenbuf[:]); err != nil {
			return
		}
		n := binary.LittleEndian.Uint32(lenbuf[:])
		if n == 0 || n > maxWakeFrame {
			return
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(f, buf); err != nil {
			return
		}
		var request routesync.RouteWake
		if err := json.Unmarshal(buf, &request); err != nil || !validRouteWake(request) {
			return
		}
		wake(request)
	}
}

const maxWakeFrame = 4 << 10

func validRouteWake(wake routesync.RouteWake) bool {
	if wake.SandboxID == "" || len(wake.SandboxID) > maxSandboxID {
		return false
	}
	hasFence := wake.NodeID != "" || wake.NodeEpoch != 0 || wake.RegistryGeneration != "" || wake.BindingDigest != ""
	if !hasFence {
		return true
	}
	return wake.NodeID != "" && len(wake.NodeID) <= maxNodeID && wake.NodeEpoch != 0 &&
		wake.RegistryGeneration != "" && len(wake.RegistryGeneration) <= maxGeneration &&
		wake.BindingDigest != "" && len(wake.BindingDigest) <= maxDigest
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
