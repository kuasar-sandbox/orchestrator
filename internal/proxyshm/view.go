package proxyshm

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
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

func (v *MasterView) ApplyDelete(sid string) {
	v.table.Delete(sid)
	v.notify.Notify()
}

func (v *MasterView) Bookmark() {
	v.table.Bookmark()
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

func (v *MasterView) NextWake(ctx context.Context) (string, bool) {
	return v.wakes.Next(ctx)
}

func (v *MasterView) Wake(sid string) {
	v.wakes.Enqueue(sid)
}

func (v *MasterView) RegisterNotifyWriter(f *os.File) func() {
	return v.notify.Add(f)
}

// WakeQueue deduplicates wake requests until routesync consumes them.
type WakeQueue struct {
	mu      sync.Mutex
	ch      chan string
	pending map[string]bool
}

func NewWakeQueue(size int) *WakeQueue {
	if size <= 0 {
		size = 1024
	}
	return &WakeQueue{ch: make(chan string, size), pending: map[string]bool{}}
}

func (q *WakeQueue) Enqueue(sid string) {
	if sid == "" {
		return
	}
	q.mu.Lock()
	if q.pending[sid] {
		q.mu.Unlock()
		return
	}
	q.pending[sid] = true
	q.mu.Unlock()
	select {
	case q.ch <- sid:
	default:
		q.mu.Lock()
		delete(q.pending, sid)
		q.mu.Unlock()
	}
}

func (q *WakeQueue) Next(ctx context.Context) (string, bool) {
	select {
	case sid := <-q.ch:
		q.mu.Lock()
		delete(q.pending, sid)
		q.mu.Unlock()
		return sid, true
	case <-ctx.Done():
		return "", false
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
	wake        func(string)
	defaultPark time.Duration
}

func NewWorkerView(table *Table, updates *Updates, wake func(string), defaultPark time.Duration) *WorkerView {
	if defaultPark <= 0 {
		defaultPark = 30 * time.Second
	}
	return &WorkerView{table: table, updates: updates, wake: wake, defaultPark: defaultPark}
}

func (v *WorkerView) Route(ctx context.Context, sid string, target proxy.ConnectTarget) (proxy.Route, error) {
	if !v.waitSynced(ctx) {
		return proxy.Route{Kind: proxy.KindNotFound}, nil
	}
	if current, found := v.table.Lookup(sid); found && current.State != routesync.StateDead {
		selected := proxy.RouteForTarget(
			current.Profile, current.EnvdUDS, current.CiUDS, current.FloatingIP,
			current.EnvdAccessToken, current.ForwardAccessToken, target,
		)
		// Do not wake a paused sandbox for a recognized target that has no
		// backend. service=exec remains this side-effect-free 501 boundary until
		// #64 installs its authenticated wake + ctl.sock path.
		if selected.Kind == proxy.KindDeny {
			return selected, nil
		}
		if current.State == routesync.StateRunning {
			return selected, nil
		}
	}
	r, ok := v.Resolve(ctx, sid)
	if !ok {
		return proxy.Route{Kind: proxy.KindNotFound}, nil
	}
	return proxy.RouteForTarget(
		r.Profile, r.EnvdUDS, r.CiUDS, r.FloatingIP,
		r.EnvdAccessToken, r.ForwardAccessToken, target,
	), nil
}

// LookupExec reads the node-local identity needed by the exec KAT gate. It is
// deliberately side-effect-free: a paused route remains paused and no Wake is
// emitted until the caller has authenticated the capability.
func (v *WorkerView) LookupExec(ctx context.Context, sid string) (proxy.ExecIdentity, bool, error) {
	if !v.waitSynced(ctx) {
		return proxy.ExecIdentity{}, false, nil
	}
	r, ok := v.table.Lookup(sid)
	identity, present := workerExecIdentity(r, ok)
	return identity, present, nil
}

// ActivateExec is entered only after the proxy has authenticated the KAT against
// expected. It wakes a paused sandbox, waits for the running route update, and
// rejects any node-local or credential identity change before returning.
func (v *WorkerView) ActivateExec(ctx context.Context, sid string, expected proxy.ExecIdentity) (proxy.ExecIdentity, bool, error) {
	if !v.waitSynced(ctx) {
		return proxy.ExecIdentity{}, false, nil
	}
	r, ok := v.table.Lookup(sid)
	identity, present := workerExecIdentity(r, ok)
	if !present || identity != expected {
		return proxy.ExecIdentity{}, false, nil
	}
	if r.State == routesync.StateRunning {
		return identity, true, nil
	}
	if v.wake != nil {
		v.wake(sid)
	}
	identity, present = v.waitExecRunning(ctx, sid, expected)
	return identity, present, nil
}

func workerExecIdentity(r routesync.RouteEntry, found bool) (proxy.ExecIdentity, bool) {
	if !found || (r.State != routesync.StateRunning && r.State != routesync.StatePaused) {
		return proxy.ExecIdentity{}, false
	}
	identity := proxy.ExecIdentity{
		NodeSandboxID: r.SandboxID,
		AuthSandboxID: r.AuthSandboxID,
		ServiceSecret: r.ServiceSecret,
	}
	if identity.NodeSandboxID == "" || identity.AuthSandboxID == "" || identity.ServiceSecret == "" {
		return proxy.ExecIdentity{}, false
	}
	return identity, true
}

func (v *WorkerView) waitExecRunning(ctx context.Context, sid string, expected proxy.ExecIdentity) (proxy.ExecIdentity, bool) {
	deadline := time.Now().Add(v.parkTimeout())
	for {
		if ctx.Err() != nil {
			return proxy.ExecIdentity{}, false
		}
		rev := v.table.Rev()
		r, ok := v.table.Lookup(sid)
		identity, present := workerExecIdentity(r, ok)
		if !present || identity != expected {
			return proxy.ExecIdentity{}, false
		}
		if r.State == routesync.StateRunning {
			return identity, true
		}
		if !v.waitChange(ctx, deadline, rev) {
			return proxy.ExecIdentity{}, false
		}
	}
}

func (v *WorkerView) Resolve(ctx context.Context, sid string) (routesync.RouteEntry, bool) {
	if !v.waitSynced(ctx) {
		return routesync.RouteEntry{}, false
	}
	if r, ok := v.table.Lookup(sid); ok && r.State == routesync.StateRunning {
		return r, true
	}
	if v.wake != nil {
		v.wake(sid)
	}
	return v.waitRunning(ctx, sid)
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

func (v *WorkerView) waitRunning(ctx context.Context, sid string) (routesync.RouteEntry, bool) {
	deadline := time.Now().Add(v.parkTimeout())
	for {
		if r, ok := v.table.Lookup(sid); ok && r.State == routesync.StateRunning {
			return r, true
		}
		if !v.waitChange(ctx, deadline, v.table.Rev()) {
			r, ok := v.table.Lookup(sid)
			return r, ok && r.State == routesync.StateRunning
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

var _ proxy.ExecRouter = (*WorkerView)(nil)

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

func (w *WakeWriter) Wake(sid string) {
	if w == nil || w.f == nil || sid == "" || strings.ContainsAny(sid, "\r\n") {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var lenbuf [4]byte
	b := []byte(sid)
	binary.LittleEndian.PutUint32(lenbuf[:], uint32(len(b)))
	_, _ = w.f.Write(lenbuf[:])
	_, _ = w.f.Write(b)
}

// ReadWakeLoop drains a worker wake pipe and enqueues requests on master.
func ReadWakeLoop(ctx context.Context, f *os.File, wake func(string)) {
	defer f.Close()
	for ctx.Err() == nil {
		var lenbuf [4]byte
		if _, err := io.ReadFull(f, lenbuf[:]); err != nil {
			return
		}
		n := binary.LittleEndian.Uint32(lenbuf[:])
		if n == 0 || n > maxSandboxID {
			return
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(f, buf); err != nil {
			return
		}
		wake(string(buf))
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
