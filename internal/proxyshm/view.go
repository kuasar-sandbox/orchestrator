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

	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

var (
	errExecActivationTimeout  = errors.New("proxyshm: exec activation timed out")
	errRouteActivationTimeout = errors.New("proxyshm: route activation timed out")
)

// MasterView is the proxy master's routesync sink and wake source.
type MasterView struct {
	table     *Table
	mmds      *MMDSView
	wakes     *WakeQueue
	notify    *Broadcaster
	defaultPk time.Duration
	log       *slog.Logger
	// upsertPrimary is table.Upsert in production. Keeping the transaction's
	// fallible commit point explicit lets tests inject every rollback shape.
	upsertPrimary func(routesync.RouteEntry) error

	policyMu    sync.RWMutex
	policyMMDS  *routesync.MMDSProxyPolicy
	policyReady chan struct{}
	policyOnce  sync.Once
}

func NewMasterView(table *Table, defaultPark time.Duration, log *slog.Logger) *MasterView {
	if defaultPark <= 0 {
		defaultPark = 30 * time.Second
	}
	view := &MasterView{
		table:       table,
		mmds:        NewMMDSView(table.Capacity()),
		wakes:       NewWakeQueue(4096),
		notify:      NewBroadcaster(),
		defaultPk:   defaultPark,
		log:         log,
		policyReady: make(chan struct{}),
	}
	view.upsertPrimary = table.Upsert
	return view
}

func (v *MasterView) BeginSync() {
	v.table.SetMMDSSynced(false)
	v.table.BeginSync()
	v.mmds.BeginSync()
	v.table.clearAllMMDSSources()
	v.notify.Notify()
}

func (v *MasterView) ApplyUpsert(r routesync.RouteEntry) error {
	if err := validateRoute(r); err != nil {
		return err
	}
	oldRoute, oldFound := v.table.Lookup(r.SandboxID)
	heapSnapshot := v.mmds.snapshotEntry(r.SandboxID)
	affectedIPv4s := make([]uint32, 0, 2)
	if oldFound && activeMMDSSourceRoute(oldRoute) {
		if ipv4, ok := parseMMDSSourceIPv4(oldRoute.FloatingIP); ok {
			affectedIPv4s = append(affectedIPv4s, ipv4)
		}
	}
	active := r.State == routesync.StateStarting || r.State == routesync.StateRunning
	incomingIPv4Valid := false
	if active {
		if ipv4, ok := parseMMDSSourceIPv4(r.FloatingIP); ok {
			affectedIPv4s = append(affectedIPv4s, ipv4)
			incomingIPv4Valid = true
		}
	}
	sourceSnapshots := v.table.snapshotMMDSSourceSlots(affectedIPv4s...)
	if active {
		// Publish the heap view first. A worker that observes an active SHM row
		// can then resolve either the new view or a conservative unavailable.
		if err := v.mmds.Upsert(r); err != nil {
			v.mmds.restoreEntry(r.SandboxID, heapSnapshot)
			if v.log != nil {
				v.log.Warn("proxyshm: apply MMDS route view", "sid", r.SandboxID, "err", err)
			}
			return err
		}
	} else {
		// Revoke the confidential view first. A worker that sampled the old
		// active SHM row immediately before this update then fails closed at
		// master lookup instead of receiving a value after pause/deletion.
		v.mmds.Delete(r.SandboxID)
	}
	conflict, sourceOwnerReplaced := v.table.updateMMDSSources(oldRoute, oldFound, r)
	if err := v.upsertPrimary(r); err != nil {
		v.table.restoreMMDSSourceSlots(sourceSnapshots)
		v.mmds.restoreEntry(r.SandboxID, heapSnapshot)
		if v.log != nil {
			v.log.Warn("proxyshm: apply route", "sid", r.SandboxID, "err", err)
		}
		return err
	}
	if active && r.FloatingIP != "" && !incomingIPv4Valid && v.log != nil {
		v.log.Warn("proxyshm: active route has invalid MMDS source IPv4",
			"sid", r.SandboxID, "floating_ip", r.FloatingIP)
	}
	if sourceOwnerReplaced {
		logMMDSSourceConflict(v.log, conflict)
	}
	v.notify.Notify()
	return nil
}

func (v *MasterView) ApplyDelete(sid string) {
	oldRoute, oldFound := v.table.Lookup(sid)
	v.mmds.Delete(sid)
	v.table.removeRouteMMDSSource(oldRoute, oldFound)
	v.table.Delete(sid)
	v.notify.Notify()
}

func (v *MasterView) Bookmark() {
	v.table.Bookmark()
	v.table.rebuildMMDSSources(v.log)
	v.mmds.Bookmark()
	// Publish readiness only after both the fixed identity view and the
	// confidential heap have completed the same full-sync generation.
	v.table.SetMMDSSynced(true)
	v.notify.Notify()
}

func (v *MasterView) SetPolicy(p routesync.Policy) {
	if p.ParkTimeoutMS <= 0 {
		p.ParkTimeoutMS = int(v.defaultPk / time.Millisecond)
	}
	if err := v.table.SetPolicy(p); err != nil && v.log != nil {
		v.log.Warn("proxyshm: set policy", "err", err)
	}
	var policyCopy *routesync.MMDSProxyPolicy
	if p.MMDS != nil {
		copy := *p.MMDS
		copy.Services = cloneStringMap(p.MMDS.Services)
		policyCopy = &copy
		if err := v.mmds.SetServices(copy.Services); err != nil && v.log != nil {
			v.log.Warn("proxyshm: reject MMDS service registry", "err", err)
		}
	} else {
		_ = v.mmds.SetServices(nil)
	}
	v.policyMu.Lock()
	v.policyMMDS = policyCopy
	v.policyMu.Unlock()
	v.policyOnce.Do(func() { close(v.policyReady) })
	v.notify.Notify()
}

// InvalidateSync is called as soon as the route-sync session ends. The base
// fixed SHM table retains its existing reconnect behavior; MMDS heap state is
// cleared immediately because it contains plaintext and service authority.
func (v *MasterView) InvalidateSync() {
	v.table.SetMMDSSynced(false)
	v.mmds.BeginSync()
	v.notify.Notify()
}

func (v *MasterView) ResolveMMDS(sandboxID, exactPath string) mmdsrpc.EndpointResponse {
	return v.mmds.Resolve(sandboxID, exactPath)
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
		_ = f.Close()
		b.mu.Unlock()
	}
}

func (b *Broadcaster) Notify() {
	// The pipes are nonblocking, so keep the lock through each write to serialize
	// f.Fd() and unix.Write with the remover's delete-and-close operation.
	b.mu.Lock()
	for _, f := range b.files {
		_, err := unix.Write(int(f.Fd()), []byte{1})
		if err == nil || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			continue
		}
	}
	b.mu.Unlock()
}

// WorkerView is a read-only proxy.Router and MMDS source backed by shared memory.
type WorkerView struct {
	table       *Table
	updates     *Updates
	wake        func(string)
	defaultPark time.Duration
	mmdsClient  *mmdsrpc.Client
	mmdsTimeout time.Duration
}

func NewWorkerView(table *Table, updates *Updates, wake func(string), defaultPark time.Duration) *WorkerView {
	if defaultPark <= 0 {
		defaultPark = 30 * time.Second
	}
	return &WorkerView{table: table, updates: updates, wake: wake, defaultPark: defaultPark}
}

// NewMMDSWorkerView adds the inherited master RPC connection used only by an
// external proxy worker. Existing non-MMDS callers retain NewWorkerView.
func NewMMDSWorkerView(table *Table, updates *Updates, wake func(string), defaultPark time.Duration, client *mmdsrpc.Client) *WorkerView {
	view := NewWorkerView(table, updates, wake, defaultPark)
	view.mmdsClient = client
	view.mmdsTimeout = 2 * time.Second
	return view
}

// LookupRoute waits for initial sync and, for an initially missing SID, passive
// route propagation. It never emits Wake or performs backend I/O.
func (v *WorkerView) LookupRoute(ctx context.Context, sid string, target proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	if !v.waitSynced(ctx) {
		if err := ctx.Err(); err != nil {
			return proxy.RouteBinding{}, false, err
		}
		return proxy.RouteBinding{}, false, nil
	}
	deadline := time.Now().Add(v.parkTimeout())
	_, _, initialRouteRev := v.table.LookupRevision(sid)
	for {
		rev := v.table.Rev()
		r, found, routeRev := v.table.LookupRevision(sid)
		binding, present := workerRouteBinding(r, found, target)
		if present {
			return binding, true, nil
		}
		// Once a record (including dead/malformed) or a terminal revision has
		// been observed, absence is authoritative. Only the initial propagation
		// gap is parked, without a Wake.
		if found || initialRouteRev != 0 || routeRev != initialRouteRev {
			return proxy.RouteBinding{}, false, nil
		}
		if !v.waitChange(ctx, deadline, rev) {
			if err := ctx.Err(); err != nil {
				return proxy.RouteBinding{}, false, err
			}
			return proxy.RouteBinding{}, false, nil
		}
	}
}

// ActivateRoute revalidates the authorized binding before Wake and after every
// shared-table revision. Only the final running record supplies the backend.
func (v *WorkerView) ActivateRoute(ctx context.Context, expected proxy.RouteBinding) (proxy.Route, bool, error) {
	if !v.waitSynced(ctx) || expected.SandboxID == "" {
		if err := ctx.Err(); err != nil {
			return proxy.Route{}, false, err
		}
		return proxy.Route{}, false, nil
	}
	r, found, _ := v.table.LookupRevision(expected.SandboxID)
	binding, present := workerRouteBinding(r, found, expected.Target)
	if !present || binding != expected {
		return proxy.Route{}, false, nil
	}
	if r.State == routesync.StateRunning {
		return workerDialRoute(r, expected), true, nil
	}
	seenStarting := r.State == routesync.StateStarting
	woke := false
	if r.State == routesync.StatePaused && v.wake != nil {
		v.wake(expected.SandboxID)
		woke = true
	}
	return v.waitRouteActivated(ctx, expected, woke, seenStarting)
}

func workerRouteBinding(r routesync.RouteEntry, found bool, target proxy.ConnectTarget) (proxy.RouteBinding, bool) {
	if !found || (r.State != routesync.StateStarting && r.State != routesync.StateRunning && r.State != routesync.StatePaused) {
		return proxy.RouteBinding{}, false
	}
	binding := proxy.BindRoute(
		r.SandboxID, r.AuthSandboxID, types.Profile(r.Profile),
		r.EnvdAccessToken, r.ForwardAccessToken, target,
	)
	if binding.SandboxID == "" || binding.AuthSandboxID == "" {
		return proxy.RouteBinding{}, false
	}
	return binding, true
}

func workerDialRoute(r routesync.RouteEntry, expected proxy.RouteBinding) proxy.Route {
	return proxy.RouteForTarget(types.Profile(r.Profile), r.EnvdUDS, r.CiUDS, r.FloatingIP, expected.Target)
}

func (v *WorkerView) waitRouteActivated(ctx context.Context, expected proxy.RouteBinding, woke, seenStarting bool) (proxy.Route, bool, error) {
	deadline := time.Now().Add(v.parkTimeout())
	for {
		if err := ctx.Err(); err != nil {
			return proxy.Route{}, false, err
		}
		rev := v.table.Rev()
		r, found := v.table.Lookup(expected.SandboxID)
		binding, present := workerRouteBinding(r, found, expected.Target)
		if !present || binding != expected {
			return proxy.Route{}, false, nil
		}
		switch r.State {
		case routesync.StateRunning:
			route := workerDialRoute(r, expected)
			if route.Kind != expected.Kind {
				return proxy.Route{}, false, nil
			}
			return route, true, nil
		case routesync.StateStarting:
			seenStarting = true
		case routesync.StatePaused:
			if seenStarting {
				return proxy.Route{}, false, nil
			}
			if !woke && v.wake != nil {
				v.wake(expected.SandboxID)
				woke = true
			}
		default:
			return proxy.Route{}, false, nil
		}
		if !v.waitChange(ctx, deadline, rev) {
			if err := ctx.Err(); err != nil {
				return proxy.Route{}, false, err
			}
			return proxy.Route{}, false, errRouteActivationTimeout
		}
	}
}

// LookupExec reads the node-local identity needed by the exec KAT gate. It is
// deliberately side-effect-free: a paused route remains paused and no Wake is
// emitted until the caller has authenticated the capability. A missing first
// sample is parked briefly because Create may return before its starting route
// reaches this worker.
func (v *WorkerView) LookupExec(ctx context.Context, sid string) (proxy.ExecIdentity, bool, error) {
	if !v.waitSynced(ctx) {
		if err := ctx.Err(); err != nil {
			return proxy.ExecIdentity{}, false, err
		}
		return proxy.ExecIdentity{}, false, nil
	}
	deadline := time.Now().Add(v.parkTimeout())
	_, _, initialRouteRev := v.table.LookupRevision(sid)
	for {
		rev := v.table.Rev()
		r, ok, routeRev := v.table.LookupRevision(sid)
		identity, present := workerExecIdentity(r, ok)
		if present {
			return identity, true, nil
		}
		// An observed route with no live, complete identity is authoritative.
		// Only an initially missing route can still be in propagation.
		if ok || initialRouteRev != 0 || routeRev != initialRouteRev {
			return proxy.ExecIdentity{}, false, nil
		}
		if !v.waitChange(ctx, deadline, rev) {
			if err := ctx.Err(); err != nil {
				return proxy.ExecIdentity{}, false, err
			}
			return proxy.ExecIdentity{}, false, nil
		}
	}
}

// ActivateExec is entered only after the proxy has authenticated the KAT against
// expected. It wakes a paused sandbox or waits without another Wake when launch
// is already starting, then rejects any identity change before returning.
func (v *WorkerView) ActivateExec(ctx context.Context, sid string, expected proxy.ExecIdentity) (proxy.ExecIdentity, bool, error) {
	if !v.waitSynced(ctx) {
		return proxy.ExecIdentity{}, false, nil
	}
	r, ok, initialRev := v.table.LookupRevision(sid)
	identity, present := workerExecIdentity(r, ok)
	if !present || identity != expected {
		return proxy.ExecIdentity{}, false, nil
	}
	if r.State == routesync.StateRunning {
		return identity, true, nil
	}
	seenStarting := r.State == routesync.StateStarting
	woke := false
	if r.State == routesync.StatePaused && v.wake != nil {
		v.wake(sid)
		woke = true
	}
	return v.waitExecRunning(ctx, sid, expected, woke, seenStarting, initialRev)
}

func workerExecIdentity(r routesync.RouteEntry, found bool) (proxy.ExecIdentity, bool) {
	if !found || (r.State != routesync.StateStarting && r.State != routesync.StateRunning && r.State != routesync.StatePaused) {
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

func (v *WorkerView) waitExecRunning(
	ctx context.Context,
	sid string,
	expected proxy.ExecIdentity,
	woke, seenStarting bool,
	initialRev uint64,
) (proxy.ExecIdentity, bool, error) {
	deadline := time.Now().Add(v.parkTimeout())
	for {
		if err := ctx.Err(); err != nil {
			return proxy.ExecIdentity{}, false, err
		}
		rev := v.table.Rev()
		r, ok, routeRev := v.table.LookupRevision(sid)
		identity, present := workerExecIdentity(r, ok)
		if !present || identity != expected {
			return proxy.ExecIdentity{}, false, nil
		}
		if r.State == routesync.StateRunning {
			return identity, true, nil
		}
		if r.State == routesync.StateStarting {
			seenStarting = true
		} else if seenStarting || (woke && routeRev != initialRev) {
			return proxy.ExecIdentity{}, false, nil
		}
		if !v.waitChange(ctx, deadline, rev) {
			if err := ctx.Err(); err != nil {
				return proxy.ExecIdentity{}, false, err
			}
			return proxy.ExecIdentity{}, false, errExecActivationTimeout
		}
	}
}

func (v *WorkerView) ByFloatingIP(ip string) (string, bool) {
	if !v.MMDSAvailable() {
		return "", false
	}
	ipv4, ok := parseMMDSSourceIPv4(ip)
	if !ok {
		return "", false
	}
	return v.table.lookupMMDSSource(ipv4)
}

func (v *WorkerView) SandboxInfo(sid string) (templateID, accessToken string, ok bool) {
	if !v.MMDSAvailable() {
		return "", "", false
	}
	return v.table.SandboxInfo(sid)
}

func (v *WorkerView) MmdsSecret(sid string) ([]byte, bool) {
	if !v.MMDSAvailable() {
		return nil, false
	}
	return v.table.MmdsSecret(sid)
}

func (v *WorkerView) Incarnation(sid string) (string, bool) {
	if !v.MMDSAvailable() {
		return "", false
	}
	return v.table.Incarnation(sid)
}

func (v *WorkerView) MMDSAvailable() bool { return v.table.MMDSSynced() }

func (v *WorkerView) MMDSRoute(ctx context.Context, sandboxID, exactPath string) (mmds.MMDSRoute, bool, error) {
	if !v.MMDSAvailable() {
		return mmds.MMDSRoute{}, false, errors.New("proxyshm: MMDS route sync unavailable")
	}
	entry, ok := v.table.Lookup(sandboxID)
	if !ok || (entry.State != routesync.StateStarting && entry.State != routesync.StateRunning) {
		return mmds.MMDSRoute{}, false, nil
	}
	if v.mmdsClient == nil {
		return mmds.MMDSRoute{}, false, errors.New("proxyshm: MMDS RPC unavailable")
	}
	rpcCtx, cancel := context.WithTimeout(ctx, v.mmdsTimeout)
	defer cancel()
	response, err := v.mmdsClient.Resolve(rpcCtx, sandboxID, exactPath)
	if err != nil {
		return mmds.MMDSRoute{}, false, err
	}
	if response.Unavailable {
		return mmds.MMDSRoute{}, false, errors.New("proxyshm: MMDS route sync unavailable")
	}
	if !response.Found {
		return mmds.MMDSRoute{}, false, nil
	}
	switch response.Type {
	case "static":
		return mmds.MMDSRoute{Type: response.Type, ContentType: response.ContentType, Body: response.Body}, true, nil
	case "secret":
		return mmds.MMDSRoute{Type: response.Type, ContentType: response.ContentType, Body: response.Body, Present: response.Present}, true, nil
	case "service":
		result := mmdssvc.Call(ctx, response.ServiceSocket, response.Service, exactPath, sandboxID)
		return mmds.MMDSRoute{Type: response.Type, StatusCode: result.StatusCode, ContentType: result.ContentType, Body: result.Body}, true, nil
	default:
		return mmds.MMDSRoute{}, false, errors.New("proxyshm: invalid MMDS RPC response")
	}
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

var _ proxy.Router = (*WorkerView)(nil)
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
