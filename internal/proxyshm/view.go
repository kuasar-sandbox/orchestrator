package proxyshm

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// defaultMMDSRPCTimeout is WorkerView's fallback when the caller passes
// mmdsRPCTimeout <= 0 — config.DefaultProxyRPCTimeout is the single source of
// truth for this value (also seeds ProxyFileConfig.ProxyRPCTimeout's default
// and ProxyRPCTimeoutDur's parse-failure fallback), so this package doesn't
// carry its own independent "2 * time.Second" literal. Deliberately a
// different knob from proxy.mode=internal's park_timeout: that one bounds a
// guest request waiting on distributed route-sync convergence (tens of
// seconds is normal), while this bounds a worker's own local socketpair round
// trip to the proxy master, which should be near-instant; reusing
// park_timeout's longer default would let a stuck master turn every MMDS
// lookup into a multi-second guest-visible stall instead of failing fast.
const defaultMMDSRPCTimeout = config.DefaultProxyRPCTimeout

var errExecActivationTimeout = errors.New("proxyshm: exec activation timed out")

// MasterView is the proxy master's routesync sink and wake source.
type MasterView struct {
	table     *Table
	mmds      *MMDSRoutes
	secrets   *MMDSSecrets
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
		mmds:      NewMMDSRoutes(),
		secrets:   NewMMDSSecrets(),
		wakes:     NewWakeQueue(4096),
		notify:    NewBroadcaster(),
		defaultPk: defaultPark,
		log:       log,
	}
}

// MMDSRoutes returns the sparse in-heap MMDS-specification store (see
// MMDSRoutes's doc comment for why this rides outside the mmap Table) —
// wired into an internal/mmdsrpc.Server per worker.
func (v *MasterView) MMDSRoutes() *MMDSRoutes { return v.mmds }

// MMDSSecrets returns the sparse in-heap decrypted-secret-blob store (see
// MMDSSecrets's doc comment) — wired into an internal/mmdsrpc.Server per
// worker alongside MMDSRoutes. Only ever populated when this master
// registered with the gated MMDSSecrets capability; otherwise it stays empty.
func (v *MasterView) MMDSSecrets() *MMDSSecrets { return v.secrets }

func (v *MasterView) BeginSync() {
	v.table.BeginSync()
	v.mmds.BeginSync()
	v.secrets.BeginSync()
	v.notify.Notify()
}

func (v *MasterView) ApplyUpsert(r routesync.RouteEntry) {
	if err := v.table.Upsert(r); err != nil {
		if v.log != nil {
			v.log.Warn("proxyshm: apply route", "sid", r.SandboxID, "err", err)
		}
		return
	}
	v.mmds.Upsert(r.SandboxID, r.MMDSRoutes)
	v.secrets.Upsert(r.SandboxID, r.MMDSSecrets)
	v.notify.Notify()
}

func (v *MasterView) ApplyDelete(sid string) {
	v.table.Delete(sid)
	v.mmds.Delete(sid)
	v.secrets.Delete(sid)
	v.notify.Notify()
}

func (v *MasterView) Bookmark() {
	v.table.Bookmark()
	v.mmds.Bookmark()
	v.secrets.Bookmark()
	v.notify.Notify()
}

// InvalidateSync implements routesync.Sink: it fails the secret view and the
// route-declaration Synced() signal closed immediately when the sync stream
// ends for any reason (disconnect, protocol error), rather than leaving them
// reporting synced for the entire reconnect backoff window until the next
// session's BeginSync/Bookmark completes.
//
// secrets: reuses MMDSSecrets.BeginSync -- eagerly discarding held plaintext
// and marking the store unsynced is exactly InvalidateSync's contract for
// secret data.
//
// mmds: reuses MMDSRoutes.BeginSync, which (unlike MMDSSecrets) does NOT
// eagerly clear byID -- calling it here only flips Synced() to false, so a
// "service" route lookup fails closed (mmdsRPCHandler checks
// MMDSRoutes.Synced() before dialing) while a "static" route stays servable
// from its last-known declaration, exactly as option A specified: only the
// route type that drives a real action against a possibly-stale declaration
// needs to fail closed, not the type that just returns immutable content.
//
// table (the base, non-MMDS route table: EnvdUDS, FloatingIP, exec identity,
// etc.) is deliberately untouched -- it holds no secret plaintext and no
// service-dial-driving declarations, and issue #42's fail-closed requirement
// is scoped to MMDS, not general route data.
//
// The next session's own BeginSync call is a no-op on top of both of these
// (same effect, one more generation bump each).
func (v *MasterView) InvalidateSync() {
	v.secrets.BeginSync()
	v.mmds.BeginSync()
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
	table          *Table
	updates        *Updates
	wake           func(string)
	defaultPark    time.Duration
	mmdsClient     *mmdsrpc.Client // nil => MMDSRoute always reports unspecified, no error
	mmdsRPCTimeout time.Duration
	services       mmdssvc.Registry // node operator's local trusted services for "service" routes; nil-safe (Call 503s on a lookup miss)
}

// NewWorkerView builds a WorkerView. mmdsClient may be nil (MMDSRoute then
// always returns ok=false, err=nil — treated as "no MMDS routes specified").
// mmdsRPCTimeout <= 0 falls back to defaultMMDSRPCTimeout. services is this
// worker's own local copy of the node operator's service registry (each
// worker independently loads the same proxy.yaml the master does).
func NewWorkerView(table *Table, updates *Updates, wake func(string), defaultPark time.Duration, mmdsClient *mmdsrpc.Client, mmdsRPCTimeout time.Duration, services mmdssvc.Registry) *WorkerView {
	if defaultPark <= 0 {
		defaultPark = 30 * time.Second
	}
	if mmdsRPCTimeout <= 0 {
		mmdsRPCTimeout = defaultMMDSRPCTimeout
	}
	return &WorkerView{table: table, updates: updates, wake: wake, defaultPark: defaultPark, mmdsClient: mmdsClient, mmdsRPCTimeout: mmdsRPCTimeout, services: services}
}

// MMDSRoute resolves sid's specified MMDS route at path by asking the proxy
// master over internal/mmdsrpc (implements mmds.Source; external mode's
// per-sandbox MMDS specifications live in the master's in-heap
// MMDSRoutes store, not the shared-memory Table -- see MMDSRoutes's doc
// comment for why).
// MMDSRoute resolves sid's specified MMDS route at path by asking the proxy
// master over internal/mmdsrpc. For a "secret" route that has never been
// configured (Retryable=true), this parks -- bounded by the master-pushed
// Policy.MMDSParkTimeoutMS -- re-asking the master on every shared-table
// change (mirrors waitRunning's loop shape), so a guest whose Create beat the
// operator's admin PUT doesn't see a spurious 404 during that narrow window.
// A secret that was configured and later revoked (Retryable=false) returns
// immediately, matching internal-mode Orchestrator.MMDSRoute.
//
// Each table revision used to decide whether to keep waiting is sampled
// *before* the RPC call it's paired with, not after: an admin PUT bumps the
// table revision as part of the same sync apply that updates the master's
// secret store, so a revision sampled after the RPC call could already
// postdate a PUT the RPC response itself doesn't yet reflect -- waitChange
// would then wait for a "future" change that already happened, sitting out
// the full park timeout despite the PUT having just landed (the same
// lost-wakeup shape internal-mode Orchestrator.MMDSRoute had). Sampling first
// guarantees any PUT landing after the sample is visible either in the RPC
// response itself (if it lands before the master answers) or as a revision
// change waitChange can see (if it lands after).
func (v *WorkerView) MMDSRoute(sid, path string) (mmds.MMDSRoute, bool, error) {
	if v.mmdsClient == nil {
		return mmds.MMDSRoute{}, false, nil
	}
	// table is nil only in unit tests that never exercise the secret-retry
	// loop below (production always provides a real shared-mmap table); rev
	// staying 0 in that case is harmless since it's never read unless that
	// loop actually runs, which itself already assumes a non-nil table (see
	// waitChange).
	var rev uint64
	if v.table != nil {
		rev = v.table.Rev()
	}
	resp, err := v.resolveMMDSOnce(sid, path)
	if err != nil {
		return mmds.MMDSRoute{}, false, err
	}
	if !resp.Found {
		return mmds.MMDSRoute{}, false, nil
	}
	if resp.Type == "service" {
		if resp.Unavailable {
			// The master's route declaration view isn't currently synced
			// (mid-resync after a disconnect, or never yet synced) -- a
			// service route drives a real dial under the sandbox's identity,
			// so fail closed (503) rather than act on a stale declaration.
			// See EndpointResponse.Unavailable's doc comment.
			return mmds.MMDSRoute{Type: resp.Type, StatusCode: http.StatusServiceUnavailable}, true, nil
		}
		return v.serviceRoute(sid, path, resp), true, nil
	}
	if resp.Type != "secret" || resp.Present || !resp.Retryable {
		return mmdsRouteFromResponse(resp)
	}
	park := v.mmdsParkTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), park)
	defer cancel()
	deadline := time.Now().Add(park)
	for {
		if !v.waitChange(ctx, deadline, rev) {
			return mmdsRouteFromResponse(resp) // last known (absent) response
		}
		rev = v.table.Rev()
		resp, err = v.resolveMMDSOnce(sid, path)
		if err != nil {
			return mmds.MMDSRoute{}, false, err
		}
		if !resp.Found {
			return mmds.MMDSRoute{}, false, nil
		}
		if resp.Type != "secret" || resp.Present || !resp.Retryable {
			return mmdsRouteFromResponse(resp)
		}
	}
}

// resolveMMDSOnce makes one bounded round trip to the proxy master, distinct
// from -- and nested inside -- MMDSRoute's overall park deadline: this bounds
// a single same-node socketpair call (near-instant), not the guest-visible
// wait for an admin PUT to land.
func (v *WorkerView) resolveMMDSOnce(sid, path string) (mmdsrpc.EndpointResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), v.mmdsRPCTimeout)
	defer cancel()
	return v.mmdsClient.Resolve(ctx, sid, path)
}

// mmdsRouteFromResponse converts one master response into the mmds.Source
// return shape. A "secret" body is base64-encoded on the wire (see
// mmdsrpc.EndpointResponse's doc comment -- secret values aren't required to
// be valid UTF-8, unlike a static route's body) and is decoded here, the one
// place that needs raw bytes.
func mmdsRouteFromResponse(resp mmdsrpc.EndpointResponse) (mmds.MMDSRoute, bool, error) {
	if resp.Type != "secret" {
		return mmds.MMDSRoute{Type: resp.Type, ContentType: resp.ContentType, Data: resp.Body}, true, nil
	}
	if resp.Unavailable {
		// The master's secret view isn't currently synced (mid-resync after
		// a disconnect, or never yet synced) -- fail closed via a hard error
		// (mmds.Server.getMeta maps err!=nil to 503), never a stale value or
		// a false "never configured" 404.
		return mmds.MMDSRoute{}, false, errors.New("proxyshm: mmds secret sync unavailable")
	}
	route := mmds.MMDSRoute{Type: resp.Type, Present: resp.Present}
	if !resp.Present {
		return route, true, nil
	}
	body, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return mmds.MMDSRoute{}, false, fmt.Errorf("proxyshm: decode mmds secret body for %q: %w", resp.Type, err)
	}
	route.ContentType = resp.ContentType
	route.Data = string(body)
	return route, true, nil
}

// serviceRoute dials resp.Target (the master-resolved operator-registered
// service name) directly over this worker's own local mmdssvc.Registry --
// the master never dials it itself, see EndpointResponse's doc comment.
func (v *WorkerView) serviceRoute(sid, path string, resp mmdsrpc.EndpointResponse) mmds.MMDSRoute {
	result := mmdssvc.Call(context.Background(), v.services, resp.Target, path, sid, v.authSubject(sid), resp.ServiceName)
	return mmds.MMDSRoute{Type: resp.Type, StatusCode: result.StatusCode, ContentType: result.ContentType, Data: result.Body, RetryAfter: result.RetryAfter}
}

// authSubject resolves sid's stable credential subject (routesync.RouteEntry.
// AuthSandboxID, already mirrored into this worker's local shared Table) for
// the X-Kuasar-Sandbox-Subject header -- falling back to sid itself if the
// table has no entry or no AuthSandboxID recorded.
func (v *WorkerView) authSubject(sid string) string {
	if v.table != nil {
		if e, ok := v.table.Lookup(sid); ok && e.AuthSandboxID != "" {
			return e.AuthSandboxID
		}
	}
	return sid
}

// mmdsParkTimeout is the master-pushed bound for MMDSRoute's secret-retry
// loop -- the external-mode mirror of config.MMDSSecretRoutesConfig's
// park_timeout, carried over routesync.Policy.MMDSParkTimeoutMS rather than
// read from local config (a worker has no config file of its own).
func (v *WorkerView) mmdsParkTimeout() time.Duration {
	p := v.table.Policy()
	if p.MMDSParkTimeoutMS > 0 {
		return time.Duration(p.MMDSParkTimeoutMS) * time.Millisecond
	}
	return v.defaultPark
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
		// Generic route resolution does not wake a paused sandbox for a target
		// with no backend. Exec CONNECT is handled earlier by the authenticated
		// LookupExec/ActivateExec path; direct Route callers remain fail-closed.
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
	return v.waitExecRunning(ctx, sid, expected)
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

func (v *WorkerView) waitExecRunning(ctx context.Context, sid string, expected proxy.ExecIdentity) (proxy.ExecIdentity, bool, error) {
	deadline := time.Now().Add(v.parkTimeout())
	for {
		if err := ctx.Err(); err != nil {
			return proxy.ExecIdentity{}, false, err
		}
		rev := v.table.Rev()
		r, ok := v.table.Lookup(sid)
		identity, present := workerExecIdentity(r, ok)
		if !present || identity != expected {
			return proxy.ExecIdentity{}, false, nil
		}
		if r.State == routesync.StateRunning {
			return identity, true, nil
		}
		if !v.waitChange(ctx, deadline, rev) {
			if err := ctx.Err(); err != nil {
				return proxy.ExecIdentity{}, false, err
			}
			return proxy.ExecIdentity{}, false, errExecActivationTimeout
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

func (v *WorkerView) Incarnation(sid string) (string, bool) {
	return v.table.Incarnation(sid)
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
