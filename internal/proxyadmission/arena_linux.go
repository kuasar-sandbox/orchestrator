//go:build linux

// Package proxyadmission owns process-shared per-Sandbox inflight counters.
// The master publishes slot generations; each live worker exclusively writes
// its own generation-tagged rows. No process-shared locks are used.
package proxyadmission

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/config"
)

const (
	arenaMagic   uint64 = 0x6b757341444d3031
	ArenaVersion uint32 = 2
	serviceCount        = 4
)

var (
	ErrLimitReached = errors.New("proxy admission: max inflight reached")
	ErrStaleBinding = errors.New("proxy admission: stale binding")
	headerSize      = align(int(unsafe.Sizeof(arenaHeader{})), 8)
	entrySize       = align(int(unsafe.Sizeof(entryHeader{})), 8)
	rowSize         = align(int(unsafe.Sizeof(counterRow{})), 8)
)

type Service uint32

const (
	ServiceForward Service = iota
	ServiceE2BEnvd
	ServiceE2BCodeInterpreter
	ServiceExec
)

// Binding contains the master-resolved route policy and stable arena reference.
// Limits live in route SHM, not in a second mutable arena policy copy.
type Binding struct {
	Slot       uint32
	Generation uint64
	Limits     config.MaxInflight
}

func (b Binding) Unlimited() bool { return b.Slot == 0 && b.Generation == 0 && b.Limits.Unlimited() }

type arenaHeader struct {
	Magic         uint64
	Version       uint32
	Workers       uint32
	RouteCapacity uint32
	EntryCapacity uint32
	EntryStride   uint32
	RowStride     uint32
	_             uint32
	MappedSize    uint64
	_2            [16]byte
}

// Only the master writes this field. Zero revokes the slot. A nonzero generation
// is never reused for another identity during the lifetime of this arena.
type entryHeader struct{ Generation uint64 }

// A live worker is the sole writer of its rows. Generation is published only
// after counters are initialized. The supervisor may clear a column only after
// that exact worker process has been reaped and before starting its replacement.
type counterRow struct {
	Generation uint64
	Counters   [serviceCount]uint64
}

type mapping struct {
	data          []byte
	header        *arenaHeader
	workers       int
	routeCapacity int
	entryCapacity int
	entryStride   int
}

// Size includes one spare entry for rollback-safe full-table replacement.
func Size(routeCapacity, workers int) (int, error) {
	if routeCapacity <= 0 || workers <= 0 {
		return 0, fmt.Errorf("proxy admission: capacity and worker count must be positive")
	}
	if uint64(routeCapacity) >= uint64(math.MaxUint32) || uint64(workers) > uint64(math.MaxUint32) {
		return 0, fmt.Errorf("proxy admission: capacity or worker count overflows layout")
	}
	rows, ok := checkedMul(workers, rowSize)
	if !ok {
		return 0, fmt.Errorf("proxy admission: worker rows overflow layout")
	}
	stride, ok := checkedAdd(entrySize, rows)
	if !ok || uint64(stride) > uint64(math.MaxUint32) {
		return 0, fmt.Errorf("proxy admission: entry stride overflows layout")
	}
	body, ok := checkedMul(routeCapacity+1, stride)
	if !ok {
		return 0, fmt.Errorf("proxy admission: entries overflow layout")
	}
	total, ok := checkedAdd(headerSize, body)
	if !ok {
		return 0, fmt.Errorf("proxy admission: mmap size overflows layout")
	}
	return total, nil
}

type MemoryReport struct {
	CounterBytes int
	HeaderBytes  int
	RowTagBytes  int
	ScratchBytes int
	MappedBytes  int
}

func Report(routeCapacity, workers int) (MemoryReport, error) {
	mapped, err := Size(routeCapacity, workers)
	if err != nil {
		return MemoryReport{}, err
	}
	// Successful Size already bounds these smaller products.
	return MemoryReport{
		CounterBytes: routeCapacity * workers * serviceCount * 8,
		HeaderBytes:  headerSize + routeCapacity*entrySize,
		RowTagBytes:  routeCapacity * workers * 8,
		ScratchBytes: entrySize + workers*rowSize,
		MappedBytes:  mapped,
	}, nil
}

type masterRecord struct {
	identity string
	binding  Binding
	syncGen  uint64
}
type Master struct {
	mapping
	file         *os.File
	mu           sync.Mutex
	records      map[string]masterRecord
	free         []uint32
	nextGen      uint64
	syncGen      uint64
	closed       atomic.Bool
	workerMu     sync.Mutex
	workerEpochs []uint64
}

func NewMaster(routeCapacity, workers int) (*Master, error) {
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		return nil, fmt.Errorf("proxy admission: unsupported architecture %s", runtime.GOARCH)
	}
	size, err := Size(routeCapacity, workers)
	if err != nil {
		return nil, err
	}
	fd, err := unix.MemfdCreate("kuasar-proxy-admission", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("proxy admission: memfd: %w", err)
	}
	file := os.NewFile(uintptr(fd), "kuasar-proxy-admission")
	fail := func(err error) (*Master, error) { _ = file.Close(); return nil, err }
	if err := file.Truncate(int64(size)); err != nil {
		return fail(err)
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fail(err)
	}
	if _, err := unix.FcntlInt(file.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); err != nil {
		_ = unix.Munmap(data)
		return fail(err)
	}
	mapped, err := mappingFromBytes(data, routeCapacity, workers, false)
	if err != nil {
		_ = unix.Munmap(data)
		return fail(err)
	}
	*mapped.header = arenaHeader{Magic: arenaMagic, Version: ArenaVersion, Workers: uint32(workers), RouteCapacity: uint32(routeCapacity), EntryCapacity: uint32(routeCapacity + 1), EntryStride: uint32(mapped.entryStride), RowStride: uint32(rowSize), MappedSize: uint64(size)}
	master := &Master{mapping: mapped, file: file, records: make(map[string]masterRecord), free: make([]uint32, routeCapacity+1), nextGen: 1, workerEpochs: make([]uint64, workers)}
	for i := range master.free {
		master.free[i] = uint32(routeCapacity + 1 - i)
	}
	return master, nil
}

func (m *Master) BeginWorker(workerIndex int, epoch uint64) error {
	if m == nil || workerIndex < 0 || workerIndex >= m.workers || epoch == 0 {
		return fmt.Errorf("proxy admission: invalid worker identity")
	}
	m.workerMu.Lock()
	defer m.workerMu.Unlock()
	if current := m.workerEpochs[workerIndex]; current != 0 {
		return fmt.Errorf("proxy admission: worker column %d is still owned by epoch %d", workerIndex, current)
	}
	m.workerEpochs[workerIndex] = epoch
	return nil
}
func (m *Master) DupFile() (*os.File, error) {
	if m == nil || m.file == nil || m.closed.Load() {
		return nil, fmt.Errorf("proxy admission: master is closed")
	}
	fd, err := unix.FcntlInt(m.file.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "kuasar-proxy-admission-worker"), nil
}
func (m *Master) Close() error {
	if m == nil || !m.closed.CompareAndSwap(false, true) {
		return nil
	}
	err := unix.Munmap(m.data)
	m.data = nil
	return errors.Join(err, m.file.Close())
}
func (m *Master) BeginSync() {
	m.mu.Lock()
	m.syncGen++
	if m.syncGen == 0 {
		m.syncGen++
	}
	m.mu.Unlock()
}
func (m *Master) Bookmark() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for sid, record := range m.records {
		if record.syncGen != m.syncGen {
			m.retire(record.binding)
			delete(m.records, sid)
		}
	}
}
func (m *Master) Delete(sid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if record, found := m.records[sid]; found {
		m.retire(record.binding)
		delete(m.records, sid)
	}
}

// ClearWorker is called only after cmd.Wait for this epoch. No live writer can
// remain in the column, so it never claims another process's lock. Route apply
// only changes entry generations and cannot form a wait cycle with cleanup.
func (m *Master) ClearWorker(workerIndex int, epoch uint64) error {
	if m == nil || workerIndex < 0 || workerIndex >= m.workers || epoch == 0 {
		return fmt.Errorf("proxy admission: invalid worker cleanup identity")
	}
	m.workerMu.Lock()
	defer m.workerMu.Unlock()
	if m.workerEpochs[workerIndex] != epoch {
		return fmt.Errorf("proxy admission: worker cleanup epoch mismatch")
	}
	for slot := 1; slot <= m.entryCapacity; slot++ {
		row := m.row(uint32(slot), workerIndex)
		atomic.StoreUint64(&row.Generation, 0)
		for i := range row.Counters {
			atomic.StoreUint64(&row.Counters[i], 0)
		}
	}
	m.workerEpochs[workerIndex] = 0
	return nil
}

// Update keeps allocation serialization in the master only. Workers never
// acquire m.mu; Commit/Rollback do not wait for any worker to make progress.
type Update struct {
	master       *Master
	sid          string
	old          masterRecord
	next         masterRecord
	newAllocated bool
	oldRevoked   bool
	done         bool
}

func (u *Update) Binding() Binding {
	if u == nil {
		return Binding{}
	}
	return u.next.binding
}
func (m *Master) PrepareUpsert(sid, identity string, limits config.MaxInflight) (_ *Update, returnErr error) {
	if m == nil || sid == "" || identity == "" || m.closed.Load() {
		return nil, fmt.Errorf("proxy admission: invalid route identity")
	}
	m.mu.Lock()
	defer func() {
		if returnErr != nil {
			m.mu.Unlock()
		}
	}()
	old, hadOld := m.records[sid]
	u := &Update{master: m, sid: sid, old: old, next: masterRecord{identity: identity, syncGen: m.syncGen, binding: Binding{Limits: limits}}}
	if hadOld && old.identity == identity && !limits.Unlimited() && old.binding.Slot != 0 {
		// Normal lifecycle/replay preserves the generation and outstanding
		// counts. Route binding equality fences effective policy changes.
		u.next.binding.Slot = old.binding.Slot
		u.next.binding.Generation = old.binding.Generation
		return u, nil
	}
	if old.binding.Slot != 0 {
		m.revoke(old.binding)
		u.oldRevoked = true
	}
	if !limits.Unlimited() {
		binding, err := m.allocate(limits)
		if err != nil {
			if u.oldRevoked {
				m.activate(old.binding)
			}
			return nil, err
		}
		u.next.binding = binding
		u.newAllocated = true
	}
	return u, nil
}
func (u *Update) Commit() {
	if u == nil || u.done {
		return
	}
	u.done = true
	if u.oldRevoked {
		u.master.retire(u.old.binding)
	}
	u.master.records[u.sid] = u.next
	u.master.mu.Unlock()
}
func (u *Update) Rollback() {
	if u == nil || u.done {
		return
	}
	u.done = true
	if u.newAllocated {
		u.master.retire(u.next.binding)
	}
	if u.oldRevoked {
		u.master.activate(u.old.binding)
	}
	u.master.mu.Unlock()
}
func (m *Master) allocate(limits config.MaxInflight) (Binding, error) {
	if len(m.free) == 0 {
		return Binding{}, fmt.Errorf("proxy admission: arena full")
	}
	if m.nextGen == 0 || m.nextGen == math.MaxUint64 {
		return Binding{}, fmt.Errorf("proxy admission: generation exhausted")
	}
	slot := m.free[len(m.free)-1]
	m.free = m.free[:len(m.free)-1]
	binding := Binding{Slot: slot, Generation: m.nextGen, Limits: limits}
	m.nextGen++
	// Rows are not cleared here. Their owners initialize them lazily under a
	// process-local slot mutex. Other generations contribute zero to this one.
	m.activate(binding)
	return binding, nil
}
func (m *Master) activate(binding Binding) {
	atomic.StoreUint64(&m.entry(binding.Slot).Generation, binding.Generation)
}
func (m *Master) revoke(binding Binding) {
	if binding.Slot != 0 {
		atomic.CompareAndSwapUint64(&m.entry(binding.Slot).Generation, binding.Generation, 0)
	}
}
func (m *Master) retire(binding Binding) {
	if binding.Slot != 0 {
		m.revoke(binding)
		m.free = append(m.free, binding.Slot)
	}
}

type Worker struct {
	mapping
	index int
	epoch uint64
	// Indexed by slot, not SID: a reused slot must serialize old releases and
	// new initialization even when it now belongs to a different Sandbox.
	slots     []sync.Mutex
	closed    atomic.Bool
	afterScan func()
	afterAdd  func()
}

func OpenWorker(file *os.File, routeCapacity, workers, workerIndex int, epoch uint64) (*Worker, error) {
	if file == nil {
		return nil, fmt.Errorf("proxy admission: missing descriptor")
	}
	defer file.Close()
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		return nil, fmt.Errorf("proxy admission: unsupported architecture %s", runtime.GOARCH)
	}
	if workerIndex < 0 || workerIndex >= workers || epoch == 0 {
		return nil, fmt.Errorf("proxy admission: invalid worker identity")
	}
	expected, err := Size(routeCapacity, workers)
	if err != nil {
		return nil, err
	}
	st, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() != int64(expected) {
		return nil, fmt.Errorf("proxy admission: mmap size %d, want %d", st.Size(), expected)
	}
	data, err := unix.Mmap(int(file.Fd()), 0, expected, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	mapped, err := mappingFromBytes(data, routeCapacity, workers, true)
	if err != nil {
		_ = unix.Munmap(data)
		return nil, err
	}
	return &Worker{mapping: mapped, index: workerIndex, epoch: epoch, slots: make([]sync.Mutex, routeCapacity+1)}, nil
}

// Close is for an owner that has already stopped all readers. Production worker
// assembly retains successful mappings for the one-shot subprocess lifetime.
func (w *Worker) Close() error {
	if w == nil || !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	err := unix.Munmap(w.data)
	w.data = nil
	return err
}
func (w *Worker) validSlot(binding Binding) bool {
	return w != nil && !w.closed.Load() && binding.Slot != 0 && binding.Generation != 0 && int(binding.Slot) <= w.entryCapacity
}

// Valid checks generation liveness only. Effective policy and credentials are
// fenced separately by the existing full RouteBinding equality in WorkerView.
func (w *Worker) Valid(binding Binding) bool {
	if binding.Unlimited() {
		return true
	}
	return w.validSlot(binding) && atomic.LoadUint64(&w.entry(binding.Slot).Generation) == binding.Generation
}

func (w *Worker) TryAcquire(binding Binding, service Service) (*Lease, error) {
	if !w.validSlot(binding) || int(service) >= serviceCount {
		return nil, ErrStaleBinding
	}
	lock := &w.slots[int(binding.Slot)-1]
	lock.Lock()
	defer lock.Unlock()
	if !w.Valid(binding) {
		return nil, ErrStaleBinding
	}
	row := w.row(binding.Slot, w.index)
	if atomic.LoadUint64(&row.Generation) != binding.Generation {
		atomic.StoreUint64(&row.Generation, 0)
		for i := range row.Counters {
			atomic.StoreUint64(&row.Counters[i], 0)
		}
		atomic.StoreUint64(&row.Generation, binding.Generation)
	}
	var total, target uint64
	for worker := 0; worker < w.workers; worker++ {
		other := w.row(binding.Slot, worker)
		before := atomic.LoadUint64(&other.Generation)
		if before != binding.Generation {
			continue
		}
		var subtotal, subtarget uint64
		for i := 0; i < serviceCount; i++ {
			count := atomic.LoadUint64(&other.Counters[i])
			subtotal = saturatedAdd(subtotal, count)
			if i == int(service) {
				subtarget = count
			}
		}
		if atomic.LoadUint64(&other.Generation) != before {
			continue
		}
		total = saturatedAdd(total, subtotal)
		target = saturatedAdd(target, subtarget)
	}
	if w.afterScan != nil {
		w.afterScan()
	}
	if !w.Valid(binding) {
		return nil, ErrStaleBinding
	}
	serviceLimit := limitFor(binding.Limits, service)
	if (binding.Limits.Total != 0 && total >= uint64(binding.Limits.Total)) || (serviceLimit != 0 && target >= uint64(serviceLimit)) {
		return nil, ErrLimitReached
	}
	if atomic.LoadUint64(&row.Counters[service]) == math.MaxUint64 {
		return nil, ErrLimitReached
	}
	atomic.AddUint64(&row.Counters[service], 1)
	if w.afterAdd != nil {
		w.afterAdd()
	}
	if !w.Valid(binding) {
		// Only this worker can write this row while it is alive; the local
		// slot lock excludes initialization by a new request in this process.
		atomic.AddUint64(&row.Counters[service], ^uint64(0))
		return nil, ErrStaleBinding
	}
	return &Lease{worker: w, binding: binding, service: service}, nil
}

type Lease struct {
	worker  *Worker
	binding Binding
	service Service
	closed  atomic.Bool
}

func (l *Lease) Release() {
	if l == nil || !l.closed.CompareAndSwap(false, true) || !l.worker.validSlot(l.binding) {
		return
	}
	w := l.worker
	lock := &w.slots[int(l.binding.Slot)-1]
	lock.Lock()
	defer lock.Unlock()
	row := w.row(l.binding.Slot, w.index)
	if atomic.LoadUint64(&row.Generation) != l.binding.Generation {
		return
	}
	cell := &row.Counters[l.service]
	if atomic.LoadUint64(cell) > 0 {
		atomic.AddUint64(cell, ^uint64(0))
	}
}

func mappingFromBytes(data []byte, routeCapacity, workers int, validate bool) (mapping, error) {
	expected, err := Size(routeCapacity, workers)
	if err != nil {
		return mapping{}, err
	}
	if len(data) != expected || len(data) < headerSize {
		return mapping{}, fmt.Errorf("proxy admission: invalid mmap size %d, want %d", len(data), expected)
	}
	first := uintptr(unsafe.Pointer(&data[0]))
	stride := entrySize + workers*rowSize
	if first%8 != 0 || headerSize%8 != 0 || stride%8 != 0 || rowSize%8 != 0 {
		return mapping{}, fmt.Errorf("proxy admission: unaligned atomic layout")
	}
	header := (*arenaHeader)(unsafe.Pointer(&data[0]))
	if validate {
		if header.Magic != arenaMagic || header.Version != ArenaVersion {
			return mapping{}, fmt.Errorf("proxy admission: unsupported arena version")
		}
		if int(header.Workers) != workers || int(header.RouteCapacity) != routeCapacity || int(header.EntryCapacity) != routeCapacity+1 || int(header.EntryStride) != stride || int(header.RowStride) != rowSize || header.MappedSize != uint64(expected) {
			return mapping{}, fmt.Errorf("proxy admission: invalid arena layout")
		}
	}
	return mapping{data: data, header: header, workers: workers, routeCapacity: routeCapacity, entryCapacity: routeCapacity + 1, entryStride: stride}, nil
}
func (m *mapping) entry(slot uint32) *entryHeader {
	offset := headerSize + (int(slot)-1)*m.entryStride
	return (*entryHeader)(unsafe.Pointer(&m.data[offset]))
}
func (m *mapping) row(slot uint32, worker int) *counterRow {
	offset := headerSize + (int(slot)-1)*m.entryStride + entrySize + worker*rowSize
	return (*counterRow)(unsafe.Pointer(&m.data[offset]))
}
func limitFor(limits config.MaxInflight, service Service) uint32 {
	switch service {
	case ServiceForward:
		return limits.Forward
	case ServiceE2BEnvd:
		return limits.E2BEnvd
	case ServiceE2BCodeInterpreter:
		return limits.E2BCodeInterpreter
	case ServiceExec:
		return limits.Exec
	default:
		return 0
	}
}
func saturatedAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}
func checkedAdd(left, right int) (int, bool) {
	if left < 0 || right < 0 || left > math.MaxInt-right {
		return 0, false
	}
	return left + right, true
}
func checkedMul(left, right int) (int, bool) {
	if left < 0 || right < 0 || (left != 0 && right > math.MaxInt/left) {
		return 0, false
	}
	return left * right, true
}
func align(value, boundary int) int { return (value + boundary - 1) &^ (boundary - 1) }
