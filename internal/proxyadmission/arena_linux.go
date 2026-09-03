//go:build linux

// Package proxyadmission owns the mutable process-shared inflight arena used by
// independent Proxy workers. Route SHM remains master-write/worker-read-only;
// each admission worker writes only its own counter row.
package proxyadmission

import (
	"errors"
	"fmt"
	"hash/fnv"
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
	arenaMagic   uint64 = 0x6b757341444d3031 // "kusaADM01"
	ArenaVersion uint32 = 1

	stateEmpty    uint64 = 0
	stateActive   uint64 = 1
	stateDraining uint64 = 2
	masterGuard   uint64 = math.MaxUint64

	serviceCount = 4
)

var (
	ErrLimitReached = errors.New("proxy admission: max inflight reached")
	ErrStaleBinding = errors.New("proxy admission: stale binding")

	headerSize = align(int(unsafe.Sizeof(arenaHeader{})), 8)
	entrySize  = align(int(unsafe.Sizeof(entryHeader{})), 8)
	rowSize    = align(int(unsafe.Sizeof(counterRow{})), 8)
)

type Service uint32

const (
	ServiceForward Service = iota
	ServiceE2BEnvd
	ServiceE2BCodeInterpreter
	ServiceExec
)

// Binding is the fixed route-view reference workers use for admission. Slot is
// one-based; zero is the unlimited fast path.
type Binding struct {
	Slot       uint32
	Generation uint64
	Limits     config.MaxInflight
}

func (b Binding) Unlimited() bool {
	return b.Slot == 0 && b.Generation == 0 && b.Limits.Unlimited()
}

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

type entryHeader struct {
	State      uint64
	Generation uint64
	Identity   uint64
	Total      uint32
	Forward    uint32
	Envd       uint32
	CI         uint32
	Exec       uint32
	_          uint32
}

type counterRow struct {
	Guard    uint64
	Counters [serviceCount]uint64
}

type mapping struct {
	data          []byte
	header        *arenaHeader
	workers       int
	routeCapacity int
	entryCapacity int
	entryStride   int
}

// Size returns the complete mmap size, including one spare transaction entry
// used to make a full-table identity replacement rollback-safe. After commit,
// the retired entry becomes the next spare.
func Size(routeCapacity, workers int) (int, error) {
	if routeCapacity <= 0 {
		return 0, fmt.Errorf("proxy admission: route capacity must be positive")
	}
	if workers <= 0 {
		return 0, fmt.Errorf("proxy admission: worker count must be positive")
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
	entries := routeCapacity + 1
	body, ok := checkedMul(entries, stride)
	if !ok {
		return 0, fmt.Errorf("proxy admission: entries overflow layout")
	}
	total, ok := checkedAdd(headerSize, body)
	if !ok || uint64(total) > uint64(math.MaxInt) {
		return 0, fmt.Errorf("proxy admission: mmap size overflows layout")
	}
	return total, nil
}

// MemoryReport separates the contract's absolute counter bytes from layout
// coordination overhead.
type MemoryReport struct {
	CounterBytes int
	HeaderBytes  int
	GuardBytes   int
	ScratchBytes int
	MappedBytes  int
}

func Report(routeCapacity, workers int) (MemoryReport, error) {
	mapped, err := Size(routeCapacity, workers)
	if err != nil {
		return MemoryReport{}, err
	}
	workerCounterBytes, ok := checkedMul(workers, serviceCount*8)
	if !ok {
		return MemoryReport{}, fmt.Errorf("proxy admission: counter report overflows")
	}
	counters, ok := checkedMul(routeCapacity, workerCounterBytes)
	if !ok {
		return MemoryReport{}, fmt.Errorf("proxy admission: counter report overflows")
	}
	workerGuardBytes, ok := checkedMul(workers, 8)
	if !ok {
		return MemoryReport{}, fmt.Errorf("proxy admission: guard report overflows")
	}
	guards, ok := checkedMul(routeCapacity, workerGuardBytes)
	if !ok {
		return MemoryReport{}, fmt.Errorf("proxy admission: guard report overflows")
	}
	headers, _ := checkedMul(routeCapacity, entrySize)
	return MemoryReport{
		CounterBytes: counters,
		HeaderBytes:  headerSize + headers,
		GuardBytes:   guards,
		ScratchBytes: entrySize + workers*rowSize,
		MappedBytes:  mapped,
	}, nil
}

type masterRecord struct {
	identity string
	binding  Binding
	syncGen  uint64
}

// Master owns entry allocation and generation transitions.
type Master struct {
	mapping
	file *os.File

	mu           sync.Mutex
	records      map[string]masterRecord
	free         []uint32
	nextGen      uint64
	syncGen      uint64
	closed       atomic.Bool
	workerMu     sync.Mutex
	workerEpochs []uint64
}

// NewMaster creates a size-sealed writable memfd and maps it shared.
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
	closeOnError := func(err error) (*Master, error) {
		_ = file.Close()
		return nil, err
	}
	if err := file.Truncate(int64(size)); err != nil {
		return closeOnError(fmt.Errorf("proxy admission: truncate: %w", err))
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return closeOnError(fmt.Errorf("proxy admission: mmap: %w", err))
	}
	if _, err := unix.FcntlInt(file.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); err != nil {
		_ = unix.Munmap(data)
		return closeOnError(fmt.Errorf("proxy admission: seal size: %w", err))
	}
	mapped, err := mappingFromBytes(data, routeCapacity, workers, false)
	if err != nil {
		_ = unix.Munmap(data)
		return closeOnError(err)
	}
	mapped.header.Magic = arenaMagic
	mapped.header.Version = ArenaVersion
	mapped.header.Workers = uint32(workers)
	mapped.header.RouteCapacity = uint32(routeCapacity)
	mapped.header.EntryCapacity = uint32(routeCapacity + 1)
	mapped.header.EntryStride = uint32(mapped.entryStride)
	mapped.header.RowStride = uint32(rowSize)
	mapped.header.MappedSize = uint64(size)
	master := &Master{
		mapping: mapped, file: file, records: make(map[string]masterRecord),
		free: make([]uint32, routeCapacity+1), nextGen: 1, workerEpochs: make([]uint64, workers),
	}
	for i := range master.free {
		master.free[i] = uint32(routeCapacity + 1 - i)
	}
	return master, nil
}

// BeginWorker records the exact process epoch that owns one writable column.
func (m *Master) BeginWorker(workerIndex int, epoch uint64) error {
	if m == nil || workerIndex < 0 || workerIndex >= m.workers || epoch == 0 || epoch == masterGuard {
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

// DupFile returns a CLOEXEC descriptor copy suitable for one worker's
// ExtraFiles handoff. The caller owns the result.
func (m *Master) DupFile() (*os.File, error) {
	if m == nil || m.file == nil || m.closed.Load() {
		return nil, fmt.Errorf("proxy admission: master is closed")
	}
	fd, err := unix.FcntlInt(m.file.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, fmt.Errorf("proxy admission: duplicate memfd: %w", err)
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

// Bookmark retires bindings not replayed in the current full sync.
func (m *Master) Bookmark() {
	m.mu.Lock()
	for sid, record := range m.records {
		if record.syncGen != m.syncGen {
			m.retireBinding(record.binding)
			delete(m.records, sid)
		}
	}
	m.mu.Unlock()
}

// Delete first prevents new acquire, then safely retires the old generation.
func (m *Master) Delete(sid string) {
	m.mu.Lock()
	if record, found := m.records[sid]; found {
		m.retireBinding(record.binding)
		delete(m.records, sid)
	}
	m.mu.Unlock()
}

// ClearWorker clears one absolute column only after the supervisor has reaped
// that exact worker epoch. It intentionally does not touch route ownership.
func (m *Master) ClearWorker(workerIndex int, epoch uint64) error {
	if m == nil || workerIndex < 0 || workerIndex >= m.workers || epoch == 0 || epoch == masterGuard {
		return fmt.Errorf("proxy admission: invalid worker cleanup identity")
	}
	m.workerMu.Lock()
	defer m.workerMu.Unlock()
	if m.workerEpochs[workerIndex] != epoch {
		return fmt.Errorf("proxy admission: worker cleanup epoch mismatch")
	}
	for slot := 1; slot <= m.entryCapacity; slot++ {
		row := m.row(uint32(slot), workerIndex)
		claimExitedWorkerRow(row)
		for index := range row.Counters {
			atomic.StoreUint64(&row.Counters[index], 0)
		}
		atomic.StoreUint64(&row.Guard, 0)
	}
	m.workerEpochs[workerIndex] = 0
	return nil
}

// Update holds the master's allocation lock until Commit or Rollback.
type Update struct {
	master       *Master
	sid          string
	old          masterRecord
	hadOld       bool
	next         masterRecord
	newAllocated bool
	oldDrained   bool
	policySlot   uint32
	oldLimits    config.MaxInflight
	done         atomic.Bool
}

func (u *Update) Binding() Binding {
	if u == nil {
		return Binding{}
	}
	return u.next.binding
}

// PrepareUpsert validates and publishes the effective entry before route SHM
// publication. The caller must finish with Commit or Rollback.
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
	u := &Update{master: m, sid: sid, old: old, hadOld: hadOld}
	u.next = masterRecord{identity: identity, syncGen: m.syncGen, binding: Binding{Limits: limits}}

	sameIdentity := hadOld && old.identity == identity
	switch {
	case limits.Unlimited():
		if hadOld && old.binding.Slot != 0 {
			m.beginDrain(old.binding)
			u.oldDrained = true
		}
	case sameIdentity && old.binding.Slot != 0:
		u.next.binding.Slot = old.binding.Slot
		u.next.binding.Generation = old.binding.Generation
		if old.binding.Limits != limits {
			u.policySlot = old.binding.Slot
			u.oldLimits = old.binding.Limits
			m.replaceLimits(old.binding.Slot, old.binding.Generation, limits)
		}
	default:
		if hadOld && old.binding.Slot != 0 {
			m.beginDrain(old.binding)
			u.oldDrained = true
		}
		binding, err := m.allocate(limits, identity)
		if err != nil {
			if u.oldDrained {
				m.reactivate(old.binding)
			}
			return nil, err
		}
		u.next.binding = binding
		u.newAllocated = true
	}
	return u, nil
}

func (u *Update) Commit() {
	if u == nil || !u.done.CompareAndSwap(false, true) {
		return
	}
	if u.oldDrained {
		u.master.retireDrained(u.old.binding)
	}
	u.master.records[u.sid] = u.next
	u.master.mu.Unlock()
}

func (u *Update) Rollback() {
	if u == nil || !u.done.CompareAndSwap(false, true) {
		return
	}
	if u.policySlot != 0 {
		u.master.replaceLimits(u.policySlot, u.next.binding.Generation, u.oldLimits)
	}
	if u.newAllocated {
		u.master.retireBinding(u.next.binding)
	}
	if u.oldDrained {
		u.master.reactivate(u.old.binding)
	}
	u.master.mu.Unlock()
}

func (m *Master) allocate(limits config.MaxInflight, identity string) (Binding, error) {
	if len(m.free) == 0 {
		return Binding{}, fmt.Errorf("proxy admission: arena full")
	}
	if m.nextGen == 0 || m.nextGen == math.MaxUint64 {
		return Binding{}, fmt.Errorf("proxy admission: generation exhausted")
	}
	slot := m.free[len(m.free)-1]
	m.free = m.free[:len(m.free)-1]
	generation := m.nextGen
	m.nextGen++
	entry := m.entry(slot)
	guards := m.claimRows(slot)
	clearEntry(entry)
	atomic.StoreUint64(&entry.Generation, generation)
	entry.Identity = hashIdentity(identity)
	storeLimits(entry, limits)
	atomic.StoreUint64(&entry.State, stateActive)
	guards()
	return Binding{Slot: slot, Generation: generation, Limits: limits}, nil
}

func (m *Master) beginDrain(binding Binding) {
	if binding.Slot == 0 {
		return
	}
	entry := m.entry(binding.Slot)
	if atomic.LoadUint64(&entry.Generation) == binding.Generation {
		atomic.CompareAndSwapUint64(&entry.State, stateActive, stateDraining)
	}
}

func (m *Master) reactivate(binding Binding) {
	if binding.Slot == 0 {
		return
	}
	entry := m.entry(binding.Slot)
	if atomic.LoadUint64(&entry.Generation) == binding.Generation {
		atomic.StoreUint64(&entry.State, stateActive)
	}
}

func (m *Master) replaceLimits(slot uint32, generation uint64, limits config.MaxInflight) {
	entry := m.entry(slot)
	atomic.StoreUint64(&entry.State, stateDraining)
	release := m.claimRows(slot)
	if atomic.LoadUint64(&entry.Generation) == generation {
		storeLimits(entry, limits)
		atomic.StoreUint64(&entry.State, stateActive)
	}
	release()
}

func (m *Master) retireBinding(binding Binding) {
	if binding.Slot == 0 {
		return
	}
	m.beginDrain(binding)
	m.retireDrained(binding)
}

func (m *Master) retireDrained(binding Binding) {
	if binding.Slot == 0 {
		return
	}
	entry := m.entry(binding.Slot)
	release := m.claimRows(binding.Slot)
	if atomic.LoadUint64(&entry.Generation) == binding.Generation {
		atomic.StoreUint64(&entry.State, stateEmpty)
		clearEntry(entry)
		for worker := 0; worker < m.workers; worker++ {
			row := m.row(binding.Slot, worker)
			for index := range row.Counters {
				atomic.StoreUint64(&row.Counters[index], 0)
			}
		}
		m.free = append(m.free, binding.Slot)
	}
	release()
}

func (m *Master) claimRows(slot uint32) func() {
	rows := make([]*counterRow, 0, m.workers)
	for worker := 0; worker < m.workers; worker++ {
		row := m.row(slot, worker)
		claimMasterRow(row)
		rows = append(rows, row)
	}
	return func() {
		for _, row := range rows {
			atomic.StoreUint64(&row.Guard, 0)
		}
	}
}

func claimMasterRow(row *counterRow) {
	for !atomic.CompareAndSwapUint64(&row.Guard, 0, masterGuard) {
		runtime.Gosched()
	}
}

// claimExitedWorkerRow may reclaim a worker epoch left in Guard by a process
// killed inside TryAcquire or Release. The caller invokes this only after
// cmd.Wait has proved that no process can still execute against this row.
// Another master operation is represented by masterGuard and must finish
// normally; it is never stolen.
func claimExitedWorkerRow(row *counterRow) {
	for {
		guard := atomic.LoadUint64(&row.Guard)
		if guard == masterGuard {
			runtime.Gosched()
			continue
		}
		if atomic.CompareAndSwapUint64(&row.Guard, guard, masterGuard) {
			return
		}
	}
}

// Worker owns exactly one writable absolute-counter column.
type Worker struct {
	mapping
	index     int
	epoch     uint64
	closed    atomic.Bool
	afterScan func()
	afterAdd  func()
}

// OpenWorker maps one inherited admission descriptor and validates its exact
// immutable layout against the frozen worker configuration.
func OpenWorker(file *os.File, routeCapacity, workers, workerIndex int, epoch uint64) (*Worker, error) {
	if file == nil {
		return nil, fmt.Errorf("proxy admission: missing descriptor")
	}
	defer file.Close()
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		return nil, fmt.Errorf("proxy admission: unsupported architecture %s", runtime.GOARCH)
	}
	if workerIndex < 0 || workerIndex >= workers || epoch == 0 || epoch == masterGuard {
		return nil, fmt.Errorf("proxy admission: invalid worker identity")
	}
	st, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("proxy admission: stat descriptor: %w", err)
	}
	expected, err := Size(routeCapacity, workers)
	if err != nil {
		return nil, err
	}
	if st.Size() != int64(expected) {
		return nil, fmt.Errorf("proxy admission: mmap size %d, want %d", st.Size(), expected)
	}
	data, err := unix.Mmap(int(file.Fd()), 0, expected, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("proxy admission: mmap worker: %w", err)
	}
	mapped, err := mappingFromBytes(data, routeCapacity, workers, true)
	if err != nil {
		_ = unix.Munmap(data)
		return nil, err
	}
	return &Worker{mapping: mapped, index: workerIndex, epoch: epoch}, nil
}

func (w *Worker) Close() error {
	if w == nil || !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	err := unix.Munmap(w.data)
	w.data = nil
	return err
}

// TryAcquire reads every worker's four absolute cells once, checks total and
// target-service limits together, then publishes this worker's increment before
// returning the lease.
func (w *Worker) TryAcquire(binding Binding, service Service) (*Lease, error) {
	if w == nil || w.closed.Load() || binding.Slot == 0 || binding.Generation == 0 || int(service) >= serviceCount {
		return nil, ErrStaleBinding
	}
	if int(binding.Slot) > w.entryCapacity {
		return nil, ErrStaleBinding
	}
	entry := w.entry(binding.Slot)
	if atomic.LoadUint64(&entry.State) != stateActive || atomic.LoadUint64(&entry.Generation) != binding.Generation {
		return nil, ErrStaleBinding
	}
	row := w.row(binding.Slot, w.index)
	for !atomic.CompareAndSwapUint64(&row.Guard, 0, w.epoch) {
		runtime.Gosched()
	}
	defer atomic.StoreUint64(&row.Guard, 0)
	if atomic.LoadUint64(&entry.State) != stateActive || atomic.LoadUint64(&entry.Generation) != binding.Generation || loadLimits(entry) != binding.Limits {
		return nil, ErrStaleBinding
	}

	var total, target uint64
	for worker := 0; worker < w.workers; worker++ {
		other := w.row(binding.Slot, worker)
		for index := 0; index < serviceCount; index++ {
			value := atomic.LoadUint64(&other.Counters[index])
			total = saturatedAdd(total, value)
			if index == int(service) {
				target = saturatedAdd(target, value)
			}
		}
	}
	if w.afterScan != nil {
		w.afterScan()
	}
	serviceLimit := limitFor(binding.Limits, service)
	if (binding.Limits.Total != 0 && total >= uint64(binding.Limits.Total)) ||
		(serviceLimit != 0 && target >= uint64(serviceLimit)) {
		return nil, ErrLimitReached
	}
	atomic.AddUint64(&row.Counters[service], 1)
	if w.afterAdd != nil {
		w.afterAdd()
	}
	return &Lease{worker: w, binding: binding, service: service}, nil
}

// Valid reports whether a binding still names the active entry and policy.
// Activation uses this after a successful acquire so an identity replacement
// cannot pass through the interval between draining the old arena generation
// and publishing the replacement route record. The unlimited path remains a
// pure value check and does not touch the arena.
func (w *Worker) Valid(binding Binding) bool {
	if binding.Unlimited() {
		return true
	}
	if w == nil || w.closed.Load() || binding.Slot == 0 || binding.Generation == 0 ||
		int(binding.Slot) > w.entryCapacity {
		return false
	}
	entry := w.entry(binding.Slot)
	row := w.row(binding.Slot, w.index)
	for !atomic.CompareAndSwapUint64(&row.Guard, 0, w.epoch) {
		runtime.Gosched()
	}
	defer atomic.StoreUint64(&row.Guard, 0)
	return atomic.LoadUint64(&entry.State) == stateActive &&
		atomic.LoadUint64(&entry.Generation) == binding.Generation &&
		loadLimits(entry) == binding.Limits
}

type Lease struct {
	worker  *Worker
	binding Binding
	service Service
	closed  atomic.Bool
}

func (l *Lease) Release() {
	if l == nil || !l.closed.CompareAndSwap(false, true) || l.worker == nil || l.worker.closed.Load() {
		return
	}
	w := l.worker
	if int(l.binding.Slot) > w.entryCapacity {
		return
	}
	row := w.row(l.binding.Slot, w.index)
	for !atomic.CompareAndSwapUint64(&row.Guard, 0, w.epoch) {
		runtime.Gosched()
	}
	defer atomic.StoreUint64(&row.Guard, 0)
	entry := w.entry(l.binding.Slot)
	state := atomic.LoadUint64(&entry.State)
	if (state != stateActive && state != stateDraining) || atomic.LoadUint64(&entry.Generation) != l.binding.Generation {
		return
	}
	cell := &row.Counters[l.service]
	for {
		current := atomic.LoadUint64(cell)
		if current == 0 {
			return
		}
		if atomic.CompareAndSwapUint64(cell, current, current-1) {
			return
		}
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
	header := (*arenaHeader)(unsafe.Pointer(&data[0]))
	stride := entrySize + workers*rowSize
	if validate {
		if header.Magic != arenaMagic || header.Version != ArenaVersion {
			return mapping{}, fmt.Errorf("proxy admission: unsupported arena version")
		}
		if int(header.Workers) != workers || int(header.RouteCapacity) != routeCapacity ||
			int(header.EntryCapacity) != routeCapacity+1 || int(header.EntryStride) != stride ||
			int(header.RowStride) != rowSize || header.MappedSize != uint64(expected) {
			return mapping{}, fmt.Errorf("proxy admission: invalid arena layout")
		}
	}
	first := uintptr(unsafe.Pointer(&data[0]))
	if first%8 != 0 || uintptr(headerSize)%8 != 0 || uintptr(stride)%8 != 0 || uintptr(rowSize)%8 != 0 {
		return mapping{}, fmt.Errorf("proxy admission: unaligned atomic layout")
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

func storeLimits(entry *entryHeader, limits config.MaxInflight) {
	entry.Total = limits.Total
	entry.Forward = limits.Forward
	entry.Envd = limits.E2BEnvd
	entry.CI = limits.E2BCodeInterpreter
	entry.Exec = limits.Exec
}

func loadLimits(entry *entryHeader) config.MaxInflight {
	return config.MaxInflight{Total: entry.Total, Forward: entry.Forward, E2BEnvd: entry.Envd, E2BCodeInterpreter: entry.CI, Exec: entry.Exec}
}

func clearEntry(entry *entryHeader) {
	atomic.StoreUint64(&entry.State, stateEmpty)
	atomic.StoreUint64(&entry.Generation, 0)
	entry.Identity = 0
	storeLimits(entry, config.MaxInflight{})
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

func hashIdentity(identity string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(identity))
	return h.Sum64()
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
