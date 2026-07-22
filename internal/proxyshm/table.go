// Package proxyshm stores the external proxy route view in shared memory.
//
// The proxy master is the only writer. Worker processes mmap the same file
// read-only and resolve routes locally on the data path. Each record is protected
// by a small seqlock, so workers either observe a complete old record or a
// complete new record while the master applies routesync updates.
package proxyshm

import (
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	magic  uint64 = 0x6b75736172505831 // "kusarPX1"
	schema uint32 = 2

	statusEmpty   uint32 = 0
	statusPresent uint32 = 1
	statusDeleted uint32 = 2

	defaultCapacity = 65536

	maxSandboxID   = 128
	maxNodeID      = 128
	maxGeneration  = 128
	maxDigest      = 64
	maxProfile     = 16
	maxTemplateID  = 128
	maxState       = 16
	maxUDS         = 256
	maxFloatingIP  = 64
	maxAccessToken = 256
	maxSnapLoc     = 32
	maxMmdsSecret  = 128
)

var (
	headerSize = alignSize(int(unsafe.Sizeof(mmapHeader{})), 8)
	recordSize = int(unsafe.Sizeof(mmapRecord{}))
)

type mmapHeader struct {
	Magic        uint64
	Schema       uint32
	Capacity     uint32
	Synced       uint32
	_            uint32
	GlobalRev    uint64
	SyncGen      uint64
	PolicySeq    uint64
	PolicyParkMS int64
	PolicyAuth   [maxProfile]byte
	_            [32]byte
}

type mmapRecord struct {
	Seq       uint64
	Hash      uint64
	Status    uint32
	_         uint32
	SyncGen   uint64
	Rev       uint64
	NodeEpoch uint64
	EventSeq  uint64

	SandboxID          [maxSandboxID]byte
	NodeID             [maxNodeID]byte
	RegistryGeneration [maxGeneration]byte
	BindingDigest      [maxDigest]byte
	Profile            [maxProfile]byte
	TemplateID         [maxTemplateID]byte
	State              [maxState]byte
	EnvdUDS            [maxUDS]byte
	CiUDS              [maxUDS]byte
	FloatingIP         [maxFloatingIP]byte
	AccessToken        [maxAccessToken]byte
	TrafficAccessToken [maxAccessToken]byte
	SnapshotLocation   [maxSnapLoc]byte
	MmdsSecret         [maxMmdsSecret]byte
}

// Table is a memory-mapped fixed-capacity route table.
type Table struct {
	path     string
	data     []byte
	header   *mmapHeader
	records  []mmapRecord
	readonly bool
}

// Create replaces path with a zeroed route table of capacity records.
func Create(path string, capacity int) (*Table, error) {
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	if err := os.MkdirAll(parentDir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	size := Size(capacity)
	if err := f.Truncate(int64(size)); err != nil {
		return nil, err
	}
	data, err := unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	t, err := tableFromMmap(path, data, false, capacity)
	if err != nil {
		_ = unix.Munmap(data)
		return nil, err
	}
	t.header.Magic = magic
	t.header.Schema = schema
	t.header.Capacity = uint32(capacity)
	return t, nil
}

// Open maps an existing table read-only. Worker processes use this path.
func Open(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < int64(headerSize+recordSize) {
		return nil, fmt.Errorf("proxyshm: %s too small", path)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	t, err := tableFromMmap(path, data, true, 0)
	if err != nil {
		_ = unix.Munmap(data)
		return nil, err
	}
	return t, nil
}

// Size returns the mmap size for capacity records.
func Size(capacity int) int {
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	return headerSize + capacity*recordSize
}

func tableFromMmap(path string, data []byte, readonly bool, expectedCapacity int) (*Table, error) {
	if len(data) < headerSize {
		return nil, errors.New("proxyshm: mmap too small")
	}
	h := (*mmapHeader)(unsafe.Pointer(&data[0]))
	if expectedCapacity == 0 {
		if h.Magic != magic || h.Schema != schema || h.Capacity == 0 {
			return nil, fmt.Errorf("proxyshm: invalid header in %s", path)
		}
		expectedCapacity = int(h.Capacity)
	}
	if len(data) < Size(expectedCapacity) {
		return nil, fmt.Errorf("proxyshm: mmap size %d smaller than expected %d", len(data), Size(expectedCapacity))
	}
	first := unsafe.Pointer(&data[headerSize])
	records := unsafe.Slice((*mmapRecord)(first), expectedCapacity)
	return &Table{path: path, data: data, header: h, records: records, readonly: readonly}, nil
}

// Close unmaps the table.
func (t *Table) Close() error {
	if t == nil || t.data == nil {
		return nil
	}
	data := t.data
	t.data = nil
	return unix.Munmap(data)
}

func (t *Table) Capacity() int { return int(atomic.LoadUint32(&t.header.Capacity)) }
func (t *Table) Rev() uint64   { return atomic.LoadUint64(&t.header.GlobalRev) }
func (t *Table) Synced() bool  { return atomic.LoadUint32(&t.header.Synced) == 1 }

func (t *Table) BeginSync() {
	if t.readonly {
		return
	}
	atomic.AddUint64(&t.header.SyncGen, 1)
}

func (t *Table) Bookmark(fullSync bool) {
	if t.readonly {
		return
	}
	if fullSync {
		gen := atomic.LoadUint64(&t.header.SyncGen)
		for i := range t.records {
			r := &t.records[i]
			if atomic.LoadUint32(&r.Status) == statusPresent && atomic.LoadUint64(&r.SyncGen) != gen {
				t.deleteRecord(r)
			}
		}
	}
	atomic.StoreUint32(&t.header.Synced, 1)
	atomic.AddUint64(&t.header.GlobalRev, 1)
}

func (t *Table) SetPolicy(p routesync.Policy) error {
	if t.readonly {
		return nil
	}
	startHeaderWrite(&t.header.PolicySeq)
	if err := putFixed(t.header.PolicyAuth[:], p.AuthMode); err != nil {
		finishHeaderWrite(&t.header.PolicySeq)
		return fmt.Errorf("policy.auth_mode: %w", err)
	}
	atomic.StoreInt64(&t.header.PolicyParkMS, int64(p.ParkTimeoutMS))
	finishHeaderWrite(&t.header.PolicySeq)
	atomic.AddUint64(&t.header.GlobalRev, 1)
	return nil
}

func (t *Table) Policy() routesync.Policy {
	for spin := 0; spin < 64; spin++ {
		seq1 := atomic.LoadUint64(&t.header.PolicySeq)
		if seq1&1 == 1 {
			runtime.Gosched()
			continue
		}
		p := routesync.Policy{
			AuthMode:      fixedString(t.header.PolicyAuth[:]),
			ParkTimeoutMS: int(atomic.LoadInt64(&t.header.PolicyParkMS)),
		}
		seq2 := atomic.LoadUint64(&t.header.PolicySeq)
		if seq1 == seq2 && seq2&1 == 0 {
			return p
		}
	}
	return routesync.Policy{}
}

func (t *Table) Upsert(in routesync.RouteEntry) error {
	if t.readonly {
		return errors.New("proxyshm: table is read-only")
	}
	if in.SandboxID == "" {
		return errors.New("proxyshm: empty sandbox id")
	}
	if err := validateRoute(in); err != nil {
		return err
	}
	idx, ok := t.findSlot(in.SandboxID, true)
	if !ok {
		return errors.New("proxyshm: route table full")
	}
	rec := &t.records[idx]
	if current, status, readable := readRecord(rec); readable && status == statusPresent &&
		sameRouteExecution(current, in) && current.EventSeq > 0 &&
		(in.EventSeq == 0 || in.EventSeq <= current.EventSeq) {
		// A replay/full sync may repeat an older event. Keep the newer payload,
		// but mark it seen in this sync generation so Bookmark does not drop it.
		startWrite(rec)
		rec.SyncGen = atomic.LoadUint64(&t.header.SyncGen)
		finishWrite(rec)
		return nil
	}
	startWrite(rec)
	rec.Hash = hashSID(in.SandboxID)
	rec.Status = statusPresent
	rec.SyncGen = atomic.LoadUint64(&t.header.SyncGen)
	rec.Rev = atomic.AddUint64(&t.header.GlobalRev, 1)
	atomic.StoreUint64(&rec.NodeEpoch, in.NodeEpoch)
	atomic.StoreUint64(&rec.EventSeq, in.EventSeq)
	_ = putFixed(rec.SandboxID[:], in.SandboxID)
	_ = putFixed(rec.NodeID[:], in.NodeID)
	_ = putFixed(rec.RegistryGeneration[:], in.RegistryGeneration)
	_ = putFixed(rec.BindingDigest[:], in.BindingDigest)
	_ = putFixed(rec.Profile[:], in.Profile)
	_ = putFixed(rec.TemplateID[:], in.TemplateID)
	_ = putFixed(rec.State[:], in.State)
	_ = putFixed(rec.EnvdUDS[:], in.EnvdUDS)
	_ = putFixed(rec.CiUDS[:], in.CiUDS)
	_ = putFixed(rec.FloatingIP[:], in.FloatingIP)
	_ = putFixed(rec.AccessToken[:], in.AccessToken)
	_ = putFixed(rec.TrafficAccessToken[:], in.TrafficAccessToken)
	_ = putFixed(rec.SnapshotLocation[:], in.SnapshotLocation)
	_ = putFixed(rec.MmdsSecret[:], in.MmdsSecret)
	finishWrite(rec)
	return nil
}

func (t *Table) Delete(sid string) bool {
	if t.readonly || sid == "" {
		return false
	}
	idx, ok := t.findSlot(sid, false)
	if !ok {
		return false
	}
	t.deleteRecord(&t.records[idx])
	return true
}

// DeleteRoute applies a live delete only when it names the currently cached
// execution. Full-sync Bookmark cleanup uses Delete directly because absence
// from that snapshot is already authoritative.
func (t *Table) DeleteRoute(delete routesync.RouteDelete) bool {
	if t.readonly || delete.Validate() != nil {
		return false
	}
	idx, ok := t.findSlot(delete.SandboxID, false)
	if !ok {
		return false
	}
	rec := &t.records[idx]
	current, status, readable := readRecord(rec)
	if !readable || status != statusPresent {
		return false
	}
	managed := routeHasExecutionFence(current)
	if managed != delete.HasExecutionFence() {
		return false
	}
	if managed && (current.NodeID != delete.NodeID || current.NodeEpoch != delete.NodeEpoch ||
		current.RegistryGeneration != delete.RegistryGeneration || current.BindingDigest != delete.BindingDigest ||
		delete.EventSeq <= current.EventSeq) {
		return false
	}
	t.deleteRecord(rec)
	return true
}

func routeHasExecutionFence(route routesync.RouteEntry) bool {
	return route.HasExecutionFence()
}

func sameRouteExecution(left, right routesync.RouteEntry) bool {
	return routeHasExecutionFence(left) && routeHasExecutionFence(right) &&
		left.SandboxID == right.SandboxID && left.NodeID == right.NodeID && left.NodeEpoch == right.NodeEpoch &&
		left.RegistryGeneration == right.RegistryGeneration && left.BindingDigest == right.BindingDigest
}

func (t *Table) deleteRecord(rec *mmapRecord) {
	startWrite(rec)
	rec.Status = statusDeleted
	rec.SyncGen = atomic.LoadUint64(&t.header.SyncGen)
	rec.Rev = atomic.AddUint64(&t.header.GlobalRev, 1)
	atomic.StoreUint64(&rec.NodeEpoch, 0)
	atomic.StoreUint64(&rec.EventSeq, 0)
	clearFixed(rec.SandboxID[:])
	clearFixed(rec.NodeID[:])
	clearFixed(rec.RegistryGeneration[:])
	clearFixed(rec.BindingDigest[:])
	clearFixed(rec.Profile[:])
	clearFixed(rec.TemplateID[:])
	clearFixed(rec.State[:])
	clearFixed(rec.EnvdUDS[:])
	clearFixed(rec.CiUDS[:])
	clearFixed(rec.FloatingIP[:])
	clearFixed(rec.AccessToken[:])
	clearFixed(rec.TrafficAccessToken[:])
	clearFixed(rec.SnapshotLocation[:])
	clearFixed(rec.MmdsSecret[:])
	finishWrite(rec)
}

func (t *Table) Lookup(sid string) (routesync.RouteEntry, bool) {
	if sid == "" || len(t.records) == 0 {
		return routesync.RouteEntry{}, false
	}
	h := hashSID(sid)
	start := int(h % uint64(len(t.records)))
	for i := 0; i < len(t.records); i++ {
		rec := &t.records[(start+i)%len(t.records)]
		status := atomic.LoadUint32(&rec.Status)
		if status == statusEmpty {
			return routesync.RouteEntry{}, false
		}
		if status != statusPresent || atomic.LoadUint64(&rec.Hash) != h {
			continue
		}
		entry, st, ok := readRecord(rec)
		if !ok {
			i--
			runtime.Gosched()
			continue
		}
		if st == statusPresent && entry.SandboxID == sid {
			return entry, true
		}
		if st == statusEmpty {
			return routesync.RouteEntry{}, false
		}
	}
	return routesync.RouteEntry{}, false
}

func (t *Table) ByFloatingIP(ip string) (string, bool) {
	if ip == "" {
		return "", false
	}
	for i := range t.records {
		entry, st, ok := readRecord(&t.records[i])
		if !ok {
			i--
			runtime.Gosched()
			continue
		}
		if st == statusPresent && entry.State == routesync.StateRunning && entry.FloatingIP == ip {
			return entry.SandboxID, true
		}
	}
	return "", false
}

func (t *Table) SandboxInfo(sid string) (templateID, accessToken string, ok bool) {
	r, ok := t.Lookup(sid)
	if !ok || r.State != routesync.StateRunning {
		return "", "", false
	}
	return r.TemplateID, r.AccessToken, true
}

func (t *Table) MmdsSecret(sid string) ([]byte, bool) {
	r, ok := t.Lookup(sid)
	if !ok || r.MmdsSecret == "" {
		return nil, false
	}
	b, err := hex.DecodeString(r.MmdsSecret)
	if err != nil {
		return nil, false
	}
	return b, true
}

func (t *Table) findSlot(sid string, insert bool) (int, bool) {
	h := hashSID(sid)
	start := int(h % uint64(len(t.records)))
	firstDeleted := -1
	for i := 0; i < len(t.records); i++ {
		idx := (start + i) % len(t.records)
		rec := &t.records[idx]
		status := atomic.LoadUint32(&rec.Status)
		switch status {
		case statusEmpty:
			if !insert {
				return 0, false
			}
			if firstDeleted >= 0 {
				return firstDeleted, true
			}
			return idx, true
		case statusDeleted:
			if insert && firstDeleted < 0 {
				firstDeleted = idx
			}
		case statusPresent:
			if rec.Hash != h {
				continue
			}
			entry, st, ok := readRecord(rec)
			if !ok {
				i--
				runtime.Gosched()
				continue
			}
			if st == statusPresent && entry.SandboxID == sid {
				return idx, true
			}
		}
	}
	if insert && firstDeleted >= 0 {
		return firstDeleted, true
	}
	return 0, false
}

func readRecord(rec *mmapRecord) (routesync.RouteEntry, uint32, bool) {
	for spin := 0; spin < 64; spin++ {
		seq1 := atomic.LoadUint64(&rec.Seq)
		if seq1&1 == 1 {
			runtime.Gosched()
			continue
		}
		st := atomic.LoadUint32(&rec.Status)
		entry := routesync.RouteEntry{
			SandboxID:          fixedString(rec.SandboxID[:]),
			NodeID:             fixedString(rec.NodeID[:]),
			NodeEpoch:          atomic.LoadUint64(&rec.NodeEpoch),
			RegistryGeneration: fixedString(rec.RegistryGeneration[:]),
			BindingDigest:      fixedString(rec.BindingDigest[:]),
			EventSeq:           atomic.LoadUint64(&rec.EventSeq),
			Profile:            fixedString(rec.Profile[:]),
			TemplateID:         fixedString(rec.TemplateID[:]),
			State:              fixedString(rec.State[:]),
			EnvdUDS:            fixedString(rec.EnvdUDS[:]),
			CiUDS:              fixedString(rec.CiUDS[:]),
			FloatingIP:         fixedString(rec.FloatingIP[:]),
			AccessToken:        fixedString(rec.AccessToken[:]),
			TrafficAccessToken: fixedString(rec.TrafficAccessToken[:]),
			SnapshotLocation:   fixedString(rec.SnapshotLocation[:]),
			MmdsSecret:         fixedString(rec.MmdsSecret[:]),
		}
		seq2 := atomic.LoadUint64(&rec.Seq)
		if seq1 == seq2 && seq2&1 == 0 {
			return entry, st, true
		}
	}
	return routesync.RouteEntry{}, statusEmpty, false
}

func startWrite(rec *mmapRecord) {
	seq := atomic.LoadUint64(&rec.Seq)
	if seq&1 == 1 {
		seq++
	}
	atomic.StoreUint64(&rec.Seq, seq+1)
}

func finishWrite(rec *mmapRecord) {
	seq := atomic.LoadUint64(&rec.Seq)
	if seq&1 == 0 {
		seq++
	}
	atomic.StoreUint64(&rec.Seq, seq+1)
}

func startHeaderWrite(seqp *uint64) {
	seq := atomic.LoadUint64(seqp)
	if seq&1 == 1 {
		seq++
	}
	atomic.StoreUint64(seqp, seq+1)
}

func finishHeaderWrite(seqp *uint64) {
	seq := atomic.LoadUint64(seqp)
	if seq&1 == 0 {
		seq++
	}
	atomic.StoreUint64(seqp, seq+1)
}

func validateRoute(r routesync.RouteEntry) error {
	if err := r.ValidateExecutionFence(); err != nil {
		return err
	}
	checks := []struct {
		name string
		val  string
		max  int
	}{
		{"sid", r.SandboxID, maxSandboxID},
		{"node_id", r.NodeID, maxNodeID},
		{"registry_generation", r.RegistryGeneration, maxGeneration},
		{"binding_digest", r.BindingDigest, maxDigest},
		{"profile", r.Profile, maxProfile},
		{"template_id", r.TemplateID, maxTemplateID},
		{"state", r.State, maxState},
		{"envd_uds", r.EnvdUDS, maxUDS},
		{"ci_uds", r.CiUDS, maxUDS},
		{"floatingip", r.FloatingIP, maxFloatingIP},
		{"access_token", r.AccessToken, maxAccessToken},
		{"traffic_access_token", r.TrafficAccessToken, maxAccessToken},
		{"snap_loc", r.SnapshotLocation, maxSnapLoc},
		{"mmds_secret", r.MmdsSecret, maxMmdsSecret},
	}
	for _, c := range checks {
		if len(c.val) > c.max {
			return fmt.Errorf("%s length %d exceeds %d", c.name, len(c.val), c.max)
		}
	}
	return nil
}

func putFixed(dst []byte, s string) error {
	if len(s) > len(dst) {
		return fmt.Errorf("length %d exceeds %d", len(s), len(dst))
	}
	clearFixed(dst)
	copy(dst, s)
	return nil
}

func clearFixed(dst []byte) {
	for i := range dst {
		dst[i] = 0
	}
}

func fixedString(src []byte) string {
	n := len(src)
	for i, b := range src {
		if b == 0 {
			n = i
			break
		}
	}
	return string(src[:n])
}

func hashSID(sid string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sid))
	return h.Sum64()
}

func alignSize(n, a int) int {
	return (n + a - 1) &^ (a - 1)
}

func parentDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return "."
}
