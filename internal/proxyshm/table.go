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
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	magic  uint64 = 0x6b75736172505831 // "kusarPX1"
	schema uint32 = 6

	statusEmpty   uint32 = 0
	statusPresent uint32 = 1

	defaultCapacity      = 65536
	maxTerminalRevisions = 4096

	maxSandboxID   = 128
	maxProfile     = 16
	maxTemplateID  = types.MaxTemplateIDBytes
	maxState       = 16
	maxUDS         = 256
	maxFloatingIP  = 64
	maxSecret      = 64
	maxFingerprint = 64
	maxAccessToken = 256
	maxSnapLoc     = 32
	maxMmdsSecret  = 128
	maxRunID       = 128
)

var (
	headerSize         = alignSize(int(unsafe.Sizeof(mmapHeader{})), 8)
	recordSize         = int(unsafe.Sizeof(mmapRecord{}))
	terminalRecordSize = int(unsafe.Sizeof(mmapTerminalRecord{}))
	mmdsSourceSlotSize = int(unsafe.Sizeof(mmapMMDSSourceSlot{}))
)

type mmapHeader struct {
	Magic         uint64
	Schema        uint32
	Capacity      uint32
	Synced        uint32
	TerminalCap   uint32
	GlobalRev     uint64
	SyncGen       uint64
	TableSeq      uint64
	PolicySeq     uint64
	PolicyParkMS  int64
	PolicyAuth    [maxProfile]byte
	MMDSSourceSeq uint64
	MMDSSynced    uint32
	MMDSSourceCap uint32
	_             [8]byte
}

type mmapRecord struct {
	Seq     uint64
	Hash    uint64
	Status  uint32
	_       uint32
	SyncGen uint64
	Rev     uint64

	SandboxID              [maxSandboxID]byte
	Profile                [maxProfile]byte
	TemplateID             [maxTemplateID]byte
	State                  [maxState]byte
	EnvdUDS                [maxUDS]byte
	CiUDS                  [maxUDS]byte
	FloatingIP             [maxFloatingIP]byte
	StableID               [maxSandboxID]byte
	APISecret              [maxSecret]byte
	APISecretFingerprint   [maxFingerprint]byte
	ManifestKeyFingerprint [maxFingerprint]byte
	ServiceSecret          [maxSecret]byte
	EnvdAccessToken        [maxAccessToken]byte
	TrafficAccessToken     [maxAccessToken]byte
	ForwardAccessToken     [maxAccessToken]byte
	SnapshotLocation       [maxSnapLoc]byte
	MmdsSecret             [maxMmdsSecret]byte
	RunID                  [maxRunID]byte
}

// mmapTerminalRecord is a bounded, credential-free correlation cache for
// Delete events. It is deliberately separate from the live open-addressed
// table so normal route deletion can reclaim its probe slot without losing the
// short-lived per-SID revision that unparks a worker.
type mmapTerminalRecord struct {
	Seq       uint64
	Hash      uint64
	Rev       uint64
	SandboxID [maxSandboxID]byte
}

// Table is a memory-mapped fixed-capacity route table.
type Table struct {
	path        string
	data        []byte
	header      *mmapHeader
	records     []mmapRecord
	terminals   []mmapTerminalRecord
	mmdsSources []mmapMMDSSourceSlot
	readonly    bool
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
	t.header.TerminalCap = uint32(terminalCapacity(capacity))
	t.header.MMDSSourceCap = uint32(mmdsSourceSlotCount)
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
	if st.Size() < int64(headerSize) {
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

// Size returns the mmap size for capacity live records, the bounded
// credential-free terminal revision cache, and the fixed MMDS source region.
func Size(capacity int) int {
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	return mappedSize(capacity, terminalCapacity(capacity))
}

func terminalCapacity(capacity int) int {
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	if capacity > maxTerminalRevisions {
		return maxTerminalRevisions
	}
	return capacity
}

func mappedSize(capacity, terminalCap int) int {
	return headerSize + capacity*recordSize + terminalCap*terminalRecordSize + mmdsSourceSlotCount*mmdsSourceSlotSize
}

func tableFromMmap(path string, data []byte, readonly bool, expectedCapacity int) (*Table, error) {
	if len(data) < headerSize {
		return nil, errors.New("proxyshm: mmap too small")
	}
	h := (*mmapHeader)(unsafe.Pointer(&data[0]))
	terminalCap := terminalCapacity(expectedCapacity)
	if expectedCapacity == 0 {
		if h.Magic != magic {
			return nil, fmt.Errorf("proxyshm: invalid magic in %s", path)
		}
		if h.Schema != schema {
			return nil, fmt.Errorf("proxyshm: unsupported schema %d in %s", h.Schema, path)
		}
		if h.Capacity == 0 {
			return nil, fmt.Errorf("proxyshm: invalid primary capacity 0 in %s", path)
		}
		if h.TerminalCap == 0 {
			return nil, fmt.Errorf("proxyshm: invalid terminal capacity 0 in %s", path)
		}
		if h.MMDSSourceCap != uint32(mmdsSourceSlotCount) {
			return nil, fmt.Errorf("proxyshm: invalid MMDS source capacity %d in %s", h.MMDSSourceCap, path)
		}
		expectedCapacity = int(h.Capacity)
		terminalCap = int(h.TerminalCap)
		if terminalCap != terminalCapacity(expectedCapacity) {
			return nil, fmt.Errorf("proxyshm: invalid terminal capacity %d in %s", terminalCap, path)
		}
	}
	expectedSize := mappedSize(expectedCapacity, terminalCap)
	if len(data) < expectedSize {
		return nil, fmt.Errorf("proxyshm: mmap size %d smaller than expected %d", len(data), expectedSize)
	}
	first := unsafe.Pointer(&data[headerSize])
	records := unsafe.Slice((*mmapRecord)(first), expectedCapacity)
	terminalOffset := headerSize + expectedCapacity*recordSize
	terminalFirst := unsafe.Pointer(&data[terminalOffset])
	terminals := unsafe.Slice((*mmapTerminalRecord)(terminalFirst), terminalCap)
	mmdsSourceOffset := terminalOffset + terminalCap*terminalRecordSize
	mmdsSourceFirst := unsafe.Pointer(&data[mmdsSourceOffset])
	mmdsSources := unsafe.Slice((*mmapMMDSSourceSlot)(mmdsSourceFirst), mmdsSourceSlotCount)
	return &Table{
		path: path, data: data, header: h, records: records, terminals: terminals,
		mmdsSources: mmdsSources, readonly: readonly,
	}, nil
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

// MMDSSynced is independent of the generic data-plane sync bit. The generic
// table deliberately remains usable while route-sync reconnects, but MMDS must
// fail closed because its confidential heap is discarded on every disconnect.
func (t *Table) MMDSSynced() bool { return atomic.LoadUint32(&t.header.MMDSSynced) == 1 }

func (t *Table) SetMMDSSynced(synced bool) {
	if t.readonly {
		return
	}
	var value uint32
	if synced {
		value = 1
	}
	atomic.StoreUint32(&t.header.MMDSSynced, value)
}

func (t *Table) BeginSync() {
	if t.readonly {
		return
	}
	atomic.AddUint64(&t.header.SyncGen, 1)
}

func (t *Table) Bookmark() {
	if t.readonly {
		return
	}
	startHeaderWrite(&t.header.TableSeq)
	defer finishHeaderWrite(&t.header.TableSeq)
	gen := atomic.LoadUint64(&t.header.SyncGen)
	stale := make([]string, 0)
	for i := range t.records {
		snapshot, ok := readRecordSnapshot(&t.records[i])
		if ok && snapshot.status == statusPresent && snapshot.syncGen != gen {
			stale = append(stale, snapshot.entry.SandboxID)
		}
	}
	for _, sid := range stale {
		idx, found := t.findSlot(sid, false)
		if !found {
			continue
		}
		rev := atomic.AddUint64(&t.header.GlobalRev, 1)
		t.writeTerminal(sid, rev)
		t.deleteLiveRecord(idx)
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
	// Variable MMDS declarations and plaintext values are master-heap only.
	// Clear them before any fixed-layout comparison or write.
	in.MMDSRoutes = ""
	in.MMDSRouteSecretValues = nil
	startHeaderWrite(&t.header.TableSeq)
	defer finishHeaderWrite(&t.header.TableSeq)
	idx, ok := t.findSlot(in.SandboxID, true)
	if !ok {
		return errors.New("proxyshm: route table full")
	}
	rec := &t.records[idx]
	gen := atomic.LoadUint64(&t.header.SyncGen)
	if current, stable := readRecordSnapshot(rec); stable && current.status == statusPresent &&
		current.hash == hashSID(in.SandboxID) && current.entry == in {
		// Replay during Subscribe-before-Range synchronization still adopts the
		// route into the current generation, but an identical business record is
		// not a lifecycle response and must not advance its per-SID revision.
		if current.syncGen != gen {
			current.syncGen = gen
			writeRecordSnapshot(rec, current)
		}
		return nil
	}
	t.writeRoute(rec, in, gen, atomic.AddUint64(&t.header.GlobalRev, 1))
	return nil
}

func (t *Table) Delete(sid string) bool {
	if t.readonly || sid == "" {
		return false
	}
	startHeaderWrite(&t.header.TableSeq)
	defer finishHeaderWrite(&t.header.TableSeq)
	rev := atomic.AddUint64(&t.header.GlobalRev, 1)
	t.writeTerminal(sid, rev)
	idx, ok := t.findSlot(sid, false)
	if ok {
		t.deleteLiveRecord(idx)
	}
	return true
}

// LookupRevision returns a route and its per-SID revision from one atomic table
// snapshot. When the live route is absent it also consults the bounded terminal
// revision cache. This prevents waiters from combining an old route with a
// newer revision while the writer publishes a starting or terminal transition.
func (t *Table) LookupRevision(sid string) (routesync.RouteEntry, bool, uint64) {
	if sid == "" || len(t.records) == 0 {
		return routesync.RouteEntry{}, false, 0
	}
	for {
		seq1 := atomic.LoadUint64(&t.header.TableSeq)
		if seq1&1 == 1 {
			runtime.Gosched()
			continue
		}
		entry, found, rev := t.lookupLiveRevision(sid)
		if !found {
			rev = t.terminalRevision(sid)
		}
		seq2 := atomic.LoadUint64(&t.header.TableSeq)
		if seq1 == seq2 && seq2&1 == 0 {
			return entry, found, rev
		}
	}
}

func (t *Table) lookupLiveRevision(sid string) (routesync.RouteEntry, bool, uint64) {
	h := hashSID(sid)
	start := int(h % uint64(len(t.records)))
	for i := 0; i < len(t.records); i++ {
		rec := &t.records[(start+i)%len(t.records)]
		// Preserve the existing hot-path probe: unrelated and empty slots do not
		// require copying the full credential-bearing record. A matching slot is
		// then re-read wholly under its seqlock below.
		status := atomic.LoadUint32(&rec.Status)
		if status == statusEmpty {
			break
		}
		if atomic.LoadUint64(&rec.Hash) != h {
			continue
		}
		snapshot, ok := readRecordSnapshot(rec)
		if !ok {
			i--
			runtime.Gosched()
			continue
		}
		if snapshot.status == statusEmpty {
			break
		}
		if snapshot.status == statusPresent && snapshot.hash == h && snapshot.entry.SandboxID == sid {
			return snapshot.entry, true, snapshot.rev
		}
	}
	return routesync.RouteEntry{}, false, 0
}

// RouteRev returns the latest live or cached terminal revision associated with
// sid. It lets a worker distinguish a terminal response to its Wake from
// unrelated global route-table traffic even when starting and rollback updates
// are coalesced before the worker samples shared memory.
func (t *Table) RouteRev(sid string) uint64 {
	_, _, rev := t.LookupRevision(sid)
	return rev
}

func (t *Table) Lookup(sid string) (routesync.RouteEntry, bool) {
	entry, found, _ := t.LookupRevision(sid)
	return entry, found
}

func (t *Table) SandboxInfo(sid string) (templateID, accessToken string, ok bool) {
	r, ok := t.Lookup(sid)
	if !ok || (r.State != routesync.StateStarting && r.State != routesync.StateRunning) {
		return "", "", false
	}
	return r.TemplateID, r.EnvdAccessToken, true
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

func (t *Table) Incarnation(sid string) (string, bool) {
	route, ok := t.Lookup(sid)
	if !ok || route.RunID == "" || (route.State != routesync.StateStarting && route.State != routesync.StateRunning) {
		return "", false
	}
	return route.RunID, true
}

func (t *Table) findSlot(sid string, insert bool) (int, bool) {
	h := hashSID(sid)
	start := int(h % uint64(len(t.records)))
	for i := 0; i < len(t.records); i++ {
		idx := (start + i) % len(t.records)
		rec := &t.records[idx]
		status := atomic.LoadUint32(&rec.Status)
		switch status {
		case statusEmpty:
			if !insert {
				return 0, false
			}
			return idx, true
		case statusPresent:
			if atomic.LoadUint64(&rec.Hash) != h {
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
	return 0, false
}

type recordSnapshot struct {
	entry   routesync.RouteEntry
	status  uint32
	hash    uint64
	syncGen uint64
	rev     uint64
}

func readRecordSnapshot(rec *mmapRecord) (recordSnapshot, bool) {
	for spin := 0; spin < 64; spin++ {
		seq1 := atomic.LoadUint64(&rec.Seq)
		if seq1&1 == 1 {
			runtime.Gosched()
			continue
		}
		snapshot := recordSnapshot{
			status:  atomic.LoadUint32(&rec.Status),
			hash:    atomic.LoadUint64(&rec.Hash),
			syncGen: atomic.LoadUint64(&rec.SyncGen),
			rev:     atomic.LoadUint64(&rec.Rev),
			entry: routesync.RouteEntry{
				SandboxID:              fixedString(rec.SandboxID[:]),
				Profile:                fixedString(rec.Profile[:]),
				TemplateID:             fixedString(rec.TemplateID[:]),
				State:                  fixedString(rec.State[:]),
				EnvdUDS:                fixedString(rec.EnvdUDS[:]),
				CiUDS:                  fixedString(rec.CiUDS[:]),
				FloatingIP:             fixedString(rec.FloatingIP[:]),
				StableID:               fixedString(rec.StableID[:]),
				APISecret:              fixedString(rec.APISecret[:]),
				APISecretFingerprint:   fixedString(rec.APISecretFingerprint[:]),
				ManifestKeyFingerprint: fixedString(rec.ManifestKeyFingerprint[:]),
				ServiceSecret:          fixedString(rec.ServiceSecret[:]),
				EnvdAccessToken:        fixedString(rec.EnvdAccessToken[:]),
				TrafficAccessToken:     fixedString(rec.TrafficAccessToken[:]),
				ForwardAccessToken:     fixedString(rec.ForwardAccessToken[:]),
				SnapshotLocation:       fixedString(rec.SnapshotLocation[:]),
				MmdsSecret:             fixedString(rec.MmdsSecret[:]),
				RunID:                  fixedString(rec.RunID[:]),
			},
		}
		seq2 := atomic.LoadUint64(&rec.Seq)
		if seq1 == seq2 && seq2&1 == 0 {
			return snapshot, true
		}
	}
	return recordSnapshot{}, false
}

func readRecord(rec *mmapRecord) (routesync.RouteEntry, uint32, bool) {
	snapshot, ok := readRecordSnapshot(rec)
	return snapshot.entry, snapshot.status, ok
}

func (t *Table) writeRoute(rec *mmapRecord, entry routesync.RouteEntry, syncGen, rev uint64) {
	writeRecordSnapshot(rec, recordSnapshot{
		entry: entry, status: statusPresent, hash: hashSID(entry.SandboxID), syncGen: syncGen, rev: rev,
	})
}

func writeRecordSnapshot(rec *mmapRecord, snapshot recordSnapshot) {
	startWrite(rec)
	rec.Hash = snapshot.hash
	rec.Status = snapshot.status
	rec.SyncGen = snapshot.syncGen
	rec.Rev = snapshot.rev
	_ = putFixed(rec.SandboxID[:], snapshot.entry.SandboxID)
	_ = putFixed(rec.Profile[:], snapshot.entry.Profile)
	_ = putFixed(rec.TemplateID[:], snapshot.entry.TemplateID)
	_ = putFixed(rec.State[:], snapshot.entry.State)
	_ = putFixed(rec.EnvdUDS[:], snapshot.entry.EnvdUDS)
	_ = putFixed(rec.CiUDS[:], snapshot.entry.CiUDS)
	_ = putFixed(rec.FloatingIP[:], snapshot.entry.FloatingIP)
	_ = putFixed(rec.StableID[:], snapshot.entry.StableID)
	_ = putFixed(rec.APISecret[:], snapshot.entry.APISecret)
	_ = putFixed(rec.APISecretFingerprint[:], snapshot.entry.APISecretFingerprint)
	_ = putFixed(rec.ManifestKeyFingerprint[:], snapshot.entry.ManifestKeyFingerprint)
	_ = putFixed(rec.ServiceSecret[:], snapshot.entry.ServiceSecret)
	_ = putFixed(rec.EnvdAccessToken[:], snapshot.entry.EnvdAccessToken)
	_ = putFixed(rec.TrafficAccessToken[:], snapshot.entry.TrafficAccessToken)
	_ = putFixed(rec.ForwardAccessToken[:], snapshot.entry.ForwardAccessToken)
	_ = putFixed(rec.SnapshotLocation[:], snapshot.entry.SnapshotLocation)
	_ = putFixed(rec.MmdsSecret[:], snapshot.entry.MmdsSecret)
	_ = putFixed(rec.RunID[:], snapshot.entry.RunID)
	finishWrite(rec)
}

func (t *Table) deleteLiveRecord(index int) {
	if index < 0 || index >= len(t.records) {
		return
	}
	hole := index
	n := len(t.records)
	for step := 1; step < n; step++ {
		scan := (index + step) % n
		snapshot, ok := readRecordSnapshot(&t.records[scan])
		if !ok {
			step--
			runtime.Gosched()
			continue
		}
		if snapshot.status == statusEmpty {
			writeRecordSnapshot(&t.records[hole], recordSnapshot{})
			return
		}
		if snapshot.status != statusPresent {
			continue
		}
		home := int(snapshot.hash % uint64(n))
		if probeDistance(home, hole, n) < probeDistance(home, scan, n) {
			writeRecordSnapshot(&t.records[hole], snapshot)
			hole = scan
		}
	}
	// A completely full table has no terminating empty slot. The backshift above
	// has still preserved every live record, leaving exactly this final hole.
	writeRecordSnapshot(&t.records[hole], recordSnapshot{})
}

func probeDistance(home, index, capacity int) int {
	return (index - home + capacity) % capacity
}

type terminalSnapshot struct {
	hash uint64
	rev  uint64
	sid  string
}

func (t *Table) writeTerminal(sid string, rev uint64) {
	if len(t.terminals) == 0 {
		return
	}
	h := hashSID(sid)
	rec := &t.terminals[int(h%uint64(len(t.terminals)))]
	startHeaderWrite(&rec.Seq)
	rec.Hash = h
	rec.Rev = rev
	_ = putFixed(rec.SandboxID[:], sid)
	finishHeaderWrite(&rec.Seq)
}

func (t *Table) terminalRevision(sid string) uint64 {
	if sid == "" || len(t.terminals) == 0 {
		return 0
	}
	h := hashSID(sid)
	rec := &t.terminals[int(h%uint64(len(t.terminals)))]
	for {
		seq1 := atomic.LoadUint64(&rec.Seq)
		if seq1&1 == 1 {
			runtime.Gosched()
			continue
		}
		snapshot := terminalSnapshot{
			hash: atomic.LoadUint64(&rec.Hash),
			rev:  atomic.LoadUint64(&rec.Rev),
			sid:  fixedString(rec.SandboxID[:]),
		}
		seq2 := atomic.LoadUint64(&rec.Seq)
		if seq1 != seq2 || seq2&1 == 1 {
			continue
		}
		if snapshot.hash == h && snapshot.sid == sid {
			return snapshot.rev
		}
		return 0
	}
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
	checks := []struct {
		name string
		val  string
		max  int
	}{
		{"sid", r.SandboxID, maxSandboxID},
		{"profile", r.Profile, maxProfile},
		{"template_id", r.TemplateID, maxTemplateID},
		{"state", r.State, maxState},
		{"envd_uds", r.EnvdUDS, maxUDS},
		{"ci_uds", r.CiUDS, maxUDS},
		{"floatingip", r.FloatingIP, maxFloatingIP},
		{"stable_id", r.StableID, maxSandboxID},
		{"api_secret", r.APISecret, maxSecret},
		{"api_secret_fingerprint", r.APISecretFingerprint, maxFingerprint},
		{"manifest_key_fingerprint", r.ManifestKeyFingerprint, maxFingerprint},
		{"service_secret", r.ServiceSecret, maxSecret},
		{"envd_access_token", r.EnvdAccessToken, maxAccessToken},
		{"traffic_access_token", r.TrafficAccessToken, maxAccessToken},
		{"forward_access_token", r.ForwardAccessToken, maxAccessToken},
		{"snap_loc", r.SnapshotLocation, maxSnapLoc},
		{"mmds_secret", r.MmdsSecret, maxMmdsSecret},
		{"run_id", r.RunID, maxRunID},
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
