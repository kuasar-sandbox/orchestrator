package session

import (
	"sort"
	"sync"
)

type Tuple struct {
	NodeEpoch  uint64 `json:"node_epoch"`
	SessionSeq uint64 `json:"session_seq"`
}

func (t Tuple) Valid() bool { return t.NodeEpoch != 0 && t.SessionSeq != 0 }

func (t Tuple) Compare(other Tuple) int {
	if t.NodeEpoch < other.NodeEpoch {
		return -1
	}
	if t.NodeEpoch > other.NodeEpoch {
		return 1
	}
	if t.SessionSeq < other.SessionSeq {
		return -1
	}
	if t.SessionSeq > other.SessionSeq {
		return 1
	}
	return 0
}

type DirectoryEntry struct {
	NodeID       string `json:"node_id"`
	EnrollmentID string `json:"enrollment_id"`
	Tuple
	HolderMemberID string `json:"holder_member_id"`
}

type DirectoryDelta struct {
	Entry   DirectoryEntry `json:"entry"`
	Up      bool           `json:"up"`
	Retired bool           `json:"retired,omitempty"`
}

type DirectoryRecord struct {
	Entry     DirectoryEntry `json:"entry"`
	Available bool           `json:"available"`
	Conflict  bool           `json:"conflict,omitempty"`
}

// Directory stores only node-to-Holder routing hints and tuple high watermarks.
// It deliberately has no capacity, liveness, or placement-load fields.
type Directory struct {
	mu        sync.RWMutex
	records   map[string]DirectoryRecord
	authority DirectoryIdentityAuthority
}

// DirectoryIdentityAuthority is the permanent System Group enrollment fence.
// Directory hints can be collected after retirement because every later delta
// is checked against this authority before it can be installed again.
type DirectoryIdentityAuthority interface {
	AllowDirectoryEntry(DirectoryEntry) bool
}

func NewDirectory(authority DirectoryIdentityAuthority) *Directory {
	return &Directory{records: make(map[string]DirectoryRecord), authority: authority}
}

func validDirectoryEntry(entry DirectoryEntry) bool {
	return entry.NodeID != "" && entry.EnrollmentID != "" && entry.Tuple.Valid() && entry.HolderMemberID != ""
}

func canonicalHolder(first, second string) string {
	if second < first {
		return second
	}
	return first
}

func (d *Directory) Apply(delta DirectoryDelta) bool {
	entry := delta.Entry
	if !validDirectoryEntry(entry) {
		return false
	}
	if delta.Up && delta.Retired {
		return false
	}
	allowed := !delta.Retired && d.authority != nil && d.authority.AllowDirectoryEntry(entry)
	d.mu.Lock()
	defer d.mu.Unlock()
	current, found := d.records[entry.NodeID]
	if delta.Retired {
		if !found || current.Entry.EnrollmentID != entry.EnrollmentID || current.Entry.NodeEpoch > entry.NodeEpoch {
			return false
		}
		delete(d.records, entry.NodeID)
		return true
	}
	if !allowed {
		if found && current.Entry.EnrollmentID == entry.EnrollmentID && current.Entry.NodeEpoch <= entry.NodeEpoch {
			delete(d.records, entry.NodeID)
		}
		return false
	}
	if !found || entry.Tuple.Compare(current.Entry.Tuple) > 0 {
		d.records[entry.NodeID] = DirectoryRecord{Entry: entry, Available: delta.Up}
		return true
	}
	comparison := entry.Tuple.Compare(current.Entry.Tuple)
	if comparison < 0 {
		return false
	}
	if entry.EnrollmentID != current.Entry.EnrollmentID || entry.HolderMemberID != current.Entry.HolderMemberID {
		canonical := canonicalHolder(current.Entry.HolderMemberID, entry.HolderMemberID)
		changed := !current.Conflict || current.Available || current.Entry.HolderMemberID != canonical
		current.Entry.HolderMemberID = canonical
		current.Entry.EnrollmentID = canonicalHolder(current.Entry.EnrollmentID, entry.EnrollmentID)
		current.Conflict = true
		current.Available = false
		d.records[entry.NodeID] = current
		return changed
	}
	if !delta.Up && current.Available {
		current.Available = false
		d.records[entry.NodeID] = current
		return true
	}
	// An equal tuple cannot be made authoritative again after a down/conflict.
	return false
}

func (d *Directory) MarkMemberUnavailable(memberID string) int {
	if memberID == "" {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	changed := 0
	for nodeID, record := range d.records {
		if record.Entry.HolderMemberID == memberID && record.Available {
			record.Available = false
			d.records[nodeID] = record
			changed++
		}
	}
	return changed
}

func (d *Directory) Lookup(nodeID string) (DirectoryEntry, bool) {
	record, found := d.lookupRecord(nodeID)
	return record.Entry, found && record.Available && !record.Conflict
}

// LookupRecord returns the retained tuple high-watermark even when its Holder
// is unavailable. Dispatch uses it to distinguish movement from a permanently
// fenced older NodeEpoch.
func (d *Directory) LookupRecord(nodeID string) (DirectoryRecord, bool) {
	return d.lookupRecord(nodeID)
}

func (d *Directory) Snapshot() []DirectoryRecord {
	d.mu.RLock()
	nodeIDs := make([]string, 0, len(d.records))
	for nodeID := range d.records {
		nodeIDs = append(nodeIDs, nodeID)
	}
	d.mu.RUnlock()
	out := make([]DirectoryRecord, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if record, found := d.lookupRecord(nodeID); found {
			out = append(out, record)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Entry.NodeID < out[j].Entry.NodeID })
	return out
}

func (d *Directory) MergeFull(records []DirectoryRecord) int {
	changed := 0
	for _, record := range records {
		if !validDirectoryEntry(record.Entry) {
			continue
		}
		if d.Apply(DirectoryDelta{Entry: record.Entry, Up: record.Available && !record.Conflict}) {
			changed++
		}
		if record.Conflict && d.authority != nil && d.authority.AllowDirectoryEntry(record.Entry) {
			d.mu.Lock()
			current, found := d.records[record.Entry.NodeID]
			if found && current.Entry.Tuple.Compare(record.Entry.Tuple) == 0 {
				canonical := canonicalHolder(current.Entry.HolderMemberID, record.Entry.HolderMemberID)
				canonicalEnrollment := canonicalHolder(current.Entry.EnrollmentID, record.Entry.EnrollmentID)
				wasChanged := !current.Conflict || current.Available ||
					current.Entry.HolderMemberID != canonical || current.Entry.EnrollmentID != canonicalEnrollment
				current.Entry.HolderMemberID = canonical
				current.Entry.EnrollmentID = canonicalEnrollment
				current.Conflict = true
				current.Available = false
				d.records[record.Entry.NodeID] = current
				if wasChanged {
					changed++
				}
			}
			d.mu.Unlock()
		}
	}
	return changed
}

func (d *Directory) lookupRecord(nodeID string) (DirectoryRecord, bool) {
	d.mu.RLock()
	record, found := d.records[nodeID]
	d.mu.RUnlock()
	if !found {
		return DirectoryRecord{}, false
	}
	if d.authority != nil && d.authority.AllowDirectoryEntry(record.Entry) {
		return record, true
	}
	d.mu.Lock()
	if current, ok := d.records[nodeID]; ok && current.Entry == record.Entry {
		delete(d.records, nodeID)
	}
	d.mu.Unlock()
	return DirectoryRecord{}, false
}
