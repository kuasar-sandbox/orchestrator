package session

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
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
	NodeID string `json:"node_id"`
	Tuple
	HolderMemberID string `json:"holder_member_id"`
}

type DirectoryDelta struct {
	Entry DirectoryEntry `json:"entry"`
	Up    bool           `json:"up"`
}

type DirectoryRecord struct {
	Entry     DirectoryEntry `json:"entry"`
	Available bool           `json:"available"`
	Conflict  bool           `json:"conflict,omitempty"`
}

// Directory stores only node-to-Holder routing hints and tuple high watermarks.
// It deliberately has no capacity, liveness, or placement-load fields.
type Directory struct {
	mu      sync.RWMutex
	records map[string]DirectoryRecord
}

func NewDirectory() *Directory {
	return &Directory{records: make(map[string]DirectoryRecord)}
}

func (d *Directory) Apply(delta DirectoryDelta) bool {
	entry := delta.Entry
	if entry.NodeID == "" || !entry.Tuple.Valid() || entry.HolderMemberID == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	current, found := d.records[entry.NodeID]
	if !found || entry.Tuple.Compare(current.Entry.Tuple) > 0 {
		d.records[entry.NodeID] = DirectoryRecord{Entry: entry, Available: delta.Up}
		return true
	}
	comparison := entry.Tuple.Compare(current.Entry.Tuple)
	if comparison < 0 {
		return false
	}
	if entry.HolderMemberID != current.Entry.HolderMemberID {
		if !current.Conflict || current.Available {
			current.Conflict = true
			current.Available = false
			d.records[entry.NodeID] = current
			return true
		}
		return false
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
	d.mu.RLock()
	defer d.mu.RUnlock()
	record, found := d.records[nodeID]
	return record.Entry, found && record.Available && !record.Conflict
}

func (d *Directory) Snapshot() []DirectoryRecord {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]DirectoryRecord, 0, len(d.records))
	for _, record := range d.records {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Entry.NodeID < out[j].Entry.NodeID })
	return out
}

func (d *Directory) MergeFull(records []DirectoryRecord) int {
	changed := 0
	for _, record := range records {
		if d.Apply(DirectoryDelta{Entry: record.Entry, Up: record.Available && !record.Conflict}) {
			changed++
		}
		if record.Conflict {
			d.mu.Lock()
			current := d.records[record.Entry.NodeID]
			if current.Entry.Tuple.Compare(record.Entry.Tuple) == 0 && !current.Conflict {
				current.Conflict = true
				current.Available = false
				d.records[record.Entry.NodeID] = current
				changed++
			}
			d.mu.Unlock()
		}
	}
	return changed
}

func DirectoryDigest(records []DirectoryRecord) [sha256.Size]byte {
	records = append([]DirectoryRecord(nil), records...)
	sort.Slice(records, func(i, j int) bool { return records[i].Entry.NodeID < records[j].Entry.NodeID })
	var input bytes.Buffer
	input.WriteString("kuasar-session-directory-v1")
	for _, record := range records {
		writeDirectoryString(&input, record.Entry.NodeID)
		var number [8]byte
		binary.BigEndian.PutUint64(number[:], record.Entry.NodeEpoch)
		input.Write(number[:])
		binary.BigEndian.PutUint64(number[:], record.Entry.SessionSeq)
		input.Write(number[:])
		writeDirectoryString(&input, record.Entry.HolderMemberID)
		if record.Available {
			input.WriteByte(1)
		} else {
			input.WriteByte(0)
		}
		if record.Conflict {
			input.WriteByte(1)
		} else {
			input.WriteByte(0)
		}
	}
	return sha256.Sum256(input.Bytes())
}

func writeDirectoryString(buffer *bytes.Buffer, value string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	buffer.Write(length[:])
	buffer.WriteString(value)
}
