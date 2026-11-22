package record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

type baseRecord struct {
	physical []byte
	raw      storage.Value
	version  Version
	chunks   int
}

func (t *Tx) loadBases(keys []Key) error {
	if t.stageErr != nil {
		return t.stageErr
	}
	if t.base == nil {
		t.base = make(map[string]baseRecord)
	}
	var missing []Key
	seen := make(map[string]bool)
	for _, key := range keys {
		if _, ok := t.base[string(key)]; !ok && !seen[string(key)] {
			missing = append(missing, key)
			seen[string(key)] = true
		}
	}
	for start := 0; start < len(missing); start += 256 {
		part := missing[start:min(start+256, len(missing))]
		physicalKeys := make([][]byte, 0, len(part)*2)
		for _, key := range part {
			phys, err := physical(key)
			if err != nil {
				return err
			}
			physicalKeys = append(physicalKeys, phys, revisionKey(phys))
		}
		rows, err := t.read.GetMany(physicalKeys)
		if err != nil {
			return err
		}
		for i, key := range part {
			version, err := decodeCounter(rows[2*i+1], key)
			if err != nil {
				return err
			}
			if err = t.pin(key, version); err != nil {
				return err
			}
			base := baseRecord{physical: physicalKeys[2*i], raw: rows[2*i], version: version}
			if base.raw.Exists {
				raw := base.raw.Bytes
				size, err := payloadSize(raw)
				if err != nil {
					return err
				}
				keySize := int(binary.BigEndian.Uint32(raw[55:59]))
				if keySize > len(raw)-headerBytes || !bytes.Equal(raw[headerBytes:headerBytes+keySize], key) || !bytes.Equal(raw[:10], version[:]) || !version.IsCommitted() {
					return invalid("record/revision identity")
				}
				base.chunks = int(binary.BigEndian.Uint32(raw[19:23]))
				expected := 0
				if size > ChunkBytes {
					expected = (size + ChunkBytes - 1) / ChunkBytes
				}
				if base.chunks != expected {
					return invalid("chunk count")
				}
			}
			t.base[string(key)] = base
		}
	}
	return nil
}
func (t *Tx) stage(key Key, payload []byte, deleted bool) error {
	if t.stageErr != nil {
		return t.stageErr
	}
	if _, err := physical(key); err != nil {
		t.stageErr = err
		return err
	}
	size := len(payload) + 2*len(key) + 128
	if old, ok := t.pending[string(key)]; ok {
		t.stagedBytes -= len(old.payload) + 2*len(old.key) + 128
	}
	t.stagedBytes += size
	if t.stagedBytes >= storage.MaxTransactionBytes {
		t.stageErr = &storage.CapacityError{Resource: "staged record bytes"}
		return t.stageErr
	}
	t.pending[string(key)] = pending{key: bytes.Clone(key), payload: bytes.Clone(payload), deleted: deleted}
	return nil
}

// Scan merges the bounded persisted stream with this transaction's staged
// writes, so index selection observes its own inserts, replacements and deletes.
func (t *Tx) Scan(scope Key, after Key, limit storage.Limit) (Page, error) {
	if t.stageErr != nil {
		return Page{}, t.stageErr
	}
	prefix, err := physical(scope)
	if err != nil {
		return Page{}, err
	}
	bounds := storage.Range{Begin: prefix, End: storage.PrefixEnd(prefix)}
	if after != nil {
		if !bytes.HasPrefix(after, scope) {
			return Page{}, invalid("cursor scope")
		}
		bounds.After, err = physical(after)
		if err != nil {
			return Page{}, err
		}
	}
	if err = storage.ValidateRange(bounds, limit); err != nil {
		return Page{}, err
	}
	type staged struct {
		physical []byte
		value    pending
	}
	var changes []staged
	for _, p := range t.pending {
		if !bytes.HasPrefix(p.key, scope) {
			continue
		}
		phys, err := physical(p.key)
		if err != nil {
			return Page{}, err
		}
		if bounds.After == nil || bytes.Compare(phys, bounds.After) > 0 {
			changes = append(changes, staged{phys, p})
		}
	}
	if len(changes) == 0 {
		return t.scanCommitted(scope, after, limit)
	}
	sort.Slice(changes, func(i, j int) bool { return bytes.Compare(changes[i].physical, changes[j].physical) < 0 })
	var rows []storage.Row
	position := 0
	more := true
	var lastNative []byte
	peek := func() (*storage.Row, error) {
		for position == len(rows) && more {
			query := bounds
			if lastNative != nil {
				query.After = lastNative
			}
			page, err := t.read.Scan(query, storage.Limit{Rows: min(limit.Rows+1, storage.MaxPageRows), Bytes: storage.MaxPageBytes})
			if err != nil {
				return nil, err
			}
			rows = page.Rows
			position = 0
			more = page.More
			if len(rows) > 0 {
				lastNative = rows[len(rows)-1].Key
			} else if more {
				return nil, invalid("empty range continuation")
			}
		}
		if position == len(rows) {
			return nil, nil
		}
		return &rows[position], nil
	}
	var out Page
	used := 0
	changeIndex := 0
	for {
		native, err := peek()
		if err != nil {
			return Page{}, err
		}
		if native == nil && changeIndex == len(changes) {
			return out, nil
		}
		var candidate Item
		var size int
		useStaged := changeIndex < len(changes) && (native == nil || bytes.Compare(changes[changeIndex].physical, native.Key) <= 0)
		if useStaged {
			change := changes[changeIndex]
			changeIndex++
			if native != nil && bytes.Equal(change.physical, native.Key) {
				position++
			}
			if change.value.deleted {
				continue
			}
			size = len(change.value.payload)
			candidate.Key = change.value.key
			if len(out.Items) == limit.Rows || (len(out.Items) > 0 && used+size > limit.Bytes) {
				out.More = true
				return out, nil
			}
			candidate.Value, err = t.Get(candidate.Key)
			if err != nil {
				return Page{}, err
			}
		} else {
			position++
			size, err = payloadSize(native.Value)
			if err != nil {
				return Page{}, err
			}
			if len(out.Items) == limit.Rows || (len(out.Items) > 0 && used+size > limit.Bytes) {
				out.More = true
				return out, nil
			}
			candidate.Value, candidate.Key, err = t.decode(native.Key, native.Value)
			if err != nil {
				return Page{}, err
			}
			if !bytes.HasPrefix(candidate.Key, scope) {
				return Page{}, fmt.Errorf("%w: scope collision", storage.ErrInvalid)
			}
			if err = t.pin(candidate.Key, candidate.Value.Version); err != nil {
				return Page{}, err
			}
		}
		out.Items = append(out.Items, candidate)
		used += size
	}
}
