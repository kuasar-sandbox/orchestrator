//go:build fdb && cgo

package fdbdriver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

const chunkBytes = 64 << 10
const headerBytes = 10 + 1 + 8 + 4 + 32
const maxPageRows = 512
const maxPageBytes = 2 << 20

type record struct {
	exists  bool
	version metadata.Version
	payload []byte
}

func invalidRecord(s string) error { return fmt.Errorf("%w: %s", metadata.ErrInvalidRecord, s) }

// Length-delimited components preserve scope without allowing a caller supplied
// separator to escape its namespace. Main records and chunks have disjoint keys.
func (s *Store) key(parts ...string) fdb.Key {
	out := append([]byte(nil), s.prefix...)
	for _, part := range parts {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		out = append(out, size[:]...)
		out = append(out, part...)
	}
	return fdb.Key(out)
}
func validateParts(parts ...string) error {
	total := 0
	for _, p := range parts {
		if p == "" || len(p) > 4096 || strings.IndexByte(p, 0) >= 0 {
			return invalidRecord("invalid key component")
		}
		total += len(p) + 4
	}
	if total > 8500 {
		return invalidRecord("key too long")
	}
	return nil
}
func mainKey(scope fdb.Key, id string) fdb.Key {
	out := append([]byte(nil), scope...)
	out = append(out, []byte(id)...)
	return out
}
func rangeFor(prefix fdb.Key) fdb.KeyRange {
	end := append(append([]byte(nil), prefix...), 0xff)
	return fdb.KeyRange{Begin: prefix, End: fdb.Key(end)}
}
func chunkRange(key fdb.Key) fdb.KeyRange {
	hash := sha256.Sum256(key)
	prefix := append([]byte(nil), key[:bytes.IndexByte(key, 0)+1]...)
	prefix = append(prefix, 0, 0, 0, 6)
	prefix = append(prefix, []byte("chunks")...)
	prefix = append(prefix, hash[:]...)
	return rangeFor(prefix)
}
func chunkKey(prefix fdb.Key, index uint32) fdb.Key {
	key := append(append([]byte(nil), prefix...), 0, 0, 0, 0)
	binary.BigEndian.PutUint32(key[len(key)-4:], index)
	return key
}
func stampPayload(payload []byte) []byte {
	out := make([]byte, 10+len(payload)+4)
	for i := 0; i < 10; i++ {
		out[i] = 0xff
	}
	copy(out[10:], payload)
	return out
}
func envelope(payload []byte, count uint32) []byte {
	out := make([]byte, headerBytes)
	for i := 0; i < 10; i++ {
		out[i] = 0xff
	}
	out[10] = 1
	binary.BigEndian.PutUint64(out[11:19], uint64(len(payload)))
	binary.BigEndian.PutUint32(out[19:23], count)
	hash := sha256.Sum256(payload)
	copy(out[23:55], hash[:])
	if count == 0 {
		out = append(out, payload...)
	}
	return append(out, 0, 0, 0, 0)
}
func recordStamp(raw []byte) (metadata.Version, error) {
	var v metadata.Version
	if raw == nil {
		return v, nil
	}
	if len(raw) < headerBytes || raw[10] != 1 {
		return v, invalidRecord("envelope header")
	}
	copy(v[:], raw[:10])
	if !v.IsCommitted() {
		return metadata.Version{}, invalidRecord("uncommitted record version")
	}
	return v, nil
}
func decodeRecord(tr fdb.Transaction, key fdb.Key, raw []byte) (record, error) {
	if raw == nil {
		return record{}, nil
	}
	v, err := recordStamp(raw)
	if err != nil {
		return record{}, err
	}
	size := binary.BigEndian.Uint64(raw[11:19])
	count := binary.BigEndian.Uint32(raw[19:23])
	if size >= transactionBytes {
		return record{}, invalidRecord("payload length")
	}
	expectedCount := uint32(0)
	if size > chunkBytes {
		expectedCount = uint32((size + chunkBytes - 1) / chunkBytes)
	}
	if count != expectedCount {
		return record{}, invalidRecord("chunk count")
	}
	payload := append([]byte(nil), raw[headerBytes:]...)
	if count > 0 {
		if len(raw) != headerBytes {
			return record{}, invalidRecord("chunked envelope suffix")
		}
		chunks := chunkRange(key)
		rows, err := tr.GetRange(chunks, fdb.RangeOptions{Limit: int(count) + 1}).GetSliceWithError()
		if err != nil {
			return record{}, err
		}
		if len(rows) != int(count) {
			return record{}, invalidRecord("missing or excess chunks")
		}
		payload = make([]byte, 0, int(size))
		for i, row := range rows {
			if !bytes.Equal(row.Key, chunkKey(chunks.Begin.FDBKey(), uint32(i))) || len(row.Value) > chunkBytes {
				return record{}, invalidRecord("chunk identity or size")
			}
			payload = append(payload, row.Value...)
		}
	}
	hash := sha256.Sum256(payload)
	if uint64(len(payload)) != size || !bytes.Equal(hash[:], raw[23:55]) {
		return record{}, invalidRecord("payload integrity")
	}
	return record{exists: true, version: v, payload: payload}, nil
}
func readRecord(tr fdb.Transaction, key fdb.Key) (record, error) {
	raw, err := tr.Get(key).Get()
	if err != nil {
		return record{}, err
	}
	return decodeRecord(tr, key, raw)
}
func putRecord(tr fdb.Transaction, key fdb.Key, payload []byte) error {
	if len(key) > 10_000 || len(payload) >= transactionBytes {
		return &metadata.CapacityError{Resource: "record bytes"}
	}
	chunks := chunkRange(key)
	tr.ClearRange(chunks)
	var count uint32
	if len(payload) > chunkBytes {
		for offset := 0; offset < len(payload); offset += chunkBytes {
			end := offset + chunkBytes
			if end > len(payload) {
				end = len(payload)
			}
			tr.Set(chunkKey(chunks.Begin.FDBKey(), count), payload[offset:end])
			count++
		}
	}
	tr.SetVersionstampedValue(key, envelope(payload, count))
	return nil
}
func clearRecord(tr fdb.Transaction, key fdb.Key) { tr.Clear(key); tr.ClearRange(chunkRange(key)) }
func putJSON[T any](tr fdb.Transaction, key fdb.Key, value T) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return putRecord(tr, key, payload)
}
func typed[T any](r record) (metadata.Versioned[T], error) {
	out := metadata.Versioned[T]{Exists: r.exists, Version: r.version}
	if r.exists {
		if err := json.Unmarshal(r.payload, &out.Value); err != nil {
			return metadata.Versioned[T]{}, invalidRecord("record JSON")
		}
	}
	return out, nil
}
func getTyped[T any](m *mutation, key fdb.Key) (metadata.Versioned[T], error) {
	r, err := m.get(key)
	if err != nil {
		return metadata.Versioned[T]{}, err
	}
	return typed[T](r)
}
func readTyped[T any](tr fdb.Transaction, key fdb.Key) (metadata.Versioned[T], error) {
	r, err := readRecord(tr, key)
	if err != nil {
		return metadata.Versioned[T]{}, err
	}
	return typed[T](r)
}

// The cursor carries a scope hash and the exact last physical key. It cannot be
// replayed in another group/repository or turn a typed list into an arbitrary scan.
func encodeCursor(scope, key fdb.Key) metadata.Cursor {
	hash := sha256.Sum256(scope)
	raw := append(append([]byte(nil), hash[:16]...), key...)
	return metadata.Cursor(base64.RawURLEncoding.EncodeToString(raw))
}
func decodeCursor(scope fdb.Key, cursor metadata.Cursor) (fdb.Key, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(string(cursor))
	hash := sha256.Sum256(scope)
	if err != nil || len(raw) <= 16 || len(raw) > 10_016 || !bytes.Equal(raw[:16], hash[:16]) || !bytes.HasPrefix(raw[16:], scope) {
		return nil, invalidRecord("cursor scope")
	}
	return fdb.Key(raw[16:]), nil
}
func listTyped[T any](ctx context.Context, s *Store, scope fdb.Key, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[T]], error) {
	if limit < 1 || limit > maxPageRows {
		return metadata.Page[metadata.Versioned[T]]{}, invalidRecord("page limit must be 1..512")
	}
	last, err := decodeCursor(scope, cursor)
	if err != nil {
		return metadata.Page[metadata.Versioned[T]]{}, err
	}
	var out metadata.Page[metadata.Versioned[T]]
	err = s.read(ctx, func(tr fdb.Transaction) error {
		out = metadata.Page[metadata.Versioned[T]]{}
		bounds := rangeFor(scope)
		begin := fdb.FirstGreaterOrEqual(bounds.Begin)
		if last != nil {
			begin = fdb.FirstGreaterThan(last)
		}
		rows, err := tr.GetRange(fdb.SelectorRange{Begin: begin, End: fdb.FirstGreaterOrEqual(bounds.End)}, fdb.RangeOptions{Limit: limit + 1}).GetSliceWithError()
		if err != nil {
			return err
		}
		used := 0
		for i, row := range rows {
			if i == limit {
				out.Next = encodeCursor(scope, rows[i-1].Key)
				break
			}
			r, err := decodeRecord(tr, row.Key, row.Value)
			if err != nil {
				return err
			}
			if used+len(r.payload) > maxPageBytes && len(out.Items) > 0 {
				out.Next = encodeCursor(scope, rows[i-1].Key)
				break
			}
			v, err := typed[T](r)
			if err != nil {
				return err
			}
			out.Items = append(out.Items, v)
			used += len(r.payload)
		}
		return nil
	})
	return out, err
}
