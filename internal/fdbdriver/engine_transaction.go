//go:build fdb && cgo

package fdbdriver

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

type engineTx struct {
	native fdb.Transaction
	prefix []byte
	meter  storage.Meter
}

func (t *engineTx) key(key []byte) fdb.Key { return fdb.Key(append(bytes.Clone(t.prefix), key...)) }
func (t *engineTx) Get(key []byte) (storage.Value, error) {
	values, err := t.GetMany([][]byte{key})
	if err != nil {
		return storage.Value{}, err
	}
	return values[0], nil
}
func (t *engineTx) GetMany(keys [][]byte) ([]storage.Value, error) {
	if len(keys) > storage.MaxPageRows {
		return nil, &storage.CapacityError{Resource: "GetMany keys"}
	}
	futures := make([]fdb.FutureByteSlice, len(keys))
	for i, key := range keys {
		if err := storage.ValidateKey(key); err != nil {
			return nil, err
		}
		if err := t.meter.Add(len(key) + 16); err != nil {
			return nil, err
		}
		futures[i] = t.native.Get(t.key(key))
	}
	out := make([]storage.Value, len(keys))
	for i, future := range futures {
		raw, err := future.Get()
		if err != nil {
			return nil, err
		}
		out[i] = storage.Value{Exists: raw != nil, Bytes: bytes.Clone(raw)}
	}
	return out, nil
}
func (t *engineTx) Scan(bounds storage.Range, limit storage.Limit) (storage.Page, error) {
	if err := storage.ValidateRange(bounds, limit); err != nil {
		return storage.Page{}, err
	}
	if err := t.meter.Add(len(bounds.Begin) + len(bounds.End) + len(bounds.After) + 32); err != nil {
		return storage.Page{}, err
	}
	begin := fdb.FirstGreaterOrEqual(t.key(bounds.Begin))
	if bounds.After != nil {
		begin = fdb.FirstGreaterThan(t.key(bounds.After))
	}
	end := t.key(bounds.End)
	if bounds.End == nil {
		end = fdb.Key(storage.PrefixEnd(t.prefix))
	}
	// Adaptive batches retain incremental reads while avoiding one tiny native
	// request per record for a caller that consumes a whole bounded page.
	rows := t.native.GetRange(fdb.SelectorRange{Begin: begin, End: fdb.FirstGreaterOrEqual(end)}, fdb.RangeOptions{Limit: limit.Rows + 1, Mode: fdb.StreamingModeIterator}).Iterator()
	var out storage.Page
	used := 0
	for rows.Advance() {
		row, err := rows.Get()
		if err != nil {
			return storage.Page{}, err
		}
		key := bytes.Clone(row.Key[len(t.prefix):])
		size := len(key) + len(row.Value)
		if len(out.Rows) == limit.Rows || (len(out.Rows) > 0 && used+size > limit.Bytes) {
			out.More = true
			break
		}
		if err = t.meter.Add(len(key) + 16); err != nil {
			return storage.Page{}, err
		}
		used += size
		out.Rows = append(out.Rows, storage.Row{Key: key, Value: bytes.Clone(row.Value)})
	}
	return out, nil
}
func (t *engineTx) Put(key, value []byte) error {
	if err := storage.ValidateKey(key); err != nil {
		return err
	}
	if len(value) > storage.MaxValueBytes {
		return &storage.CapacityError{Resource: "value bytes"}
	}
	if err := t.meter.Add(len(key) + len(value) + 32); err != nil {
		return err
	}
	t.native.Set(t.key(key), value)
	return nil
}
func (t *engineTx) Delete(key []byte) error {
	if err := storage.ValidateKey(key); err != nil {
		return err
	}
	if err := t.meter.Add(len(key) + 16); err != nil {
		return err
	}
	t.native.Clear(t.key(key))
	return nil
}
