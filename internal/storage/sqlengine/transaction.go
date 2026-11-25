package sqlengine

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

type transaction struct {
	tx      *sql.Tx
	ctx     context.Context
	owner   *owner
	meter   storage.Meter
	changed bool
}

func (t *transaction) param(n int) string {
	if t.owner.postgres {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}
func (t *transaction) Get(key []byte) (storage.Value, error) {
	values, err := t.GetMany([][]byte{key})
	if err != nil {
		return storage.Value{}, err
	}
	return values[0], nil
}
func (t *transaction) GetMany(keys [][]byte) ([]storage.Value, error) {
	if len(keys) > storage.MaxPageRows {
		return nil, &storage.CapacityError{Resource: "GetMany keys"}
	}
	out := make([]storage.Value, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	args := []any{t.owner.namespace}
	placeholders := make([]string, len(keys))
	for i, key := range keys {
		if err := storage.ValidateKey(key); err != nil {
			return nil, err
		}
		if err := t.meter.Add(len(key) + 16); err != nil {
			return nil, err
		}
		args = append(args, key)
		placeholders[i] = t.param(i + 2)
	}
	rows, err := t.tx.QueryContext(t.ctx, "SELECT key,value FROM kuasar_storage_v2 WHERE namespace="+t.param(1)+" AND key IN ("+strings.Join(placeholders, ",")+")", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make(map[string][]byte, len(keys))
	for rows.Next() {
		var key, value []byte
		if err = rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		values[string(key)] = value
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for i, key := range keys {
		if value, ok := values[string(key)]; ok {
			out[i] = storage.Value{Exists: true, Bytes: bytes.Clone(value)}
		}
	}
	return out, nil
}
func (t *transaction) Scan(bounds storage.Range, limit storage.Limit) (storage.Page, error) {
	if err := storage.ValidateRange(bounds, limit); err != nil {
		return storage.Page{}, err
	}
	if err := t.meter.Add(len(bounds.Begin) + len(bounds.End) + len(bounds.After) + 32); err != nil {
		return storage.Page{}, err
	}
	query, args := t.scanQuery(bounds, limit)
	rows, err := t.tx.QueryContext(t.ctx, query, args...)
	if err != nil {
		return storage.Page{}, err
	}
	defer rows.Close()
	var out storage.Page
	used := 0
	for rows.Next() {
		var row storage.Row
		if err = rows.Scan(&row.Key, &row.Value); err != nil {
			return storage.Page{}, err
		}
		if len(out.Rows) == limit.Rows || (len(out.Rows) > 0 && used+len(row.Key)+len(row.Value) > limit.Bytes) {
			out.More = true
			break
		}
		if err = t.meter.Add(len(row.Key) + 16); err != nil {
			return storage.Page{}, err
		}
		used += len(row.Key) + len(row.Value)
		out.Rows = append(out.Rows, row)
	}
	return out, rows.Err()
}

// scanQuery receives validated bounds. After is at least Begin, so its exclusive
// lower bound subsumes Begin. Keeping both predicates lets SQLite seek to Begin
// and filter every preceding key before reaching After on each cursor page.
func (t *transaction) scanQuery(bounds storage.Range, limit storage.Limit) (string, []any) {
	lower, comparison := bounds.Begin, ">="
	if bounds.After != nil {
		lower, comparison = bounds.After, ">"
	}
	args := []any{t.owner.namespace, nonNil(lower)}
	query := "SELECT key,value FROM kuasar_storage_v2 WHERE namespace=" + t.param(1) + " AND key " + comparison + " " + t.param(2)
	if bounds.End != nil {
		args = append(args, bounds.End)
		query += " AND key < " + t.param(len(args))
	}
	args = append(args, limit.Rows+1)
	query += " ORDER BY key LIMIT " + t.param(len(args))
	return query, args
}

func (t *transaction) Put(key, value []byte) error {
	if err := storage.ValidateKey(key); err != nil {
		return err
	}
	if len(value) > storage.MaxValueBytes {
		return &storage.CapacityError{Resource: "value bytes"}
	}
	if err := t.meter.Add(len(key) + len(value) + 32); err != nil {
		return err
	}
	query := "INSERT INTO kuasar_storage_v2(namespace,key,value) VALUES(" + t.param(1) + "," + t.param(2) + "," + t.param(3) + ") ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value"
	_, err := t.tx.ExecContext(t.ctx, query, t.owner.namespace, key, nonNil(value))
	if err == nil {
		t.changed = true
	}
	return err
}
func (t *transaction) Delete(key []byte) error {
	if err := storage.ValidateKey(key); err != nil {
		return err
	}
	if err := t.meter.Add(len(key) + 16); err != nil {
		return err
	}
	_, err := t.tx.ExecContext(t.ctx, "DELETE FROM kuasar_storage_v2 WHERE namespace="+t.param(1)+" AND key="+t.param(2), t.owner.namespace, key)
	if err == nil {
		t.changed = true
	}
	return err
}
func nonNil(v []byte) []byte {
	if v == nil {
		return []byte{}
	}
	return v
}
