package clusterstore

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// defaultRetention is the change-log depth kept for catch-up Watch when the
// caller passes retention <= 0. Mirrors cluster.md channel.revision_retention.
const defaultRetention = 10000

// watchBuf is the per-watcher live-event buffer; a watcher that overruns it is
// dropped (channel closed) to signal the consumer to full-resync.
const watchBuf = 256

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS kv (
  key      TEXT PRIMARY KEY,
  value    BLOB,
  mod_rev  INTEGER NOT NULL,
  lease_id INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_kv_lease ON kv(lease_id);
CREATE TABLE IF NOT EXISTS changelog (
  rev   INTEGER PRIMARY KEY,
  key   TEXT NOT NULL,
  typ   INTEGER NOT NULL,
  value BLOB
);
CREATE TABLE IF NOT EXISTS leases (
  lease_id     INTEGER PRIMARY KEY,
  expires_unix INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v INTEGER NOT NULL
);
`

type sqliteStore struct {
	db        *sql.DB
	retention int64
	now       func() int64 // time source (overridable in tests)

	mu       sync.Mutex // serializes mutations + watch registration + hub
	rev      int64      // current revision (mirrors meta 'rev')
	leaseSeq int64      // monotonic lease id source (mirrors meta 'lease_seq')
	watchers map[*watcher]struct{}

	closeOnce sync.Once
	closed    chan struct{}
}

type watcher struct {
	prefix string
	ch     chan Event
}

// Open opens (creating if needed) the sqlite-backed cluster store at path with
// the given change-log retention (<=0 → default). The file should be 0600.
func Open(path string, retention int) (Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("clusterstore: open %s: %w", path, err)
	}
	if _, err := db.ExecContext(context.Background(), sqliteSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("clusterstore: init schema: %w", err)
	}
	s := &sqliteStore{
		db:        db,
		retention: int64(retention),
		now:       func() int64 { return time.Now().Unix() },
		watchers:  make(map[*watcher]struct{}),
		closed:    make(chan struct{}),
	}
	if s.retention <= 0 {
		s.retention = defaultRetention
	}
	if err := s.loadCounters(); err != nil {
		db.Close()
		return nil, err
	}
	go s.sweepLoop()
	return s, nil
}

func (s *sqliteStore) loadCounters() error {
	for _, k := range []struct {
		name string
		dst  *int64
	}{{"rev", &s.rev}, {"lease_seq", &s.leaseSeq}} {
		var v int64
		err := s.db.QueryRowContext(context.Background(), `SELECT v FROM meta WHERE k=?`, k.name).Scan(&v)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("clusterstore: load %s: %w", k.name, err)
		}
		*k.dst = v
	}
	return nil
}

func (s *sqliteStore) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.mu.Lock()
		for w := range s.watchers {
			delete(s.watchers, w)
			close(w.ch)
		}
		s.mu.Unlock()
	})
	return s.db.Close()
}

// --- reads ---

func (s *sqliteStore) Get(ctx context.Context, key string) (KV, bool, error) {
	var kv KV
	kv.Key = key
	err := s.db.QueryRowContext(ctx, `SELECT value, mod_rev FROM kv WHERE key=?`, key).Scan(&kv.Value, &kv.ModRev)
	if err == sql.ErrNoRows {
		return KV{}, false, nil
	}
	if err != nil {
		return KV{}, false, fmt.Errorf("clusterstore: get %s: %w", key, err)
	}
	return kv, true, nil
}

func (s *sqliteStore) Range(ctx context.Context, prefix string, fn func(KV) error) error {
	lo, hi := prefixRange(prefix)
	var rows *sql.Rows
	var err error
	if hi == "" {
		rows, err = s.db.QueryContext(ctx, `SELECT key, value, mod_rev FROM kv WHERE key>=? ORDER BY key ASC`, lo)
	} else {
		rows, err = s.db.QueryContext(ctx, `SELECT key, value, mod_rev FROM kv WHERE key>=? AND key<? ORDER BY key ASC`, lo, hi)
	}
	if err != nil {
		return fmt.Errorf("clusterstore: range %q: %w", prefix, err)
	}
	defer rows.Close()
	for rows.Next() {
		var kv KV
		if err := rows.Scan(&kv.Key, &kv.Value, &kv.ModRev); err != nil {
			return err
		}
		if err := fn(kv); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *sqliteStore) Rev(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rev, nil
}

// --- writes (all funnel through mutate, under s.mu) ---

func (s *sqliteStore) Put(ctx context.Context, key string, val []byte) (int64, error) {
	return s.PutLeased(ctx, key, val, 0)
}

func (s *sqliteStore) PutLeased(ctx context.Context, key string, val []byte, lease LeaseID) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putLocked(ctx, key, val, lease)
}

func (s *sqliteStore) Delete(ctx context.Context, key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM kv WHERE key=?`, key).Scan(&exists); err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return s.deleteLocked(ctx, key)
}

func (s *sqliteStore) CAS(ctx context.Context, key string, expectRev int64, val []byte) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cur int64
	err := s.db.QueryRowContext(ctx, `SELECT mod_rev FROM kv WHERE key=?`, key).Scan(&cur)
	if err == sql.ErrNoRows {
		if expectRev != 0 {
			return 0, false, nil // expected an existing rev, key absent
		}
	} else if err != nil {
		return 0, false, err
	} else if cur != expectRev {
		return 0, false, nil
	}
	rev, err := s.putLocked(ctx, key, val, 0)
	return rev, err == nil, err
}

// putLocked / deleteLocked assume s.mu is held. They bump the revision, write
// the kv row + change-log entry + meta atomically, prune old change-log, and
// notify watchers.
func (s *sqliteStore) putLocked(ctx context.Context, key string, val []byte, lease LeaseID) (int64, error) {
	rev := s.rev + 1
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO kv (key,value,mod_rev,lease_id) VALUES (?,?,?,?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value, mod_rev=excluded.mod_rev, lease_id=excluded.lease_id`,
		key, val, rev, int64(lease)); err != nil {
		tx.Rollback()
		return 0, err
	}
	if err := s.commitMutation(ctx, tx, rev, key, EventPut, val); err != nil {
		return 0, err
	}
	s.rev = rev
	s.notify(Event{Type: EventPut, Key: key, Value: val, Rev: rev})
	return rev, nil
}

func (s *sqliteStore) deleteLocked(ctx context.Context, key string) (int64, error) {
	rev := s.rev + 1
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM kv WHERE key=?`, key); err != nil {
		tx.Rollback()
		return 0, err
	}
	if err := s.commitMutation(ctx, tx, rev, key, EventDelete, nil); err != nil {
		return 0, err
	}
	s.rev = rev
	s.notify(Event{Type: EventDelete, Key: key, Rev: rev})
	return rev, nil
}

func (s *sqliteStore) commitMutation(ctx context.Context, tx *sql.Tx, rev int64, key string, typ EventType, val []byte) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO changelog (rev,key,typ,value) VALUES (?,?,?,?)`, rev, key, int(typ), val); err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta (k,v) VALUES ('rev',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, rev); err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM changelog WHERE rev<=?`, rev-s.retention); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// --- watch ---

func (s *sqliteStore) Watch(ctx context.Context, prefix string, fromRev int64) (<-chan Event, error) {
	s.mu.Lock()
	if fromRev > 0 && fromRev < s.rev-s.retention {
		s.mu.Unlock()
		return nil, ErrCompacted
	}
	var replay []Event
	if fromRev > 0 && fromRev < s.rev {
		var err error
		if replay, err = s.readChangelog(ctx, prefix, fromRev); err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}
	w := &watcher{prefix: prefix, ch: make(chan Event, watchBuf)}
	s.watchers[w] = struct{}{}
	s.mu.Unlock()

	out := make(chan Event)
	go func() {
		defer close(out)
		defer s.unregister(w)
		for _, ev := range replay {
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			case <-s.closed:
				return
			}
		}
		for {
			select {
			case ev, ok := <-w.ch:
				if !ok {
					return // dropped (overflow) or store closed → consumer resyncs
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			case <-s.closed:
				return
			}
		}
	}()
	return out, nil
}

// readChangelog returns prefix-matching change-log events strictly after
// fromRev, in revision order. Assumes s.mu held.
func (s *sqliteStore) readChangelog(ctx context.Context, prefix string, fromRev int64) ([]Event, error) {
	lo, hi := prefixRange(prefix)
	q := `SELECT rev,key,typ,value FROM changelog WHERE rev>? AND key>=?`
	args := []any{fromRev, lo}
	if hi != "" {
		q += ` AND key<?`
		args = append(args, hi)
	}
	q += ` ORDER BY rev ASC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		var typ int
		if err := rows.Scan(&ev.Rev, &ev.Key, &typ, &ev.Value); err != nil {
			return nil, err
		}
		ev.Type = EventType(typ)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// notify delivers ev to every matching watcher. Assumes s.mu held. A watcher
// whose buffer is full is dropped (closed) — the consumer reconnects and
// resyncs, which is cheaper than blocking every writer on one slow reader.
func (s *sqliteStore) notify(ev Event) {
	for w := range s.watchers {
		if !hasPrefix(ev.Key, w.prefix) {
			continue
		}
		select {
		case w.ch <- ev:
		default:
			delete(s.watchers, w)
			close(w.ch)
		}
	}
}

func (s *sqliteStore) unregister(w *watcher) {
	s.mu.Lock()
	if _, ok := s.watchers[w]; ok {
		delete(s.watchers, w)
		close(w.ch)
	}
	s.mu.Unlock()
}

// --- leases ---

func (s *sqliteStore) Grant(ctx context.Context, ttlSec int64) (LeaseID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.leaseSeq + 1
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO leases (lease_id,expires_unix) VALUES (?,?)`, id, s.now()+ttlSec); err != nil {
		tx.Rollback()
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta (k,v) VALUES ('lease_seq',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, id); err != nil {
		tx.Rollback()
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	s.leaseSeq = id
	return LeaseID(id), nil
}

func (s *sqliteStore) KeepAlive(ctx context.Context, lease LeaseID, ttlSec int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `UPDATE leases SET expires_unix=? WHERE lease_id=?`, s.now()+ttlSec, int64(lease))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("clusterstore: lease %d not found", lease)
	}
	return nil
}

func (s *sqliteStore) Revoke(ctx context.Context, lease LeaseID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revokeLocked(ctx, int64(lease))
}

// revokeLocked deletes every key bound to the lease (emitting events) then the
// lease row. Assumes s.mu held.
func (s *sqliteStore) revokeLocked(ctx context.Context, lease int64) error {
	rows, err := s.db.QueryContext(ctx, `SELECT key FROM kv WHERE lease_id=?`, lease)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, k)
	}
	rows.Close()
	for _, k := range keys {
		if _, err := s.deleteLocked(ctx, k); err != nil {
			return err
		}
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM leases WHERE lease_id=?`, lease)
	return err
}

func (s *sqliteStore) sweepLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-t.C:
			s.sweepExpired()
		}
	}
}

func (s *sqliteStore) sweepExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := context.Background()
	rows, err := s.db.QueryContext(ctx, `SELECT lease_id FROM leases WHERE expires_unix<=?`, s.now())
	if err != nil {
		return
	}
	var expired []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return
		}
		expired = append(expired, id)
	}
	rows.Close()
	for _, id := range expired {
		_ = s.revokeLocked(ctx, id)
	}
}

// --- helpers ---

// prefixRange returns [lo, hi) key bounds for a prefix. hi=="" means unbounded
// above (empty prefix, or a prefix of all 0xff bytes — neither occurs for the
// registry's ASCII keys).
func prefixRange(prefix string) (lo, hi string) {
	if prefix == "" {
		return "", ""
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return prefix, string(b[:i+1])
		}
	}
	return prefix, ""
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
