// Package sqlengine implements the generic storage contract on SQLite and
// PostgreSQL. Only SQL dialect, connection setup and error classification differ.
package sqlengine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/kuasar-sandbox/orchestrator/internal/observebudget"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/observe"
	"modernc.org/sqlite"
)

type owner struct {
	db           *sql.DB
	namespace    []byte
	postgres     bool
	refs         int
	budget       *observebudget.Budget
	poller       *observe.Poller
	stopListener func()
	channel      string
}

var owners = struct {
	sync.Mutex
	items map[string]*owner
}{items: make(map[string]*owner)}

type Engine struct {
	owner        *owner
	id           string
	life         *storage.Lifetime
	observations *observe.Manager
	once         sync.Once
	closeErr     error
}

func open(ctx context.Context, id, driver, dsn, namespace string, postgres bool) (*Engine, error) {
	if namespace == "" || len(namespace) > 512 {
		return nil, fmt.Errorf("%w: explicit namespace required (1..512 bytes)", storage.ErrInvalid)
	}
	id += "\x00" + namespace
	owners.Lock()
	defer owners.Unlock()
	o := owners.items[id]
	if o == nil {
		db, err := sql.Open(driver, dsn)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(16)
		db.SetMaxIdleConns(4)
		if err = db.PingContext(ctx); err != nil {
			db.Close()
			return nil, &storage.Unavailable{Cause: err}
		}
		err = initializeSchema(ctx, db, postgres)
		if err != nil {
			db.Close()
			return nil, &storage.Unavailable{Cause: err}
		}
		hash := sha256.Sum256([]byte(namespace))
		o = &owner{db: db, namespace: hash[:], postgres: postgres, budget: observebudget.NewBudget(observebudget.DefaultObservationLimit), channel: "kuasar_storage_" + hex.EncodeToString(hash[:16])}
		o.poller = observe.NewPoller(func(ctx context.Context, keys [][]byte) (out []storage.Value, err error) {
			err = o.view(ctx, func(tx storage.ReadTx) error { out, err = tx.GetMany(keys); return err })
			return
		})
		if postgres {
			o.stopListener = startListener(dsn, o.channel, o.poller)
		}
		owners.items[id] = o
	}
	o.refs++
	return &Engine{owner: o, id: id, life: storage.NewLifetime(), observations: observe.New(o.budget)}, nil
}
func (e *Engine) Close() error {
	e.once.Do(func() {
		e.life.Stop()
		e.observations.Close()
		e.life.Close()
		owners.Lock()
		defer owners.Unlock()
		e.owner.refs--
		if e.owner.refs == 0 {
			delete(owners.items, e.id)
			if e.owner.stopListener != nil {
				e.owner.stopListener()
			}
			e.owner.poller.Close()
			e.closeErr = e.owner.db.Close()
		}
	})
	return e.closeErr
}
func (e *Engine) View(ctx context.Context, fn func(storage.ReadTx) error) error {
	ctx, done, err := e.life.Enter(ctx)
	if err != nil {
		return err
	}
	defer done()
	return e.owner.view(ctx, fn)
}
func (o *owner) view(ctx context.Context, fn func(storage.ReadTx) error) error {
	tx, err := o.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		return classify(err, false)
	}
	defer tx.Rollback()
	return classify(fn(&transaction{tx: tx, ctx: ctx, owner: o}), false)
}
func (e *Engine) Update(ctx context.Context, fn func(storage.Tx) error) error {
	ctx, done, err := e.life.Enter(ctx)
	if err != nil {
		return err
	}
	defer done()
	conn, err := e.owner.db.Conn(ctx)
	if err != nil {
		return classify(err, false)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return classify(err, false)
	}
	defer tx.Rollback()
	if e.owner.postgres {
		if _, err = tx.ExecContext(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
			return classify(err, false)
		}
	}
	wrapped := &transaction{tx: tx, ctx: ctx, owner: e.owner}
	if err = fn(wrapped); err != nil {
		return classify(err, false)
	}
	if err = wrapped.meter.Err(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if e.owner.postgres && wrapped.changed {
		// Wakeups carry no user data and are committed atomically with the write.
		if _, err = tx.ExecContext(ctx, "SELECT pg_notify($1, '')", e.owner.channel); err != nil {
			return classify(err, false)
		}
	}
	if err = tx.Commit(); err != nil {
		// A failed SQLite COMMIT can leave its native transaction open even
		// though database/sql has marked sql.Tx done. Never pool that session.
		// Closing a session cannot undo a transaction that already committed.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return classify(err, true)
	}
	if wrapped.changed {
		e.owner.poller.Wake()
	}
	return nil
}
func classify(err error, committing bool) error {
	if err == nil {
		return nil
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		if pg.Code == "40001" || pg.Code == "40P01" {
			return fmt.Errorf("%w: %s", storage.ErrRetry, pg.Code)
		}
		if committing {
			// A FATAL disconnect can race durable COMMIT. Only the explicit
			// serialization/deadlock responses above prove non-commit.
			return &storage.Unknown{Cause: err}
		}
		return &storage.Unavailable{Cause: err}
	}
	var sq *sqlite.Error
	if errors.As(err, &sq) && (sq.Code()&255 == 5 || sq.Code()&255 == 6) {
		return fmt.Errorf("%w: sqlite contention", storage.ErrRetry)
	}
	if committing {
		return &storage.Unknown{Cause: err}
	}
	// Preserve application and context errors exactly. They cannot authorize retry.
	return err
}
func (e *Engine) ReserveObservation(ctx context.Context) (context.Context, func(), error) {
	return e.observations.Reserve(ctx)
}
func (e *Engine) Observe(ctx context.Context, conditions []storage.Condition) (storage.Invalidation, error) {
	copyConditions := make([]storage.Condition, len(conditions))
	for i, c := range conditions {
		if err := storage.ValidateKey(c.Key); err != nil {
			return nil, err
		}
		copyConditions[i] = storage.Condition{Key: append([]byte(nil), c.Key...), Value: storage.Clone(c.Value)}
	}
	return e.observations.Run(ctx, len(copyConditions), func(ctx context.Context) error { return e.owner.poller.Wait(ctx, copyConditions) })
}
func (e *Engine) ObservationSlots() int     { return e.owner.budget.Used() }
func (e *Engine) ActiveObservations() int64 { return e.observations.Active() }
