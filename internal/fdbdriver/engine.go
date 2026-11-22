//go:build fdb && cgo

package fdbdriver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/observebudget"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/observe"
)

const apiVersion = 730

var apiOnce sync.Once
var apiError error

type EngineOptions struct{ ClusterFile, Prefix string }
type engineOwner struct {
	db     fdb.Database
	refs   int
	budget *observebudget.Budget
}

var engineOwners = struct {
	sync.Mutex
	byFile map[string]*engineOwner
}{byFile: make(map[string]*engineOwner)}

type Engine struct {
	owner        *engineOwner
	file         string
	prefix       []byte
	life         *storage.Lifetime
	observations *observe.Manager
	once         sync.Once
}

func OpenEngine(options EngineOptions) (*Engine, error) {
	if options.ClusterFile == "" || options.Prefix == "" || len(options.Prefix) > 512 || options.Prefix[0] == 0xff || strings.IndexByte(options.Prefix, 0) >= 0 {
		return nil, fmt.Errorf("%w: explicit cluster file and namespace required", storage.ErrInvalid)
	}
	file, err := filepath.Abs(options.ClusterFile)
	if err != nil {
		return nil, err
	}
	file, err = filepath.EvalSymlinks(file)
	if err != nil {
		return nil, err
	}
	if _, err = os.Stat(file); err != nil {
		return nil, err
	}
	apiOnce.Do(func() { apiError = fdb.APIVersion(apiVersion) })
	if apiError != nil {
		return nil, apiError
	}
	engineOwners.Lock()
	defer engineOwners.Unlock()
	o := engineOwners.byFile[file]
	if o == nil {
		db, err := fdb.OpenDatabase(file)
		if err != nil {
			return nil, err
		}
		o = &engineOwner{db: db, budget: observebudget.NewBudget(observebudget.DefaultObservationLimit)}
		engineOwners.byFile[file] = o
	}
	o.refs++
	return &Engine{owner: o, file: file, prefix: append([]byte(options.Prefix), 0), life: storage.NewLifetime(), observations: observe.New(o.budget)}, nil
}
func (e *Engine) Close() error {
	e.once.Do(func() {
		e.life.Stop()
		e.observations.Close()
		e.life.Close()
		engineOwners.Lock()
		defer engineOwners.Unlock()
		e.owner.refs--
		if e.owner.refs == 0 {
			e.owner.db.Close()
			delete(engineOwners.byFile, e.file)
		}
	})
	return nil
}
func (e *Engine) transaction(ctx context.Context) (fdb.Transaction, func(), error) {
	tr, err := e.owner.db.CreateTransaction()
	if err != nil {
		return tr, nil, err
	}
	timeout := 4 * time.Second
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < timeout {
		timeout = time.Until(deadline)
	}
	if err = tr.Options().SetTimeout(max(1, timeout.Milliseconds())); err != nil {
		tr.Cancel()
		return tr, nil, err
	}
	stop := context.AfterFunc(ctx, tr.Cancel)
	return tr, func() { stop(); tr.Cancel() }, nil
}
func engineError(ctx context.Context, err error, committing bool) error {
	if err == nil {
		return nil
	}
	var native fdb.Error
	if errors.As(err, &native) {
		if native.Code == 1032 {
			return &storage.CapacityError{Resource: "native observations"}
		}
		if nativePredicate(int(fdb.ErrorPredicateRetryableNotCommitted), native.Code) {
			return fmt.Errorf("%w: native error %d", storage.ErrRetry, native.Code)
		}
		if committing {
			return &storage.Unknown{Cause: err}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &storage.Unavailable{Cause: err}
	}
	if committing {
		return &storage.Unknown{Cause: err}
	}
	return err
}
func (e *Engine) View(ctx context.Context, fn func(storage.ReadTx) error) error {
	ctx, done, err := e.life.Enter(ctx)
	if err != nil {
		return err
	}
	defer done()
	tr, cleanup, err := e.transaction(ctx)
	if err != nil {
		return engineError(ctx, err, false)
	}
	defer cleanup()
	return engineError(ctx, fn(&engineTx{native: tr, prefix: e.prefix}), false)
}
func (e *Engine) Update(ctx context.Context, fn func(storage.Tx) error) error {
	ctx, done, err := e.life.Enter(ctx)
	if err != nil {
		return err
	}
	defer done()
	tr, cleanup, err := e.transaction(ctx)
	if err != nil {
		return engineError(ctx, err, false)
	}
	defer cleanup()
	tx := &engineTx{native: tr, prefix: e.prefix}
	if err = fn(tx); err != nil {
		return engineError(ctx, err, false)
	}
	if err = tx.meter.Err(); err != nil {
		return err
	}
	size, err := tr.GetApproximateSize().Get()
	if err != nil {
		return engineError(ctx, err, false)
	}
	if size >= 10_000_000 {
		return &storage.CapacityError{Resource: "native transaction bytes"}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return engineError(ctx, tr.Commit().Get(), true)
}
func (e *Engine) ReserveObservation(ctx context.Context) (context.Context, func(), error) {
	return e.observations.Reserve(ctx)
}
func (e *Engine) ObservationSlots() int     { return e.owner.budget.Used() }
func (e *Engine) ActiveObservations() int64 { return e.observations.Active() }
