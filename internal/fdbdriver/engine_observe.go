//go:build fdb && cgo

package fdbdriver

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

func (e *Engine) Observe(ctx context.Context, conditions []storage.Condition) (storage.Invalidation, error) {
	copyConditions := make([]storage.Condition, len(conditions))
	for i, c := range conditions {
		if err := storage.ValidateKey(c.Key); err != nil {
			return nil, err
		}
		copyConditions[i] = storage.Condition{Key: bytes.Clone(c.Key), Value: storage.Clone(c.Value)}
	}
	return e.observations.Run(ctx, len(copyConditions), func(ctx context.Context) error {
		for attempt := 1; ; attempt++ {
			futures, err := e.arm(ctx, copyConditions)
			if err == nil {
				return waitEngineFutures(ctx, futures)
			}
			if !errors.Is(err, storage.ErrRetry) {
				return err
			}
			timer := time.NewTimer(time.Duration(min(attempt, 25)) * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	})
}
func (e *Engine) arm(ctx context.Context, conditions []storage.Condition) ([]fdb.FutureNil, error) {
	tr, cleanup, err := e.transaction(ctx)
	if err != nil {
		return nil, engineError(ctx, err, false)
	}
	defer cleanup()
	tx := &engineTx{native: tr, prefix: e.prefix}
	keys := make([][]byte, len(conditions))
	for i, c := range conditions {
		keys[i] = c.Key
	}
	values, err := tx.GetMany(keys)
	if err != nil {
		return nil, engineError(ctx, err, false)
	}
	for i, c := range conditions {
		if !storage.Equal(c.Value, values[i]) {
			return nil, nil
		}
	}
	futures := make([]fdb.FutureNil, len(conditions))
	for i, c := range conditions {
		futures[i] = tr.Watch(tx.key(c.Key))
	}
	if err = tr.Commit().Get(); err != nil {
		for _, future := range futures {
			future.Cancel()
		}
		for _, future := range futures {
			_ = future.Get()
		}
		// Arming writes no data, so retrying uncertain observation setup is safe.
		var native fdb.Error
		if errors.As(err, &native) && native.Code != 1032 && nativePredicate(int(fdb.ErrorPredicateRetryable), native.Code) {
			return nil, storage.ErrRetry
		}
		return nil, engineError(ctx, err, false)
	}
	return futures, nil
}
func waitEngineFutures(ctx context.Context, futures []fdb.FutureNil) error {
	if len(futures) == 0 {
		return nil
	}
	results := make(chan error, len(futures))
	var workers sync.WaitGroup
	for _, future := range futures {
		workers.Add(1)
		go func(f fdb.FutureNil) { defer workers.Done(); results <- f.Get() }(future)
	}
	var err error
	select {
	case err = <-results:
	case <-ctx.Done():
		err = ctx.Err()
	}
	for _, future := range futures {
		future.Cancel()
	}
	workers.Wait()
	err = engineError(ctx, err, false)
	if errors.Is(err, storage.ErrRetry) {
		// A watch failure invalidates the observation. Retry only applies to an
		// aborted transaction, not to an already armed consumer subscription.
		return &storage.Unavailable{Cause: err}
	}
	return err
}
