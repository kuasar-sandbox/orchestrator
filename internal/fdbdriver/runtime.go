//go:build fdb && cgo

// Package fdbdriver is the native FoundationDB implementation of the cluster's
// typed metadata repositories. No database, key or transaction handle escapes it.
package fdbdriver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/observebudget"
)

const apiVersion = 730
const transactionTimeout = 4 * time.Second
const transactionBytes = 10_000_000

type Options struct {
	ClusterFile string
	Prefix      string
}
type databaseOwner struct {
	db     fdb.Database
	budget *observebudget.Budget
	refs   int
}

var owners = struct {
	sync.Mutex
	byFile map[string]*databaseOwner
}{byFile: make(map[string]*databaseOwner)}
var apiOnce sync.Once
var apiError error

type Store struct {
	owner              *databaseOwner
	clusterFile        string
	prefix             []byte
	ctx                context.Context
	cancel             context.CancelFunc
	mu                 sync.Mutex
	closed             bool
	done               chan struct{}
	work               sync.WaitGroup
	activeObservations atomic.Int64
	// Test-only fault boundaries are deliberately private.
	beforeCommit         func(int, fdb.Transaction) error
	afterNativeCommit    func(int) error
	afterReadBeforeWatch func()
}

func Open(options Options) (*Store, error) {
	if options.ClusterFile == "" {
		return nil, fmt.Errorf("fdbdriver: an explicit cluster file is required")
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
	if options.Prefix == "" {
		options.Prefix = "/orchestrator/metadata/v1/"
	}
	if len(options.Prefix) > 512 || options.Prefix[0] == 0xff || strings.IndexByte(options.Prefix, 0) >= 0 {
		return nil, fmt.Errorf("fdbdriver: invalid metadata prefix")
	}
	apiOnce.Do(func() { apiError = fdb.APIVersion(apiVersion) })
	if apiError != nil {
		return nil, apiError
	}
	owners.Lock()
	defer owners.Unlock()
	owner := owners.byFile[file]
	if owner == nil {
		db, err := fdb.OpenDatabase(file)
		if err != nil {
			return nil, err
		}
		owner = &databaseOwner{db: db, budget: observebudget.NewBudget(observebudget.DefaultObservationLimit)}
		owners.byFile[file] = owner
	}
	owner.refs++
	ctx, cancel := context.WithCancel(context.Background())
	return &Store{owner: owner, clusterFile: file, prefix: append([]byte(options.Prefix), 0), ctx: ctx, cancel: cancel, done: make(chan struct{})}, nil
}

// Close cancels this store's reads, writes, reservations and native futures and
// waits for reclamation before the last owner destroys the database handle.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.done
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	s.work.Wait()
	owners.Lock()
	s.owner.refs--
	if s.owner.refs == 0 {
		s.owner.db.Close()
		delete(owners.byFile, s.clusterFile)
	}
	owners.Unlock()
	close(s.done)
	return nil
}
func (s *Store) operation(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, nil, &metadata.Unavailable{Cause: errors.New("database is closed")}
	}
	s.work.Add(1)
	s.mu.Unlock()
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	var once sync.Once
	release := func() { once.Do(func() { stop(); cancel(); s.work.Done() }) }
	return child, release, nil
}
func (s *Store) newTransaction(ctx context.Context) (fdb.Transaction, func(), error) {
	if err := ctx.Err(); err != nil {
		return fdb.Transaction{}, nil, err
	}
	tr, err := s.owner.db.CreateTransaction()
	if err != nil {
		return fdb.Transaction{}, nil, err
	}
	timeout := transactionTimeout
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < timeout {
		timeout = time.Until(deadline)
	}
	ms := timeout.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	if err = tr.Options().SetTimeout(ms); err != nil {
		tr.Cancel()
		return fdb.Transaction{}, nil, err
	}
	stop := context.AfterFunc(ctx, tr.Cancel)
	return tr, func() { stop(); tr.Cancel() }, nil
}
func knownNotCommitted(err error) bool {
	var e fdb.Error
	return errors.As(err, &e) && nativePredicate(int(fdb.ErrorPredicateRetryableNotCommitted), e.Code)
}
func classifyCommit(err error) error {
	if knownNotCommitted(err) {
		return &metadata.Unavailable{Cause: err}
	}
	return &metadata.Unknown{Cause: err}
}
func retryNative(tr fdb.Transaction, err error) bool {
	var e fdb.Error
	return errors.As(err, &e) && knownNotCommitted(err) && tr.OnError(e).Get() == nil
}
func unavailable(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var e fdb.Error
	if errors.As(err, &e) {
		return &metadata.Unavailable{Cause: err}
	}
	return err
}

func (s *Store) read(ctx context.Context, fn func(fdb.Transaction) error) error {
	ctx, release, err := s.operation(ctx)
	if err != nil {
		return err
	}
	defer release()
	tr, cleanup, err := s.newTransaction(ctx)
	if err != nil {
		return unavailable(ctx, err)
	}
	defer cleanup()
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		err = fn(tr)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryNative(tr, err) {
			return unavailable(ctx, err)
		}
	}
}

type mutation struct {
	tr     fdb.Transaction
	pinned map[string]metadata.Version
	cache  map[string]record
}

func (m *mutation) get(key fdb.Key) (record, error) {
	if r, ok := m.cache[string(key)]; ok {
		return r, nil
	}
	r, err := readRecord(m.tr, key)
	if err != nil {
		return record{}, err
	}
	if expected, ok := m.pinned[string(key)]; ok {
		if r.version != expected {
			return record{}, &metadata.Conflict{Expected: expected, Actual: r.version}
		}
	} else {
		m.pinned[string(key)] = r.version
	}
	m.cache[string(key)] = r
	return r, nil
}
func (m *mutation) expect(key fdb.Key, expected metadata.Version) (record, error) {
	r, err := m.get(key)
	if err != nil {
		return record{}, err
	}
	if expected != r.version {
		return record{}, &metadata.Conflict{Expected: expected, Actual: r.version}
	}
	return r, nil
}

// Only metadata-only private callbacks use this loop. Every observed record is
// pinned on its first successful read, so a native retry cannot adopt a later
// business precondition. Unknown is returned without executing OnError.
func (s *Store) mutate(ctx context.Context, fn func(*mutation) (bool, error)) (metadata.Version, error) {
	ctx, release, err := s.operation(ctx)
	if err != nil {
		return metadata.Version{}, err
	}
	defer release()
	tr, cleanup, err := s.newTransaction(ctx)
	if err != nil {
		return metadata.Version{}, unavailable(ctx, err)
	}
	defer cleanup()
	pinned := make(map[string]metadata.Version)
	for attempt := 1; ; attempt++ {
		if err = ctx.Err(); err != nil {
			return metadata.Version{}, err
		}
		changed, err := fn(&mutation{tr: tr, pinned: pinned, cache: make(map[string]record)})
		if err != nil {
			if ctx.Err() != nil {
				return metadata.Version{}, ctx.Err()
			}
			if retryNative(tr, err) {
				continue
			}
			return metadata.Version{}, unavailable(ctx, err)
		}
		if !changed {
			return metadata.Version{}, nil
		}
		size, err := tr.GetApproximateSize().Get()
		if err != nil {
			if retryNative(tr, err) {
				continue
			}
			return metadata.Version{}, unavailable(ctx, err)
		}
		if size >= transactionBytes {
			return metadata.Version{}, &metadata.CapacityError{Resource: "native transaction bytes"}
		}
		stamp := tr.GetVersionstamp()
		if s.beforeCommit != nil {
			if err = s.beforeCommit(attempt, tr); err != nil {
				return metadata.Version{}, err
			}
		}
		if err = ctx.Err(); err != nil {
			return metadata.Version{}, err
		}
		err = tr.Commit().Get()
		if err == nil && s.afterNativeCommit != nil {
			err = s.afterNativeCommit(attempt)
		}
		if err != nil {
			if knownNotCommitted(err) {
				if ctx.Err() != nil {
					return metadata.Version{}, ctx.Err()
				}
				if retryNative(tr, err) {
					continue
				}
			}
			return metadata.Version{}, classifyCommit(err)
		}
		value, err := stamp.Get()
		if err != nil {
			return metadata.Version{}, &metadata.Unknown{Cause: err}
		}
		var version metadata.Version
		copy(version[:], value)
		if len(value) != len(version) || !version.IsCommitted() {
			return metadata.Version{}, &metadata.Unknown{Cause: fmt.Errorf("invalid native versionstamp")}
		}
		return version, nil
	}
}
