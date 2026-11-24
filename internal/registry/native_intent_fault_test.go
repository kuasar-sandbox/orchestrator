package registry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metastore"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/sqlengine"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/storagetest"
	"github.com/kuasar-sandbox/orchestrator/internal/testutil/metadatastore"
)

var registryNativeBackends = map[string]storagetest.Factory{
	"sqlite": func(t *testing.T) func() storagetest.Backend {
		path := filepath.Join(t.TempDir(), "registry.sqlite")
		return func() storagetest.Backend {
			e, err := sqlengine.OpenSQLite(t.Context(), path, "registry-faults")
			if err != nil {
				t.Fatal(err)
			}
			return e
		}
	},
	"postgres": func(t *testing.T) func() storagetest.Backend {
		dsn := os.Getenv("STORAGE_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("set STORAGE_TEST_POSTGRES_DSN for real PostgreSQL Registry faults")
		}
		namespace := "registry-faults-" + newID()
		return func() storagetest.Backend {
			e, err := sqlengine.OpenPostgres(t.Context(), dsn, namespace)
			if err != nil {
				t.Fatal(err)
			}
			return e
		}
	},
}

var errNativeIntentAbort = errors.New("fixture: abort after staging the native intent transaction")

// Inject at the physical transaction boundary, not by inventing repository
// results. Rollback runs all real writes then aborts the native transaction;
// Unknown commits those writes and loses only the completion reply.
type nativeIntentFaultEngine struct {
	storage.Engine
	mode              atomic.Int32
	injected          atomic.Int32
	writes            atomic.Int32
	afterCommit       func()
	abortBeforeCommit atomic.Bool
}

type countingIntentTx struct {
	storage.Tx
	writes int32
}

func (t *countingIntentTx) Put(key, value []byte) error { t.writes++; return t.Tx.Put(key, value) }
func (t *countingIntentTx) Delete(key []byte) error     { t.writes++; return t.Tx.Delete(key) }

func (e *nativeIntentFaultEngine) Update(ctx context.Context, fn func(storage.Tx) error) error {
	mode := e.mode.Swap(0)
	if mode == 0 {
		return e.Engine.Update(ctx, fn)
	}
	var writes int32
	err := e.Engine.Update(ctx, func(tx storage.Tx) error {
		counted := &countingIntentTx{Tx: tx}
		if err := fn(counted); err != nil {
			return err
		}
		writes = counted.writes
		if mode == 2 && e.abortBeforeCommit.Swap(false) {
			return storage.ErrRetry
		}
		if mode == 1 {
			return errNativeIntentAbort
		}
		return nil
	})
	if errors.Is(err, storage.ErrRetry) {
		// A definitely aborted native attempt has not reached the requested
		// commit boundary. Keep the fault armed for the repository retry.
		e.mode.Store(mode)
		return err
	}
	if errors.Is(err, errNativeIntentAbort) || (err == nil && mode == 2) {
		e.injected.Add(1)
		e.writes.Add(writes)
	}
	if err == nil && mode == 2 {
		if e.afterCommit != nil {
			e.afterCommit()
		}
		return &metadata.Unknown{Cause: errors.New("fixture: native commit reply lost")}
	}
	return err
}

type captureNativeIntent struct {
	metadata.RouteRepository
	engine *nativeIntentFaultEngine
	mode   int32
	record metadata.SandboxRecord
}

func (f *captureNativeIntent) CommitGroupIntent(ctx context.Context, version metadata.Version, record metadata.SandboxRecord, ref metadata.NodeSandboxRef, admission metadata.GroupAdmission) (metadata.Version, error) {
	f.record = record
	f.engine.mode.Store(f.mode)
	defer f.engine.mode.Store(0)
	return f.RouteRepository.CommitGroupIntent(ctx, version, record, ref, admission)
}

func newNativeFaultRegistry(t *testing.T, open func() storagetest.Backend) (*Registry, *nativeIntentFaultEngine) {
	t.Helper()
	backend := open()
	engine := &nativeIntentFaultEngine{Engine: backend}
	native := metastore.New(engine, backend)
	t.Cleanup(func() {
		if err := native.Close(); err != nil {
			t.Error(err)
		}
	})
	stores, err := NewStores(native.Repositories(), metadatastore.Owners{ID: "registry"})
	if err != nil {
		t.Fatal(err)
	}
	return New(stores, 25*time.Millisecond, nil), engine
}

func TestRegistryNativeIntentFaults(t *testing.T) {
	for backend, factory := range registryNativeBackends {
		t.Run(backend, func(t *testing.T) {
			for _, window := range []string{"F01_native_rollback", "F02_native_commit_reply_lost", "F07_generic_rejection_timeout"} {
				t.Run(window, func(t *testing.T) {
					open := factory(t)
					r, engine := newNativeFaultRegistry(t, open)
					publishTestGroups(t, r, "n1", "/g")
					fault := &captureNativeIntent{RouteRepository: r.stores.routes, engine: engine}
					switch window {
					case "F01_native_rollback":
						fault.mode = 1
					case "F02_native_commit_reply_lost":
						fault.mode = 2
						// Force a real rolled-back attempt before the successful
						// commit so retry scheduling cannot consume the fault.
						engine.abortBeforeCommit.Store(true)
					}
					r.stores.routes = fault
					owner := &groupCommandOwner{}
					owner.call = func(_ context.Context, _ string, cmd *routesync.Command) (*routesync.CmdAck, error) {
						return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, HTTPStatus: 409}, nil
					}
					r.SetNodeOwner(owner)
					_, err := r.ReserveSandbox(t.Context(), testCreateReserve("/g", "native-fault", nil))
					if err == nil {
						t.Fatal("fault unexpectedly succeeded")
					}
					if fault.mode == 1 && !errors.Is(err, errNativeIntentAbort) {
						t.Fatalf("rollback result: %v", err)
					}
					if fault.mode == 2 && !errors.Is(err, metadata.ErrUnknown) {
						t.Fatalf("unknown result: %v", err)
					}
					if fault.mode != 0 && (engine.injected.Load() != 1 || engine.writes.Load() == 0 || len(owner.commands) != 0) {
						t.Fatalf("fault boundary: injections=%d staged=%d commands=%d", engine.injected.Load(), engine.writes.Load(), len(owner.commands))
					}
					if fault.mode == 0 && len(owner.commands) != 1 {
						t.Fatalf("generic rejection dispatched %d times", len(owner.commands))
					}
					// A fresh Registry and a separately opened native connection read
					// the result. No process-local flight or repository cache survives.
					fresh, _ := newNativeFaultRegistry(t, open)
					record, _, found, readErr := fresh.stores.GetSandbox(t.Context(), "/g", "native-fault")
					if readErr != nil || found != (fault.mode == 2) {
						t.Fatalf("durable route: exists=%v err=%v", found, readErr)
					}
					if found && (record.SandboxID != fault.record.SandboxID || record.NodeSandboxID != fault.record.NodeSandboxID || record.CreateTemplateID != fault.record.CreateTemplateID || record.GroupVersion != fault.record.GroupVersion) {
						t.Fatal("Unknown changed the committed identity or input binding")
					}
					owned, readErr := fresh.stores.ownership.ReadSandboxOwner(t.Context(), "n1", fault.record.NodeSandboxID)
					if readErr != nil || owned.Exists != (fault.mode != 1) {
						t.Fatalf("cleanup ownership: exists=%v err=%v", owned.Exists, readErr)
					}
					profile, readErr := fresh.stores.nodes.ReadNodeProfile(t.Context(), "n1")
					if readErr != nil {
						t.Fatal(readErr)
					}
					if err := fresh.stores.groups.ApplyGroupSupport(t.Context(), profile.Registration, 2, nil, "/g"); err != nil {
						t.Fatal(err)
					}
					auth, readErr := fresh.stores.groups.ReadGroupAuth(t.Context(), "/g", testAPIFingerprint)
					if readErr != nil || auth.Exists != (fault.mode != 1) {
						t.Fatalf("retained auth after withdrawal: exists=%v err=%v", auth.Exists, readErr)
					}
					t.Logf("%s: staged native writes=%d, Node commands=%d, durable route=%t, exact ownership=%t, retained auth=%t", window, engine.writes.Load(), len(owner.commands), found, owned.Exists, auth.Exists)
				})
			}
		})
	}
}
