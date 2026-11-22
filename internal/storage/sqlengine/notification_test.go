package sqlengine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

func TestPostgresNotificationLossAndReconnect(t *testing.T) {
	dsn := os.Getenv("STORAGE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL")
	}
	e, err := OpenPostgres(t.Context(), dsn, fmt.Sprintf("notification-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	key := []byte("persistent-change-token")
	before := []byte("one")
	if err := e.Update(t.Context(), func(tx storage.Tx) error { return tx.Put(key, before) }); err != nil {
		t.Fatal(err)
	}
	await := func(o storage.Invalidation) {
		t.Helper()
		select {
		case <-o.Done():
		case <-time.After(5 * time.Second):
			o.Cancel()
			t.Fatal("notification did not invalidate")
		}
	}
	// Find only this namespace's LISTEN session, then cut its real connection.
	query := "LISTEN " + pgx.Identifier{e.owner.channel}.Sanitize()
	deadline := time.Now().Add(5 * time.Second)
	var pid int
	for {
		err = e.owner.db.QueryRowContext(t.Context(), "SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND query=$1 AND pid<>pg_backend_pid()", query).Scan(&pid)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener was not established: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	o, err := e.Observe(t.Context(), []storage.Condition{{Key: key, Value: storage.Value{Exists: true, Bytes: before}}})
	if err != nil {
		t.Fatal(err)
	}
	// Manager admission is synchronous; ensure the poller has attached before
	// cutting continuity, so this tests an active subscription.
	deadline = time.Now().Add(5 * time.Second)
	for e.observations.Active() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("observation did not start")
		}
		time.Sleep(time.Millisecond)
	}
	// A changed value written directly, without NOTIFY or the local wakeup,
	// must still invalidate by inspecting the durable token.
	if _, err := e.owner.db.ExecContext(t.Context(), "UPDATE kuasar_storage_v2 SET value=$1 WHERE namespace=$2 AND key=$3", []byte("two"), e.owner.namespace, key); err != nil {
		t.Fatal(err)
	}
	await(o)
	if err := o.Err(); err != nil {
		t.Fatal(err)
	}
	o.Cancel()
	o, err = e.Observe(t.Context(), []storage.Condition{{Key: key, Value: storage.Value{Exists: true, Bytes: []byte("two")}}})
	if err != nil {
		t.Fatal(err)
	}
	// Ensure the new subscription is attached: the test stays inside the driver
	// package but does not alter production observer behavior or fake a database.
	time.Sleep(150 * time.Millisecond)
	var terminated bool
	if err := e.owner.db.QueryRowContext(t.Context(), "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate our listener: %v %v", terminated, err)
	}
	await(o)
	if !errors.Is(o.Err(), storage.ErrUnavailable) {
		t.Fatalf("lost observation continuity reported success: %v", o.Err())
	}
	o.Cancel()
	// Ordinary transactions remain usable while the listener reconnects.
	if err := e.View(t.Context(), func(tx storage.ReadTx) error {
		v, err := tx.Get(key)
		if err == nil && (!v.Exists || !bytes.Equal(v.Bytes, []byte("two"))) {
			return errors.New("notification failure lost durable state")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		var nextPID int
		err = e.owner.db.QueryRowContext(t.Context(), "SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND query=$1 AND pid<>pg_backend_pid()", query).Scan(&nextPID)
		if err == nil && nextPID != pid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener did not reconnect after termination: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	o, err = e.Observe(t.Context(), []storage.Condition{{Key: key, Value: storage.Value{Exists: true, Bytes: []byte("two")}}})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Cancel()
	if err := e.Update(t.Context(), func(tx storage.Tx) error { return tx.Put(key, []byte("three")) }); err != nil {
		t.Fatal(err)
	}
	await(o)
	if err := o.Err(); err != nil {
		t.Fatalf("observation after reconnect: %v", err)
	}
}
