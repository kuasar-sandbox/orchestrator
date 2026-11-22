package sqlengine

import (
	"context"
	"database/sql"
	"errors"
)

func initializeSchema(ctx context.Context, db *sql.DB, postgres bool) error {
	if !postgres {
		_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS kuasar_storage_v2 (namespace BLOB NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY(namespace,key))")
		return err
	}
	var fsync string
	if err := db.QueryRowContext(ctx, "SHOW fsync").Scan(&fsync); err != nil {
		return err
	}
	if fsync != "on" {
		return errors.New("postgres: metadata requires fsync=on")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// CREATE TABLE IF NOT EXISTS alone can race pg_type insertion during cold
	// multi-process startup. This database-scoped lock is used only for schema
	// initialization, never for ordinary metadata transactions.
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", int64(0x4b5353544f524532)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS kuasar_storage_v2 (namespace BYTEA NOT NULL, key BYTEA NOT NULL, value BYTEA NOT NULL, PRIMARY KEY(namespace,key))"); err != nil {
		return err
	}
	return tx.Commit()
}
