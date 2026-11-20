package store

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
)

var singleCheckpointConstraint = regexp.MustCompile(`(?i)CHECK\s*\(\s*"?max_checkpoints"?\s*=\s*1\s*\)`)

// The supported v1 schema has a column-local CHECK and a leaf checkpoint
// table. Replace that one column in place instead of rebuilding/deleting the
// sandboxes parent: no cascade or parent DELETE trigger may run on upgrade.
// Both changes are one immediate transaction with foreign_keys left enabled.
func migrateCheckpointSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var parentSQL string
	if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type='table' AND name='sandboxes'`).Scan(&parentSQL); err != nil {
		return err
	}
	if singleCheckpointConstraint.MatchString(parentSQL) {
		for _, statement := range []string{
			`ALTER TABLE sandboxes RENAME COLUMN max_checkpoints TO checkpoint_quota_v1`,
			`ALTER TABLE sandboxes ADD COLUMN max_checkpoints INTEGER NOT NULL DEFAULT 1 CHECK(typeof(max_checkpoints)='integer' AND max_checkpoints>0)`,
			`UPDATE sandboxes SET max_checkpoints=checkpoint_quota_v1`,
			`ALTER TABLE sandboxes DROP COLUMN checkpoint_quota_v1`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("replace checkpoint quota constraint: %w", err)
			}
		}
	}
	var sidPK, idPK int
	if err := tx.QueryRowContext(ctx, `SELECT pk FROM pragma_table_info('sandbox_checkpoints') WHERE name='sandbox_id'`).Scan(&sidPK); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT pk FROM pragma_table_info('sandbox_checkpoints') WHERE name='checkpoint_id'`).Scan(&idPK); err != nil {
		return err
	}
	switch {
	case sidPK == 1 && idPK == 2:
	case sidPK == 1 && idPK == 0:
		// Retain explicit indexes/triggers belonging to the leaf table too.
		rows, err := tx.QueryContext(ctx, `SELECT sql FROM sqlite_schema WHERE tbl_name='sandbox_checkpoints' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name`)
		if err != nil {
			return err
		}
		var dependent []string
		for rows.Next() {
			var statement string
			if err := rows.Scan(&statement); err != nil {
				rows.Close()
				return err
			}
			dependent = append(dependent, statement)
		}
		err = rows.Err()
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		for _, statement := range []string{
			`CREATE TABLE sandbox_checkpoints_v2 (
                sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
                checkpoint_id TEXT NOT NULL, source_kind TEXT NOT NULL, source_ref TEXT NOT NULL, sandbox_ref TEXT NOT NULL,
                PRIMARY KEY(sandbox_id,checkpoint_id))`,
			`INSERT INTO sandbox_checkpoints_v2(sandbox_id,checkpoint_id,source_kind,source_ref,sandbox_ref) SELECT sandbox_id,checkpoint_id,source_kind,source_ref,sandbox_ref FROM sandbox_checkpoints`,
			`DROP TABLE sandbox_checkpoints`,
			`ALTER TABLE sandbox_checkpoints_v2 RENAME TO sandbox_checkpoints`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("migrate checkpoint key: %w", err)
			}
		}
		for _, statement := range dependent {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("restore checkpoint schema dependency: %w", err)
			}
		}
	default:
		return fmt.Errorf("unsupported checkpoint primary key (%d,%d)", sidPK, idPK)
	}
	rows, err := tx.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	invalid := rows.Next()
	err = rows.Err()
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if invalid {
		return fmt.Errorf("checkpoint migration foreign key integrity failure")
	}
	return tx.Commit()
}
