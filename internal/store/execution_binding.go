package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

// CASExecutionBinding atomically replaces one protected opaque Binding by its
// current digest. A retry that observes the exact replacement is idempotently
// successful. Recovery rebind may change Registry History Generation and Binding
// contents, but never the concrete object or its node identity/epoch.
func (s *Store) CASExecutionBinding(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID, expectedDigest, replacement string,
) (bool, error) {
	if objectID == "" || expectedDigest == "" || replacement == "" {
		return false, errors.New("store: object ID, expected Binding digest, and replacement are required")
	}
	newBinding, err := clusterstate.DecodeExecutionBinding(replacement)
	if err != nil {
		return false, err
	}
	if newBinding.Kind != kind || newBinding.ObjectID != objectID {
		return false, errors.New("store: replacement Binding identifies a different object")
	}
	table, idColumn, err := bindingTable(kind)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, fmt.Errorf("store: begin execution Binding CAS: %w", err)
	}
	defer tx.Rollback()

	var rawMetadata string
	query := "SELECT metadata_json FROM " + table + " WHERE " + idColumn + "=?"
	if err := tx.QueryRowContext(ctx, query, objectID).Scan(&rawMetadata); errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("store: read execution Binding: %w", err)
	}
	metadata := map[string]string{}
	if err := json.Unmarshal([]byte(rawMetadata), &metadata); err != nil {
		return false, fmt.Errorf("store: decode object metadata: %w", err)
	}
	oldBinding, oldOpaque, err := clusterstate.ExecutionBindingFromMetadata(metadata)
	if err != nil {
		return false, err
	}
	if oldBinding.Kind != kind || oldBinding.ObjectID != objectID {
		return false, errors.New("store: current Binding identifies a different object")
	}
	if oldOpaque == replacement {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("store: commit idempotent execution Binding CAS: %w", err)
		}
		return true, nil
	}
	oldDigest, err := clusterstate.ExecutionBindingDigest(oldOpaque)
	if err != nil {
		return false, err
	}
	if oldDigest != expectedDigest {
		return false, nil
	}
	if oldBinding.NodeID != newBinding.NodeID || oldBinding.NodeEpoch != newBinding.NodeEpoch ||
		oldBinding.Group != newBinding.Group || oldBinding.RouteKey != newBinding.RouteKey ||
		oldBinding.DemandDigest != newBinding.DemandDigest ||
		oldBinding.DispatchSpecDigest != newBinding.DispatchSpecDigest {
		return false, errors.New("store: replacement Binding changes immutable execution identity")
	}
	metadata[clusterstate.ObjectMetadataKey] = replacement
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return false, fmt.Errorf("store: encode object metadata: %w", err)
	}
	update := "UPDATE " + table + " SET metadata_json=? WHERE " + idColumn + "=? AND metadata_json=?"
	result, err := tx.ExecContext(ctx, update, string(encoded), objectID, rawMetadata)
	if err != nil {
		return false, fmt.Errorf("store: replace execution Binding: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: execution Binding CAS result: %w", err)
	}
	if changed != 1 {
		return false, nil
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit execution Binding CAS: %w", err)
	}
	return true, nil
}

func bindingTable(kind clusterstate.ExecutionKind) (table, idColumn string, err error) {
	switch kind {
	case clusterstate.ExecutionKindSandbox:
		return "sandboxes", "id", nil
	case clusterstate.ExecutionKindBuild:
		return "builds", "build_id", nil
	default:
		return "", "", fmt.Errorf("store: unsupported execution kind %d", kind)
	}
}
