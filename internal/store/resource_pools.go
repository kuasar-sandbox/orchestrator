package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

type RecoveryPlacement struct {
	SourcePool      string `json:"source_pool"`
	DestinationPool string `json:"destination_pool,omitempty"`
	CapacityMemory  uint64 `json:"capacity_memory"`
	CapacityCPU     uint64 `json:"capacity_cpu"`
	Budget          uint64 `json:"budget"`
}

func (s *Store) RecoveryPlacement(ctx context.Context, sid string) (RecoveryPlacement, error) {
	var raw string
	var p RecoveryPlacement
	err := s.db.QueryRowContext(ctx, `SELECT placement FROM resource_recovery_placement WHERE sandbox_id=?`, sid).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	err = json.Unmarshal([]byte(raw), &p)
	return p, err
}
func (s *Store) DeleteRecoveryPlacement(ctx context.Context, sid string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM resource_recovery_placement WHERE sandbox_id=?`, sid)
	return err
}
func (s *Store) LoadPoolPressure(ctx context.Context, name string) (NodePressure, error) {
	var raw string
	var p NodePressure
	err := s.db.QueryRowContext(ctx, `SELECT record FROM resource_pool_pressure WHERE name=?`, name).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	err = json.Unmarshal([]byte(raw), &p)
	return p, err
}
func (s *Store) SavePoolPressure(ctx context.Context, name string, p NodePressure) error {
	switch p.Zone {
	case "green", "yellow", "red", "critical":
	default:
		return fmt.Errorf("invalid pressure zone %q", p.Zone)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO resource_pool_pressure(name,record) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET record=excluded.record WHERE json_extract(excluded.record,'$.Version')>=json_extract(resource_pool_pressure.record,'$.Version')`, name, string(raw))
	return err
}
