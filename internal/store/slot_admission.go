package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"github.com/google/uuid"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

func (s *Store) ConfigureSandboxSlotAdmission(capacity uint64, queueLimit int) error {
	if capacity == 0 || queueLimit <= 0 {
		return errors.New("store: positive Sandbox slot capacity and queue limit are required")
	}
	s.slotMu.Lock()
	defer s.slotMu.Unlock()
	if s.slotCapacity != 0 && (s.slotCapacity != capacity || s.slotQueueLimit != queueLimit) {
		return errors.New("store: Sandbox slot Admission is already configured differently")
	}
	s.slotCapacity = capacity
	s.slotQueueLimit = queueLimit
	return nil
}

func (s *Store) sandboxSlotPolicy() (uint64, int, error) {
	s.slotMu.RLock()
	defer s.slotMu.RUnlock()
	if s.slotCapacity == 0 || s.slotQueueLimit <= 0 {
		return 0, 0, errors.New("store: Sandbox slot Admission is not configured")
	}
	return s.slotCapacity, s.slotQueueLimit, nil
}

func validateSlotAdmissionInput(sandboxID, demandDigest string) error {
	if sandboxID == "" {
		return errors.New("store: Sandbox ID is required")
	}
	digest, err := hex.DecodeString(demandDigest)
	if err != nil || len(digest) != sha256.Size {
		return errors.New("store: demand digest must be SHA-256 hex")
	}
	return nil
}

type slotAdmissionRecord struct {
	result   nodectl.PreparedAdmissionResult
	slots    uint64
	sequence uint64
}

func scanSlotAdmission(row interface{ Scan(...any) error }) (slotAdmissionRecord, error) {
	var record slotAdmissionRecord
	var slots, sequence []byte
	err := row.Scan(
		&record.result.SandboxID, &record.result.DemandDigest, &slots,
		&record.result.State, &record.result.ReservationToken, &sequence,
		&record.result.Reason,
	)
	if err != nil {
		return slotAdmissionRecord{}, err
	}
	if record.slots, err = decodeUint64(slots); err != nil {
		return slotAdmissionRecord{}, err
	}
	if record.sequence, err = decodeUint64(sequence); err != nil {
		return slotAdmissionRecord{}, err
	}
	return record, nil
}

const slotAdmissionColumns = `sandbox_id,demand_digest,slot_units,state,reservation_token,queue_sequence,reason`

func (s *Store) GetAdmission(sandboxID, demandDigest string) (nodectl.PreparedAdmissionResult, error) {
	if err := validateSlotAdmissionInput(sandboxID, demandDigest); err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	record, err := scanSlotAdmission(s.db.QueryRowContext(context.Background(),
		`SELECT `+slotAdmissionColumns+` FROM sandbox_slot_admissions WHERE sandbox_id=?`, sandboxID))
	if errors.Is(err, sql.ErrNoRows) {
		return nodectl.PreparedAdmissionResult{}, nodectl.ErrPreparedAdmissionMissing
	}
	if err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	if record.result.DemandDigest != demandDigest {
		return nodectl.PreparedAdmissionResult{}, nodectl.ErrPreparedAdmissionConflict
	}
	return record.result, nil
}

func (s *Store) PrepareAdmission(
	sandboxID, demandDigest string,
	demand nodectl.SandboxAdmissionDemand,
) (nodectl.PreparedAdmissionResult, error) {
	if err := validateSlotAdmissionInput(sandboxID, demandDigest); err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	if demand.SlotUnits == 0 {
		return nodectl.PreparedAdmissionResult{}, errors.New("store: Sandbox slot demand is required")
	}
	capacity, queueLimit, err := s.sandboxSlotPolicy()
	if err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	defer tx.Rollback()
	existing, err := scanSlotAdmission(tx.QueryRowContext(ctx,
		`SELECT `+slotAdmissionColumns+` FROM sandbox_slot_admissions WHERE sandbox_id=?`, sandboxID))
	if err == nil {
		if existing.result.DemandDigest != demandDigest {
			return nodectl.PreparedAdmissionResult{}, nodectl.ErrPreparedAdmissionConflict
		}
		if err := tx.Commit(); err != nil {
			return nodectl.PreparedAdmissionResult{}, err
		}
		return existing.result, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nodectl.PreparedAdmissionResult{}, err
	}

	active, queued, nextSequence, err := slotAdmissionUsageTx(ctx, tx)
	if err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	result := nodectl.PreparedAdmissionResult{SandboxID: sandboxID, DemandDigest: demandDigest}
	sequence := uint64(0)
	switch {
	case demand.SlotUnits > capacity:
		result.State = nodectl.PreparedRejected
		result.Reason = "exceeds_slot_capacity"
	case queued == 0 && active <= capacity-demand.SlotUnits:
		result.State = nodectl.PreparedAdmitted
		result.ReservationToken = uuid.NewString()
	case queued >= uint64(queueLimit):
		result.State = nodectl.PreparedRejected
		result.Reason = "queue_full"
	case nextSequence == math.MaxUint64:
		return nodectl.PreparedAdmissionResult{}, errors.New("store: Sandbox queue sequence exhausted")
	default:
		result.State = nodectl.PreparedQueued
		result.ReservationToken = uuid.NewString()
		sequence = nextSequence + 1
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sandbox_slot_admissions
(sandbox_id,demand_digest,slot_units,state,reservation_token,queue_sequence,reason,updated_unix)
VALUES (?,?,?,?,?,?,?,?)`, sandboxID, demandDigest, encodeUint64(demand.SlotUnits), result.State,
		result.ReservationToken, encodeUint64(sequence), result.Reason, time.Now().Unix()); err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	s.notifySlotAdmission()
	return result, nil
}

func (s *Store) ClaimAdmission(sandboxID, demandDigest string) (nodectl.PreparedAdmissionResult, error) {
	return s.updateSlotAdmission(sandboxID, demandDigest, func(record *slotAdmissionRecord) error {
		if record.result.State == nodectl.PreparedAdmitted {
			record.result.State = nodectl.PreparedClaimed
		}
		return nil
	})
}

func (s *Store) ReleaseAdmission(sandboxID, demandDigest, reason string) (nodectl.PreparedAdmissionResult, error) {
	result, err := s.updateSlotAdmission(sandboxID, demandDigest, func(record *slotAdmissionRecord) error {
		if record.result.State != nodectl.PreparedReleased {
			record.result.State = nodectl.PreparedReleased
			record.result.Reason = reason
		}
		return nil
	})
	if err == nil {
		s.notifySlotAdmission()
	}
	return result, err
}

func (s *Store) updateSlotAdmission(
	sandboxID, demandDigest string,
	update func(*slotAdmissionRecord) error,
) (nodectl.PreparedAdmissionResult, error) {
	if err := validateSlotAdmissionInput(sandboxID, demandDigest); err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	defer tx.Rollback()
	record, err := scanSlotAdmission(tx.QueryRowContext(ctx,
		`SELECT `+slotAdmissionColumns+` FROM sandbox_slot_admissions WHERE sandbox_id=?`, sandboxID))
	if errors.Is(err, sql.ErrNoRows) {
		return nodectl.PreparedAdmissionResult{}, nodectl.ErrPreparedAdmissionMissing
	}
	if err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	if record.result.DemandDigest != demandDigest {
		return nodectl.PreparedAdmissionResult{}, nodectl.ErrPreparedAdmissionConflict
	}
	if err := update(&record); err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sandbox_slot_admissions
SET state=?,reason=?,updated_unix=? WHERE sandbox_id=?`, record.result.State,
		record.result.Reason, time.Now().Unix(), sandboxID); err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return nodectl.PreparedAdmissionResult{}, err
	}
	return record.result, nil
}

func (s *Store) PromoteQueued() ([]nodectl.PreparedAdmissionResult, error) {
	capacity, _, err := s.sandboxSlotPolicy()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	active, _, _, err := slotAdmissionUsageTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+slotAdmissionColumns+` FROM sandbox_slot_admissions
WHERE state=? ORDER BY queue_sequence,sandbox_id`, nodectl.PreparedQueued)
	if err != nil {
		return nil, err
	}
	var queue []slotAdmissionRecord
	for rows.Next() {
		record, err := scanSlotAdmission(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		queue = append(queue, record)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	changed := make([]nodectl.PreparedAdmissionResult, 0)
	for _, record := range queue {
		if record.slots > capacity || active > capacity-record.slots {
			break
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sandbox_slot_admissions
SET state=?,updated_unix=? WHERE sandbox_id=? AND state=?`, nodectl.PreparedAdmitted,
			time.Now().Unix(), record.result.SandboxID, nodectl.PreparedQueued); err != nil {
			return nil, err
		}
		record.result.State = nodectl.PreparedAdmitted
		active += record.slots
		changed = append(changed, record.result)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if len(changed) > 0 {
		s.notifySlotAdmission()
	}
	return changed, nil
}

func slotAdmissionUsageTx(ctx context.Context, tx *sql.Tx) (active, queued, nextSequence uint64, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT slot_units,state,queue_sequence FROM sandbox_slot_admissions`)
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var slotsBlob, sequenceBlob []byte
		var state string
		if err := rows.Scan(&slotsBlob, &state, &sequenceBlob); err != nil {
			return 0, 0, 0, err
		}
		slots, err := decodeUint64(slotsBlob)
		if err != nil {
			return 0, 0, 0, err
		}
		sequence, err := decodeUint64(sequenceBlob)
		if err != nil {
			return 0, 0, 0, err
		}
		switch state {
		case nodectl.PreparedAdmitted, nodectl.PreparedClaimed:
			if slots > math.MaxUint64-active {
				return 0, 0, 0, errors.New("store: Sandbox slot usage overflow")
			}
			active += slots
		case nodectl.PreparedQueued:
			queued++
		}
		if sequence > nextSequence {
			nextSequence = sequence
		}
	}
	return active, queued, nextSequence, rows.Err()
}

func (s *Store) FinalizeAdmission(sandboxID, demandDigest string) error {
	if err := validateSlotAdmissionInput(sandboxID, demandDigest); err != nil {
		return err
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	record, err := scanSlotAdmission(tx.QueryRowContext(ctx,
		`SELECT `+slotAdmissionColumns+` FROM sandbox_slot_admissions WHERE sandbox_id=?`, sandboxID))
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	if record.result.DemandDigest != demandDigest {
		return nodectl.ErrPreparedAdmissionConflict
	}
	if record.result.State != nodectl.PreparedRejected && record.result.State != nodectl.PreparedReleased {
		return nodectl.ErrPreparedAdmissionState
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM sandbox_slot_admissions WHERE sandbox_id=?`, sandboxID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Wake() <-chan struct{} { return s.slotWake }

// ReconcileOrphanSandboxAdmissions removes controller records whose matching
// durable Sandbox workflow was never committed before a process crash.
func (s *Store) ReconcileOrphanSandboxAdmissions(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `DELETE FROM sandbox_slot_admissions
WHERE NOT EXISTS (
  SELECT 1 FROM node_workflows
  WHERE object_kind=? AND object_id=sandbox_slot_admissions.sandbox_id
)`, clusterstate.ExecutionKindSandbox)
	if err != nil {
		return err
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if deleted > 0 {
		s.notifySlotAdmission()
	}
	return nil
}

func (s *Store) SandboxSlotAdmissionUsage(ctx context.Context) (nodectl.PreparedAdmissionUsage, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT slot_units,state FROM sandbox_slot_admissions`)
	if err != nil {
		return nodectl.PreparedAdmissionUsage{}, err
	}
	defer rows.Close()
	var usage nodectl.PreparedAdmissionUsage
	for rows.Next() {
		var slotsBlob []byte
		var state string
		if err := rows.Scan(&slotsBlob, &state); err != nil {
			return nodectl.PreparedAdmissionUsage{}, err
		}
		slots, err := decodeUint64(slotsBlob)
		if err != nil {
			return nodectl.PreparedAdmissionUsage{}, err
		}
		switch state {
		case nodectl.PreparedQueued:
			usage.QueueDepth++
			usage.SlotUsed += slots
		case nodectl.PreparedAdmitted, nodectl.PreparedClaimed:
			usage.AdmittedSlots += slots
			usage.SlotUsed += slots
		}
	}
	return usage, rows.Err()
}

func (s *Store) notifySlotAdmission() {
	select {
	case s.slotWake <- struct{}{}:
	default:
	}
}

var _ interface {
	GetAdmission(string, string) (nodectl.PreparedAdmissionResult, error)
	PrepareAdmission(string, string, nodectl.SandboxAdmissionDemand) (nodectl.PreparedAdmissionResult, error)
	ClaimAdmission(string, string) (nodectl.PreparedAdmissionResult, error)
	ReleaseAdmission(string, string, string) (nodectl.PreparedAdmissionResult, error)
	FinalizeAdmission(string, string) error
	PromoteQueued() ([]nodectl.PreparedAdmissionResult, error)
	Wake() <-chan struct{}
	ReconcileOrphanSandboxAdmissions(context.Context) error
} = (*Store)(nil)
