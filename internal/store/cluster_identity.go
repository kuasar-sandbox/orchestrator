package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

var (
	ErrClusterIdentityNotEnrolled = errors.New("store: cluster identity is not enrolled")
	ErrClusterIdentityEnrolled    = errors.New("store: cluster identity is already enrolled")
)

type ClusterIdentity struct {
	NodeID        string
	EnrollmentID  string
	NodeEpoch     uint64
	SessionSeq    uint64
	BootID        string
	DataEndpoint  string
	ResetRequired bool
	UpdatedUnix   int64
}

type ClusterStartIdentity struct {
	ClusterIdentity
	EpochAdvanced bool
	AdvanceReason string
}

// EnrollClusterIdentity creates the one durable identity row. Cluster mode must
// call this explicitly; conductor startup never recreates a missing identity
// from hostname/config because missing durable epoch state requires a new node ID.
func (s *Store) EnrollClusterIdentity(ctx context.Context, nodeID, bootID, dataEndpoint string) (ClusterIdentity, error) {
	if err := validateClusterIdentityInput(nodeID, bootID, dataEndpoint); err != nil {
		return ClusterIdentity{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return ClusterIdentity{}, fmt.Errorf("store: begin cluster identity enrollment: %w", err)
	}
	defer tx.Rollback()
	if _, err := readClusterIdentity(tx.QueryRowContext(ctx, `
SELECT node_id,enrollment_id,node_epoch,session_seq,boot_id,data_endpoint,reset_required,updated_unix
FROM cluster_identity WHERE singleton=1`)); err == nil {
		return ClusterIdentity{}, ErrClusterIdentityEnrolled
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ClusterIdentity{}, err
	}

	identity := ClusterIdentity{
		NodeID:       nodeID,
		EnrollmentID: uuid.NewString(),
		NodeEpoch:    1,
		SessionSeq:   0,
		BootID:       bootID,
		DataEndpoint: dataEndpoint,
		UpdatedUnix:  time.Now().Unix(),
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO cluster_identity
  (singleton,node_id,enrollment_id,node_epoch,session_seq,boot_id,data_endpoint,reset_required,updated_unix)
VALUES (1,?,?,?,?,?,?,?,?)`,
		identity.NodeID, identity.EnrollmentID, encodeUint64(identity.NodeEpoch),
		encodeUint64(identity.SessionSeq), identity.BootID, identity.DataEndpoint,
		0, identity.UpdatedUnix); err != nil {
		return ClusterIdentity{}, fmt.Errorf("store: enroll cluster identity: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ClusterIdentity{}, fmt.Errorf("store: commit cluster identity enrollment: %w", err)
	}
	return identity, nil
}

func (s *Store) GetClusterIdentity(ctx context.Context) (ClusterIdentity, error) {
	identity, err := readClusterIdentity(s.db.QueryRowContext(ctx, `
SELECT node_id,enrollment_id,node_epoch,session_seq,boot_id,data_endpoint,reset_required,updated_unix
FROM cluster_identity WHERE singleton=1`))
	if errors.Is(err, sql.ErrNoRows) {
		return ClusterIdentity{}, ErrClusterIdentityNotEnrolled
	}
	return identity, err
}

// PrepareClusterStart validates the enrolled identity and advances NodeEpoch
// when a new host boot or data endpoint requires it. The increment and reset
// gate are committed before the caller fences old executions, so Registry cannot
// accept the new epoch until CompleteClusterEpochReset succeeds.
func (s *Store) PrepareClusterStart(
	ctx context.Context,
	expectedNodeID, bootID, dataEndpoint string,
) (ClusterStartIdentity, error) {
	if bootID == "" || dataEndpoint == "" {
		return ClusterStartIdentity{}, errors.New("store: boot ID and data endpoint are required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return ClusterStartIdentity{}, fmt.Errorf("store: begin cluster identity start: %w", err)
	}
	defer tx.Rollback()
	identity, err := readClusterIdentity(tx.QueryRowContext(ctx, `
SELECT node_id,enrollment_id,node_epoch,session_seq,boot_id,data_endpoint,reset_required,updated_unix
FROM cluster_identity WHERE singleton=1`))
	if errors.Is(err, sql.ErrNoRows) {
		return ClusterStartIdentity{}, ErrClusterIdentityNotEnrolled
	}
	if err != nil {
		return ClusterStartIdentity{}, err
	}
	if expectedNodeID != "" && expectedNodeID != identity.NodeID {
		return ClusterStartIdentity{}, fmt.Errorf("store: configured node ID %q does not match enrolled identity %q", expectedNodeID, identity.NodeID)
	}

	bootChanged := identity.BootID != bootID
	endpointChanged := identity.DataEndpoint != dataEndpoint
	result := ClusterStartIdentity{ClusterIdentity: identity, EpochAdvanced: identity.ResetRequired}
	if identity.ResetRequired {
		result.AdvanceReason = "pending_local_execution_reset"
	}
	if !bootChanged && !endpointChanged {
		if err := tx.Commit(); err != nil {
			return ClusterStartIdentity{}, fmt.Errorf("store: commit cluster identity read: %w", err)
		}
		return result, nil
	}
	if identity.NodeEpoch == math.MaxUint64 {
		return ClusterStartIdentity{}, errors.New("store: node epoch exhausted")
	}
	identity.NodeEpoch++
	identity.SessionSeq = 0
	identity.BootID = bootID
	identity.DataEndpoint = dataEndpoint
	identity.UpdatedUnix = time.Now().Unix()
	identity.ResetRequired = true
	result.ClusterIdentity = identity
	result.EpochAdvanced = true
	switch {
	case bootChanged && endpointChanged:
		result.AdvanceReason = "host_reboot_and_data_endpoint_change"
	case bootChanged:
		result.AdvanceReason = "host_reboot"
	default:
		result.AdvanceReason = "data_endpoint_change"
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE cluster_identity
SET node_epoch=?,session_seq=?,boot_id=?,data_endpoint=?,reset_required=1,updated_unix=?
WHERE singleton=1`, encodeUint64(identity.NodeEpoch), encodeUint64(0), bootID,
		dataEndpoint, identity.UpdatedUnix); err != nil {
		return ClusterStartIdentity{}, fmt.Errorf("store: advance node epoch: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ClusterStartIdentity{}, fmt.Errorf("store: commit node epoch: %w", err)
	}
	return result, nil
}

func (s *Store) CompleteClusterEpochReset(ctx context.Context, expectedEpoch uint64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE cluster_identity
SET reset_required=0,updated_unix=? WHERE singleton=1 AND node_epoch=? AND reset_required=1`,
		time.Now().Unix(), encodeUint64(expectedEpoch))
	if err != nil {
		return fmt.Errorf("store: complete cluster epoch reset: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		identity, getErr := s.GetClusterIdentity(ctx)
		if getErr != nil {
			return getErr
		}
		if identity.NodeEpoch != expectedEpoch {
			return errors.New("store: cluster epoch changed before reset completion")
		}
	}
	return nil
}

// NextClusterSession increments and durably commits SessionSeq before a node-link
// connection attempt. expectedEpoch prevents an old process from advancing a
// newer identity row.
func (s *Store) NextClusterSession(ctx context.Context, expectedEpoch uint64) (ClusterIdentity, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return ClusterIdentity{}, fmt.Errorf("store: begin session sequence: %w", err)
	}
	defer tx.Rollback()
	identity, err := readClusterIdentity(tx.QueryRowContext(ctx, `
SELECT node_id,enrollment_id,node_epoch,session_seq,boot_id,data_endpoint,reset_required,updated_unix
FROM cluster_identity WHERE singleton=1`))
	if errors.Is(err, sql.ErrNoRows) {
		return ClusterIdentity{}, ErrClusterIdentityNotEnrolled
	}
	if err != nil {
		return ClusterIdentity{}, err
	}
	if identity.NodeEpoch != expectedEpoch {
		return ClusterIdentity{}, fmt.Errorf("store: node epoch changed from %d to %d", expectedEpoch, identity.NodeEpoch)
	}
	if identity.ResetRequired {
		return ClusterIdentity{}, errors.New("store: node epoch reset is not complete")
	}
	if identity.SessionSeq == math.MaxUint64 {
		return ClusterIdentity{}, errors.New("store: session sequence exhausted")
	}
	identity.SessionSeq++
	identity.UpdatedUnix = time.Now().Unix()
	if _, err := tx.ExecContext(ctx, `
UPDATE cluster_identity SET session_seq=?,updated_unix=? WHERE singleton=1`,
		encodeUint64(identity.SessionSeq), identity.UpdatedUnix); err != nil {
		return ClusterIdentity{}, fmt.Errorf("store: increment session sequence: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ClusterIdentity{}, fmt.Errorf("store: commit session sequence: %w", err)
	}
	return identity, nil
}

func validateClusterIdentityInput(nodeID, bootID, dataEndpoint string) error {
	switch {
	case nodeID == "":
		return errors.New("store: node ID is required")
	case len(nodeID) > clusterstate.MaxExecutionBindingNodeIDSize:
		return fmt.Errorf("store: node ID exceeds %d bytes", clusterstate.MaxExecutionBindingNodeIDSize)
	case bootID == "":
		return errors.New("store: boot ID is required")
	case dataEndpoint == "":
		return errors.New("store: data endpoint is required")
	default:
		return nil
	}
}

type rowScanner interface {
	Scan(...any) error
}

func readClusterIdentity(row rowScanner) (ClusterIdentity, error) {
	var identity ClusterIdentity
	var epoch, session []byte
	var resetRequired int
	if err := row.Scan(
		&identity.NodeID,
		&identity.EnrollmentID,
		&epoch,
		&session,
		&identity.BootID,
		&identity.DataEndpoint,
		&resetRequired,
		&identity.UpdatedUnix,
	); err != nil {
		return ClusterIdentity{}, err
	}
	identity.ResetRequired = resetRequired != 0
	var err error
	if identity.NodeEpoch, err = decodeUint64(epoch); err != nil {
		return ClusterIdentity{}, fmt.Errorf("store: decode node epoch: %w", err)
	}
	if identity.SessionSeq, err = decodeUint64(session); err != nil {
		return ClusterIdentity{}, fmt.Errorf("store: decode session sequence: %w", err)
	}
	return identity, nil
}

func encodeUint64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func decodeUint64(encoded []byte) (uint64, error) {
	if len(encoded) != 8 {
		return 0, fmt.Errorf("invalid uint64 length %d", len(encoded))
	}
	return binary.BigEndian.Uint64(encoded), nil
}
