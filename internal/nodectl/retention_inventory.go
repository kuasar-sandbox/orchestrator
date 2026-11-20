package nodectl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func retentionDir(socket string) string { return socket + ".retained" }
func retentionPath(socket, sid string) string {
	return filepath.Join(retentionDir(socket), resource.LeaseFilename(sid))
}

func validateRetainedReceipt(r *retainedReceipt) error {
	if r == nil || r.Version != 1 || r.SandboxID == "" || r.Source.SandboxID != r.SandboxID || r.Producer.SandboxID != r.SandboxID || r.CheckpointID == "" ||
		r.Holder.PID != r.Source.PeerPID || r.Holder.StartTime != r.SourceStartTime || r.Holder.SocketPath == "" ||
		r.Memory.MemoryID != r.Capture.MemoryID || r.Memory.Epoch != r.Capture.Epoch || r.Memory.Bytes == 0 ||
		r.Memory.Bytes != r.Source.ReservationMemory || r.Memory.SourcePool == "" || r.SourceStartTime == 0 || r.SourceFile.Inode == 0 {
		return fmt.Errorf("invalid retained memory receipt")
	}
	if err := r.Capture.Validate(); err != nil {
		return err
	}
	if err := r.OwnerLease.Validate(); err != nil {
		return err
	}
	if r.OwnerLease.SandboxID != r.SandboxID || r.OwnerFile.Inode == 0 {
		return fmt.Errorf("invalid retained owner identity")
	}
	switch r.Stage {
	case retentionPrepared, retentionHolding, retentionAdopting, retentionActive:
	default:
		return fmt.Errorf("unsupported retention stage %q", r.Stage)
	}
	if r.Stage == retentionAdopting || r.Stage == retentionActive {
		if r.Handoff == nil || r.Handoff.ID == "" || r.Handoff.Token == "" || r.Handoff.File.Inode == 0 || r.Handoff.Pool == "" ||
			r.Handoff.InitialBudget == 0 || r.Handoff.InitialBudget > r.Handoff.Lease.CapacityMemory {
			return fmt.Errorf("invalid retained handoff intent")
		}
		if err := r.Handoff.Lease.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func writeRetainedReceipt(socket string, r *retainedReceipt) error {
	if err := validateRetainedReceipt(r); err != nil {
		return err
	}
	dir := retentionDir(socket)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(payload) > resource.MaxMessageBytes {
		return fmt.Errorf("retention receipt too large")
	}
	f, err := os.CreateTemp(dir, ".retention-*")
	if err != nil {
		return err
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	if _, err := f.Write(append(payload, '\n')); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), retentionPath(socket, r.SandboxID)); err != nil {
		return err
	}
	return syncRetentionDir(dir)
}

func syncRetentionDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func removeRetainedReceipt(socket, sid string) error {
	if err := os.Remove(retentionPath(socket, sid)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncRetentionDir(retentionDir(socket))
}

func readRetainedReceipt(path string) (*retainedReceipt, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, resource.MaxMessageBytes+1))
	dec.DisallowUnknownFields()
	var r retainedReceipt
	if err := dec.Decode(&r); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("trailing retention receipt data")
	}
	if err := validateRetainedReceipt(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

func readRetentionCapture(sid string, c resource.CaptureRetention) (*memorycheckpoint.CaptureRecord, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	r, err := memorycheckpoint.ReadCaptureRecord(c.CapturePath)
	if err != nil {
		return nil, fmt.Errorf("capture proof: %w", err)
	}
	if err := validateRetentionCapture(sid, c, r); err != nil {
		return nil, err
	}
	return r, nil
}

func validateRetentionCapture(sid string, c resource.CaptureRetention, r *memorycheckpoint.CaptureRecord) error {
	op := r.HostOperationID
	if op == "" {
		op = r.CheckpointID
	}
	if r.Producer.SandboxID != sid || r.MemoryID != c.MemoryID || r.Epoch != c.Epoch || op != c.OperationID {
		return fmt.Errorf("capture proof identity changed")
	}
	return nil
}

func validateRetentionProof(r *retainedReceipt, proof *memorycheckpoint.CaptureRecord) error {
	if proof.Producer != r.Producer || proof.CheckpointID != r.CheckpointID || proof.Holder != r.Holder {
		return fmt.Errorf("capture producer or holder identity changed")
	}
	return nil
}

// Validate the durable receipt before asking the SDK to publish a physical
// release fact. The same nonblocking proof is used at startup and periodically;
// no resource mutex waits for a producer or borrowed RAM reader to exit.
func recoverRetentionCapture(r *retainedReceipt) (*memorycheckpoint.CaptureRecord, error) {
	proof, err := readRetentionCapture(r.SandboxID, r.Capture)
	if err != nil {
		return nil, err
	}
	if err := validateRetentionProof(r, proof); err != nil {
		return nil, err
	}
	if !proof.Committed || !proof.RuntimeRetired || proof.RAMReleased || proof.Aborted || proof.Discarded {
		return proof, nil
	}
	recovered, err := memorycheckpoint.RecoverMemoryRelease(context.Background(), r.Capture.CapturePath, *proof)
	if err != nil {
		return nil, err
	}
	proof = &recovered.Record
	if err := validateRetentionCapture(r.SandboxID, r.Capture, proof); err != nil {
		return nil, err
	}
	if err := validateRetentionProof(r, proof); err != nil {
		return nil, err
	}
	return proof, nil
}

func retentionProcessStartTime(pid int) (uint64, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0, fmt.Errorf("invalid process stat")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) <= 19 {
		return 0, fmt.Errorf("short process stat")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

// recoverRetained precedes every live-lease/managed/cgroup scan. It restores
// the original source charge even when both source and active owner are gone.
func (i *Inventory) recoverRetained(state *State, controlPIDs map[int]bool) error {
	dir := retentionDir(i.ControllerSocket)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		r, err := readRetainedReceipt(filepath.Join(dir, e.Name()))
		if err != nil {
			return fmt.Errorf("retention inventory %s: %w", e.Name(), err)
		}
		if e.Name() != resource.LeaseFilename(r.SandboxID) || r.OwnerLease.ControllerSocket != i.ControllerSocket {
			return fmt.Errorf("retention inventory identity mismatch")
		}
		changed := false
		proof, proofErr := recoverRetentionCapture(r)
		if proofErr == nil {
			if proof.Aborted && r.Stage == retentionPrepared {
				// The original VM still owns its ordinary B. Remove only the
				// capture intent; the normal lease/cgroup scan restores B.
				if err := removeRetainedReceipt(i.ControllerSocket, r.SandboxID); err != nil {
					return err
				}
				continue
			}
			if proof.RAMReleased && !r.Settled {
				r.Settled, changed = true, true
			}
			if proof.Committed && proof.RuntimeRetired && r.Stage == retentionPrepared {
				r.Stage, changed = retentionHolding, true
			}
		} else {
			i.Logf("retaining uncertain checkpoint memory sid=%s: %v", r.SandboxID, proofErr)
		}
		if next := adoptionRecovery(i.ControllerSocket, r); next != r {
			r, changed = next, true
		}
		if changed {
			if err := writeRetainedReceipt(i.ControllerSocket, r); err != nil {
				return err
			}
		}
		if err := state.restoreRetained(r); err != nil {
			return err
		}
		if start, err := retentionProcessStartTime(r.Source.PeerPID); err == nil && start == r.SourceStartTime {
			controlPIDs[r.Source.PeerPID] = true
		}
	}
	return nil
}

func (s *State) retainedSourceLease(sid string, pid int, cgroup string) bool {
	r := s.retainedSnapshot(sid)
	if r == nil || r.Source.PeerPID != pid || r.Source.CgroupPath != cgroup {
		return false
	}
	start, err := retentionProcessStartTime(pid)
	if err != nil || start != r.SourceStartTime {
		return false
	}
	_, owner, file, err := resource.InspectLeaseIdentity(r.Source.LeasePath)
	return err == nil && owner == pid && file == r.SourceFile
}

// ReconcileRetained consumes monotonic capture facts and ends abandoned owner
// admissions. EOF, file readiness or a dead owner never prove source RAM was
// freed. Recovery may publish RAMReleased only after the SDK proves the exact
// producer is gone and the original RAM-pin inode has no remaining references.
func (i *Inventory) ReconcileRetained(state *State) error {
	return i.reconcileRetained(state, "")
}

// ReconcileRetainedMemory lets the lifecycle owner settle an exact positive
// capture fact before deleting its directory. Missing/unknown proof never
// clears a pending obligation; callers recheck RetainedMemory after success.
func (s *State) ReconcileRetainedMemory(sid string) error {
	r := s.retainedSnapshot(sid)
	if r == nil || (r.Settled && r.Stage != retentionAdopting) {
		return nil
	}
	i := &Inventory{ControllerSocket: r.OwnerLease.ControllerSocket}
	return i.reconcileRetained(s, sid)
}

func (i *Inventory) reconcileRetained(state *State, sid string) error {
	root := state.retentionOwner()
	root.retentionOps.Lock()
	defer root.retentionOps.Unlock()
	for _, r := range state.retainedSnapshots() {
		if sid != "" && r.SandboxID != sid {
			continue
		}
		var err error
		r, err = i.reconcileAdoption(state, r)
		if err != nil {
			return err
		}
		if r.Settled {
			continue
		}
		proof, err := recoverRetentionCapture(r)
		if err != nil {
			if i.Logf != nil {
				i.Logf("retaining uncertain checkpoint memory sid=%s: %v", r.SandboxID, err)
			}
			continue
		}
		if proof.Aborted && r.Stage == retentionPrepared {
			if err := removeRetainedReceipt(i.ControllerSocket, r.SandboxID); err != nil {
				return err
			}
			if err := state.abortRetained(r.SandboxID, r.Capture); err != nil {
				return err
			}
		} else if proof.RAMReleased {
			next := cloneRetained(r)
			next.Settled = true
			if err := writeRetainedReceipt(i.ControllerSocket, next); err != nil {
				return err
			}
			if err := state.releaseRetained(r.SandboxID, r.Capture); err != nil {
				return err
			}
		} else if r.Stage == retentionPrepared && proof.Committed && proof.RuntimeRetired {
			next := cloneRetained(r)
			next.Stage = retentionHolding
			if err := writeRetainedReceipt(i.ControllerSocket, next); err != nil {
				return err
			}
			if _, err := state.commitRetained(next); err != nil {
				return err
			}
		}
	}
	return nil
}
