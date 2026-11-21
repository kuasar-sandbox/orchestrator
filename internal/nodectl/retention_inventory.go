package nodectl

import (
	"bytes"
	"context"
	"crypto/sha256"
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

func validateRetainedReceipt(r *retainedAccount) error {
	if r == nil || r.Version != 2 || r.SandboxID == "" || r.ID == "" || r.Revision == 0 {
		return fmt.Errorf("invalid retained account")
	}
	if err := r.OwnerLease.Validate(); err != nil {
		return err
	}
	if r.OwnerLease.SandboxID != r.SandboxID || r.OwnerFile.Inode == 0 {
		return fmt.Errorf("invalid retained owner identity")
	}
	switch r.Stage {
	case retentionHolding, retentionAdopting, retentionActive:
	default:
		return fmt.Errorf("unsupported retained account stage %q", r.Stage)
	}
	if r.Stage == retentionAdopting && r.Handoff == nil {
		return fmt.Errorf("missing retained handoff intent")
	}
	if h := r.Handoff; h != nil {
		if h.ID == "" || h.AccountRevision == 0 || h.AccountRevision > r.Revision || h.Token == "" || h.File.Inode == 0 || h.Pool == "" ||
			h.InitialBudget == 0 || h.InitialBudget > h.Lease.CapacityMemory || h.Lease.SandboxID != r.SandboxID {
			return fmt.Errorf("invalid retained handoff intent")
		}
		if err := h.Lease.Validate(); err != nil {
			return err
		}
	}
	prepared := 0
	keys := make(map[resource.CaptureRetention]bool)
	paths := make(map[string]bool)
	ids := make(map[string]bool)
	for _, e := range r.Captures {
		if e == nil || e.Source.SandboxID != r.SandboxID || e.Producer.SandboxID != r.SandboxID || e.CheckpointID == "" ||
			e.Holder.PID != e.Source.PeerPID || e.Holder.StartTime != e.SourceStartTime || e.Holder.SocketPath == "" ||
			e.Memory.MemoryID != e.Capture.MemoryID || e.Memory.Epoch != e.Capture.Epoch || e.Memory.Bytes == 0 ||
			e.Memory.Bytes != e.Source.ReservationMemory || e.Memory.SourcePool == "" || e.SourceStartTime == 0 || e.SourceFile.Inode == 0 {
			return fmt.Errorf("invalid retained capture")
		}
		if err := e.Capture.Validate(); err != nil {
			return err
		}
		key := fmt.Sprintf("%s:%d", e.Capture.MemoryID, e.Capture.Epoch)
		if keys[e.Capture] || paths[e.Capture.CapturePath] || ids[key] {
			return fmt.Errorf("duplicate retained capture")
		}
		keys[e.Capture], paths[e.Capture.CapturePath], ids[key] = true, true, true
		switch e.Stage {
		case retentionPrepared:
			prepared++
		case retentionHeld, retentionSettled:
		default:
			return fmt.Errorf("unsupported capture stage %q", e.Stage)
		}
	}
	if prepared > 1 || prepared != 0 && r.Stage != retentionActive {
		return fmt.Errorf("invalid prepared Budget owner")
	}
	return nil
}

// Durable account files are local inventory, not resource wire frames. The
// 64 KiB transport limit must not become an accidental holder-count limit.
func writeRetainedReceipt(socket string, r *retainedAccount) error {
	if err := validateRetainedReceipt(r); err != nil {
		return err
	}
	dir := retentionDir(socket)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return err
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

// This shape exists only for upgrading the previous single-capture inventory.
// Runtime mutation and new persistence always use a version-2 account.
type legacyRetainedReceipt struct {
	Version         int                             `json:"version"`
	SandboxID       string                          `json:"sandbox_id"`
	Capture         resource.CaptureRetention       `json:"capture"`
	Producer        memorycheckpoint.RunIdentity    `json:"producer"`
	CheckpointID    string                          `json:"checkpoint_id"`
	Holder          memorycheckpoint.HolderIdentity `json:"holder"`
	Memory          resource.RetainedMemory         `json:"memory"`
	Stage           string                          `json:"stage"`
	Settled         bool                            `json:"settled,omitempty"`
	Source          Reservation                     `json:"source"`
	SourceStartTime uint64                          `json:"source_start_time"`
	SourceFile      resource.LeaseFileIdentity      `json:"source_file"`
	OwnerLease      resource.Lease                  `json:"owner_lease"`
	OwnerFile       resource.LeaseFileIdentity      `json:"owner_file"`
	OwnerToken      string                          `json:"owner_token"`
	Handoff         *retainedHandoff                `json:"handoff,omitempty"`
}

func strictRetentionJSON(payload []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing retention account data")
	}
	return nil
}

func readRetainedReceipt(path string) (*retainedAccount, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var version struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(payload, &version); err != nil {
		return nil, err
	}
	var r retainedAccount
	switch version.Version {
	case 1:
		var legacy legacyRetainedReceipt
		if err := strictRetentionJSON(payload, &legacy); err != nil {
			return nil, err
		}
		stage := retentionHeld
		if legacy.Stage == retentionPrepared {
			stage = retentionPrepared
		}
		if legacy.Settled {
			stage = retentionSettled
		}
		accountStage := legacy.Stage
		if accountStage == retentionPrepared {
			accountStage = retentionActive
			if legacy.Settled {
				accountStage = retentionHolding
			}
		}
		sum := sha256.Sum256([]byte(legacy.SandboxID + "\x00" + legacy.Capture.CapturePath + "\x00" + legacy.Capture.MemoryID))
		r = retainedAccount{Version: 2, SandboxID: legacy.SandboxID, ID: fmt.Sprintf("%x", sum[:]), Revision: 1, Stage: accountStage,
			OwnerLease: legacy.OwnerLease, OwnerFile: legacy.OwnerFile, OwnerToken: legacy.OwnerToken, Handoff: legacy.Handoff,
			Captures: []*retainedCapture{{Capture: legacy.Capture, Producer: legacy.Producer, CheckpointID: legacy.CheckpointID,
				Holder: legacy.Holder, Memory: legacy.Memory, Stage: stage, Source: legacy.Source, SourceStartTime: legacy.SourceStartTime, SourceFile: legacy.SourceFile}}}
		if r.Handoff != nil {
			r.Handoff.AccountRevision = 1
		}
	case 2:
		if err := strictRetentionJSON(payload, &r); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported retained inventory version %d", version.Version)
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

func validateRetentionProof(r *retainedCapture, proof *memorycheckpoint.CaptureRecord) error {
	if proof.Producer != r.Producer || proof.CheckpointID != r.CheckpointID || proof.Holder != r.Holder {
		return fmt.Errorf("capture producer or holder identity changed")
	}
	return nil
}

// Validate the durable receipt before asking the SDK to publish a physical
// release fact. The same nonblocking proof is used at startup and periodically;
// no resource mutex waits for a producer or borrowed RAM reader to exit.
func recoverRetentionCapture(r *retainedCapture) (*memorycheckpoint.CaptureRecord, error) {
	proof, err := readRetentionCapture(r.Producer.SandboxID, r.Capture)
	if err != nil {
		return nil, err
	}
	if err := validateRetentionProof(r, proof); err != nil {
		return nil, err
	}
	if proof.Committed && !proof.RuntimeRetired || proof.RAMReleased || proof.Aborted || proof.Discarded {
		return proof, nil
	}
	recovered, err := memorycheckpoint.RecoverMemoryRelease(context.Background(), r.Capture.CapturePath, *proof)
	if err != nil {
		return nil, err
	}
	proof = &recovered.Record
	if err := validateRetentionCapture(r.Producer.SandboxID, r.Capture, proof); err != nil {
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

// Recover every capture before scanning any ordinary lease/process/cgroup.
// Settled entries remain zero-charge identity evidence until SID retirement.
func (i *Inventory) recoverRetained(state *State, controlPIDs map[int]bool) error {
	dir := retentionDir(i.ControllerSocket)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		r, err := readRetainedReceipt(filepath.Join(dir, entry.Name()))
		if err != nil {
			return fmt.Errorf("retention inventory %s: %w", entry.Name(), err)
		}
		if entry.Name() != resource.LeaseFilename(r.SandboxID) || r.OwnerLease.ControllerSocket != i.ControllerSocket {
			return fmt.Errorf("retention inventory identity mismatch")
		}
		var keep []*retainedCapture
		for _, e := range r.Captures {
			if e.Stage != retentionSettled {
				proof, err := recoverRetentionCapture(e)
				if err == nil {
					if proof.Aborted && e.Stage == retentionPrepared {
						r.Revision++
						continue
					}
					if proof.RAMReleased {
						if e.Stage == retentionPrepared {
							r.Stage, r.Handoff = retentionHolding, nil
						}
						e.Stage = retentionSettled
						r.Revision++
					} else if proof.Committed && proof.RuntimeRetired && e.Stage == retentionPrepared {
						e.Stage, r.Stage, r.Handoff = retentionHeld, retentionHolding, nil
						r.Revision++
					}
				} else if i.Logf != nil {
					i.Logf("retaining uncertain checkpoint memory sid=%s memory=%s: %v", r.SandboxID, e.Memory.MemoryID, err)
				}
			}
			keep = append(keep, e)
		}
		r.Captures = keep
		r = adoptionRecovery(i.ControllerSocket, r)
		// Rewrite here also durably completes a legacy inventory upgrade.
		if err := writeRetainedReceipt(i.ControllerSocket, r); err != nil {
			return err
		}
		if err := state.restoreRetained(r); err != nil {
			return err
		}
		if err := i.recoverRetainedCandidate(state, r, controlPIDs); err != nil {
			return err
		}
		for _, e := range r.Captures {
			if start, err := retentionProcessStartTime(e.Source.PeerPID); err == nil && start == e.SourceStartTime {
				controlPIDs[e.Source.PeerPID] = true
			}
		}
	}
	return nil
}

func (s *State) retainedSourceLease(sid string, pid int, cgroup string) bool {
	r := s.retainedSnapshot(sid)
	if r == nil {
		return false
	}
	start, err := retentionProcessStartTime(pid)
	if err != nil {
		return false
	}
	for _, e := range r.Captures {
		if e.Source.PeerPID != pid || e.Source.CgroupPath != cgroup || e.SourceStartTime != start {
			continue
		}
		_, owner, file, err := resource.InspectLeaseIdentity(e.Source.LeasePath)
		if err == nil && owner == pid && file == e.SourceFile {
			return true
		}
	}
	return false
}

func (i *Inventory) ReconcileRetained(state *State) error { return i.reconcileRetained(state, "") }

func (s *State) ReconcileRetainedMemory(sid string) error {
	r := s.retainedSnapshot(sid)
	if r == nil {
		return nil
	}
	i := &Inventory{ControllerSocket: r.OwnerLease.ControllerSocket}
	return i.reconcileRetained(s, sid)
}

func withoutRetainedCapture(r *retainedAccount, c resource.CaptureRetention) *retainedAccount {
	next := cloneRetained(r)
	next.Captures = next.Captures[:0]
	for _, e := range r.Captures {
		if !sameCapture(e.Capture, c) {
			next.Captures = append(next.Captures, cloneCapture(e))
		}
	}
	next.Revision++
	return next
}

func settledRetainedCapture(r *retainedAccount, c resource.CaptureRetention) *retainedAccount {
	next := cloneRetained(r)
	e := next.capture(c)
	if e.Stage == retentionPrepared {
		next.Stage, next.Handoff = retentionHolding, nil
	}
	e.Stage = retentionSettled
	next.Revision++
	return next
}

func heldRetainedCapture(r *retainedAccount, c resource.CaptureRetention) *retainedAccount {
	next := cloneRetained(r)
	next.capture(c).Stage = retentionHeld
	next.Stage, next.Handoff = retentionHolding, nil
	next.Revision++
	return next
}

// EOF or FileReady never settles RAM. Physical facts are validated per entry;
// an owner handoff remains independent, including after the last R is settled.
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
		for _, e := range r.Captures {
			if e.Stage == retentionSettled {
				continue
			}
			proof, err := recoverRetentionCapture(e)
			if err != nil {
				if i.Logf != nil {
					i.Logf("retaining uncertain checkpoint memory sid=%s memory=%s: %v", r.SandboxID, e.Memory.MemoryID, err)
				}
				continue
			}
			var next *retainedAccount
			switch {
			case proof.Aborted && e.Stage == retentionPrepared:
				next = withoutRetainedCapture(r, e.Capture)
				if err = writeRetainedReceipt(i.ControllerSocket, next); err == nil {
					err = state.abortRetained(next, e.Capture)
				}
			case proof.RAMReleased:
				next = settledRetainedCapture(r, e.Capture)
				if err = writeRetainedReceipt(i.ControllerSocket, next); err == nil {
					err = state.releaseRetained(next, e.Capture)
				}
			case e.Stage == retentionPrepared && proof.Committed && proof.RuntimeRetired:
				next = heldRetainedCapture(r, e.Capture)
				if err = writeRetainedReceipt(i.ControllerSocket, next); err == nil {
					_, err = state.commitRetained(next, e.Capture)
				}
			}
			if err != nil {
				return err
			}
			if next != nil {
				r = next
			}
		}
	}
	return nil
}

// A static capture has no node receipt. Fallback is legal only after checking
// the complete local inventory; a released hint cannot hide another pending R.
func releasedRetentionInventory(sid string, c resource.CaptureRetention, proof *memorycheckpoint.CaptureRecord) error {
	dir := filepath.Dir(c.CapturePath)
	if filepath.Base(dir) != "memory" {
		dir = filepath.Dir(dir)
	}
	if filepath.Base(dir) != "memory" {
		return fmt.Errorf("invalid capture inventory directory")
	}
	dir = filepath.Dir(dir)
	path, err := memorycheckpoint.RecordPath(dir, proof.CheckpointID, proof.MemoryID)
	if err != nil || path != c.CapturePath {
		return fmt.Errorf("capture inventory identity changed")
	}
	records, err := memorycheckpoint.ListRecords(dir)
	if err != nil {
		return err
	}
	for _, r := range records {
		if r.Producer.SandboxID != sid {
			return fmt.Errorf("capture inventory SID changed")
		}
		if !r.Aborted && !r.RAMReleased {
			return fmt.Errorf("checkpoint memory has no transferable node account")
		}
	}
	return nil
}

// RetireRetainedMemory belongs to the SID's terminal finalizer, after its
// managed execution paths have been removed under the lifecycle/source fence.
// Ordinary R settlement never calls this: zero-charge identities and an empty
// account must survive while old leases/cgroups or a successor are discoverable.
func (s *State) RetireRetainedMemory(sid string) error {
	root := s.retentionOwner()
	root.retentionOps.Lock()
	defer root.retentionOps.Unlock()
	r := s.retainedSnapshot(sid)
	if r == nil {
		return nil
	}
	if r.pending() || r.Stage == retentionAdopting {
		return fmt.Errorf("retained account still owns memory or a handoff")
	}
	if _, active := s.retentionReservation(sid); active {
		return fmt.Errorf("retained account still has an active Budget")
	}
	canonical, err := observeRetainedLease(resource.LeasePath(r.OwnerLease.ControllerSocket, sid))
	if err != nil {
		return err
	}
	if canonical.locked {
		return fmt.Errorf("retained owner lease is still discoverable")
	}
	for _, e := range r.Captures {
		lease, err := observeRetainedLease(e.Source.LeasePath)
		if err != nil {
			return err
		}
		if lease.locked {
			return fmt.Errorf("retained source lease is still discoverable")
		}
		populated, err := readCgroupPopulated(e.Source.CgroupPath)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if populated {
			return fmt.Errorf("retained source cgroup is still populated")
		}
	}
	if err := removeRetainedReceipt(r.OwnerLease.ControllerSocket, sid); err != nil {
		return err
	}
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	current := root.retained[sid]
	if current == nil || current.ID != r.ID || current.Revision != r.Revision {
		return fmt.Errorf("retained account changed during terminal retirement")
	}
	delete(root.retained, sid)
	return nil
}
