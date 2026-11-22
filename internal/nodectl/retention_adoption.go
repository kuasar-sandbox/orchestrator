package nodectl

import (
	"fmt"
	"os"
	"reflect"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

type retainedLeaseObservation struct {
	exists bool
	file   resource.LeaseFileIdentity
	locked bool
	owner  int
	lease  resource.Lease
}

// Path inspection never opens a lease itself: the resource helpers reuse the
// pinned local handle when an SDK and controller share a PID. An extra close
// on that inode would silently release this process's POSIX lock.
func observeRetainedLease(path string) (retainedLeaseObservation, error) {
	var out retainedLeaseObservation
	before, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	st, ok := before.Sys().(*syscall.Stat_t)
	if !ok || !before.Mode().IsRegular() {
		return out, fmt.Errorf("handoff lease is not a regular file")
	}
	out.exists, out.file = true, resource.LeaseFileIdentity{Device: uint64(st.Dev), Inode: st.Ino}
	out.owner, out.locked, err = resource.LeaseLockOwner(path)
	if err != nil {
		return out, err
	}
	if out.locked {
		var file resource.LeaseFileIdentity
		out.lease, out.owner, file, err = resource.InspectLeaseIdentity(path)
		if err != nil {
			return out, err
		}
		if file != out.file {
			return out, fmt.Errorf("handoff lease changed during inspection")
		}
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		return out, fmt.Errorf("handoff lease changed during inspection")
	}
	return out, nil
}

// adoptionRecovery is called under retentionOps, which also serializes lease
// publication. It distinguishes a published ACK loss from an ended candidate.
// PID exit is one way to end a lease; an embedded SDK may close the candidate
// while its process remains alive. Neither transport EOF nor an inspection
// error proves that the attempt ended.
func adoptionRecovery(socket string, r *retainedAccount) *retainedAccount {
	if r.Stage != retentionAdopting || r.Handoff == nil {
		return r
	}
	h := r.Handoff
	canonical, err := observeRetainedLease(resource.LeasePath(socket, r.SandboxID))
	if err != nil {
		return r
	}
	if canonical.exists && canonical.file == h.File {
		if canonical.locked {
			if canonical.owner != h.Lease.PID || !reflect.DeepEqual(canonical.lease, h.Lease) {
				return r
			}
			if h.StartTime != 0 {
				if start, err := retentionProcessStartTime(h.Lease.PID); err != nil || start != h.StartTime {
					return r
				}
			}
			out := cloneRetained(r)
			out.OwnerLease, out.OwnerFile, out.OwnerToken = h.Lease, h.File, h.Token
			out.Stage = retentionActive
			out.Revision++
			return out
		}
		// The exact published candidate has also ended. No ACK could have
		// preceded active receipt persistence, so its B is still provisional.
	} else if canonical.exists && canonical.file != r.OwnerFile {
		return r // An unrelated inode is unknown, not permission to erase B.
	}
	candidate, err := observeRetainedLease(h.Candidate)
	if err != nil || (candidate.exists && (candidate.file != h.File || candidate.locked)) {
		return r
	}
	// Publication cannot recreate a deleted candidate, and this unique inode
	// cannot be relocked/reused after the owner's lifecycle handle closes.
	out := cloneRetained(r)
	out.Stage, out.Handoff = retentionHolding, nil
	out.Revision++
	return out
}

// reconcileAdoption is independent of source RAM settlement. A completed R
// may still have a failed, unacknowledged successor admission to clean up.
func (i *Inventory) reconcileAdoption(state *State, r *retainedAccount) (*retainedAccount, error) {
	next := adoptionRecovery(i.ControllerSocket, r)
	if next == r {
		return r, nil
	}
	if err := writeRetainedReceipt(i.ControllerSocket, next); err != nil {
		return nil, err
	}
	if next.Stage == retentionActive {
		if err := state.setRetainedReceipt(next); err != nil {
			return nil, err
		}
	} else {
		conn, err := state.resetRetainedAdoption(r, next)
		if err != nil {
			return nil, err
		}
		if conn != nil {
			_ = conn.Close()
		}
	}
	return next, nil
}

// recoverRetainedCandidate runs before ordinary lease and managed inventory.
// An unpublished candidate is a distinct, live execution owner even though its
// canonical lease still belongs to the retired producer. Only the persisted
// handoff's complete identity can authorize recovery of this provisional B.
func (i *Inventory) recoverRetainedCandidate(state *State, r *retainedAccount, controlPIDs map[int]bool) error {
	if r.Stage != retentionAdopting || r.Handoff == nil {
		return nil
	}
	h := r.Handoff
	lease, owner, file, err := resource.InspectLeaseCandidate(i.ControllerSocket, r.SandboxID, h.Candidate)
	if err != nil {
		return fmt.Errorf("recover retained candidate: %w", err)
	}
	if owner != h.Lease.PID || file != h.File || !reflect.DeepEqual(lease, h.Lease) ||
		lease.SandboxID != r.SandboxID || lease.ControllerSocket != i.ControllerSocket || h.StartTime == 0 {
		return fmt.Errorf("retained candidate identity changed")
	}
	start, err := retentionProcessStartTime(owner)
	if err != nil || start != h.StartTime {
		return fmt.Errorf("retained candidate process identity changed")
	}
	if !i.cgroupAllowed(lease.CgroupPath) {
		return fmt.Errorf("retained candidate cgroup outside configured roots")
	}
	if err := i.ValidateManaged(LiveLease{Lease: lease, OwnerPID: owner, Path: h.Candidate}, owner); err != nil {
		return fmt.Errorf("recover managed retained candidate: %w", err)
	}
	maximum, err := resource.CacheMaximumFromFeatures(lease.ClientFeatures)
	if err != nil || h.InitialCache > maximum {
		return fmt.Errorf("invalid retained candidate cache bound")
	}
	if err := validateCacheReclaimState(lease.ClientFeatures, h.CacheReclaim, maximum); err != nil {
		return err
	}
	if state.ResolvePool != nil {
		pool, err := state.ResolvePool(r.SandboxID, owner, lease.CgroupPath)
		if err != nil {
			return fmt.Errorf("resolve retained candidate pool: %w", err)
		}
		if pool != h.Pool {
			return fmt.Errorf("retained candidate pool changed from %q to %q", h.Pool, pool)
		}
	}
	if err := state.restoreRetainedCandidate(r, maximum); err != nil {
		return err
	}
	controlPIDs[owner] = true
	return nil
}

func (s *State) restoreRetainedCandidate(receipt *retainedAccount, cacheMaximum uint64) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	r := root.retained[receipt.SandboxID]
	if r == nil || r.Stage != retentionAdopting || r.Handoff == nil ||
		!reflect.DeepEqual(r.Handoff, receipt.Handoff) || r.ID != receipt.ID {
		return fmt.Errorf("retained candidate recovery intent changed")
	}
	h := r.Handoff
	p := root.retainedPool(h.Pool)
	if p == nil {
		return fmt.Errorf("unknown retained candidate pool %q", h.Pool)
	}
	if root.pools != nil {
		root.mu.Lock()
		defer root.mu.Unlock()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bySID[r.SandboxID] != nil {
		return fmt.Errorf("retained candidate already reserved")
	}
	now := time.Now()
	lease := h.Lease
	active := &Reservation{
		Token: h.Token, SandboxID: r.SandboxID, PeerPID: lease.PID, CgroupPath: lease.CgroupPath,
		Capacity:              Resources{MemoryBytes: lease.CapacityMemory, CPUMilli: lease.CapacityCPUMilli},
		ConfiguredAllocatable: Resources{MemoryBytes: lease.FloorMemory, CPUMilli: lease.FloorCPUMilli},
		ReservationMemory:     h.InitialBudget, InitialBudget: h.InitialBudget,
		CacheReservation: h.InitialCache, CacheMax: cacheMaximum,
		CacheCoordinated: h.CacheReclaim != nil,
		Stage:            StageStartup, StageEnteredAt: now, LastHeartbeatAt: now,
		Provisional: true, RecoverySource: RecoveryLease, RecoveryKey: "retained-candidate:" + h.ID,
		LeasePath: h.Candidate, ClientFeatures: append([]string(nil), lease.ClientFeatures...),
	}
	if err := p.insertLocked(active); err != nil {
		return err
	}
	if root.pools != nil {
		root.bindings[r.SandboxID] = h.Pool
	}
	return nil
}

// Acceptance first runs the same atomic admission gates as ordinary B and
// installs that charge, then persists its recoverable identity. No accepted
// intent can sit between an R-only snapshot and a later unaccounted admission.
// A failed inventory fsync is ambiguous: keep the exact B/handoff for replay or
// candidate-close reconciliation. Failed admission never changes any R entry.
func (s *State) acceptRetainedHandoff(next *retainedAccount, spec AdmitSpec, persist func() error) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	old := root.retained[next.SandboxID]
	if old == nil || old.ID != next.ID || next.Revision != old.Revision+1 || next.Stage != retentionAdopting ||
		next.Handoff == nil || old.Stage == retentionAdopting || old.prepared() != nil {
		return fmt.Errorf("retained account changed before handoff acceptance")
	}
	h := next.Handoff
	if spec.Token != h.Token || spec.PeerPID != h.Lease.PID || spec.InitialBudget != h.InitialBudget || spec.InitialCache != h.InitialCache || spec.RetainedAdoption != h.ID {
		return fmt.Errorf("retained handoff Budget contract changed")
	}
	if _, _, err := root.admit(spec); err != nil {
		return err
	}
	root.retained[next.SandboxID] = cloneRetained(next)
	return persist()
}
