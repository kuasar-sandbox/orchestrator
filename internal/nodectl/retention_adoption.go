package nodectl

import (
	"fmt"
	"os"
	"reflect"
	"syscall"

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
func adoptionRecovery(socket string, r *retainedReceipt) *retainedReceipt {
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
	return out
}

// reconcileAdoption is independent of source RAM settlement. A completed R
// may still have a failed, unacknowledged successor admission to clean up.
func (i *Inventory) reconcileAdoption(state *State, r *retainedReceipt) (*retainedReceipt, error) {
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
