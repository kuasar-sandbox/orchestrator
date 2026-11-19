package nodectl

import (
	"fmt"
	"net"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

const (
	retentionPrepared = "prepared"
	retentionHolding  = "holding"
	retentionAdopting = "adopting"
	retentionActive   = "active"
)

// The receipt extends the existing inventory; it is not an independently
// admitted reservation. Prepared owns the original B; holding/active own one R
// projected into SourcePool. CaptureRecord owns all data/release facts.
type retainedReceipt struct {
	Version         int                             `json:"version"`
	SandboxID       string                          `json:"sandbox_id"`
	Capture         resource.CaptureRetention       `json:"capture"`
	Producer        memorycheckpoint.RunIdentity    `json:"producer"`
	CheckpointID    string                          `json:"checkpoint_id"`
	Holder          memorycheckpoint.HolderIdentity `json:"holder"`
	Memory          resource.RetainedMemory         `json:"memory"`
	Stage           string                          `json:"stage"`
	Settled         bool                            `json:"settled,omitempty"` // the exact R release has been durably accounted
	Source          Reservation                     `json:"source"`
	SourceStartTime uint64                          `json:"source_start_time"`
	SourceFile      resource.LeaseFileIdentity      `json:"source_file"`
	OwnerLease      resource.Lease                  `json:"owner_lease"`
	OwnerFile       resource.LeaseFileIdentity      `json:"owner_file"`
	OwnerToken      string                          `json:"owner_token"`
	Handoff         *retainedHandoff                `json:"handoff,omitempty"`
}

type retainedHandoff struct {
	ID            string                      `json:"id"`
	Candidate     string                      `json:"candidate"`
	File          resource.LeaseFileIdentity  `json:"file"`
	Lease         resource.Lease              `json:"lease"`
	Token         string                      `json:"token"`
	Pool          string                      `json:"pool"`
	InitialBudget uint64                      `json:"initial_budget"`
	InitialCache  uint64                      `json:"initial_cache"`
	CacheReclaim  *resource.CacheReclaimState `json:"cache_reclaim,omitempty"`
}

func cloneRetained(r *retainedReceipt) *retainedReceipt {
	if r == nil {
		return nil
	}
	out := *r
	out.Source = cloneReservation(&r.Source)
	out.OwnerLease.ClientFeatures = append([]string(nil), r.OwnerLease.ClientFeatures...)
	if r.Handoff != nil {
		h := *r.Handoff
		h.Lease.ClientFeatures = append([]string(nil), h.Lease.ClientFeatures...)
		h.CacheReclaim = resource.CloneCacheReclaimState(h.CacheReclaim)
		out.Handoff = &h
	}
	return &out
}

func (s *State) retentionOwner() *State {
	if s.retentionRoot != nil {
		return s.retentionRoot
	}
	return s
}

func (s *State) refreshRetainedCleanupLocked(sid string) {
	s.initPressureLocked()
	ob, ok := s.pressure.obligations[sid]
	if !ok {
		return
	}
	want := ob.cleanupOwner || (ob.accountCharge && (s.bySID[sid] != nil || s.retainedBytes[sid] != 0))
	if ob.cleanup != want {
		if want {
			s.pressure.cleanup++
		} else {
			s.pressure.cleanup--
		}
		ob.cleanup = want
	}
	if !want && ob.retired {
		delete(s.pressure.obligations, sid)
	} else {
		s.pressure.obligations[sid] = ob
	}
}

func (s *State) retainedPool(name string) *State {
	if s.pools != nil {
		return s.pools[name]
	}
	if name == s.PoolName || (name == "default" && s.PoolName == "") {
		return s
	}
	return nil
}

// RetainedMemory includes a prepared obligation even though it is still
// charged as B. Unsupported clients must not treat that SID as ready.
func (s *State) RetainedMemory(sid string) (resource.RetainedMemory, bool) {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	r := root.retained[sid]
	if r == nil || r.Settled {
		return resource.RetainedMemory{}, false
	}
	return r.Memory, true
}

func (s *State) retainedSnapshot(sid string) *retainedReceipt {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	return cloneRetained(root.retained[sid])
}

func (s *State) retainedSnapshots() []*retainedReceipt {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	out := make([]*retainedReceipt, 0, len(root.retained))
	for _, r := range root.retained {
		out = append(out, cloneRetained(r))
	}
	return out
}

func (s *State) checkRetainedOwnerLocked(sid string, pid int, adoption string, budget uint64) error {
	r := s.retained[sid]
	if r == nil || r.Settled {
		return nil
	}
	switch r.Stage {
	case retentionPrepared:
		if pid == r.Source.PeerPID && budget == r.Memory.Bytes {
			return nil
		}
	case retentionAdopting:
		if r.Handoff != nil && adoption == r.Handoff.ID && pid == r.Handoff.Lease.PID {
			return nil
		}
	case retentionActive:
		if pid == r.OwnerLease.PID {
			return nil
		}
	}
	return fmt.Errorf("sandbox %q has pending checkpoint memory; explicit owner handoff required", sid)
}

func sameCapture(a, b resource.CaptureRetention) bool { return a == b }

func (s *State) prepareRetained(r *retainedReceipt) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	if old := root.retained[r.SandboxID]; old != nil && !old.Settled {
		if sameCapture(old.Capture, r.Capture) {
			return nil
		}
		return fmt.Errorf("previous checkpoint memory is still retained")
	}
	p := root.forSID(r.SandboxID)
	p.mu.Lock()
	defer p.mu.Unlock()
	active := p.bySID[r.SandboxID]
	if active == nil || active.PeerPID != r.Source.PeerPID || active.Token != r.Source.Token || active.ReservationMemory != r.Memory.Bytes {
		return fmt.Errorf("capture reservation owner or Budget changed")
	}
	if root.retained == nil {
		root.retained = make(map[string]*retainedReceipt)
	}
	root.retained[r.SandboxID] = cloneRetained(r)
	active.RetentionPrepared = true
	return nil
}

// commitRetained changes accounting roles in one pool critical section. It
// performs no cgroup, balloon or Guest Budget operation on the frozen owner.
func (s *State) commitRetained(r *retainedReceipt) (net.Conn, error) {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	old := root.retained[r.SandboxID]
	if old == nil || !sameCapture(old.Capture, r.Capture) {
		return nil, fmt.Errorf("retention intent changed")
	}
	if old.Stage != retentionPrepared || old.Settled {
		return nil, nil
	}
	p := root.retainedPool(old.Memory.SourcePool)
	if p == nil {
		return nil, fmt.Errorf("retained source pool disappeared")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	active := p.bySID[r.SandboxID]
	if active == nil || active.PeerPID != old.Source.PeerPID || active.ReservationMemory != old.Memory.Bytes {
		return nil, fmt.Errorf("prepared reservation changed")
	}
	if _, err := checkedAggregateAdd(p.reservedMemory-active.ReservationMemory, old.Memory.Bytes, "retained memory"); err != nil {
		return nil, err
	}
	conn := active.Conn
	if _, err := p.detachLocked(r.SandboxID, false); err != nil {
		return nil, err
	}
	if p.retainedBytes == nil {
		p.retainedBytes = make(map[string]uint64)
	}
	if p.retainedCgroups == nil {
		p.retainedCgroups = make(map[string]string)
	}
	p.retainedBytes[r.SandboxID] = old.Memory.Bytes
	p.retainedCgroups[r.SandboxID] = old.Source.CgroupPath
	p.reservedMemory += old.Memory.Bytes
	p.refreshRetainedCleanupLocked(r.SandboxID)
	p.initPressureLocked()
	p.pressure.lastReservationChange = p.pressure.clock()
	p.signalPressureLocked()
	root.retained[r.SandboxID] = cloneRetained(r)
	return conn, nil
}

func (s *State) abortRetained(sid string, capture resource.CaptureRetention) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	r := root.retained[sid]
	if r == nil || r.Settled {
		return nil
	}
	if !sameCapture(r.Capture, capture) || r.Stage != retentionPrepared {
		return fmt.Errorf("retention is not abortable")
	}
	p := root.retainedPool(r.Memory.SourcePool)
	p.mu.Lock()
	if active := p.bySID[sid]; active != nil {
		active.RetentionPrepared = false
	}
	p.mu.Unlock()
	delete(root.retained, sid)
	return nil
}

func (s *State) setRetainedReceipt(r *retainedReceipt) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	old := root.retained[r.SandboxID]
	if old == nil || !sameCapture(old.Capture, r.Capture) {
		return fmt.Errorf("retention identity changed")
	}
	root.retained[r.SandboxID] = cloneRetained(r)
	return nil
}

func (s *State) releaseRetained(sid string, capture resource.CaptureRetention) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	r := root.retained[sid]
	if r == nil {
		return nil
	}
	if !sameCapture(r.Capture, capture) {
		return fmt.Errorf("retention generation changed")
	}
	if r.Settled {
		return nil
	}
	p := root.retainedPool(r.Memory.SourcePool)
	if p == nil {
		return fmt.Errorf("retained source pool disappeared")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if r.Stage == retentionPrepared {
		// RAMReleased also proves the prepared producer no longer owns B.
		if active := p.bySID[sid]; active != nil && active.PeerPID == r.Source.PeerPID {
			if _, err := p.detachLocked(sid, true); err != nil {
				return err
			}
		}
	} else if amount := p.retainedBytes[sid]; amount != 0 {
		next, err := checkedAggregateSub(p.reservedMemory, amount, "retained memory")
		if err != nil {
			return err
		}
		p.reservedMemory = next
		delete(p.retainedBytes, sid)
		delete(p.retainedCgroups, sid)
	}
	nextReceipt := cloneRetained(r)
	nextReceipt.Settled = true
	root.retained[sid] = nextReceipt
	p.refreshRetainedCleanupLocked(sid)
	if p.pressure.exclusive == sid && p.bySID[sid] == nil {
		p.pressure.exclusive = ""
	}
	p.initPressureLocked()
	p.pressure.lastReservationChange = p.pressure.clock()
	p.signalPressureLocked()
	return nil
}

func (s *State) restoreRetained(r *retainedReceipt) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	if old := root.retained[r.SandboxID]; old != nil {
		if sameCapture(old.Capture, r.Capture) {
			return nil
		}
		return fmt.Errorf("conflicting retained memory inventory")
	}
	p := root.retainedPool(r.Memory.SourcePool)
	if p == nil {
		return fmt.Errorf("unknown retained memory pool %q", r.Memory.SourcePool)
	}
	if root.pools != nil {
		root.mu.Lock()
		defer root.mu.Unlock()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if r.Stage == retentionPrepared && !r.Settled {
		active := cloneReservation(&r.Source)
		active.Conn, active.RetentionPrepared = nil, true
		active.LastHeartbeatAt = time.Time{}
		if err := p.insertLocked(&active); err != nil {
			return err
		}
		if root.pools != nil {
			root.bindings[r.SandboxID] = r.Memory.SourcePool
		}
	} else if !r.Settled {
		next, err := checkedAggregateAdd(p.reservedMemory, r.Memory.Bytes, "retained memory")
		if err != nil {
			return err
		}
		p.reservedMemory = next
		if p.retainedBytes == nil {
			p.retainedBytes = make(map[string]uint64)
		}
		if p.retainedCgroups == nil {
			p.retainedCgroups = make(map[string]string)
		}
		p.retainedBytes[r.SandboxID] = r.Memory.Bytes
		p.retainedCgroups[r.SandboxID] = r.Source.CgroupPath
	}
	if root.retained == nil {
		root.retained = make(map[string]*retainedReceipt)
	}
	root.retained[r.SandboxID] = cloneRetained(r)
	p.refreshRetainedCleanupLocked(r.SandboxID)
	return nil
}

func (s *State) retainedSourceCgroup(path string) bool {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	for _, r := range root.retained {
		if !r.Settled && r.Source.CgroupPath == path {
			return true
		}
	}
	return false
}
