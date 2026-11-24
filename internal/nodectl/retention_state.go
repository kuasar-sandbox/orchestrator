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
	retentionHeld     = "held"
	retentionSettled  = "settled"
	retentionHolding  = "holding"
	retentionAdopting = "adopting"
	retentionActive   = "active"
)

// One durable account owns the SID's lease handoff. Each capture independently
// owns its original B -> R -> settled transition in its immutable source pool.
// Neither the selected restore graph nor current-memory.json defines this set.
type retainedAccount struct {
	Version    int                        `json:"version"`
	SandboxID  string                     `json:"sandbox_id"`
	ID         string                     `json:"id"`
	Revision   uint64                     `json:"revision"`
	Stage      string                     `json:"stage"`
	Captures   []*retainedCapture         `json:"captures"`
	OwnerLease resource.Lease             `json:"owner_lease"`
	OwnerFile  resource.LeaseFileIdentity `json:"owner_file"`
	OwnerToken string                     `json:"owner_token"`
	Handoff    *retainedHandoff           `json:"handoff,omitempty"`
}

type retainedCapture struct {
	Capture         resource.CaptureRetention       `json:"capture"`
	Producer        memorycheckpoint.RunIdentity    `json:"producer"`
	CheckpointID    string                          `json:"checkpoint_id"`
	Holder          memorycheckpoint.HolderIdentity `json:"holder"`
	VMM             *memorycheckpoint.VMMIdentity   `json:"vmm,omitempty"`
	Memory          resource.RetainedMemory         `json:"memory"`
	Stage           string                          `json:"stage"`
	Source          Reservation                     `json:"source"`
	SourceStartTime uint64                          `json:"source_start_time"`
	SourceFile      resource.LeaseFileIdentity      `json:"source_file"`
}

type retainedHandoff struct {
	ID              string                      `json:"id"`
	AccountRevision uint64                      `json:"account_revision"`
	Candidate       string                      `json:"candidate"`
	File            resource.LeaseFileIdentity  `json:"file"`
	Lease           resource.Lease              `json:"lease"`
	StartTime       uint64                      `json:"start_time,omitempty"`
	Token           string                      `json:"token"`
	Pool            string                      `json:"pool"`
	InitialBudget   uint64                      `json:"initial_budget"`
	InitialCache    uint64                      `json:"initial_cache"`
	CacheReclaim    *resource.CacheReclaimState `json:"cache_reclaim,omitempty"`
}

func cloneCapture(r *retainedCapture) *retainedCapture {
	out := *r
	out.Source = cloneReservation(&r.Source)
	if r.VMM != nil {
		v := *r.VMM
		out.VMM = &v
	}
	return &out
}

func cloneRetained(r *retainedAccount) *retainedAccount {
	if r == nil {
		return nil
	}
	out := *r
	out.Captures = make([]*retainedCapture, len(r.Captures))
	for i, c := range r.Captures {
		out.Captures[i] = cloneCapture(c)
	}
	out.OwnerLease.ClientFeatures = append([]string(nil), r.OwnerLease.ClientFeatures...)
	if r.Handoff != nil {
		h := *r.Handoff
		h.Lease.ClientFeatures = append([]string(nil), h.Lease.ClientFeatures...)
		h.CacheReclaim = resource.CloneCacheReclaimState(h.CacheReclaim)
		out.Handoff = &h
	}
	return &out
}

func (r *retainedAccount) reference() *resource.RetainedAccount {
	if r == nil {
		return nil
	}
	return &resource.RetainedAccount{ID: r.ID, Revision: r.Revision}
}

func (r *retainedAccount) capture(c resource.CaptureRetention) *retainedCapture {
	if r != nil {
		for _, e := range r.Captures {
			if sameCapture(e.Capture, c) {
				return e
			}
		}
	}
	return nil
}

func (r *retainedAccount) pending() bool {
	if r != nil {
		for _, e := range r.Captures {
			if e.Stage != retentionSettled {
				return true
			}
		}
	}
	return false
}

func (r *retainedAccount) prepared() *retainedCapture {
	if r != nil {
		for _, e := range r.Captures {
			if e.Stage == retentionPrepared {
				return e
			}
		}
	}
	return nil
}

func sameCapture(a, b resource.CaptureRetention) bool { return a == b }

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

// RetainedMemory reports a pending obligation for lifecycle barriers. Callers
// settling a specific capture must use RetainedCapture; this is not a total B.
func (s *State) RetainedMemory(sid string) (resource.RetainedMemory, bool) {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	if r := root.retained[sid]; r != nil {
		for _, e := range r.Captures {
			if e.Stage != retentionSettled {
				return e.Memory, true
			}
		}
	}
	return resource.RetainedMemory{}, false
}

func (s *State) RetainedCapture(sid, memoryID string, epoch uint64) (resource.RetainedMemory, bool) {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	if r := root.retained[sid]; r != nil {
		for _, e := range r.Captures {
			if e.Stage != retentionSettled && e.Memory.MemoryID == memoryID && e.Memory.Epoch == epoch {
				return e.Memory, true
			}
		}
	}
	return resource.RetainedMemory{}, false
}

// RetainedCaptureOperation is used before the host has learned a MemoryID.
// It fences only the exact producer/operation, never unrelated old holders.
func (s *State) RetainedCaptureOperation(producer memorycheckpoint.RunIdentity, checkpointID, operationID string) bool {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	if account := root.retained[producer.SandboxID]; account != nil {
		for _, entry := range account.Captures {
			if entry.Stage != retentionSettled && entry.Producer == producer && entry.CheckpointID == checkpointID && entry.Capture.OperationID == operationID {
				return true
			}
		}
	}
	return false
}

func (s *State) retainedSnapshot(sid string) *retainedAccount {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	return cloneRetained(root.retained[sid])
}

func (s *State) retainedSnapshots() []*retainedAccount {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	out := make([]*retainedAccount, 0, len(root.retained))
	for _, r := range root.retained {
		out = append(out, cloneRetained(r))
	}
	return out
}

func (s *State) checkRetainedOwnerLocked(sid string, pid int, adoption string, budget uint64) error {
	r := s.retained[sid]
	if r == nil {
		return nil
	}
	if e := r.prepared(); e != nil {
		if pid == e.Source.PeerPID && budget == e.Memory.Bytes {
			return nil
		}
	} else {
		switch r.Stage {
		case retentionAdopting:
			if r.Handoff != nil && adoption == r.Handoff.ID && pid == r.Handoff.Lease.PID && budget == r.Handoff.InitialBudget {
				return nil
			}
		case retentionActive:
			if pid == r.OwnerLease.PID {
				return nil
			}
		}
	}
	return fmt.Errorf("sandbox %q has a retained account; explicit owner handoff required", sid)
}

// The authenticated lease/process identity owns a prepared B, not its current
// connection token. StateSync can rotate that token while retaining the same
// producer and frozen budget. Server/inventory boundaries verify the lease inode
// and process start time; these fields fence the in-memory reservation transition.
func sameCaptureReservation(active *Reservation, capture *retainedCapture) bool {
	return active != nil && active.SandboxID == capture.Source.SandboxID &&
		active.PeerPID == capture.Source.PeerPID && active.CgroupPath == capture.Source.CgroupPath &&
		active.LeasePath == capture.Source.LeasePath && active.ReservationMemory == capture.Memory.Bytes
}

// prepareRetained protects only this execution's B. Earlier entries remain
// independent R obligations, even if this runtime restored another graph.
func (s *State) prepareRetained(r *retainedAccount, capture resource.CaptureRetention) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	e := r.capture(capture)
	if e == nil || e.Stage != retentionPrepared {
		return fmt.Errorf("missing prepared capture")
	}
	if old := root.retained[r.SandboxID]; old != nil {
		if old.ID != r.ID {
			return fmt.Errorf("retained account changed")
		}
		if prior := old.capture(capture); prior != nil {
			return nil
		}
		if old.prepared() != nil {
			return fmt.Errorf("active Budget already has a capture intent")
		}
	}
	p := root.forSID(r.SandboxID)
	p.mu.Lock()
	defer p.mu.Unlock()
	active := p.bySID[r.SandboxID]
	if !sameCaptureReservation(active, e) || active.Token != e.Source.Token {
		return fmt.Errorf("capture reservation owner or Budget changed")
	}
	if root.retained == nil {
		root.retained = make(map[string]*retainedAccount)
	}
	root.retained[r.SandboxID] = cloneRetained(r)
	active.RetentionPrepared = true
	return nil
}

// commitRetained transfers B into one R, without any guest/cgroup operation.
func (s *State) commitRetained(r *retainedAccount, capture resource.CaptureRetention) (net.Conn, error) {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	old := root.retained[r.SandboxID]
	e := old.capture(capture)
	if old == nil || old.ID != r.ID || e == nil {
		return nil, fmt.Errorf("retention intent changed")
	}
	if e.Stage != retentionPrepared {
		return nil, nil
	}
	p := root.retainedPool(e.Memory.SourcePool)
	if p == nil {
		return nil, fmt.Errorf("retained source pool disappeared")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	active := p.bySID[r.SandboxID]
	if !sameCaptureReservation(active, e) || !active.RetentionPrepared {
		return nil, fmt.Errorf("prepared reservation changed")
	}
	amount, err := checkedAggregateAdd(p.retainedBytes[r.SandboxID], e.Memory.Bytes, "retained memory")
	if err != nil {
		return nil, err
	}
	conn := active.Conn
	if _, err := p.detachLocked(r.SandboxID, false); err != nil {
		return nil, err
	}
	if p.retainedBytes == nil {
		p.retainedBytes = make(map[string]uint64)
	}
	p.retainedBytes[r.SandboxID] = amount
	p.reservedMemory += e.Memory.Bytes
	p.refreshRetainedCleanupLocked(r.SandboxID)
	p.initPressureLocked()
	p.pressure.lastReservationChange = p.pressure.clock()
	p.signalPressureLocked()
	root.retained[r.SandboxID] = cloneRetained(r)
	return conn, nil
}

func (s *State) abortRetained(next *retainedAccount, capture resource.CaptureRetention) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	r := root.retained[next.SandboxID]
	e := r.capture(capture)
	if e == nil {
		return nil
	}
	if e.Stage != retentionPrepared || r.ID != next.ID || next.capture(capture) != nil {
		return fmt.Errorf("retention is not abortable")
	}
	p := root.retainedPool(e.Memory.SourcePool)
	if p == nil {
		return fmt.Errorf("retained source pool disappeared")
	}
	p.mu.Lock()
	if active := p.bySID[r.SandboxID]; sameCaptureReservation(active, e) && active.RetentionPrepared {
		active.RetentionPrepared = false
	}
	p.mu.Unlock()
	root.retained[r.SandboxID] = cloneRetained(next)
	return nil
}

func (s *State) setRetainedReceipt(r *retainedAccount) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	old := root.retained[r.SandboxID]
	if old == nil || old.ID != r.ID || r.Revision < old.Revision {
		return fmt.Errorf("retained account changed")
	}
	root.retained[r.SandboxID] = cloneRetained(r)
	return nil
}

// Failed admission frees only its provisional B/cache. Entries and even an
// empty account survive until the SID's terminal lifecycle explicitly retires it.
func (s *State) resetRetainedAdoption(previous, next *retainedAccount) (net.Conn, error) {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	current := root.retained[previous.SandboxID]
	if current == nil || current.ID != previous.ID || current.Stage != retentionAdopting ||
		current.Handoff == nil || previous.Handoff == nil || current.Handoff.ID != previous.Handoff.ID ||
		current.Handoff.File != previous.Handoff.File || current.Handoff.Token != previous.Handoff.Token {
		return nil, fmt.Errorf("retained handoff changed before cleanup")
	}
	h := previous.Handoff
	p := root.retainedPool(h.Pool)
	if p == nil {
		return nil, fmt.Errorf("handoff pool disappeared")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var conn net.Conn
	if active := p.bySID[previous.SandboxID]; active != nil {
		if active.Token != h.Token || active.PeerPID != h.Lease.PID || active.CgroupPath != h.Lease.CgroupPath || active.RetentionPrepared {
			return nil, fmt.Errorf("handoff reservation changed before cleanup")
		}
		conn = active.Conn
		if _, err := p.deleteLocked(previous.SandboxID); err != nil {
			return nil, err
		}
	}
	root.retained[previous.SandboxID] = cloneRetained(next)
	p.refreshRetainedCleanupLocked(previous.SandboxID)
	return conn, nil
}

func (s *State) releaseRetained(next *retainedAccount, capture resource.CaptureRetention) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	r := root.retained[next.SandboxID]
	e := r.capture(capture)
	if r == nil || r.ID != next.ID || e == nil {
		return fmt.Errorf("retention generation changed")
	}
	if e.Stage == retentionSettled {
		return nil
	}
	if n := next.capture(capture); n == nil || n.Stage != retentionSettled {
		return fmt.Errorf("missing settlement")
	}
	p := root.retainedPool(e.Memory.SourcePool)
	if p == nil {
		return fmt.Errorf("retained source pool disappeared")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if e.Stage == retentionPrepared {
		if active := p.bySID[r.SandboxID]; sameCaptureReservation(active, e) && active.RetentionPrepared {
			if _, err := p.detachLocked(r.SandboxID, true); err != nil {
				return err
			}
		}
	} else {
		amount, err := checkedAggregateSub(p.retainedBytes[r.SandboxID], e.Memory.Bytes, "retained SID memory")
		if err != nil {
			return err
		}
		total, err := checkedAggregateSub(p.reservedMemory, e.Memory.Bytes, "retained memory")
		if err != nil {
			return err
		}
		p.reservedMemory = total
		if amount == 0 {
			delete(p.retainedBytes, r.SandboxID)
		} else {
			p.retainedBytes[r.SandboxID] = amount
		}
	}
	root.retained[r.SandboxID] = cloneRetained(next)
	p.refreshRetainedCleanupLocked(r.SandboxID)
	p.initPressureLocked()
	if p.pressure.exclusive == r.SandboxID && p.bySID[r.SandboxID] == nil {
		p.pressure.exclusive = ""
	}
	p.pressure.lastReservationChange = p.pressure.clock()
	p.signalPressureLocked()
	return nil
}

func (s *State) restoreRetained(r *retainedAccount) error {
	root := s.retentionOwner()
	root.retentionMu.Lock()
	defer root.retentionMu.Unlock()
	if old := root.retained[r.SandboxID]; old != nil {
		if old.ID == r.ID && old.Revision == r.Revision {
			return nil
		}
		return fmt.Errorf("conflicting retained memory inventory")
	}
	for _, e := range r.Captures {
		if e.Stage != retentionSettled && root.retainedPool(e.Memory.SourcePool) == nil {
			return fmt.Errorf("unknown retained memory pool %q", e.Memory.SourcePool)
		}
	}
	if root.pools != nil {
		root.mu.Lock()
		defer root.mu.Unlock()
	}
	for _, e := range r.Captures {
		if e.Stage == retentionSettled {
			continue
		}
		p := root.retainedPool(e.Memory.SourcePool)
		p.mu.Lock()
		var err error
		if e.Stage == retentionPrepared {
			active := cloneReservation(&e.Source)
			active.Conn, active.RetentionPrepared, active.LastHeartbeatAt = nil, true, time.Time{}
			err = p.insertLocked(&active)
			if err == nil && root.pools != nil {
				root.bindings[r.SandboxID] = e.Memory.SourcePool
			}
		} else {
			var total, amount uint64
			total, err = checkedAggregateAdd(p.reservedMemory, e.Memory.Bytes, "retained memory")
			if err == nil {
				amount, err = checkedAggregateAdd(p.retainedBytes[r.SandboxID], e.Memory.Bytes, "retained SID memory")
			}
			if err == nil {
				if p.retainedBytes == nil {
					p.retainedBytes = make(map[string]uint64)
				}
				p.reservedMemory, p.retainedBytes[r.SandboxID] = total, amount
			}
		}
		p.refreshRetainedCleanupLocked(r.SandboxID)
		p.mu.Unlock()
		if err != nil {
			return err
		}
	}
	if root.retained == nil {
		root.retained = make(map[string]*retainedAccount)
	}
	root.retained[r.SandboxID] = cloneRetained(r)
	return nil
}
