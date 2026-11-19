package nodectl

import (
	"fmt"
	"net"
	"os"
	"slices"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func (s *State) retentionReservation(sid string) (Reservation, bool) {
	for _, p := range s.PoolStates() {
		p.mu.Lock()
		r := p.bySID[sid]
		if r != nil {
			out := cloneReservation(r)
			p.mu.Unlock()
			return out, true
		}
		p.mu.Unlock()
	}
	return Reservation{}, false
}

func (s *Server) validateRetainedSession(sid string, peerPID int, token string, r *retainedReceipt) error {
	if s.Inventory == nil || peerPID <= 0 {
		return fmt.Errorf("retention inventory or peer identity unavailable")
	}
	live, err := s.Inventory.LookupLiveLease(sid)
	if err != nil {
		return err
	}
	if err := s.Inventory.ValidateManaged(live, peerPID); err != nil {
		return err
	}
	if !live.Lease.Supports(resource.FeatureRetainedMemoryV1) {
		return fmt.Errorf("checkpoint memory handoff is not negotiated")
	}
	_, owner, file, err := resource.InspectLeaseIdentity(live.Path)
	if err != nil || owner != peerPID {
		return fmt.Errorf("retention lease owner changed")
	}
	if r == nil {
		active, found := s.State.retentionReservation(sid)
		if !found || active.PeerPID != peerPID || active.Token != token || !resource.HasFeature(active.ClientFeatures, resource.FeatureRetainedMemoryV1) {
			return fmt.Errorf("retention requires the current admitted session")
		}
		return nil
	}
	if r.OwnerLease.PID != peerPID || r.OwnerFile != file {
		return fmt.Errorf("retained memory owner has changed")
	}
	if r.Stage == retentionPrepared || r.Stage == retentionActive {
		if active, found := s.State.retentionReservation(sid); found && (active.PeerPID != peerPID || (active.Conn != nil && !active.Provisional && active.Token != token)) {
			return fmt.Errorf("retention session token changed")
		}
	}
	return nil
}

func (s *Server) handleRetained(peerPID int, req *Message, token string) *Message {
	bad := func(err error) *Message { return &Message{Type: TypeError, Msg: "checkpoint memory: " + err.Error()} }
	if req.Retention == nil || s.Inventory == nil || req.SandboxID == "" {
		return bad(fmt.Errorf("retention identity unavailable"))
	}
	root := s.State.retentionOwner()
	root.retentionOps.Lock()
	defer root.retentionOps.Unlock()
	proof, err := readRetentionCapture(req.SandboxID, *req.Retention)
	if err != nil {
		return bad(err)
	}
	r := s.State.retainedSnapshot(req.SandboxID)
	if req.Type == resource.TypePrepareRetained && r != nil && !r.Settled && !sameCapture(r.Capture, *req.Retention) {
		// RAM readiness and its resource ACK can race the next capture. The
		// previous receipt is settled from its own exact proof first; the
		// new capture's proof cannot release an old R.
		oldProof, err := readRetentionCapture(r.SandboxID, r.Capture)
		if err == nil {
			err = validateRetentionProof(r, oldProof)
		}
		if err == nil && oldProof.RAMReleased {
			next := cloneRetained(r)
			next.Settled = true
			if err := writeRetainedReceipt(s.Identity, next); err != nil {
				return bad(err)
			}
			if err := s.State.releaseRetained(r.SandboxID, r.Capture); err != nil {
				return bad(err)
			}
			r = next
		} else if err == nil && oldProof.Aborted && r.Stage == retentionPrepared {
			if err := removeRetainedReceipt(s.Identity, r.SandboxID); err != nil {
				return bad(err)
			}
			if err := s.State.abortRetained(r.SandboxID, r.Capture); err != nil {
				return bad(err)
			}
			r = nil
		}
	}
	if r != nil && r.Settled && !sameCapture(r.Capture, *req.Retention) && req.Type == resource.TypePrepareRetained {
		r = nil
	}
	if r != nil && !sameCapture(r.Capture, *req.Retention) {
		// A newer capture may replace the settled receipt before the old
		// producer receives its release ACK. Its exact released record and
		// live process identity are enough for a no-op ACK, never authority
		// over the current receipt. Memory IDs identify captures; epochs are
		// scoped to one memory ID and need not increase between captures.
		if req.Type == resource.TypeReleaseRetained && r.Capture.MemoryID != req.Retention.MemoryID &&
			proof.RAMReleased && proof.Holder.PID == peerPID && peerPID > 0 {
			start, err := retentionProcessStartTime(peerPID)
			if err == nil && start == proof.Holder.StartTime {
				return &Message{Type: TypeAck}
			}
		}
		return bad(fmt.Errorf("another retention generation owns this SID"))
	}
	if r != nil {
		if err := validateRetentionProof(r, proof); err != nil {
			return bad(err)
		}
	}
	producerRelease := false
	if req.Type == resource.TypeReleaseRetained && r != nil && proof.RAMReleased && peerPID == r.Source.PeerPID {
		start, err := retentionProcessStartTime(peerPID)
		producerRelease = err == nil && start == r.SourceStartTime
	}
	if !producerRelease {
		if err := s.validateRetainedSession(req.SandboxID, peerPID, token, r); err != nil {
			return bad(err)
		}
	}
	switch req.Type {
	case resource.TypePrepareRetained:
		if proof.Aborted {
			return bad(fmt.Errorf("capture was aborted"))
		}
		if r == nil {
			if proof.Committed || proof.RuntimeRetired || proof.RAMReleased {
				return bad(fmt.Errorf("capture already crossed its preparation boundary"))
			}
			active, found := s.State.retentionReservation(req.SandboxID)
			if !found {
				return bad(fmt.Errorf("active reservation disappeared"))
			}
			lease, owner, file, err := resource.InspectLeaseIdentity(resource.LeasePath(s.Identity, req.SandboxID))
			if err != nil || owner != peerPID {
				return bad(fmt.Errorf("source lease changed"))
			}
			start, err := retentionProcessStartTime(peerPID)
			if err != nil {
				return bad(err)
			}
			if proof.Holder.PID != 0 && (proof.Holder.PID != peerPID || (proof.Holder.StartTime != 0 && proof.Holder.StartTime != start)) {
				return bad(fmt.Errorf("capture holder does not own source reservation"))
			}
			r = &retainedReceipt{Version: 1, SandboxID: req.SandboxID, Capture: *req.Retention,
				Producer: proof.Producer, CheckpointID: proof.CheckpointID, Holder: proof.Holder,
				Memory: resource.RetainedMemory{MemoryID: req.Retention.MemoryID, Epoch: req.Retention.Epoch, SourcePool: s.State.SandboxPool(req.SandboxID), Bytes: active.ReservationMemory},
				Stage:  retentionPrepared, Source: active, SourceStartTime: start, SourceFile: file, OwnerLease: lease, OwnerFile: file, OwnerToken: token}
			if err := s.State.prepareRetained(r); err != nil {
				return bad(err)
			}
		}
		// A failed fsync is ambiguous. Keep B protected and let this exact
		// operation replay or explicitly abort against the capture proof.
		if err := writeRetainedReceipt(s.Identity, r); err != nil {
			return bad(err)
		}
	case resource.TypeAbortRetained:
		if r == nil {
			return &Message{Type: TypeAck}
		}
		if r.Stage != retentionPrepared || proof.Committed || proof.RuntimeRetired || proof.RAMReleased {
			return bad(fmt.Errorf("capture can no longer be aborted"))
		}
		if err := removeRetainedReceipt(s.Identity, req.SandboxID); err != nil {
			return bad(err)
		}
		if err := s.State.abortRetained(req.SandboxID, *req.Retention); err != nil {
			return bad(err)
		}
		return &Message{Type: TypeAck}
	case resource.TypeCommitRetained:
		if r == nil {
			return bad(fmt.Errorf("retention was not prepared"))
		}
		if !proof.Committed || !proof.RuntimeRetired {
			return bad(fmt.Errorf("runtime retirement is not committed"))
		}
		if r.Stage == retentionPrepared && !r.Settled {
			r.Stage = retentionHolding
			if err := writeRetainedReceipt(s.Identity, r); err != nil {
				return bad(err)
			}
			if _, err := s.State.commitRetained(r); err != nil {
				return bad(err)
			}
		}
	case resource.TypeReleaseRetained:
		if r == nil {
			return &Message{Type: TypeAck}
		}
		if !proof.RAMReleased {
			return bad(fmt.Errorf("physical checkpoint RAM has not been released"))
		}
		if !r.Settled {
			next := cloneRetained(r)
			next.Settled = true
			if err := writeRetainedReceipt(s.Identity, next); err != nil {
				return bad(err)
			}
			if err := s.State.releaseRetained(req.SandboxID, *req.Retention); err != nil {
				return bad(err)
			}
			r = next
		}
	default:
		return bad(fmt.Errorf("unknown retention operation"))
	}
	if s.Admission != nil {
		s.Admission.PushWake()
	}
	return &Message{Type: TypeAck, Retained: &r.Memory}
}

func (s *Server) validateOrdinaryRetainedOwner(peerPID int, req *Message) error {
	r := s.State.retainedSnapshot(req.SandboxID)
	if r == nil || r.Settled {
		return nil
	}
	if r.Stage != retentionActive && r.Stage != retentionPrepared {
		return fmt.Errorf("pending checkpoint memory requires explicit owner handoff")
	}
	if r.OwnerLease.PID != peerPID {
		return fmt.Errorf("pending checkpoint memory belongs to another owner")
	}
	_, pid, file, err := resource.InspectLeaseIdentity(resource.LeasePath(s.Identity, req.SandboxID))
	if err != nil || pid != peerPID || file != r.OwnerFile {
		return fmt.Errorf("pending checkpoint memory lease identity changed")
	}
	return nil
}

func (s *Server) buildRetainedAdmitOK(conn net.Conn, peerPID int, req *Message, token *string) (*Message, error) {
	if req.Retention == nil || req.HandoffID == "" || req.LeaseCandidate == "" || s.Inventory == nil ||
		!resource.HasFeature(req.ClientFeatures, resource.FeatureRetainedMemoryV1) || !resource.HasFeature(req.ClientFeatures, resource.FeatureStateSyncV1) {
		return nil, fmt.Errorf("retained admission requires explicit capabilities and identity")
	}
	root := s.State.retentionOwner()
	root.retentionOps.Lock()
	defer root.retentionOps.Unlock()
	r := s.State.retainedSnapshot(req.SandboxID)
	if r == nil || !sameCapture(r.Capture, *req.Retention) {
		return nil, fmt.Errorf("retained capture identity unavailable or changed")
	}
	proof, err := readRetentionCapture(req.SandboxID, *req.Retention)
	if err != nil {
		return nil, err
	}
	if err := validateRetentionProof(r, proof); err != nil {
		return nil, err
	}
	if !proof.Committed || !proof.RuntimeRetired {
		return nil, fmt.Errorf("previous runtime is not retired")
	}
	lease, owner, file, err := resource.InspectLeaseCandidate(s.Identity, req.SandboxID, req.LeaseCandidate)
	if os.IsNotExist(err) && r.Handoff != nil && r.Handoff.ID == req.HandoffID {
		lease, owner, file, err = resource.InspectLeaseIdentity(resource.LeasePath(s.Identity, req.SandboxID))
	}
	if err != nil {
		return nil, err
	}
	if owner != peerPID || lease.PID != peerPID || lease.ControllerSocket != s.Identity || lease.SandboxID != req.SandboxID ||
		lease.CapacityMemory != req.CapacityMemoryBytes || lease.CapacityCPUMilli != uint64(req.CapacityCPU)*1000 ||
		lease.FloorMemory != req.FloorMemoryBytes || lease.FloorCPUMilli != cpuMilliCeil(req.FloorCPU) ||
		lease.CgroupPath != req.CgroupPath || !slices.Equal(lease.ClientFeatures, req.ClientFeatures) {
		return nil, fmt.Errorf("candidate lease does not match admission contract or peer")
	}
	if err := s.Inventory.ValidateManaged(LiveLease{Lease: lease, OwnerPID: owner, Path: req.LeaseCandidate}, peerPID); err != nil {
		return nil, err
	}
	if !s.Inventory.cgroupAllowed(lease.CgroupPath) {
		return nil, fmt.Errorf("candidate cgroup outside configured roots")
	}
	maximum, err := resource.CacheMaximumFromFeatures(lease.ClientFeatures)
	if err != nil || maximum != req.CacheMaximum {
		return nil, fmt.Errorf("candidate cache bound differs from lease")
	}
	if r.Stage == retentionPrepared {
		return nil, fmt.Errorf("previous retention commit has not completed")
	}
	launch, err := s.Admission.resolveLaunch(req.SandboxID, peerPID)
	if err != nil {
		return nil, err
	}
	if launch.SavedSource && req.AllocatableAtSnapshot == 0 {
		return nil, fmt.Errorf("saved-source handoff requires BudgetAtSnapshot")
	}
	spec := admitSpecFromRequest(conn, peerPID, req)
	spec.LeasePath, spec.Admission = resource.LeasePath(s.Identity, req.SandboxID), &launch
	spec.RetainedAdoption = req.HandoffID
	if r.Handoff == nil || r.Handoff.ID != req.HandoffID {
		if _, busy := s.State.retentionReservation(req.SandboxID); busy {
			return nil, fmt.Errorf("retained SID still has an active execution")
		}
		if r.Stage == retentionAdopting {
			return nil, fmt.Errorf("another owner handoff is pending")
		}
		if r.Stage == retentionActive {
			// A new attempt after a stopped successor may take the obligation,
			// but never steal a still-locked active owner's canonical lease.
			if _, locked, err := resource.LeaseLockOwner(spec.LeasePath); err == nil && locked {
				return nil, fmt.Errorf("retained active owner is still live")
			}
		}
		r.Stage = retentionAdopting
		r.Handoff = &retainedHandoff{ID: req.HandoffID, Candidate: req.LeaseCandidate, File: file, Lease: lease,
			Token: NewToken(), Pool: s.State.SandboxPool(req.SandboxID), InitialBudget: spec.InitialBudget,
			InitialCache: spec.InitialCache, CacheReclaim: resource.CloneCacheReclaimState(req.CacheReclaim)}
		if err := writeRetainedReceipt(s.Identity, r); err != nil {
			return nil, err
		}
		if err := s.State.setRetainedReceipt(r); err != nil {
			return nil, err
		}
	} else if r.Handoff.File != file || r.Handoff.Lease.PID != peerPID || r.Handoff.Candidate != req.LeaseCandidate ||
		r.Handoff.InitialBudget != spec.InitialBudget || r.Handoff.InitialCache != spec.InitialCache {
		return nil, fmt.Errorf("handoff replay contract changed")
	}
	spec.Token = r.Handoff.Token
	_, old, err := s.cacheCoordinator().Admit(spec, req.CacheReclaim)
	if err != nil {
		return nil, err
	}
	if s.phaseHook != nil {
		s.phaseHook("retention_admitted")
	}
	if err := resource.PublishLeaseCandidate(s.Identity, req.SandboxID, req.LeaseCandidate, file, peerPID); err != nil {
		return nil, err
	}
	if s.phaseHook != nil {
		s.phaseHook("retention_published")
	}
	r.OwnerLease, r.OwnerFile, r.OwnerToken = lease, file, spec.Token
	r.Stage = retentionActive
	if err := writeRetainedReceipt(s.Identity, r); err != nil {
		return nil, err
	}
	if err := s.State.setRetainedReceipt(r); err != nil {
		return nil, err
	}
	if old != nil && old != conn {
		_ = old.Close()
	}
	if token != nil {
		*token = spec.Token
	}
	return &Message{Type: TypeAdmitResponse, Status: StatusAdmitted, Token: spec.Token,
		GrantedInitialAlloc: spec.InitialBudget, GrantedInitialCache: spec.InitialCache,
		ClientFeatures: featureResponse(req.ClientFeatures, req.CacheReclaim != nil), Retained: &r.Memory}, nil
}
