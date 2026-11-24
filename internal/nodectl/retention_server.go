package nodectl

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"slices"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
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

func (s *Server) handleRetainedAccount(req *Message) *Message {
	if req.SandboxID == "" || s.Inventory == nil || !resource.HasFeature(req.ClientFeatures, resource.FeatureRetainedMemoryV2) {
		return &Message{Type: TypeError, Msg: "retained account capability and SID are required"}
	}
	return &Message{Type: TypeAck, RetainedAccount: s.State.retainedSnapshot(req.SandboxID).reference(), ClientFeatures: featureResponse(req.ClientFeatures)}
}

func (s *Server) validateRetainedSession(sid string, peerPID int, token string, r *retainedAccount, prepare bool) error {
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
	if prepare && !live.Lease.Supports(resource.FeatureRetainedMemoryV2) ||
		!prepare && !live.Lease.Supports(resource.FeatureRetainedMemoryV2) && !live.Lease.Supports(resource.FeatureRetainedMemoryV1) {
		return fmt.Errorf("checkpoint memory handoff is not negotiated")
	}
	_, owner, file, err := resource.InspectLeaseIdentity(live.Path)
	if err != nil || owner != peerPID {
		return fmt.Errorf("retention lease owner changed")
	}
	if r == nil {
		active, found := s.State.retentionReservation(sid)
		if !found || active.PeerPID != peerPID || active.Token != token || !resource.HasFeature(active.ClientFeatures, resource.FeatureRetainedMemoryV2) {
			return fmt.Errorf("retention requires the current admitted session")
		}
		return nil
	}
	if r.OwnerLease.PID != peerPID || r.OwnerFile != file {
		return fmt.Errorf("retained memory owner has changed")
	}
	if r.Stage == retentionActive {
		if active, found := s.State.retentionReservation(sid); found && (active.PeerPID != peerPID || active.Conn != nil && !active.Provisional && active.Token != token) {
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
	e := r.capture(*req.Retention)
	if e != nil {
		if err := validateRetentionProof(e, proof); err != nil {
			return bad(err)
		}
	}
	producerFinal := false
	if e != nil && peerPID > 0 && peerPID == e.Source.PeerPID &&
		(req.Type == resource.TypeReleaseRetained && proof.RAMReleased || req.Type == resource.TypeCommitRetained && e.Stage != retentionPrepared && proof.Committed && proof.RuntimeRetired) {
		start, err := retentionProcessStartTime(peerPID)
		producerFinal = err == nil && start == e.SourceStartTime
	}
	if !producerFinal {
		if err := s.validateRetainedSession(req.SandboxID, peerPID, token, r, req.Type == resource.TypePrepareRetained); err != nil {
			return bad(err)
		}
	}
	switch req.Type {
	case resource.TypePrepareRetained:
		if proof.Aborted || proof.Deleted {
			return bad(fmt.Errorf("capture was aborted or deleted"))
		}
		if e == nil {
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
			if proof.Holder.PID != peerPID || proof.Holder.StartTime != start {
				return bad(fmt.Errorf("capture holder does not own source reservation"))
			}
			if proof.VMM != nil {
				if !resource.HasFeature(active.ClientFeatures, resource.FeatureDeferredVMMExit) || proof.VMM.Cgroup.Path != active.CgroupPath {
					return bad(fmt.Errorf("deferred VMM does not own the source reservation"))
				}
				vmm, err := memorycheckpoint.IdentifyVMM(proof.VMM.PID, active.CgroupPath)
				if err != nil || vmm != *proof.VMM {
					return bad(fmt.Errorf("deferred VMM source identity not proven: %v", err))
				}
			}
			if r == nil {
				r = &retainedAccount{Version: 2, SandboxID: req.SandboxID, ID: NewToken(), Revision: 1, Stage: retentionActive,
					OwnerLease: lease, OwnerFile: file, OwnerToken: token}
			} else {
				if r.Stage != retentionActive {
					return bad(fmt.Errorf("capture requires an active account owner"))
				}
				for _, previous := range r.Captures {
					if previous.Capture.CapturePath == req.Retention.CapturePath || previous.Capture.MemoryID == req.Retention.MemoryID && previous.Capture.Epoch == req.Retention.Epoch {
						return bad(fmt.Errorf("retained capture identity changed"))
					}
				}
				r.Revision++
			}
			e = &retainedCapture{Capture: *req.Retention, Producer: proof.Producer, CheckpointID: proof.CheckpointID, Holder: proof.Holder, VMM: proof.VMM,
				Memory: resource.RetainedMemory{MemoryID: req.Retention.MemoryID, Epoch: req.Retention.Epoch, SourcePool: s.State.SandboxPool(req.SandboxID), Bytes: active.ReservationMemory},
				Stage:  retentionPrepared, Source: active, SourceStartTime: start, SourceFile: file}
			r.Captures = append(r.Captures, e)
			if err := s.State.prepareRetained(r, e.Capture); err != nil {
				return bad(err)
			}
		}
		// Failed fsync is ambiguous: protect B until exact replay/abort.
		if err := writeRetainedReceipt(s.Identity, r); err != nil {
			return bad(err)
		}
	case resource.TypeAbortRetained:
		if e == nil {
			return &Message{Type: TypeAck}
		}
		if e.Stage != retentionPrepared || proof.Committed || proof.RuntimeRetired || proof.RAMReleased {
			return bad(fmt.Errorf("capture can no longer be aborted"))
		}
		next := withoutRetainedCapture(r, e.Capture)
		if err := writeRetainedReceipt(s.Identity, next); err != nil {
			return bad(err)
		}
		if err := s.State.abortRetained(next, e.Capture); err != nil {
			return bad(err)
		}
		return &Message{Type: TypeAck}
	case resource.TypeCommitRetained:
		if e == nil {
			return bad(fmt.Errorf("retention was not prepared"))
		}
		if !proof.Committed || !proof.RuntimeRetired {
			return bad(fmt.Errorf("runtime retirement is not committed"))
		}
		if e.Stage == retentionPrepared {
			next := heldRetainedCapture(r, e.Capture)
			if err := writeRetainedReceipt(s.Identity, next); err != nil {
				return bad(err)
			}
			if _, err := s.State.commitRetained(next, e.Capture); err != nil {
				return bad(err)
			}
			r = next
		}
	case resource.TypeReleaseRetained:
		if !proof.RAMReleased {
			return bad(fmt.Errorf("physical checkpoint RAM has not been released"))
		}
		if e == nil {
			return bad(fmt.Errorf("retained capture identity is unavailable"))
		}
		if e.Stage != retentionSettled {
			next := settledRetainedCapture(r, e.Capture)
			if err := writeRetainedReceipt(s.Identity, next); err != nil {
				return bad(err)
			}
			if err := s.State.releaseRetained(next, e.Capture); err != nil {
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
	return &Message{Type: TypeAck, Retained: &e.Memory, RetainedAccount: r.reference()}
}

func (s *Server) validateOrdinaryRetainedOwner(peerPID int, req *Message) error {
	r := s.State.retainedSnapshot(req.SandboxID)
	if r == nil {
		return nil
	}
	if r.Stage != retentionActive || r.OwnerLease.PID != peerPID {
		return fmt.Errorf("retained account requires explicit owner handoff")
	}
	_, pid, file, err := resource.InspectLeaseIdentity(resource.LeasePath(s.Identity, req.SandboxID))
	if err != nil || pid != peerPID || file != r.OwnerFile {
		return fmt.Errorf("retained account lease identity changed")
	}
	return nil
}

func (s *Server) buildRetainedAdmitOK(conn net.Conn, peerPID int, req *Message, token *string) (*Message, error) {
	if req.RetainedAccount == nil || req.HandoffID == "" || req.LeaseCandidate == "" || s.Inventory == nil ||
		!resource.HasFeature(req.ClientFeatures, resource.FeatureRetainedMemoryV2) || !resource.HasFeature(req.ClientFeatures, resource.FeatureStateSyncV1) {
		return nil, fmt.Errorf("retained admission requires explicit capabilities and identity")
	}
	root := s.State.retentionOwner()
	root.retentionOps.Lock()
	defer root.retentionOps.Unlock()
	r := s.State.retainedSnapshot(req.SandboxID)
	if r == nil || req.RetainedAccount.ID != r.ID || req.RetainedAccount.Validate() != nil {
		return nil, fmt.Errorf("retained account identity unavailable or changed")
	}
	var err error
	r, err = s.Inventory.reconcileAdoption(s.State, r)
	if err != nil {
		return nil, err
	}
	if (r.Handoff == nil || r.Handoff.ID != req.HandoffID) && req.RetainedAccount.Revision != r.Revision {
		return &Message{Type: TypeAdmitResponse, Status: StatusRejected, Reason: resource.ReasonRetentionAccountChanged,
			ClientFeatures: featureResponse(req.ClientFeatures), Msg: "retained account changed before acceptance; refresh its reference"}, nil
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
	if r.prepared() != nil {
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
		start, err := retentionProcessStartTime(peerPID)
		if err != nil {
			return nil, err
		}
		r.Stage = retentionAdopting
		r.Handoff = &retainedHandoff{ID: req.HandoffID, AccountRevision: req.RetainedAccount.Revision, Candidate: req.LeaseCandidate, File: file, Lease: lease,
			Token: NewToken(), StartTime: start, Pool: s.State.SandboxPool(req.SandboxID), InitialBudget: spec.InitialBudget,
			InitialCache: spec.InitialCache, CacheReclaim: resource.CloneCacheReclaimState(req.CacheReclaim)}
		r.Revision++
		if err := validateCacheReclaimState(lease.ClientFeatures, req.CacheReclaim, maximum); err != nil {
			return nil, err
		}
		if c := req.CacheReclaim; c != nil && (c.LastSeq != 0 || c.Task != nil || c.Done != nil || c.Intent != nil) {
			return nil, fmt.Errorf("initial cache reclaim state is not empty")
		}
		spec.Token, spec.CacheCoordinated = r.Handoff.Token, req.CacheReclaim != nil
		// Even an ambiguous persistence failure owns a connection/B cleanup.
		if token != nil {
			*token = spec.Token
		}
		if err := s.State.acceptRetainedHandoff(r, spec, func() error { return writeRetainedReceipt(s.Identity, r) }); err != nil {
			return nil, err
		}
		if s.phaseHook != nil {
			s.phaseHook("retention_intent")
		}
	} else if r.Handoff.File != file || r.Handoff.Lease.PID != peerPID || r.Handoff.Candidate != req.LeaseCandidate ||
		r.Handoff.InitialBudget != spec.InitialBudget || r.Handoff.InitialCache != spec.InitialCache ||
		!reflect.DeepEqual(r.Handoff.Lease, lease) || !reflect.DeepEqual(r.Handoff.CacheReclaim, req.CacheReclaim) {
		return nil, fmt.Errorf("handoff replay contract changed")
	}
	// Retry an ambiguous inventory fsync before granting/publishing this same
	// candidate. Its B remains reserved from acceptance, even if all R settle.
	if err := writeRetainedReceipt(s.Identity, r); err != nil {
		return nil, err
	}
	spec.Token = r.Handoff.Token
	_, old, err := s.cacheCoordinator().Admit(spec, req.CacheReclaim)
	if err != nil {
		return nil, err
	}
	// The server must detach this connection on EOF even if publication or
	// receipt persistence fails before the client receives its token.
	if token != nil {
		*token = spec.Token
	}
	if old != nil && old != conn {
		_ = old.Close()
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
	r.Revision++
	if err := writeRetainedReceipt(s.Identity, r); err != nil {
		return nil, err
	}
	if err := s.State.setRetainedReceipt(r); err != nil {
		return nil, err
	}
	return &Message{Type: TypeAdmitResponse, Status: StatusAdmitted, Token: spec.Token,
		GrantedInitialAlloc: spec.InitialBudget, GrantedInitialCache: spec.InitialCache,
		ClientFeatures: featureResponse(req.ClientFeatures, req.CacheReclaim != nil), RetainedAccount: r.reference()}, nil
}
