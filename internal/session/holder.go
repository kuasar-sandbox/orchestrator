package session

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
)

var (
	ErrHolderLimit         = errors.New("session: Holder registration limit reached")
	ErrStaleSession        = errors.New("session: stale node-link session")
	ErrEndpointChanged     = errors.New("session: data endpoint changed within NodeEpoch")
	ErrRegistrationChanged = errors.New("session: stable registration data changed within NodeEpoch")
	ErrEnrollmentChanged   = errors.New("session: node enrollment identity changed")
	ErrSessionUnavailable  = errors.New("session: current node-link session is unavailable")
	ErrPermitUnavailable   = errors.New("session: matching Serve Permit is unavailable")
	ErrKeyLeaseUnavailable = errors.New("session: exact node key lease is not durably acknowledged")
	ErrDispatchNotSent     = errors.New("session: dispatch was not sent")
)

type ServeIdentity struct {
	ClusterID            string `json:"cluster_id"`
	RegistryGeneration   string `json:"registry_generation"`
	SystemEpoch          uint64 `json:"system_epoch"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
}

func (i ServeIdentity) Validate() error {
	digest, err := hex.DecodeString(i.RegistryLayoutDigest)
	if i.ClusterID == "" || i.RegistryGeneration == "" || i.SystemEpoch == 0 || err != nil || len(digest) != 32 {
		return errors.New("session: incomplete serving identity")
	}
	return nil
}

type PermitGate interface {
	AllowSessionWork(ServeIdentity) bool
}

type SessionEndpoint interface {
	FenceStaleSession()
	AdmitAndDispatch(context.Context, DispatchCommand) (DispatchReply, error)
}

type DispatchCommand struct {
	ServeIdentity ServeIdentity
	Kind          cluster.ExecutionKind
	Group         string
	RouteKey      string
	ObjectID      string
	NodeID        string
	NodeEpoch     uint64
	SessionSeq    uint64
	DataEndpoint  string
	Intent        cluster.DispatchIntent
	Binding       cluster.ExecutionBindingIntent
}

type DispatchReply struct {
	Outcome cluster.DispatchOutcome
	Reason  string
}

type Registration struct {
	NodeID       string
	EnrollmentID string
	Tuple
	DataEndpoint     string
	RuntimeDigest    string
	LoadModelVersion uint16
	SandboxSlots     uint64
	BuildSlots       uint64
	BuildCPU         uint64
	BuildMemory      uint64
	BuildStorage     uint64
	FailureDomain    string
}

func (r Registration) Validate() error {
	if r.NodeID == "" || r.EnrollmentID == "" || !r.Tuple.Valid() || r.DataEndpoint == "" ||
		r.LoadModelVersion == 0 || (r.SandboxSlots == 0 && r.BuildSlots == 0) {
		return errors.New("session: incomplete node registration")
	}
	return nil
}

type heldSession struct {
	registration Registration
	endpoint     SessionEndpoint
	snapshot     placement.PlacementLoadSnapshot
	observedAt   time.Time
	hasSnapshot  bool
	keyLeases    map[string]int64
}

type Lease struct {
	holder *Holder
	nodeID string
	tuple  Tuple
	once   sync.Once
}

func (l *Lease) Close() {
	if l == nil || l.holder == nil {
		return
	}
	l.once.Do(func() { l.holder.remove(l.nodeID, l.tuple) })
}

type DeltaPublisher interface {
	PublishSessionDelta(DirectoryDelta)
}

type Holder struct {
	mu       sync.RWMutex
	memberID string
	limit    int
	clock    func() time.Time
	gate     PermitGate
	pub      DeltaPublisher
	enroll   EnrollmentAuthority
	active   map[string]*heldSession
	high     map[string]Registration
	keyOps   [64]sync.Mutex
}

func NewHolder(memberID string, limit int, clock func() time.Time, gate PermitGate, publisher DeltaPublisher, enrollment EnrollmentAuthority) (*Holder, error) {
	if memberID == "" || limit <= 0 || enrollment == nil {
		return nil, errors.New("session: Holder member ID, positive limit, and Enrollment authority are required")
	}
	if clock == nil {
		clock = time.Now
	}
	return &Holder{
		memberID: memberID, limit: limit, clock: clock, gate: gate, pub: publisher, enroll: enrollment,
		active: make(map[string]*heldSession), high: make(map[string]Registration),
	}, nil
}

func (h *Holder) Register(ctx context.Context, registration Registration, endpoint SessionEndpoint) (*Lease, error) {
	if err := registration.Validate(); err != nil {
		return nil, err
	}
	if endpoint == nil {
		return nil, errors.New("session: node-link endpoint is required")
	}
	enrollment := NodeEnrollment{
		NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
		NodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
	}
	var old SessionEndpoint
	err := h.enroll.RunSessionRegistration(ctx, enrollment, func() error {
		h.mu.Lock()
		previous, seen := h.high[registration.NodeID]
		if seen {
			if registration.EnrollmentID != previous.EnrollmentID {
				h.mu.Unlock()
				return ErrEnrollmentChanged
			}
			if registration.NodeEpoch == previous.NodeEpoch && registration.DataEndpoint != previous.DataEndpoint {
				h.mu.Unlock()
				return ErrEndpointChanged
			}
			if registration.NodeEpoch == previous.NodeEpoch && !registrationStableWithinEpoch(registration, previous) {
				h.mu.Unlock()
				return ErrRegistrationChanged
			}
			if registration.Tuple.Compare(previous.Tuple) <= 0 {
				h.mu.Unlock()
				return ErrStaleSession
			}
		}
		if current := h.active[registration.NodeID]; current == nil && len(h.active) >= h.limit {
			h.mu.Unlock()
			return ErrHolderLimit
		} else if current != nil {
			old = current.endpoint
		}
		h.high[registration.NodeID] = registration
		h.active[registration.NodeID] = &heldSession{
			registration: registration, endpoint: endpoint, keyLeases: make(map[string]int64),
		}
		h.mu.Unlock()

		if old != nil {
			old.FenceStaleSession()
		}
		h.publish(DirectoryDelta{Entry: h.directoryEntry(registration), Up: true})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Lease{holder: h, nodeID: registration.NodeID, tuple: registration.Tuple}, nil
}

// RetireIdentity removes tuple fencing state only after the Enrollment
// authority confirms that the node identity can never register again.
func (h *Holder) RetireIdentity(ctx context.Context, retirement IdentityRetirement) (bool, error) {
	if err := retirement.Validate(); err != nil {
		return false, err
	}
	return h.enroll.RunIdentityRetirement(ctx, retirement, func() (bool, error) {
		var endpoint SessionEndpoint
		var registration Registration
		h.mu.Lock()
		previous, seen := h.high[retirement.NodeID]
		if !seen || previous.EnrollmentID != retirement.EnrollmentID || previous.NodeEpoch > retirement.LastNodeEpoch {
			h.mu.Unlock()
			return false, nil
		}
		registration = previous
		delete(h.high, retirement.NodeID)
		if held := h.active[retirement.NodeID]; held != nil &&
			held.registration.EnrollmentID == retirement.EnrollmentID && held.registration.NodeEpoch <= retirement.LastNodeEpoch {
			endpoint = held.endpoint
			delete(h.active, retirement.NodeID)
		}
		h.mu.Unlock()

		if endpoint != nil {
			endpoint.FenceStaleSession()
		}
		h.publish(DirectoryDelta{Entry: h.directoryEntry(registration), Retired: true})
		return true, nil
	})
}

func (h *Holder) UpdateSnapshot(tuple Tuple, snapshot placement.PlacementLoadSnapshot) error {
	if err := placement.ValidateSnapshot(snapshot); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	session := h.active[snapshot.NodeID]
	if session == nil || tuple.Compare(session.registration.Tuple) != 0 || snapshot.NodeEpoch != tuple.NodeEpoch ||
		snapshot.SessionSeq != tuple.SessionSeq || snapshot.DataEndpoint != session.registration.DataEndpoint ||
		snapshot.LoadModelVersion != session.registration.LoadModelVersion {
		return ErrStaleSession
	}
	if session.hasSnapshot && snapshot.SampleSeq <= session.snapshot.SampleSeq {
		return ErrStaleSession
	}
	registration := session.registration
	if snapshot.RuntimeDigest != registration.RuntimeDigest || snapshot.SandboxSlotCapacity != registration.SandboxSlots ||
		snapshot.BuildSlotCapacity != registration.BuildSlots || snapshot.BuildCPUCapacity != registration.BuildCPU ||
		snapshot.BuildMemoryCapacity != registration.BuildMemory || snapshot.BuildStorageCapacity != registration.BuildStorage {
		return errors.New("session: placement snapshot changed stable registration data")
	}
	session.snapshot = snapshot
	session.observedAt = h.clock()
	session.hasSnapshot = true
	return nil
}

type ProbeCall struct {
	ServeIdentity ServeIdentity
	Request       placement.PlacementProbeRequest
}

func (h *Holder) Probe(ctx context.Context, call ProbeCall) (placement.PlacementProbeResponse, error) {
	if err := ctx.Err(); err != nil {
		return placement.PlacementProbeResponse{}, err
	}
	if err := h.CheckServe(call.ServeIdentity); err != nil {
		return placement.PlacementProbeResponse{}, err
	}
	h.mu.RLock()
	session := h.active[call.Request.NodeID]
	if session == nil || session.registration.NodeEpoch != call.Request.ExpectedNodeEpoch ||
		session.registration.SessionSeq != call.Request.ExpectedSessionSeq || !session.hasSnapshot {
		h.mu.RUnlock()
		return placement.PlacementProbeResponse{}, ErrSessionUnavailable
	}
	snapshot := session.snapshot
	observedAt := session.observedAt
	h.mu.RUnlock()
	age := h.clock().Sub(observedAt)
	return placement.ProbePlacement(snapshot, age, call.Request), nil
}

func (h *Holder) CheckServe(identity ServeIdentity) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if h.gate == nil || !h.gate.AllowSessionWork(identity) {
		return ErrPermitUnavailable
	}
	return nil
}

func (h *Holder) AdmitAndDispatch(ctx context.Context, command DispatchCommand) (DispatchReply, error) {
	if err := ctx.Err(); err != nil {
		return DispatchReply{}, err
	}
	if err := h.CheckServe(command.ServeIdentity); err != nil {
		return DispatchReply{}, err
	}
	if command.ServeIdentity.RegistryGeneration == "" || command.ServeIdentity.RegistryGeneration != command.Binding.RegistryGeneration ||
		command.NodeID == "" || command.NodeID != command.Binding.NodeID || command.NodeEpoch == 0 ||
		command.NodeEpoch != command.Binding.NodeEpoch || command.DataEndpoint == "" || command.DataEndpoint != command.Binding.DataEndpoint {
		return DispatchReply{}, errors.New("session: dispatch target does not match committed Binding intent")
	}
	if err := command.Intent.Validate(); err != nil {
		return DispatchReply{}, err
	}
	if err := command.Binding.ValidateWorkflow(command.Kind, command.ObjectID, command.Group, command.RouteKey, command.Intent); err != nil {
		return DispatchReply{}, err
	}

	h.mu.RLock()
	held := h.active[command.NodeID]
	if held == nil || held.registration.NodeEpoch != command.NodeEpoch || held.registration.DataEndpoint != command.DataEndpoint {
		h.mu.RUnlock()
		return DispatchReply{}, ErrSessionUnavailable
	}
	keyLeaseRef, err := dispatchKeyLeaseRef(command)
	if err != nil {
		h.mu.RUnlock()
		return DispatchReply{}, err
	}
	if held.keyLeases[keyLeaseRefID(keyLeaseRef)] <= h.clock().Unix() {
		h.mu.RUnlock()
		return DispatchReply{}, errors.Join(ErrDispatchNotSent, ErrKeyLeaseUnavailable)
	}
	endpoint := held.endpoint
	command.SessionSeq = held.registration.SessionSeq
	h.mu.RUnlock()

	reply, err := endpoint.AdmitAndDispatch(ctx, command)
	if err != nil {
		return DispatchReply{}, err
	}
	if err := reply.Outcome.Validate(); err != nil {
		return DispatchReply{}, err
	}
	return reply, nil
}

func (h *Holder) ProbeBatch(ctx context.Context, calls []ProbeCall) []placement.PlacementProbeResponse {
	responses := make([]placement.PlacementProbeResponse, len(calls))
	for index, call := range calls {
		response, err := h.Probe(ctx, call)
		if err != nil {
			response = placement.PlacementProbeResponse{
				Class: placement.ProbeStale, NodeID: call.Request.NodeID,
				NodeEpoch: call.Request.ExpectedNodeEpoch, SessionSeq: call.Request.ExpectedSessionSeq,
				LoadModelVersion: call.Request.LoadModelVersion, Reason: err.Error(),
			}
		}
		responses[index] = response
	}
	return responses
}

func (h *Holder) remove(nodeID string, tuple Tuple) {
	h.mu.Lock()
	session := h.active[nodeID]
	if session == nil || tuple.Compare(session.registration.Tuple) != 0 {
		h.mu.Unlock()
		return
	}
	registration := session.registration
	delete(h.active, nodeID)
	h.mu.Unlock()
	h.publish(DirectoryDelta{Entry: h.directoryEntry(registration), Up: false})
}

func (h *Holder) directoryEntry(registration Registration) DirectoryEntry {
	return DirectoryEntry{
		NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
		Tuple: registration.Tuple, HolderMemberID: h.memberID,
	}
}

func registrationStableWithinEpoch(left, right Registration) bool {
	return left.EnrollmentID == right.EnrollmentID && left.DataEndpoint == right.DataEndpoint &&
		left.RuntimeDigest == right.RuntimeDigest && left.LoadModelVersion == right.LoadModelVersion &&
		left.SandboxSlots == right.SandboxSlots && left.BuildSlots == right.BuildSlots &&
		left.BuildCPU == right.BuildCPU && left.BuildMemory == right.BuildMemory &&
		left.BuildStorage == right.BuildStorage && left.FailureDomain == right.FailureDomain
}

func (h *Holder) publish(delta DirectoryDelta) {
	if h.pub != nil {
		h.pub.PublishSessionDelta(delta)
	}
}

func (h *Holder) Active() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.active)
}

func (h *Holder) TrackedIdentities() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.high)
}

func (h *Holder) Registration(nodeID string) (Registration, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	session := h.active[nodeID]
	if session == nil {
		return Registration{}, false
	}
	return session.registration, true
}

func SessionMovedError(nodeID string, current DirectoryEntry, found bool) error {
	if !found {
		return fmt.Errorf("%w: node %s has no current Holder", ErrSessionUnavailable, nodeID)
	}
	return fmt.Errorf("%w: node %s is held by %s at (%d,%d)", ErrStaleSession, nodeID,
		current.HolderMemberID, current.NodeEpoch, current.SessionSeq)
}
