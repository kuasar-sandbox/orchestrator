package session

import (
	"context"
	"encoding/hex"
	"errors"
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
	ErrKeyLeaseSuperseded  = errors.New("session: key lease operation was superseded by a newer operation")
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
		r.LoadModelVersion == 0 || r.SandboxSlots == 0 {
		return errors.New("session: incomplete node registration")
	}
	if err := cluster.ValidateTCPDataEndpoint(r.DataEndpoint); err != nil {
		return err
	}
	if r.LoadModelVersion != placement.LoadModelVersion {
		return errors.New("session: unsupported placement load model version")
	}
	return nil
}

type heldSession struct {
	registration Registration
	endpoint     SessionEndpoint
	commandMu    sync.Mutex
	leaseMu      sync.RWMutex
	snapshot     placement.PlacementLoadSnapshot
	observedAt   time.Time
	hasSnapshot  bool
	keyLeases    map[string]int64
	keyLeaseSeq  map[string]uint64
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
	err := h.enroll.RunSessionRegistration(ctx, enrollment, func() error {
		for {
			h.mu.Lock()
			if err := h.validateRegistrationLocked(registration); err != nil {
				h.mu.Unlock()
				return err
			}
			current := h.active[registration.NodeID]
			if current == nil {
				h.high[registration.NodeID] = registration
				h.active[registration.NodeID] = newHeldSession(registration, endpoint)
				h.mu.Unlock()
				break
			}
			h.mu.Unlock()

			// A command on this node may be slow. Wait without retaining the
			// Holder-wide lock, then revalidate before replacing the session.
			current.commandMu.Lock()
			next := newHeldSession(registration, endpoint)
			next.commandMu.Lock()
			h.mu.Lock()
			if h.active[registration.NodeID] != current {
				h.mu.Unlock()
				next.commandMu.Unlock()
				current.commandMu.Unlock()
				continue
			}
			if err := h.validateRegistrationLocked(registration); err != nil {
				h.mu.Unlock()
				next.commandMu.Unlock()
				current.commandMu.Unlock()
				return err
			}
			h.high[registration.NodeID] = registration
			h.active[registration.NodeID] = next
			h.mu.Unlock()

			current.endpoint.FenceStaleSession()
			current.commandMu.Unlock()
			next.commandMu.Unlock()
			break
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
		for {
			h.mu.Lock()
			registration, ok := h.retirementRegistrationLocked(retirement)
			if !ok {
				h.mu.Unlock()
				return false, nil
			}
			current := h.active[retirement.NodeID]
			if current == nil {
				delete(h.high, retirement.NodeID)
				h.mu.Unlock()
				h.publish(DirectoryDelta{Entry: h.directoryEntry(registration), Retired: true})
				return true, nil
			}
			if current.registration.EnrollmentID != retirement.EnrollmentID ||
				current.registration.NodeEpoch > retirement.LastNodeEpoch {
				h.mu.Unlock()
				return false, nil
			}
			h.mu.Unlock()

			current.commandMu.Lock()
			h.mu.Lock()
			registration, ok = h.retirementRegistrationLocked(retirement)
			if !ok {
				h.mu.Unlock()
				current.commandMu.Unlock()
				return false, nil
			}
			if h.active[retirement.NodeID] != current {
				h.mu.Unlock()
				current.commandMu.Unlock()
				continue
			}
			delete(h.high, retirement.NodeID)
			delete(h.active, retirement.NodeID)
			h.mu.Unlock()

			current.endpoint.FenceStaleSession()
			current.commandMu.Unlock()
			h.publish(DirectoryDelta{Entry: h.directoryEntry(registration), Retired: true})
			return true, nil
		}
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

	held, err := h.lockCommandSession(command.NodeID, command.NodeEpoch, command.DataEndpoint, nil)
	if err != nil {
		return DispatchReply{}, ErrSessionUnavailable
	}
	defer held.commandMu.Unlock()
	keyLeaseRef, err := dispatchKeyLeaseRef(command)
	if err != nil {
		return DispatchReply{}, err
	}
	held.leaseMu.RLock()
	leaseExpires := held.keyLeases[keyLeaseRefID(keyLeaseRef)]
	held.leaseMu.RUnlock()
	if leaseExpires <= h.clock().Unix() {
		return DispatchReply{}, errors.Join(ErrDispatchNotSent, ErrKeyLeaseUnavailable)
	}
	// A command can wait behind another dispatch while its generation Permit
	// expires. Recheck after acquiring the per-session command fence and just
	// before handing the command to the transport.
	if err := h.CheckServe(command.ServeIdentity); err != nil {
		return DispatchReply{}, errors.Join(ErrDispatchNotSent, err)
	}
	endpoint := held.endpoint
	command.SessionSeq = held.registration.SessionSeq

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
	h.mu.RLock()
	session := h.active[nodeID]
	if session == nil || tuple.Compare(session.registration.Tuple) != 0 {
		h.mu.RUnlock()
		return
	}
	h.mu.RUnlock()

	session.commandMu.Lock()
	h.mu.Lock()
	if h.active[nodeID] != session || tuple.Compare(session.registration.Tuple) != 0 {
		h.mu.Unlock()
		session.commandMu.Unlock()
		return
	}
	registration := session.registration
	delete(h.active, nodeID)
	h.mu.Unlock()
	session.commandMu.Unlock()
	h.publish(DirectoryDelta{Entry: h.directoryEntry(registration), Up: false})
}

// lockCommandSession prevents a SessionSeq/NodeEpoch replacement from racing
// a command that has already selected the current endpoint. Callers must unlock
// commandMu without reacquiring Holder.mu.
func (h *Holder) lockCommandSession(
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	expected *heldSession,
) (*heldSession, error) {
	h.mu.RLock()
	held := h.active[nodeID]
	if held == nil || expected != nil && held != expected || held.registration.NodeEpoch != nodeEpoch ||
		held.registration.DataEndpoint != dataEndpoint {
		h.mu.RUnlock()
		return nil, ErrSessionUnavailable
	}
	h.mu.RUnlock()

	held.commandMu.Lock()
	h.mu.RLock()
	current := h.active[nodeID]
	valid := current == held && (expected == nil || current == expected) &&
		held.registration.NodeEpoch == nodeEpoch && held.registration.DataEndpoint == dataEndpoint
	h.mu.RUnlock()
	if !valid {
		held.commandMu.Unlock()
		return nil, ErrSessionUnavailable
	}
	return held, nil
}

func newHeldSession(registration Registration, endpoint SessionEndpoint) *heldSession {
	return &heldSession{
		registration: registration, endpoint: endpoint,
		keyLeases: make(map[string]int64), keyLeaseSeq: make(map[string]uint64),
	}
}

// validateRegistrationLocked validates against Holder high-watermarks and
// capacity. The caller holds h.mu.
func (h *Holder) validateRegistrationLocked(registration Registration) error {
	previous, seen := h.high[registration.NodeID]
	if seen {
		if registration.EnrollmentID != previous.EnrollmentID {
			return ErrEnrollmentChanged
		}
		if registration.NodeEpoch == previous.NodeEpoch && registration.DataEndpoint != previous.DataEndpoint {
			return ErrEndpointChanged
		}
		if registration.NodeEpoch == previous.NodeEpoch && !registrationStableWithinEpoch(registration, previous) {
			return ErrRegistrationChanged
		}
		if registration.Tuple.Compare(previous.Tuple) <= 0 {
			return ErrStaleSession
		}
	}
	if h.active[registration.NodeID] == nil && len(h.active) >= h.limit {
		return ErrHolderLimit
	}
	return nil
}

// retirementRegistrationLocked returns the high-watermark covered by a
// committed retirement. The caller holds h.mu.
func (h *Holder) retirementRegistrationLocked(retirement IdentityRetirement) (Registration, bool) {
	registration, found := h.high[retirement.NodeID]
	if !found || registration.EnrollmentID != retirement.EnrollmentID ||
		registration.NodeEpoch > retirement.LastNodeEpoch {
		return Registration{}, false
	}
	return registration, true
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
