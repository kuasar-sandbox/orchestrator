package nodectl

import (
	"encoding/hex"
	"errors"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	PreparedQueued   = "QUEUED"
	PreparedAdmitted = "ADMITTED"
	PreparedClaimed  = "CLAIMED"
	PreparedRejected = "REJECTED"
	PreparedReleased = "RELEASED"
)

var (
	ErrPreparedAdmissionConflict = errors.New("nodectl: prepared admission conflicts with existing demand")
	ErrPreparedAdmissionMissing  = errors.New("nodectl: prepared admission is missing")
	ErrPreparedAdmissionState    = errors.New("nodectl: prepared admission state does not permit the operation")
)

type SandboxAdmissionDemand struct {
	CapacityMemoryBytes   uint64  `json:"capacity_memory_bytes"`
	CapacityCPU           int     `json:"capacity_cpu"`
	FloorMemoryBytes      uint64  `json:"floor_memory_bytes"`
	FloorCPU              float64 `json:"floor_cpu"`
	StartupBudgetMemory   uint64  `json:"startup_budget_memory"`
	AllocatableAtSnapshot uint64  `json:"allocatable_at_snapshot"`
	CgroupPath            string  `json:"cgroup_path,omitempty"`
}

func (d SandboxAdmissionDemand) message(sandboxID string) *Message {
	return &Message{
		Type: TypeAdmit, SandboxID: sandboxID,
		CapacityMemoryBytes: d.CapacityMemoryBytes, CapacityCPU: d.CapacityCPU,
		FloorMemoryBytes: d.FloorMemoryBytes, FloorCPU: d.FloorCPU,
		StartupBudgetMemory: d.StartupBudgetMemory, AllocatableAtSnapshot: d.AllocatableAtSnapshot,
		CgroupPath: d.CgroupPath,
	}
}

func (d SandboxAdmissionDemand) Validate() error {
	if d.CapacityCPU < 0 || d.FloorCPU < 0 || math.IsNaN(d.FloorCPU) || math.IsInf(d.FloorCPU, 0) {
		return errors.New("nodectl: sandbox CPU demand is invalid")
	}
	if d.FloorMemoryBytes > d.CapacityMemoryBytes || d.FloorCPU > float64(d.CapacityCPU) {
		return errors.New("nodectl: sandbox floor exceeds capacity")
	}
	if computeEffectiveStartupBudget(d.message("validation")) == 0 {
		return errors.New("nodectl: sandbox startup demand is empty")
	}
	return nil
}

type PreparedSandboxAdmission struct {
	SandboxID        string                 `json:"sandbox_id"`
	DemandDigest     string                 `json:"demand_digest"`
	Demand           SandboxAdmissionDemand `json:"demand"`
	State            string                 `json:"state"`
	ReservationToken string                 `json:"reservation_token"`
	QueueSequence    uint64                 `json:"queue_sequence,omitempty"`
	QueuedAt         time.Time              `json:"queued_at,omitempty"`
	Reason           string                 `json:"reason,omitempty"`
	UpdatedAt        time.Time              `json:"updated_at"`
}

type PreparedAdmissionResult struct {
	SandboxID        string
	DemandDigest     string
	State            string
	ReservationToken string
	Reason           string
}

func preparedResult(record *PreparedSandboxAdmission) PreparedAdmissionResult {
	return PreparedAdmissionResult{
		SandboxID: record.SandboxID, DemandDigest: record.DemandDigest,
		State: record.State, ReservationToken: record.ReservationToken, Reason: record.Reason,
	}
}

// PreparedAdmissionController is the durable, asynchronous cluster Admission
// path. It is independent of the legacy connection-bound Admit queue.
type PreparedAdmissionController struct {
	mu        sync.Mutex
	state     *State
	admission *AdmissionController
	persister *Persister
	queueMax  int
	queueTTL  time.Duration
	clock     func() time.Time
	wakeCh    chan struct{}
}

func NewPreparedAdmissionController(
	state *State,
	admission *AdmissionController,
	persister *Persister,
	policy AdmissionPolicy,
) (*PreparedAdmissionController, error) {
	if state == nil || admission == nil || persister == nil || policy.QueueMaxDepth <= 0 {
		return nil, errors.New("nodectl: prepared admission requires state, admission, persister, and positive queue limit")
	}
	return &PreparedAdmissionController{
		state: state, admission: admission, persister: persister,
		queueMax: policy.QueueMaxDepth, queueTTL: policy.QueueTTL, clock: time.Now,
		wakeCh: make(chan struct{}, 1),
	}, nil
}

func (c *PreparedAdmissionController) Wake() <-chan struct{} { return c.wakeCh }

func validatePreparedInput(sandboxID, demandDigest string) error {
	if sandboxID == "" {
		return errors.New("nodectl: sandbox ID is required")
	}
	digest, err := hex.DecodeString(demandDigest)
	if err != nil || len(digest) != 32 {
		return errors.New("nodectl: demand digest must be a SHA-256 hex digest")
	}
	return nil
}

func (c *PreparedAdmissionController) GetAdmission(
	sandboxID string,
	demandDigest string,
) (PreparedAdmissionResult, error) {
	if err := validatePreparedInput(sandboxID, demandDigest); err != nil {
		return PreparedAdmissionResult{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Lock()
	defer c.state.Unlock()
	record := c.state.PreparedSandboxAdmissions[sandboxID]
	if record == nil {
		return PreparedAdmissionResult{}, ErrPreparedAdmissionMissing
	}
	if record.DemandDigest != demandDigest {
		return PreparedAdmissionResult{}, ErrPreparedAdmissionConflict
	}
	return preparedResult(record), nil
}

func (c *PreparedAdmissionController) PrepareAdmission(
	sandboxID string,
	demandDigest string,
	demand SandboxAdmissionDemand,
) (PreparedAdmissionResult, error) {
	if err := validatePreparedInput(sandboxID, demandDigest); err != nil {
		return PreparedAdmissionResult{}, err
	}
	if err := demand.Validate(); err != nil {
		return PreparedAdmissionResult{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.state.Lock()
	if existing := c.state.PreparedSandboxAdmissions[sandboxID]; existing != nil {
		result := preparedResult(existing)
		c.state.Unlock()
		if existing.DemandDigest != demandDigest {
			return PreparedAdmissionResult{}, ErrPreparedAdmissionConflict
		}
		return result, nil
	}
	c.state.Unlock()

	outcome := c.admission.AnalyzeAndConsume(demand.message(sandboxID))
	now := c.clock()
	record := &PreparedSandboxAdmission{
		SandboxID: sandboxID, DemandDigest: demandDigest, Demand: demand,
		ReservationToken: NewToken(), UpdatedAt: now,
	}
	switch outcome.Status {
	case OutcomeAdmitted:
		record.State = PreparedAdmitted
	case OutcomeShortTermBlock:
		record.State = PreparedQueued
		record.QueuedAt = now
	case OutcomePreCheckReject, OutcomeLongTermReject:
		record.State = PreparedRejected
		record.ReservationToken = ""
		record.Reason = outcome.RejectCode
	default:
		return PreparedAdmissionResult{}, errors.New("nodectl: unknown prepared admission outcome")
	}

	c.state.Lock()
	if record.State == PreparedQueued {
		if preparedQueueDepthLocked(c.state) >= c.queueMax {
			record.State = PreparedRejected
			record.ReservationToken = ""
			record.Reason = "queue_full"
		} else {
			if c.state.NextPreparedQueueSeq == math.MaxUint64 {
				c.state.Unlock()
				return PreparedAdmissionResult{}, errors.New("nodectl: prepared Admission queue sequence exhausted")
			}
			c.state.NextPreparedQueueSeq++
			record.QueueSequence = c.state.NextPreparedQueueSeq
		}
	}
	if record.State == PreparedAdmitted {
		if err := c.insertReservationLocked(record); err != nil {
			c.state.Unlock()
			return PreparedAdmissionResult{}, err
		}
	}
	c.state.PreparedSandboxAdmissions[sandboxID] = record
	if err := c.persister.Flush(c.state); err != nil {
		delete(c.state.PreparedSandboxAdmissions, sandboxID)
		if record.State == PreparedAdmitted {
			c.state.Remove(record.ReservationToken)
		}
		c.state.Unlock()
		return PreparedAdmissionResult{}, err
	}
	c.state.Unlock()
	result := preparedResult(record)
	c.notify()
	return result, nil
}

func (c *PreparedAdmissionController) ClaimAdmission(sandboxID, demandDigest string) (PreparedAdmissionResult, error) {
	if err := validatePreparedInput(sandboxID, demandDigest); err != nil {
		return PreparedAdmissionResult{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Lock()
	record := c.state.PreparedSandboxAdmissions[sandboxID]
	if record == nil {
		c.state.Unlock()
		return PreparedAdmissionResult{}, ErrPreparedAdmissionMissing
	}
	if record.DemandDigest != demandDigest {
		c.state.Unlock()
		return PreparedAdmissionResult{}, ErrPreparedAdmissionConflict
	}
	if record.State != PreparedAdmitted && record.State != PreparedClaimed {
		result := preparedResult(record)
		c.state.Unlock()
		return result, nil
	}
	if record.State == PreparedAdmitted {
		previous := *record
		record.State = PreparedClaimed
		record.UpdatedAt = c.clock()
		if err := c.persister.Flush(c.state); err != nil {
			*record = previous
			c.state.Unlock()
			return PreparedAdmissionResult{}, err
		}
	}
	result := preparedResult(record)
	c.state.Unlock()
	return result, nil
}

func (c *PreparedAdmissionController) ReleaseAdmission(sandboxID, demandDigest, reason string) (PreparedAdmissionResult, error) {
	if err := validatePreparedInput(sandboxID, demandDigest); err != nil {
		return PreparedAdmissionResult{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Lock()
	record := c.state.PreparedSandboxAdmissions[sandboxID]
	if record == nil {
		c.state.Unlock()
		return PreparedAdmissionResult{}, ErrPreparedAdmissionMissing
	}
	if record.DemandDigest != demandDigest {
		c.state.Unlock()
		return PreparedAdmissionResult{}, ErrPreparedAdmissionConflict
	}
	if record.State == PreparedReleased {
		result := preparedResult(record)
		c.state.Unlock()
		return result, nil
	}
	previous := *record
	reservation := c.state.Lookup(record.ReservationToken)
	record.State = PreparedReleased
	record.Reason = reason
	record.UpdatedAt = c.clock()
	c.state.Remove(record.ReservationToken)
	if err := c.persister.Flush(c.state); err != nil {
		*record = previous
		if reservation != nil {
			c.state.Reservations[reservation.Token] = reservation
		}
		c.state.Unlock()
		return PreparedAdmissionResult{}, err
	}
	result := preparedResult(record)
	c.state.Unlock()
	c.admission.PushWake()
	c.notify()
	return result, nil
}

// FinalizeAdmission removes local SID/demand dedupe only after Registry proves
// delayed-command fencing is no longer required and local resources are gone.
func (c *PreparedAdmissionController) FinalizeAdmission(sandboxID, demandDigest string) error {
	if err := validatePreparedInput(sandboxID, demandDigest); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Lock()
	record := c.state.PreparedSandboxAdmissions[sandboxID]
	if record == nil {
		c.state.Unlock()
		return nil
	}
	if record.DemandDigest != demandDigest {
		c.state.Unlock()
		return ErrPreparedAdmissionConflict
	}
	if record.State != PreparedRejected && record.State != PreparedReleased {
		c.state.Unlock()
		return ErrPreparedAdmissionState
	}
	if record.ReservationToken != "" && c.state.Lookup(record.ReservationToken) != nil {
		c.state.Unlock()
		return ErrPreparedAdmissionState
	}
	delete(c.state.PreparedSandboxAdmissions, sandboxID)
	if err := c.persister.Flush(c.state); err != nil {
		c.state.PreparedSandboxAdmissions[sandboxID] = record
		c.state.Unlock()
		return err
	}
	c.state.Unlock()
	return nil
}

func (c *PreparedAdmissionController) PromoteQueued() ([]PreparedAdmissionResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Lock()
	queue := make([]*PreparedSandboxAdmission, 0)
	for _, record := range c.state.PreparedSandboxAdmissions {
		if record.State == PreparedQueued {
			queue = append(queue, record)
		}
	}
	c.state.Unlock()
	sort.Slice(queue, func(i, j int) bool { return queue[i].QueueSequence < queue[j].QueueSequence })

	changed := make([]PreparedAdmissionResult, 0)
	for _, queued := range queue {
		outcome := c.admission.AnalyzeAndConsume(queued.Demand.message(queued.SandboxID))
		if outcome.Status == OutcomeShortTermBlock {
			if c.queueTTL <= 0 || c.clock().Sub(queued.QueuedAt) < c.queueTTL {
				break
			}
			outcome = Outcome{Status: OutcomeLongTermReject, RejectCode: "queue_expired"}
		}
		c.state.Lock()
		record := c.state.PreparedSandboxAdmissions[queued.SandboxID]
		if record == nil || record.State != PreparedQueued || record.DemandDigest != queued.DemandDigest {
			c.state.Unlock()
			continue
		}
		previous := *record
		switch outcome.Status {
		case OutcomeAdmitted:
			record.State = PreparedAdmitted
			if err := c.insertReservationLocked(record); err != nil {
				c.state.Unlock()
				return changed, err
			}
		case OutcomeLongTermReject, OutcomePreCheckReject:
			record.State = PreparedRejected
			record.ReservationToken = ""
			record.Reason = outcome.RejectCode
		default:
			c.state.Unlock()
			return changed, errors.New("nodectl: unknown queued admission outcome")
		}
		record.UpdatedAt = c.clock()
		if err := c.persister.Flush(c.state); err != nil {
			if record.State == PreparedAdmitted {
				c.state.Remove(record.ReservationToken)
			}
			*record = previous
			c.state.Unlock()
			return changed, err
		}
		result := preparedResult(record)
		c.state.Unlock()
		changed = append(changed, result)
		c.notify()
	}
	return changed, nil
}

func (c *PreparedAdmissionController) insertReservationLocked(record *PreparedSandboxAdmission) error {
	demand := record.Demand
	budget := computeEffectiveStartupBudget(demand.message(record.SandboxID))
	now := c.clock()
	return c.state.Insert(&Reservation{
		Token: record.ReservationToken, SandboxID: record.SandboxID, CgroupPath: demand.CgroupPath,
		Capacity:          Resources{MemoryBytes: demand.CapacityMemoryBytes, CPUMilli: uint64(demand.CapacityCPU) * 1000},
		Floor:             Resources{MemoryBytes: demand.FloorMemoryBytes, CPUMilli: uint64(demand.FloorCPU * 1000)},
		AllocatableNowMem: budget, EffectiveStartupBudget: budget,
		Stage: StageAdmitted, StageEnteredAt: now, LastHeartbeatAt: now,
	})
}

func preparedQueueDepthLocked(state *State) int {
	depth := 0
	for _, record := range state.PreparedSandboxAdmissions {
		if record.State == PreparedQueued {
			depth++
		}
	}
	return depth
}

func (c *PreparedAdmissionController) notify() {
	select {
	case c.wakeCh <- struct{}{}:
	default:
	}
}
