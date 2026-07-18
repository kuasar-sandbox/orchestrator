package nodeexec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

var (
	ErrWorkflowConflict = errors.New("nodeexec: workflow conflicts with existing dispatch")
	ErrWorkflowMissing  = errors.New("nodeexec: workflow is missing")
	ErrWorkflowState    = errors.New("nodeexec: workflow state does not permit the operation")
	ErrSessionFenced    = errors.New("nodeexec: node-link session is fenced")
)

type LocalSessionIdentity struct {
	NodeID       string
	NodeEpoch    uint64
	SessionSeq   uint64
	DataEndpoint string
}

func (i LocalSessionIdentity) Validate() error {
	if i.NodeID == "" || i.NodeEpoch == 0 || i.SessionSeq == 0 || i.DataEndpoint == "" {
		return errors.New("nodeexec: incomplete local session identity")
	}
	return nil
}

type AdmissionState string

const (
	AdmissionRejected  AdmissionState = "REJECTED"
	AdmissionQueued    AdmissionState = "QUEUED"
	AdmissionAdmitted  AdmissionState = "ADMITTED"
	AdmissionLaunching AdmissionState = "LAUNCHING"
	AdmissionRunning   AdmissionState = "RUNNING"
	AdmissionTerminal  AdmissionState = "TERMINAL"
)

type DispatchRecord struct {
	Kind                  clusterstate.ExecutionKind
	ObjectID              string
	Group                 string
	RouteKey              string
	NodeID                string
	NodeEpoch             uint64
	DataEndpoint          string
	NormalizedDemand      []byte
	DemandDigest          string
	DispatchSpec          []byte
	DispatchSpecDigest    string
	ProviderPolicyVersion string
	OpaqueBinding         string
	BindingDigest         string
	BuildDemand           placement.BuildDemand
}

func DispatchRecordFromCommand(command session.DispatchCommand) (DispatchRecord, error) {
	record := DispatchRecord{
		Kind: command.Kind, ObjectID: command.ObjectID, Group: command.Group, RouteKey: command.RouteKey,
		NodeID: command.NodeID, NodeEpoch: command.NodeEpoch, DataEndpoint: command.DataEndpoint,
		NormalizedDemand:      append([]byte(nil), command.Intent.NormalizedDemand...),
		DemandDigest:          command.Intent.DemandDigest,
		DispatchSpec:          append([]byte(nil), command.Intent.DispatchSpec...),
		DispatchSpecDigest:    command.Intent.DispatchSpecDigest,
		ProviderPolicyVersion: command.Intent.ProviderPolicyVersion,
		OpaqueBinding:         command.Binding.OpaqueBinding, BindingDigest: command.Binding.BindingDigest,
	}
	if err := command.Intent.Validate(); err != nil {
		return DispatchRecord{}, err
	}
	if err := command.Binding.ValidateWorkflow(command.Kind, command.ObjectID, command.Group, command.RouteKey, command.Intent); err != nil {
		return DispatchRecord{}, err
	}
	binding, err := clusterstate.DecodeExecutionBinding(command.Binding.OpaqueBinding)
	if err != nil {
		return DispatchRecord{}, err
	}
	if binding.NodeID != command.NodeID || binding.NodeEpoch != command.NodeEpoch {
		return DispatchRecord{}, errors.New("nodeexec: Binding identifies another node execution")
	}
	demand, err := placement.ParseNormalizedDemand(command.Intent.NormalizedDemand)
	if err != nil {
		return DispatchRecord{}, err
	}
	if command.Kind == clusterstate.ExecutionKindBuild {
		if demand.Build == nil {
			return DispatchRecord{}, errors.New("nodeexec: Build dispatch has no Build demand")
		}
		record.BuildDemand = *demand.Build
	} else if demand.Sandbox == nil {
		return DispatchRecord{}, errors.New("nodeexec: Sandbox dispatch has no Sandbox demand")
	}
	return record, nil
}

// DispatchCommandFromWire reconstructs the final authenticated cluster command
// without inventing another execution identity. Sandbox uses SID; Build uses
// BuildID. Local enrolled identity supplies the immutable data endpoint.
func DispatchCommandFromWire(wire *routesync.Command, local LocalSessionIdentity) (session.DispatchCommand, error) {
	if wire == nil {
		return session.DispatchCommand{}, errors.New("nodeexec: nil dispatch command")
	}
	if err := local.Validate(); err != nil {
		return session.DispatchCommand{}, err
	}
	command := session.DispatchCommand{
		Group: wire.Group, RouteKey: wire.RouteKey,
		NodeID: local.NodeID, NodeEpoch: wire.NodeEpoch, SessionSeq: wire.SessionSeq,
		DataEndpoint: local.DataEndpoint,
		Intent: clusterstate.DispatchIntent{
			NormalizedDemand:      append([]byte(nil), wire.NormalizedDemand...),
			DemandDigest:          wire.DemandDigest,
			DispatchSpec:          append([]byte(nil), wire.DispatchSpec...),
			DispatchSpecDigest:    wire.DispatchSpecDigest,
			ProviderPolicyVersion: wire.ProviderPolicy,
		},
		Binding: clusterstate.ExecutionBindingIntent{
			NodeID: local.NodeID, NodeEpoch: wire.NodeEpoch, DataEndpoint: local.DataEndpoint,
			StorageGeneration: wire.StorageGeneration,
			OpaqueBinding:     wire.Binding, BindingDigest: wire.BindingDigest,
		},
	}
	switch wire.Kind {
	case routesync.CmdSandboxAdmitDispatch:
		command.Kind = clusterstate.ExecutionKindSandbox
		command.ObjectID = wire.SID
	case routesync.CmdBuildAdmitDispatch:
		command.Kind = clusterstate.ExecutionKindBuild
		command.ObjectID = wire.BuildID
	default:
		return session.DispatchCommand{}, errors.New("nodeexec: wire command is not AdmitAndDispatch")
	}
	if _, err := DispatchRecordFromCommand(command); err != nil {
		return session.DispatchCommand{}, err
	}
	return command, nil
}

func (r DispatchRecord) Validate() error {
	if r.ObjectID == "" || r.Group == "" || r.NodeID == "" || r.NodeEpoch == 0 || r.DataEndpoint == "" ||
		len(r.NormalizedDemand) == 0 || len(r.DispatchSpec) == 0 || r.ProviderPolicyVersion == "" || r.OpaqueBinding == "" {
		return errors.New("nodeexec: incomplete dispatch record")
	}
	if r.Kind == clusterstate.ExecutionKindSandbox && r.RouteKey == "" {
		return errors.New("nodeexec: Sandbox route key is required")
	}
	if r.Kind == clusterstate.ExecutionKindBuild && r.RouteKey != "" {
		return errors.New("nodeexec: Build route key must be empty")
	}
	if err := checkDigest(r.NormalizedDemand, r.DemandDigest); err != nil {
		return fmt.Errorf("nodeexec: demand: %w", err)
	}
	if err := checkDigest(r.DispatchSpec, r.DispatchSpecDigest); err != nil {
		return fmt.Errorf("nodeexec: dispatch spec: %w", err)
	}
	binding, err := clusterstate.DecodeExecutionBinding(r.OpaqueBinding)
	if err != nil {
		return err
	}
	bindingDigest, err := clusterstate.ExecutionBindingDigest(r.OpaqueBinding)
	if err != nil {
		return err
	}
	if bindingDigest != r.BindingDigest || binding.Kind != r.Kind || binding.ObjectID != r.ObjectID ||
		binding.Group != r.Group || binding.RouteKey != r.RouteKey || binding.NodeID != r.NodeID || binding.NodeEpoch != r.NodeEpoch ||
		hex.EncodeToString(binding.DemandDigest[:]) != r.DemandDigest ||
		hex.EncodeToString(binding.DispatchSpecDigest[:]) != r.DispatchSpecDigest {
		return errors.New("nodeexec: dispatch record and Binding disagree")
	}
	parsed, err := placement.ParseNormalizedDemand(r.NormalizedDemand)
	if err != nil {
		return err
	}
	switch r.Kind {
	case clusterstate.ExecutionKindSandbox:
		if parsed.Kind != placement.ObjectSandbox {
			return errors.New("nodeexec: Sandbox record carries another demand kind")
		}
	case clusterstate.ExecutionKindBuild:
		if parsed.Kind != placement.ObjectBuild || parsed.Build == nil || *parsed.Build != r.BuildDemand {
			return errors.New("nodeexec: Build record demand projection differs from normalized demand")
		}
	default:
		return errors.New("nodeexec: unsupported execution kind")
	}
	return nil
}

func (r DispatchRecord) SameDispatch(other DispatchRecord) bool {
	return r.Kind == other.Kind && r.ObjectID == other.ObjectID && r.Group == other.Group &&
		r.RouteKey == other.RouteKey && r.NodeID == other.NodeID && r.NodeEpoch == other.NodeEpoch &&
		r.DataEndpoint == other.DataEndpoint && r.DemandDigest == other.DemandDigest &&
		r.DispatchSpecDigest == other.DispatchSpecDigest && r.ProviderPolicyVersion == other.ProviderPolicyVersion &&
		r.OpaqueBinding == other.OpaqueBinding &&
		r.BindingDigest == other.BindingDigest && bytes.Equal(r.NormalizedDemand, other.NormalizedDemand) &&
		bytes.Equal(r.DispatchSpec, other.DispatchSpec)
}

func checkDigest(value []byte, encoded string) error {
	digest, err := hex.DecodeString(encoded)
	if err != nil || len(digest) != sha256.Size {
		return errors.New("digest is not SHA-256 hex")
	}
	want := sha256.Sum256(value)
	if !bytes.Equal(digest, want[:]) {
		return errors.New("digest does not match bytes")
	}
	return nil
}

type WorkflowRecord struct {
	DispatchRecord
	AdmissionState    AdmissionState
	Result            clusterstate.DispatchOutcome
	Reason            string
	ReservationToken  string
	QueueSequence     uint64
	ResourceClaimed   bool
	ObjectState       string
	EventSeq          uint64
	AckedEventSeq     uint64
	LatestEvent       *routesync.ExecutionEvent
	WorkflowFinalized bool
}

// AdmissionDecision is the durable node-local result that is journaled before
// the Holder receives an accepted or definitive response. Result never changes
// on retry even when a queued workflow is later admitted.
type AdmissionDecision struct {
	State            AdmissionState
	Result           clusterstate.DispatchOutcome
	Reason           string
	ReservationToken string
}

func (d AdmissionDecision) Validate() error {
	switch d.Result {
	case clusterstate.DispatchAcceptedAdmitted:
		if d.State != AdmissionAdmitted || d.ReservationToken == "" || d.Reason != "" {
			return errors.New("nodeexec: admitted decision requires an admitted reservation")
		}
	case clusterstate.DispatchAcceptedQueued:
		if d.State != AdmissionQueued || d.ReservationToken == "" || d.Reason != "" {
			return errors.New("nodeexec: queued decision requires a durable reservation token")
		}
	case clusterstate.DispatchDefinitiveReject:
		if d.State != AdmissionRejected || d.ReservationToken != "" || d.Reason == "" {
			return errors.New("nodeexec: definitive rejection requires a reason and no reservation")
		}
	default:
		return errors.New("nodeexec: decision is not a durable Admission outcome")
	}
	return nil
}

// EventUpdate contains only the bounded projection supplied by node execution
// code. Store code fills and fences identity, Binding, generation, and event_seq.
type EventUpdate struct {
	State              string
	AccessToken        string
	TrafficAccessToken string
	TemplateRef        string
	SnapshotLocation   string
	ArtifactRef        string
	Reason             string
}

type BuildCapacity struct {
	Slots      uint64
	CPU        uint64
	Memory     uint64
	Storage    uint64
	QueueLimit int
}

func (c BuildCapacity) Validate() error {
	if c.Slots == 0 || c.QueueLimit <= 0 {
		return errors.New("nodeexec: Build capacity and queue limit are required")
	}
	return nil
}

func (c BuildCapacity) CanEverFit(demand placement.BuildDemand) bool {
	return demand.Slots <= c.Slots && demand.CPU <= c.CPU &&
		demand.Memory <= c.Memory && demand.Storage <= c.Storage
}

type BuildUsage struct {
	Slots   uint64
	CPU     uint64
	Memory  uint64
	Storage uint64
}

func (u BuildUsage) Add(demand placement.BuildDemand) BuildUsage {
	return BuildUsage{
		Slots: saturatingAdd(u.Slots, demand.Slots), CPU: saturatingAdd(u.CPU, demand.CPU),
		Memory: saturatingAdd(u.Memory, demand.Memory), Storage: saturatingAdd(u.Storage, demand.Storage),
	}
}

func (u BuildUsage) Fits(capacity BuildCapacity, demand placement.BuildDemand) bool {
	if !capacity.CanEverFit(demand) {
		return false
	}
	return u.Slots <= capacity.Slots-demand.Slots &&
		u.CPU <= capacity.CPU-demand.CPU &&
		u.Memory <= capacity.Memory-demand.Memory &&
		u.Storage <= capacity.Storage-demand.Storage
}

func saturatingAdd(left, right uint64) uint64 {
	if right > math.MaxUint64-left {
		return math.MaxUint64
	}
	return left + right
}
