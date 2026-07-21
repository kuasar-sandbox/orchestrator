package routesync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"

	"github.com/kuasar-sandbox/orchestrator/internal/placementproto"
)

// Cluster node-link message types (node.md §10 / cluster.md). They extend the
// Msg union for the node <-> registry channel: the node DIALS the registry and is
// the execution-state authority (its sandbox routes flow as ID-only
// Upsert/Delete + Bookmark), while the registry resolves cluster identity from
// its per-node ownership table and sends Commands the other way. The frame codec and
// the ServeAuthority loop are the same routesync engine the proxy plane uses;
// only the handshake (NodeRegister vs Hello) and the uplink (Command vs Wake)
// differ. Only Sandbox lifecycle facts use the ordinary event stream; Build
// lifecycle remains node-local and appears only in explicit recovery reports.
const (
	TypeNodeRegister   = "node_register"   // node -> registry (node identity; first up-frame)
	TypePlacementLoad  = "placement_load"  // node -> Holder request-time placement snapshot
	TypeCommand        = "command"         // Holder -> node fenced workflow command
	TypeCmdAck         = "cmd_ack"         // node -> registry (command accepted / rejected)
	TypeExecutionEvent = "execution_event" // node -> registry durable Sandbox fact
	TypeEventAck       = "event_ack"       // registry -> node (committed event watermark)
)

type PlacementLoadSnapshot = placementproto.PlacementLoadSnapshot

// NodeLinkPath is the HTTP path node-ctl conductor serve dials to open its node_link
// channel to the registry.
const NodeLinkPath = "/node-link/session"

type SessionTuple struct {
	NodeEpoch  uint64
	SessionSeq uint64
}

// Command kinds (Command.Kind) — the lifecycle + key primitives the registry
// drives the node with. The node executes via its existing e2b lifecycle (the
// command just carries the intent) and reports the terminal state on the route
// stream. Command callers wait for ack acceptance; Reserve completion waits on
// the route event.
const (
	CmdSandboxAdmitDispatch = "sandbox_admit_dispatch"
	CmdBuildAdmitDispatch   = "build_admit_dispatch"
	CmdSandboxResume        = "sandbox_resume"
	CmdSandboxDelete        = "sandbox_delete"
	CmdKeyPut               = "key_put"
	CmdKeyDrop              = "key_drop"
	CmdRebindExecution      = "rebind_execution"
	CmdAckRecoveryEvent     = "ack_recovery_event"
	CmdFinalizeWorkflow     = "finalize_workflow"
	CmdCollectRecovery      = "collect_recovery_report"
)

const (
	MaxRecoveryPageObjects = uint32(256)
	MaxRecoveryPageBytes   = uint32(768 << 10)
)

// RecoveryReportRequest asks the current fenced node-link session for one page
// of a source Registry History Generation execution report. The report itself is derived only
// from durable node-local workflows and protected object Bindings.
type RecoveryReportRequest struct {
	RecoveryEpoch              uint64 `json:"recovery_epoch"`
	SourceClusterID            string `json:"source_cluster_id"`
	SourceRegistryGeneration   string `json:"source_registry_generation"`
	SourceRegistryLayoutDigest string `json:"source_registry_layout_digest"`
	TargetRegistryGeneration   string `json:"target_registry_generation"`
	TargetRegistryLayoutDigest string `json:"target_registry_layout_digest"`
	Offset                     uint64 `json:"offset,omitempty"`
	Limit                      uint32 `json:"limit"`
	MaxBytes                   uint32 `json:"max_bytes"`
	ExpectedReportDigest       string `json:"expected_report_digest,omitempty"`
}

func (r RecoveryReportRequest) Validate() error {
	if r.RecoveryEpoch == 0 || r.SourceClusterID == "" || r.SourceRegistryGeneration == "" ||
		r.TargetRegistryGeneration == "" || r.SourceRegistryGeneration == r.TargetRegistryGeneration ||
		!validSHA256(r.SourceRegistryLayoutDigest) || !validSHA256(r.TargetRegistryLayoutDigest) ||
		r.Limit == 0 || r.Limit > MaxRecoveryPageObjects || r.MaxBytes == 0 || r.MaxBytes > MaxRecoveryPageBytes ||
		(r.ExpectedReportDigest != "" && !validSHA256(r.ExpectedReportDigest)) {
		return errors.New("routesync: invalid recovery report request")
	}
	return nil
}

// RecoveryExecutionFact is one node-authoritative object snapshot. It carries
// no new execution identity: Object.ObjectID is the SandboxID or BuildID and the
// protected opaque Binding supplies the frozen logical identity.
type RecoveryExecutionFact struct {
	Object                RecoveryObjectSnapshot `json:"object"`
	NormalizedDemand      []byte                 `json:"normalized_demand"`
	DemandDigest          string                 `json:"demand_digest"`
	DispatchSpec          []byte                 `json:"dispatch_spec"`
	DispatchSpecDigest    string                 `json:"dispatch_spec_digest"`
	ProviderPolicyVersion string                 `json:"provider_policy_version"`
	AdmissionState        string                 `json:"admission_state"`
	ResourceClaimed       bool                   `json:"resource_claimed,omitempty"`
}

func (f RecoveryExecutionFact) Validate() error {
	if err := f.Object.Validate(); err != nil {
		return err
	}
	if len(f.NormalizedDemand) == 0 || len(f.DispatchSpec) == 0 || f.ProviderPolicyVersion == "" ||
		f.AdmissionState == "" || !digestBytes(f.DemandDigest, f.NormalizedDemand) ||
		!digestBytes(f.DispatchSpecDigest, f.DispatchSpec) {
		return errors.New("routesync: invalid recovery execution fact")
	}
	return nil
}

type RecoveryObjectSnapshot struct {
	ObjectKind         string `json:"object_kind"`
	ObjectID           string `json:"object_id"`
	NodeID             string `json:"node_id"`
	NodeEpoch          uint64 `json:"node_epoch"`
	RegistryGeneration string `json:"registry_generation"`
	Binding            string `json:"binding"`
	BindingDigest      string `json:"binding_digest"`
	EventSeq           uint64 `json:"event_seq,omitempty"`
	State              string `json:"state"`

	DataEndpoint       string `json:"data_endpoint,omitempty"`
	TargetPort         int    `json:"target_port,omitempty"`
	AccessToken        string `json:"access_token,omitempty"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	TemplateRef        string `json:"template_ref,omitempty"`
	SnapshotRef        string `json:"snapshot_ref,omitempty"`
	SnapshotLocation   string `json:"snapshot_location,omitempty"`
	ArtifactRef        string `json:"artifact_ref,omitempty"`
	Reason             string `json:"reason,omitempty"`
}

func (s RecoveryObjectSnapshot) Validate() error {
	if s.ObjectID == "" || s.NodeID == "" || s.NodeEpoch == 0 || s.RegistryGeneration == "" || s.State == "" {
		return errors.New("routesync: incomplete recovery object snapshot")
	}
	if s.ObjectKind != "sandbox" && s.ObjectKind != "build" {
		return errors.New("routesync: invalid recovery object kind")
	}
	if s.ObjectKind == "sandbox" && s.EventSeq == 0 || s.ObjectKind == "build" && s.EventSeq != 0 {
		return errors.New("routesync: recovery event sequence is valid only for Sandbox snapshots")
	}
	digest, err := hex.DecodeString(s.BindingDigest)
	if err != nil || len(digest) != sha256.Size || s.Binding == "" {
		return errors.New("routesync: recovery Binding digest must be SHA-256 hex")
	}
	want := sha256.Sum256([]byte(s.Binding))
	if hex.EncodeToString(want[:]) != s.BindingDigest {
		return errors.New("routesync: recovery Binding digest mismatch")
	}
	return nil
}

// RecoveryReportPage is returned in the CmdAck for CmdCollectRecovery. Every
// page repeats the whole-report digest so a coordinator never combines pages
// from different node snapshots.
type RecoveryReportPage struct {
	RecoveryEpoch              uint64                  `json:"recovery_epoch"`
	SourceClusterID            string                  `json:"source_cluster_id"`
	SourceRegistryGeneration   string                  `json:"source_registry_generation"`
	SourceRegistryLayoutDigest string                  `json:"source_registry_layout_digest"`
	TargetRegistryGeneration   string                  `json:"target_registry_generation"`
	TargetRegistryLayoutDigest string                  `json:"target_registry_layout_digest"`
	NodeID                     string                  `json:"node_id"`
	NodeEpoch                  uint64                  `json:"node_epoch"`
	SessionSeq                 uint64                  `json:"session_seq"`
	ReportDigest               string                  `json:"report_digest"`
	TotalObjects               uint64                  `json:"total_objects"`
	Offset                     uint64                  `json:"offset"`
	NextOffset                 uint64                  `json:"next_offset"`
	Complete                   bool                    `json:"complete"`
	Objects                    []RecoveryExecutionFact `json:"objects"`
}

func (p RecoveryReportPage) ValidateFor(request RecoveryReportRequest, nodeID string, nodeEpoch, sessionSeq uint64) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if p.RecoveryEpoch != request.RecoveryEpoch || p.SourceClusterID != request.SourceClusterID ||
		p.SourceRegistryGeneration != request.SourceRegistryGeneration ||
		p.SourceRegistryLayoutDigest != request.SourceRegistryLayoutDigest ||
		p.TargetRegistryGeneration != request.TargetRegistryGeneration ||
		p.TargetRegistryLayoutDigest != request.TargetRegistryLayoutDigest || p.NodeID != nodeID ||
		p.NodeEpoch != nodeEpoch || p.SessionSeq != sessionSeq || p.Offset != request.Offset ||
		!validSHA256(p.ReportDigest) || p.Offset > p.TotalObjects || p.NextOffset < p.Offset ||
		p.NextOffset > p.TotalObjects || uint64(len(p.Objects)) != p.NextOffset-p.Offset ||
		uint32(len(p.Objects)) > request.Limit || p.NextOffset > p.Offset+uint64(request.Limit) ||
		p.Complete != (p.NextOffset == p.TotalObjects) ||
		(request.ExpectedReportDigest != "" && p.ReportDigest != request.ExpectedReportDigest) {
		return errors.New("routesync: recovery report page identity mismatch")
	}
	encoded, err := json.Marshal(p.Objects)
	if err != nil || uint32(len(encoded)) > request.MaxBytes {
		return errors.New("routesync: recovery report page exceeds its byte limit")
	}
	for index := range p.Objects {
		if err := p.Objects[index].Validate(); err != nil {
			return err
		}
		if p.Objects[index].Object.NodeID != nodeID || p.Objects[index].Object.NodeEpoch != nodeEpoch ||
			p.Objects[index].Object.RegistryGeneration != request.SourceRegistryGeneration {
			return errors.New("routesync: recovery report object belongs to another NodeEpoch")
		}
		if index > 0 && compareRecoveryFacts(p.Objects[index-1], p.Objects[index]) >= 0 {
			return errors.New("routesync: recovery report page is not strictly ordered")
		}
	}
	return nil
}

func CanonicalRecoveryReportDigest(objects []RecoveryExecutionFact) (string, error) {
	digester := NewRecoveryReportDigester()
	for index := range objects {
		if _, err := digester.Add(objects[index]); err != nil {
			return "", fmt.Errorf("routesync: recovery report object %d: %w", index, err)
		}
	}
	digest, _, err := digester.Finish()
	return digest, err
}

// RecoveryReportDigester hashes the canonical JSON array incrementally so a
// node can prove one transactionally consistent report without retaining every
// dispatch specification in memory.
type RecoveryReportDigester struct {
	hash         hash.Hash
	count        uint64
	previousKind string
	previousID   string
	finished     bool
}

func NewRecoveryReportDigester() *RecoveryReportDigester {
	d := &RecoveryReportDigester{hash: sha256.New()}
	_, _ = d.hash.Write([]byte{'['})
	return d
}

func (d *RecoveryReportDigester) Add(object RecoveryExecutionFact) ([]byte, error) {
	if d == nil || d.hash == nil || d.finished {
		return nil, errors.New("routesync: recovery report digester is closed")
	}
	if err := object.Validate(); err != nil {
		return nil, err
	}
	if d.count != 0 {
		previous := RecoveryExecutionFact{Object: RecoveryObjectSnapshot{
			ObjectKind: d.previousKind, ObjectID: d.previousID,
		}}
		if compareRecoveryFacts(previous, object) >= 0 {
			return nil, errors.New("routesync: recovery report is not strictly ordered")
		}
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	if d.count != 0 {
		_, _ = d.hash.Write([]byte{','})
	}
	_, _ = d.hash.Write(encoded)
	d.previousKind, d.previousID = object.Object.ObjectKind, object.Object.ObjectID
	d.count++
	return encoded, nil
}

func (d *RecoveryReportDigester) Finish() (string, uint64, error) {
	if d == nil || d.hash == nil || d.finished {
		return "", 0, errors.New("routesync: recovery report digester is closed")
	}
	_, _ = d.hash.Write([]byte{']'})
	d.finished = true
	return hex.EncodeToString(d.hash.Sum(nil)), d.count, nil
}

func compareRecoveryFacts(left, right RecoveryExecutionFact) int {
	if compared := bytes.Compare([]byte(left.Object.ObjectKind), []byte(right.Object.ObjectKind)); compared != 0 {
		return compared
	}
	return bytes.Compare([]byte(left.Object.ObjectID), []byte(right.Object.ObjectID))
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func digestBytes(value string, payload []byte) bool {
	digest := sha256.Sum256(payload)
	return value == hex.EncodeToString(digest[:])
}

type EventAck struct {
	ObjectKind         string `json:"object_kind"` // sandbox
	ObjectID           string `json:"object_id"`
	RegistryGeneration string `json:"registry_generation"`
	BindingDigest      string `json:"binding_digest"`
	EventSeq           uint64 `json:"event_seq"`
}

func (a EventAck) Validate() error {
	if a.ObjectKind != "sandbox" || a.ObjectID == "" || a.RegistryGeneration == "" || a.EventSeq == 0 {
		return errors.New("routesync: incomplete execution event ACK")
	}
	if !validSHA256(a.BindingDigest) {
		return errors.New("routesync: execution event ACK Binding digest must be SHA-256 hex")
	}
	return nil
}

// EventCursor is process-local replay pagination, not an execution identity or
// authority token. An empty cursor starts at the beginning of the durable set.
type EventCursor struct {
	ObjectKind string
	ObjectID   string
}

// ExecutionEvent is the bounded latest-event wire projection backed by the
// node-local durable outbox. Fields irrelevant to the object kind/state remain
// empty.
type ExecutionEvent struct {
	ObjectKind         string `json:"object_kind"`
	ObjectID           string `json:"object_id"`
	NodeID             string `json:"node_id"`
	NodeEpoch          uint64 `json:"node_epoch"`
	RegistryGeneration string `json:"registry_generation"`
	Binding            string `json:"binding"`
	BindingDigest      string `json:"binding_digest"`
	EventSeq           uint64 `json:"event_seq"`
	State              string `json:"state"`

	DataEndpoint       string `json:"data_endpoint,omitempty"`
	TargetPort         int    `json:"target_port,omitempty"`
	AccessToken        string `json:"access_token,omitempty"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	TemplateRef        string `json:"template_ref,omitempty"`
	SnapshotRef        string `json:"snapshot_ref,omitempty"`
	SnapshotLocation   string `json:"snapshot_location,omitempty"`
	ArtifactRef        string `json:"artifact_ref,omitempty"`
	Reason             string `json:"reason,omitempty"`
}

const MaxExecutionEventBytes = 256 << 10

func (e ExecutionEvent) Validate() error {
	if e.ObjectID == "" || e.NodeID == "" || e.NodeEpoch == 0 || e.RegistryGeneration == "" ||
		e.EventSeq == 0 || e.State == "" {
		return errors.New("routesync: incomplete execution event")
	}
	if e.ObjectKind != "sandbox" {
		return errors.New("routesync: only Sandbox execution events are valid")
	}
	digest, err := hex.DecodeString(e.BindingDigest)
	if err != nil || len(digest) != 32 || e.Binding == "" {
		return errors.New("routesync: execution event Binding digest must be SHA-256 hex")
	}
	wantDigest := sha256.Sum256([]byte(e.Binding))
	if hex.EncodeToString(wantDigest[:]) != e.BindingDigest {
		return errors.New("routesync: execution event Binding digest mismatch")
	}
	return nil
}

// CmdAck statuses.
const (
	AckAccepted = "accepted"
	AckRejected = "rejected"

	DispatchAcceptedAdmitted = "ACCEPTED_ADMITTED"
	DispatchAcceptedQueued   = "ACCEPTED_QUEUED"
	DispatchDefinitiveReject = "DEFINITIVE_REJECT"
	DispatchSessionMoved     = "SESSION_MOVED"
	DispatchConflict         = "CONFLICT"
	DispatchWrongBinding     = "WRONG_BINDING"
	DispatchUnknown          = "UNKNOWN"
)

// NodeRegister is the node's first up-frame on node-link: its identity + capacity,
// so the registry can place sandboxes (and later builds) on it and forward the
// data plane to it (cluster.md / §6.1).
type NodeRegister struct {
	Version          int               `json:"version"`
	NodeID           string            `json:"node_id"`
	EnrollmentID     string            `json:"enrollment_id,omitempty"`
	NodeEpoch        uint64            `json:"node_epoch,omitempty"`
	SessionSeq       uint64            `json:"session_seq,omitempty"`
	LoadModelVersion uint16            `json:"load_model_version,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"` // zone / pool / slot / node (nodeSelectors)
	Capabilities     map[string]bool   `json:"capabilities,omitempty"`
	Draining         bool              `json:"draining,omitempty"`
	Capacity         int               `json:"capacity,omitempty"`       // max sandboxes (headroom signal)
	BuildCapacity    *BuildResources   `json:"build_capacity,omitempty"` // CPU/mem/storage build pool (§7.5)
	DataEndpoint     string            `json:"data_endpoint,omitempty"`  // host:port the router forwards data-plane to
	RuntimeDigest    string            `json:"runtime_digest,omitempty"` // guest runtime identity
	FailureDomain    string            `json:"failure_domain,omitempty"`
}

type NodeLinkRedirect struct {
	Targets []NodeLinkTarget `json:"targets"`
}

type NodeLinkTarget struct {
	MemberID string `json:"member_id,omitempty"`
	Endpoint string `json:"endpoint"`
}

// BuildResources is a node's build resource pool (or a build's request), kept
// independent of sandbox memory because builds run in their own slice (§7.5).
type BuildResources struct {
	Slots   int   `json:"slots,omitempty"`
	CPU     int   `json:"cpu,omitempty"`     // milli-cores
	Mem     int64 `json:"mem,omitempty"`     // bytes
	Storage int64 `json:"storage,omitempty"` // bytes
}

// Command is a tuple- and Binding-fenced workflow operation. Admission is
// idempotent by SID or BuildID plus the immutable intent digests. Terminal
// results are emitted through the durable execution-event outbox.
type Command struct {
	CmdID                  string                 `json:"cmd_id"`
	Kind                   string                 `json:"kind"`
	SID                    string                 `json:"sid,omitempty"`
	BuildID                string                 `json:"build_id,omitempty"`
	NodeEpoch              uint64                 `json:"node_epoch,omitempty"`
	SessionSeq             uint64                 `json:"session_seq,omitempty"`
	RegistryGeneration     string                 `json:"registry_generation,omitempty"`
	Binding                string                 `json:"binding,omitempty"`
	BindingDigest          string                 `json:"binding_digest,omitempty"`
	OldBindingDigest       string                 `json:"old_binding_digest,omitempty"`
	DemandDigest           string                 `json:"demand_digest,omitempty"`
	DispatchSpecDigest     string                 `json:"dispatch_spec_digest,omitempty"`
	AuthKeyFingerprint     string                 `json:"auth_key_fingerprint,omitempty"`
	ManifestKeyFingerprint string                 `json:"manifest_key_fingerprint,omitempty"`
	Group                  string                 `json:"group,omitempty"`
	RouteKey               string                 `json:"route_key,omitempty"`
	NormalizedDemand       []byte                 `json:"normalized_demand,omitempty"`
	DispatchSpec           []byte                 `json:"dispatch_spec,omitempty"`
	ProviderPolicy         string                 `json:"provider_policy_version,omitempty"`
	KeyLease               *NodeKeyLeaseV1        `json:"key_lease,omitempty"`
	KeyLeaseRef            *NodeKeyLeaseRefV1     `json:"key_lease_ref,omitempty"`
	Recovery               *RecoveryReportRequest `json:"recovery,omitempty"`
	EventAck               *EventAck              `json:"event_ack,omitempty"`
}

// CmdAck acknowledges a Command's receipt; the terminal outcome arrives via the
// route stream, not here.
type CmdAck struct {
	CmdID                 string                  `json:"cmd_id"`
	Status                string                  `json:"status"` // AckAccepted | AckRejected
	Outcome               string                  `json:"outcome,omitempty"`
	Reason                string                  `json:"reason,omitempty"`
	KeyLeaseRef           *NodeKeyLeaseRefV1      `json:"key_lease_ref,omitempty"`
	Recovery              *RecoveryReportPage     `json:"recovery,omitempty"`
	RebindObject          *RecoveryObjectSnapshot `json:"rebind_object,omitempty"`
	RebindAdmissionState  string                  `json:"rebind_admission_state,omitempty"`
	RebindResourceClaimed bool                    `json:"rebind_resource_claimed,omitempty"`
}
