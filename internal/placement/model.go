package placement

import (
	"errors"
	"math"
	"math/bits"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/placementproto"
)

const (
	LoadModelVersion      uint16 = placementproto.LoadModelVersion
	SnapshotPeriod               = 500 * time.Millisecond
	MaximumProbeSampleAge        = time.Second
	PartsPerMillion       uint32 = 1_000_000
)

type ProbeClass string

const (
	ProbeImmediate  ProbeClass = "IMMEDIATE"
	ProbeWouldQueue ProbeClass = "WOULD_QUEUE"
	ProbeReject     ProbeClass = "REJECT"
	ProbeStale      ProbeClass = "STALE"
)

type ObjectKind string

const (
	ObjectSandbox ObjectKind = "sandbox"
	ObjectBuild   ObjectKind = "build"
)

type SandboxDemand struct {
	SlotUnits             uint64 `json:"slot_units"`
	StartupBudgetMemory   uint64 `json:"startup_budget_memory"`
	FloorMemory           uint64 `json:"floor_memory"`
	AllocatableAtSnapshot uint64 `json:"allocatable_at_snapshot"`
}

func (d SandboxDemand) EffectiveStartupMemory() uint64 {
	return max(d.StartupBudgetMemory, d.FloorMemory, d.AllocatableAtSnapshot)
}

type BuildDemand struct {
	Slots   uint64 `json:"slots"`
	CPU     uint64 `json:"cpu"`
	Memory  uint64 `json:"memory"`
	Storage uint64 `json:"storage"`
}

type PlacementProbeRequest struct {
	Kind               ObjectKind     `json:"kind"`
	NodeID             string         `json:"node_id"`
	ExpectedNodeEpoch  uint64         `json:"expected_node_epoch"`
	ExpectedSessionSeq uint64         `json:"expected_session_seq"`
	LoadModelVersion   uint16         `json:"load_model_version"`
	RuntimeDigest      string         `json:"runtime_digest,omitempty"`
	Sandbox            *SandboxDemand `json:"sandbox,omitempty"`
	Build              *BuildDemand   `json:"build,omitempty"`
}

type PlacementLoadSnapshot = placementproto.PlacementLoadSnapshot

type RateComponents struct {
	SlotPPM    uint32 `json:"slot_ppm,omitempty"`
	MemoryPPM  uint32 `json:"memory_ppm,omitempty"`
	StartupPPM uint32 `json:"startup_ppm,omitempty"`
	CPUPPM     uint32 `json:"cpu_ppm,omitempty"`
	StoragePPM uint32 `json:"storage_ppm,omitempty"`
}

func (c RateComponents) Maximum() uint32 {
	return max(c.SlotPPM, c.MemoryPPM, c.StartupPPM, c.CPUPPM, c.StoragePPM)
}

type PlacementProbeResponse struct {
	Class ProbeClass `json:"class"`

	NodeID       string        `json:"node_id"`
	NodeEpoch    uint64        `json:"node_epoch"`
	SessionSeq   uint64        `json:"session_seq"`
	DataEndpoint string        `json:"data_endpoint"`
	SampleSeq    uint64        `json:"sample_seq"`
	SampleAge    time.Duration `json:"sample_age"`

	LoadModelVersion uint16         `json:"load_model_version"`
	RatePPM          uint32         `json:"rate_ppm"`
	Components       RateComponents `json:"components"`
	Reason           string         `json:"reason,omitempty"`
}

func ProbePlacement(snapshot PlacementLoadSnapshot, sampleAge time.Duration, request PlacementProbeRequest) PlacementProbeResponse {
	response := PlacementProbeResponse{
		NodeID: snapshot.NodeID, NodeEpoch: snapshot.NodeEpoch, SessionSeq: snapshot.SessionSeq,
		DataEndpoint: snapshot.DataEndpoint, SampleSeq: snapshot.SampleSeq, SampleAge: sampleAge,
		LoadModelVersion: snapshot.LoadModelVersion,
	}
	stale := func(reason string) PlacementProbeResponse {
		response.Class, response.Reason = ProbeStale, reason
		return response
	}
	reject := func(reason string) PlacementProbeResponse {
		response.Class, response.Reason = ProbeReject, reason
		return response
	}
	if snapshot.NodeID == "" || snapshot.NodeID != request.NodeID || snapshot.NodeEpoch == 0 || snapshot.SessionSeq == 0 ||
		snapshot.DataEndpoint == "" || snapshot.SampleSeq == 0 {
		return stale("incomplete or mismatched session snapshot")
	}
	if snapshot.NodeEpoch != request.ExpectedNodeEpoch || snapshot.SessionSeq != request.ExpectedSessionSeq {
		return stale("session tuple changed")
	}
	if request.LoadModelVersion != LoadModelVersion || snapshot.LoadModelVersion != request.LoadModelVersion {
		return stale("load model version mismatch")
	}
	if sampleAge < 0 || sampleAge > MaximumProbeSampleAge {
		return stale("placement sample is stale")
	}
	if snapshot.Draining {
		return reject("node is draining")
	}
	switch snapshot.WaterZone {
	case "red", "critical":
		return reject("node safety state rejects admission")
	case "green", "yellow":
	case "":
		if snapshot.SandboxResourceController {
			return reject("node safety state is unknown")
		}
	default:
		return reject("node safety state is unknown")
	}
	if request.RuntimeDigest != "" && snapshot.RuntimeDigest != request.RuntimeDigest {
		return reject("runtime is incompatible")
	}

	var class ProbeClass
	var components RateComponents
	var reason string
	switch request.Kind {
	case ObjectSandbox:
		if request.Sandbox == nil || request.Build != nil {
			return reject("invalid sandbox demand")
		}
		class, components, reason = projectSandbox(snapshot, *request.Sandbox)
	case ObjectBuild:
		if request.Build == nil || request.Sandbox != nil {
			return reject("invalid build demand")
		}
		class, components, reason = projectBuild(snapshot, *request.Build)
	default:
		return reject("invalid placement object kind")
	}
	response.Class = class
	response.Components = components
	response.RatePPM = components.Maximum()
	response.Reason = reason
	return response
}

func projectSandbox(snapshot PlacementLoadSnapshot, demand SandboxDemand) (ProbeClass, RateComponents, string) {
	var components RateComponents
	if demand.SlotUnits == 0 || snapshot.SandboxSlotCapacity == 0 {
		return ProbeReject, components, "sandbox slot capacity/demand is unknown"
	}
	if demand.SlotUnits > snapshot.SandboxSlotCapacity {
		return ProbeReject, components, "sandbox can never fit slot capacity"
	}
	if snapshot.SandboxSlotHardLimit > 0 && sumExceeds(snapshot.SandboxSlotUsed, demand.SlotUnits, snapshot.SandboxSlotHardLimit) {
		return ProbeReject, components, "sandbox slot hard limit reached"
	}
	components.SlotPPM = ratioPPM(saturatingAdd(snapshot.SandboxSlotUsed, demand.SlotUnits), snapshot.SandboxSlotCapacity)

	if snapshot.SandboxResourceController {
		effectiveMemory := demand.EffectiveStartupMemory()
		if effectiveMemory == 0 || snapshot.BuildReservedMemory >= snapshot.AllocatablePoolMemory {
			return ProbeReject, RateComponents{}, "sandbox memory capacity/demand is unknown"
		}
		allocatablePool := snapshot.AllocatablePoolMemory - snapshot.BuildReservedMemory
		if snapshot.StartupPoolMemory == 0 || effectiveMemory > allocatablePool || effectiveMemory > snapshot.StartupPoolMemory {
			return ProbeReject, RateComponents{}, "sandbox can never fit memory pools"
		}
		components.MemoryPPM = ratioPPM(saturatingAdd(snapshot.NodeAllocatedMemory, effectiveMemory), allocatablePool)
		components.StartupPPM = ratioPPM(saturatingAdd(snapshot.StartupAllocatedMemory, effectiveMemory), snapshot.StartupPoolMemory)
	}
	if components.Maximum() <= PartsPerMillion && snapshot.SandboxRateTokenAvailable {
		return ProbeImmediate, components, ""
	}
	if snapshot.SandboxQueueLimit == 0 || snapshot.SandboxQueueDepth >= snapshot.SandboxQueueLimit {
		return ProbeReject, components, "sandbox queue is full"
	}
	return ProbeWouldQueue, components, "sandbox requires queueing"
}

func projectBuild(snapshot PlacementLoadSnapshot, demand BuildDemand) (ProbeClass, RateComponents, string) {
	var components RateComponents
	if demand.Slots == 0 || snapshot.BuildSlotCapacity == 0 ||
		(demand.CPU > 0 && snapshot.BuildCPUCapacity == 0) ||
		(demand.Memory > 0 && snapshot.BuildMemoryCapacity == 0) ||
		(demand.Storage > 0 && snapshot.BuildStorageCapacity == 0) {
		return ProbeReject, components, "build capacity/demand is unknown"
	}
	if demand.Slots > snapshot.BuildSlotCapacity || demand.CPU > snapshot.BuildCPUCapacity ||
		demand.Memory > snapshot.BuildMemoryCapacity || demand.Storage > snapshot.BuildStorageCapacity {
		return ProbeReject, components, "build can never fit resource pool"
	}
	if snapshot.BuildSlotHardLimit > 0 && sumExceeds(snapshot.BuildSlotsUsed, demand.Slots, snapshot.BuildSlotHardLimit) {
		return ProbeReject, components, "build slot hard limit reached"
	}
	components.SlotPPM = ratioPPM(saturatingAdd(snapshot.BuildSlotsUsed, demand.Slots), snapshot.BuildSlotCapacity)
	if demand.CPU > 0 {
		components.CPUPPM = ratioPPM(saturatingAdd(snapshot.BuildCPUUsed, demand.CPU), snapshot.BuildCPUCapacity)
	}
	if demand.Memory > 0 {
		components.MemoryPPM = ratioPPM(saturatingAdd(snapshot.BuildMemoryUsed, demand.Memory), snapshot.BuildMemoryCapacity)
	}
	if demand.Storage > 0 {
		components.StoragePPM = ratioPPM(saturatingAdd(snapshot.BuildStorageUsed, demand.Storage), snapshot.BuildStorageCapacity)
	}
	if components.Maximum() <= PartsPerMillion && snapshot.BuildRateTokenAvailable {
		return ProbeImmediate, components, ""
	}
	if snapshot.BuildQueueLimit == 0 || snapshot.BuildQueueDepth >= snapshot.BuildQueueLimit {
		return ProbeReject, components, "build queue is full"
	}
	return ProbeWouldQueue, components, "build requires queueing"
}

// ChooseP2C applies the frozen V1 ordering. tie must provide an unbiased random
// bit; node identity is intentionally absent from comparison.
func ChooseP2C(a, b PlacementProbeResponse, tie func() bool) PlacementProbeResponse {
	ra, rb := probeClassRank(a.Class), probeClassRank(b.Class)
	switch {
	case ra < rb:
		return a
	case rb < ra:
		return b
	case a.RatePPM < b.RatePPM:
		return a
	case b.RatePPM < a.RatePPM:
		return b
	case tie != nil && tie():
		return b
	default:
		return a
	}
}

func probeClassRank(class ProbeClass) uint8 {
	switch class {
	case ProbeImmediate:
		return 0
	case ProbeWouldQueue:
		return 1
	default:
		return 2
	}
}

func ratioPPM(numerator, denominator uint64) uint32 {
	if denominator == 0 {
		return math.MaxUint32
	}
	whole, remainder := numerator/denominator, numerator%denominator
	if whole > uint64(math.MaxUint32)/uint64(PartsPerMillion) {
		return math.MaxUint32
	}
	value := whole * uint64(PartsPerMillion)
	hi, lo := bits.Mul64(remainder, uint64(PartsPerMillion))
	fraction, rem := bits.Div64(hi, lo, denominator)
	if rem > 0 {
		fraction++
	}
	if value > uint64(math.MaxUint32)-fraction {
		return math.MaxUint32
	}
	return uint32(value + fraction)
}

func sumExceeds(a, b, limit uint64) bool {
	return a > limit || b > limit-a
}

func saturatingAdd(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func ValidateSnapshot(snapshot PlacementLoadSnapshot) error {
	if snapshot.NodeID == "" || snapshot.NodeEpoch == 0 || snapshot.SessionSeq == 0 || snapshot.DataEndpoint == "" ||
		snapshot.SampleSeq == 0 || snapshot.LoadModelVersion == 0 {
		return errors.New("placement: incomplete load snapshot")
	}
	return nil
}
