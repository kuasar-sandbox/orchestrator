package placement

import (
	"math"
	"testing"
	"time"
)

const testCatalogDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestRatioPPMRoundsUpAndSaturates(t *testing.T) {
	if got := ratioPPM(1, 3); got != 333_334 {
		t.Fatalf("1/3 PPM = %d", got)
	}
	if got := ratioPPM(1, 1); got != PartsPerMillion {
		t.Fatalf("1/1 PPM = %d", got)
	}
	if got := ratioPPM(math.MaxUint64, 1); got != math.MaxUint32 {
		t.Fatalf("saturated PPM = %d", got)
	}
}

func TestProjectedSandboxRateUsesNetAllocatablePool(t *testing.T) {
	snapshot := baseSnapshot()
	request := PlacementProbeRequest{
		Kind: ObjectSandbox, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest,
		Sandbox: &SandboxDemand{SlotUnits: 1, StartupBudgetMemory: 2 << 30, FloorMemory: 1 << 30, AllocatableAtSnapshot: 512 << 20},
	}
	response := ProbePlacement(snapshot, 100*time.Millisecond, request)
	if response.Class != ProbeImmediate || response.Components.SlotPPM != 300_000 ||
		response.Components.MemoryPPM != 750_000 || response.Components.StartupPPM != 750_000 || response.RatePPM != 750_000 {
		t.Fatalf("probe = %+v", response)
	}

	// The producer's allocatable pool already excludes the Build reservation;
	// carrying it for observability must not subtract it a second time.
	snapshot.BuildReservedMemory = 7 << 30
	response = ProbePlacement(snapshot, 100*time.Millisecond, request)
	if response.Class != ProbeImmediate || response.Components.MemoryPPM != 750_000 {
		t.Fatalf("double-counted build reservation = %+v", response)
	}

	snapshot.EmergencyReservedMemory = 3 << 30
	response = ProbePlacement(snapshot, 100*time.Millisecond, request)
	if response.Class != ProbeWouldQueue || response.Components.MemoryPPM != 1_200_000 {
		t.Fatalf("emergency-reserved headroom = %+v", response)
	}
	snapshot.EmergencyReservedMemory = 7 << 30
	response = ProbePlacement(snapshot, 100*time.Millisecond, request)
	if response.Class != ProbeReject {
		t.Fatalf("insufficient ordinary memory pool = %+v", response)
	}
}

func TestProbeFreshnessAndUnknownCapacityFailClosed(t *testing.T) {
	snapshot := baseSnapshot()
	request := PlacementProbeRequest{
		Kind: ObjectBuild, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest, Build: &BuildDemand{Slots: 1, CPU: 1000},
	}
	if got := ProbePlacement(snapshot, MaximumProbeSampleAge+time.Nanosecond, request); got.Class != ProbeStale {
		t.Fatalf("stale probe = %+v", got)
	}
	snapshot.BuildCPUCapacity = 0
	if got := ProbePlacement(snapshot, 0, request); got.Class != ProbeReject {
		t.Fatalf("unknown capacity probe = %+v", got)
	}
	request.LoadModelVersion++
	if got := ProbePlacement(baseSnapshot(), 0, request); got.Class != ProbeStale {
		t.Fatalf("model mismatch probe = %+v", got)
	}
	snapshot = baseSnapshot()
	snapshot.LoadModelVersion++
	if err := ValidateSnapshot(snapshot); err == nil {
		t.Fatal("unsupported load-model snapshot was accepted")
	}
}

func TestProbeRejectsChangedCatalogIdentity(t *testing.T) {
	snapshot := baseSnapshot()
	request := PlacementProbeRequest{
		Kind: ObjectSandbox, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest,
		Sandbox: &SandboxDemand{SlotUnits: 1, StartupBudgetMemory: 1},
	}
	snapshot.CatalogDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if got := ProbePlacement(snapshot, 0, request); got.Class != ProbeReject {
		t.Fatalf("changed Catalog identity probe = %+v", got)
	}
}

func TestQueueFullIsTransientRatherThanDefinitiveRejection(t *testing.T) {
	snapshot := baseSnapshot()
	snapshot.SandboxRateTokenAvailable = false
	snapshot.SandboxQueueDepth = snapshot.SandboxQueueLimit
	sandbox := PlacementProbeRequest{
		Kind: ObjectSandbox, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest, Sandbox: &SandboxDemand{SlotUnits: 1, StartupBudgetMemory: 1},
	}
	if got := ProbePlacement(snapshot, 0, sandbox); got.Class != ProbeStale {
		t.Fatalf("full Sandbox queue probe = %+v", got)
	}

	snapshot = baseSnapshot()
	snapshot.BuildRateTokenAvailable = false
	snapshot.BuildQueueDepth = snapshot.BuildQueueLimit
	build := PlacementProbeRequest{
		Kind: ObjectBuild, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest, Build: &BuildDemand{Slots: 1},
	}
	if got := ProbePlacement(snapshot, 0, build); got.Class != ProbeStale {
		t.Fatalf("full Build queue probe = %+v", got)
	}
}

func TestProbeRejectsUnknownSafetyState(t *testing.T) {
	snapshot := baseSnapshot()
	request := PlacementProbeRequest{
		Kind: ObjectSandbox, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest, Sandbox: &SandboxDemand{SlotUnits: 1, StartupBudgetMemory: 1},
	}
	snapshot.WaterZone = "unknown"
	if got := ProbePlacement(snapshot, 0, request); got.Class != ProbeReject {
		t.Fatalf("unknown safety state = %+v", got)
	}
	snapshot.WaterZone = ""
	if got := ProbePlacement(snapshot, 0, request); got.Class != ProbeReject {
		t.Fatalf("missing controller safety state = %+v", got)
	}
	snapshot.SandboxResourceController = false
	if got := ProbePlacement(snapshot, 0, request); got.Class != ProbeImmediate {
		t.Fatalf("controller-disabled empty safety state = %+v", got)
	}
}

func TestProbeRetriesDynamicSafetyStates(t *testing.T) {
	request := PlacementProbeRequest{
		Kind: ObjectSandbox, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest, Sandbox: &SandboxDemand{SlotUnits: 1, StartupBudgetMemory: 1},
	}
	for _, zone := range []string{"red", "critical"} {
		snapshot := baseSnapshot()
		snapshot.WaterZone = zone
		if got := ProbePlacement(snapshot, 0, request); got.Class != ProbeStale {
			t.Fatalf("%s safety state = %+v", zone, got)
		}
	}
}

func TestProbeRetriesDrainingCandidate(t *testing.T) {
	snapshot := baseSnapshot()
	snapshot.Draining = true
	request := PlacementProbeRequest{
		Kind: ObjectSandbox, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest,
		Sandbox: &SandboxDemand{SlotUnits: 1, StartupBudgetMemory: 1},
	}
	if got := ProbePlacement(snapshot, 0, request); got.Class != ProbeStale {
		t.Fatalf("draining candidate = %+v", got)
	}
}

func TestProbeRetriesTemporaryHardLimitSaturation(t *testing.T) {
	sandboxSnapshot := baseSnapshot()
	sandboxSnapshot.SandboxSlotHardLimit = 3
	sandbox := PlacementProbeRequest{
		Kind: ObjectSandbox, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest, Sandbox: &SandboxDemand{SlotUnits: 2, StartupBudgetMemory: 1},
	}
	if got := ProbePlacement(sandboxSnapshot, 0, sandbox); got.Class != ProbeStale {
		t.Fatalf("temporarily saturated Sandbox hard limit = %+v", got)
	}
	sandbox.Sandbox.SlotUnits = 4
	if got := ProbePlacement(sandboxSnapshot, 0, sandbox); got.Class != ProbeReject {
		t.Fatalf("permanently oversized Sandbox hard-limit demand = %+v", got)
	}

	buildSnapshot := baseSnapshot()
	buildSnapshot.BuildSlotHardLimit = 2
	build := PlacementProbeRequest{
		Kind: ObjectBuild, NodeID: "n1", ExpectedNodeEpoch: 7, ExpectedSessionSeq: 11,
		LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest, Build: &BuildDemand{Slots: 2},
	}
	if got := ProbePlacement(buildSnapshot, 0, build); got.Class != ProbeStale {
		t.Fatalf("temporarily saturated Build hard limit = %+v", got)
	}
	build.Build.Slots = 3
	if got := ProbePlacement(buildSnapshot, 0, build); got.Class != ProbeReject {
		t.Fatalf("permanently oversized Build hard-limit demand = %+v", got)
	}
}

func TestP2CUsesClassRateThenRandomTie(t *testing.T) {
	immediate := PlacementProbeResponse{Class: ProbeImmediate, RatePPM: 900_000, NodeID: "z"}
	queued := PlacementProbeResponse{Class: ProbeWouldQueue, RatePPM: 100_000, NodeID: "a"}
	if got := ChooseP2C(immediate, queued, nil); got != immediate {
		t.Fatalf("class ordering chose %+v", got)
	}
	a := PlacementProbeResponse{Class: ProbeImmediate, RatePPM: 400_000, NodeID: "z"}
	b := PlacementProbeResponse{Class: ProbeImmediate, RatePPM: 300_000, NodeID: "a"}
	if got := ChooseP2C(a, b, nil); got != b {
		t.Fatalf("rate ordering chose %+v", got)
	}
	b.RatePPM = a.RatePPM
	if got := ChooseP2C(a, b, func() bool { return true }); got != b {
		t.Fatalf("random tie chose %+v", got)
	}
	if got := ChooseP2C(a, b, func() bool { return false }); got != a {
		t.Fatalf("random tie chose %+v", got)
	}
}

func baseSnapshot() PlacementLoadSnapshot {
	return PlacementLoadSnapshot{
		NodeID: "n1", NodeEpoch: 7, SessionSeq: 11, DataEndpoint: "10.0.0.1:8443",
		SampleSeq: 3, LoadModelVersion: LoadModelVersion, CatalogDigest: testCatalogDigest, WaterZone: "green",
		SandboxSlotCapacity: 10, SandboxSlotHardLimit: 20, SandboxSlotUsed: 2,
		SandboxQueueLimit: 100, SandboxRateTokenAvailable: true,
		SandboxResourceController: true, NodeAllocatedMemory: 4 << 30,
		AllocatablePoolMemory: 8 << 30, BuildReservedMemory: 0,
		StartupAllocatedMemory: 4 << 30, StartupPoolMemory: 8 << 30,
		BuildSlotCapacity: 4, BuildSlotHardLimit: 16, BuildSlotsUsed: 1,
		BuildCPUCapacity: 4000, BuildCPUUsed: 1000,
		BuildMemoryCapacity: 8 << 30, BuildMemoryUsed: 1 << 30,
		BuildStorageCapacity: 100 << 30, BuildStorageUsed: 10 << 30,
		BuildQueueLimit: 100, BuildRateTokenAvailable: true,
	}
}
