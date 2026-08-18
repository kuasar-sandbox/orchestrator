package orch

import (
	"math"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type stubProbe struct {
	zone        string
	alloc, pool int64
	draining    bool
}

func (p stubProbe) Snapshot() ResourceProbeSnapshot {
	return ResourceProbeSnapshot{Zone: p.zone, Allocated: p.alloc, Pool: p.pool, Draining: p.draining}
}

func TestHeartbeatTelemetry(t *testing.T) {
	o := testOrch(t)

	// No probe (static cgroup): zone defaults green, not draining, no water level.
	hb := o.Heartbeat()
	if hb.Zone != "green" || hb.Draining || hb.Allocated != 0 || hb.Pool != 0 {
		t.Fatalf("no-probe heartbeat: %+v", hb)
	}

	// With a probe: water level + drain propagate to the cluster (the drain-bug fix).
	o.SetResourceProbe(stubProbe{zone: "yellow", alloc: 4 << 30, pool: 16 << 30, draining: true})
	hb = o.Heartbeat()
	if hb.Zone != "yellow" || !hb.Draining || hb.Allocated != 4<<30 || hb.Pool != 16<<30 {
		t.Fatalf("probe heartbeat: %+v", hb)
	}
}

func TestClusterNodeInfo(t *testing.T) {
	registrationMax, executionMax := int64(16), int64(3)
	registrationCPU, executionCPU := config.CPUCores("64"), config.CPUCores("6")
	registrationMemory, executionMemory := "256GiB", "12GiB"
	o := testOrchCfg(t, &config.Config{Builder: config.BuilderConfig{Admission: config.BuilderAdmissionConfig{
		Registration: &config.BuildAdmissionLimitConfig{MaxBuilds: &registrationMax, Resources: config.BuildAdmissionResourcesConfig{CPU: &registrationCPU, Memory: &registrationMemory}},
		Execution:    &config.BuildAdmissionLimitConfig{MaxBuilds: &executionMax, Resources: config.BuildAdmissionResourcesConfig{CPU: &executionCPU, Memory: &executionMemory}},
	}}})
	o.cfg.Sandbox.Capacity = 100
	capacity, registration, execution, _ := o.ClusterNodeInfo()
	if capacity != 100 {
		t.Fatalf("capacity = %d, want 100", capacity)
	}
	if registration == nil || registration.MaxBuilds != 16 || registration.Resources.CPU != 64000 || registration.Resources.Memory != 256<<30 {
		t.Fatalf("registration capacity: %+v", registration)
	}
	if execution == nil || execution.MaxBuilds != 3 || execution.Resources.CPU != 6000 || execution.Resources.Memory != 12<<30 {
		t.Fatalf("execution capacity: %+v", execution)
	}
}

func TestSaturatedBuildUsageFailsClosedForUnlimitedDimensions(t *testing.T) {
	usage := saturatedBuildUsage()
	if usage.Builds != math.MaxInt64 || usage.Resources == nil ||
		usage.Resources.CPU != math.MaxInt64 || usage.Resources.Memory != math.MaxInt64 ||
		usage.Resources.Storage != math.MaxInt64 {
		t.Fatalf("saturated usage = %+v", usage)
	}
	if (types.BuildAdmissionLimit{}).AllowsAdd(usage.Builds, usage.Resources.Types(),
		types.BuildResources{CPU: 1, Memory: 1}) {
		t.Fatal("unlimited admission accepted a request after durable usage failure")
	}
}
