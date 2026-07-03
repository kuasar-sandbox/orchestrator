package orch

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

type stubProbe struct {
	zone        string
	alloc, pool int64
	draining    bool
}

func (p stubProbe) Zone() string          { return p.zone }
func (p stubProbe) AllocatedBytes() int64 { return p.alloc }
func (p stubProbe) PoolBytes() int64      { return p.pool }
func (p stubProbe) Draining() bool        { return p.draining }

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
	o := testOrchCfg(t, &config.Config{Builder: config.BuilderConfig{VCPU: 2, Memory: "4GiB", MaxConcurrent: 3}})
	o.cfg.Sandbox.Capacity = 100
	capacity, buildCap, _ := o.ClusterNodeInfo()
	if capacity != 100 {
		t.Fatalf("capacity = %d, want 100", capacity)
	}
	// build pool = MaxConcurrent × per-build (2 cores=2000 milli, 4GiB).
	if buildCap == nil || buildCap.CPU != 6000 || buildCap.Mem != 3*(4<<30) {
		t.Fatalf("build capacity: %+v", buildCap)
	}
}
