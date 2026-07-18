package nodectl

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestResolveDerivesBuildReservationAndNetGrantPool(t *testing.T) {
	rc := &config.ResourceListenConfig{}
	rc.Resources.PhysicalMemory = "64GiB"
	rc.Resources.PhysicalCPU = "8"
	rc.Resources.HostReserved.Memory = "8GiB"
	builder := config.BuilderConfig{
		MaxConcurrent: 3,
		VCPU:          2,
		CPUQuota:      "450%",
		Memory:        "4GiB",
		MemoryMax:     "10GiB",
	}

	got, err := Resolve(rc, builder)
	if err != nil {
		t.Fatal(err)
	}
	if got.BuildReserved.MemoryBytes != 10<<30 {
		t.Fatalf("BuildReserved.MemoryBytes = %d, want %d", got.BuildReserved.MemoryBytes, uint64(10<<30))
	}
	if got.BuildReserved.CPUMilli != 4500 {
		t.Fatalf("BuildReserved.CPUMilli = %d, want 4500", got.BuildReserved.CPUMilli)
	}
	// ApplyDefaults sets a 10% operational margin and a 5% grant rate.
	netBeforeMargin := uint64(46 << 30)
	wantPool := uint64(float64(netBeforeMargin) * 0.90)
	wantGrant := uint64(float64(wantPool) * 0.05)
	if got.MemoryGrantPerSecBytes != wantGrant {
		t.Fatalf("MemoryGrantPerSecBytes = %d, want %d", got.MemoryGrantPerSecBytes, wantGrant)
	}
}

func TestResolveRejectsBuildReservationBeyondPhysicalMemory(t *testing.T) {
	rc := &config.ResourceListenConfig{}
	rc.Resources.PhysicalMemory = "16GiB"
	rc.Resources.PhysicalCPU = "8"
	rc.Resources.HostReserved.Memory = "8GiB"

	_, err := Resolve(rc, config.BuilderConfig{MaxConcurrent: 3, Memory: "4GiB"})
	if err == nil || !strings.Contains(err.Error(), "host_reserved.memory + build_reserved exceeds physical memory") {
		t.Fatalf("Resolve error = %v", err)
	}
}

func TestResolveRejectsBuildReservationBeyondPhysicalCPU(t *testing.T) {
	rc := &config.ResourceListenConfig{}
	rc.Resources.PhysicalMemory = "64GiB"
	rc.Resources.PhysicalCPU = "4"
	rc.Resources.HostReserved.Memory = "8GiB"
	rc.Resources.HostReserved.CPU = 1

	_, err := Resolve(rc, config.BuilderConfig{MaxConcurrent: 2, VCPU: 2, Memory: "4GiB"})
	if err == nil || !strings.Contains(err.Error(), "host_reserved.cpu + build_reserved exceeds physical CPU") {
		t.Fatalf("Resolve error = %v", err)
	}
}

func TestResolveCapsBuildCPUReservationAtAggregateQuota(t *testing.T) {
	rc := &config.ResourceListenConfig{}
	rc.Resources.PhysicalMemory = "64GiB"
	rc.Resources.PhysicalCPU = "8"
	rc.Resources.HostReserved.Memory = "8GiB"
	rc.Resources.HostReserved.CPU = 1

	got, err := Resolve(rc, config.BuilderConfig{
		MaxConcurrent: 3, VCPU: 2, CPUQuota: "250%", Memory: "4GiB",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.BuildReserved.CPUMilli != 2500 {
		t.Fatalf("BuildReserved.CPUMilli = %d, want aggregate quota 2500", got.BuildReserved.CPUMilli)
	}
}
