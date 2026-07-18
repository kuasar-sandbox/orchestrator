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
