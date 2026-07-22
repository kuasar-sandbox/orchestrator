package orch

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPinSnapshotAdmissionCapacityUsesFrozenCapacity(t *testing.T) {
	metadata := map[string]string{
		sandboxcfg.NsResource: `{"capacity":{"cpu":8,"memory":"16GiB"},"allocatable":{"memory":"1GiB"},"startup":{"memory":"2GiB"}}`,
	}
	resources, err := sandboxcfg.ResolveResourcesWithDefaults(metadata, 2, 2<<30)
	if err != nil {
		t.Fatal(err)
	}
	resources, err = pinSnapshotAdmissionCapacity(metadata, resources, snapInfo{
		CapCPU: 4, CapMem: "4GiB", HasCapacity: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resources.CapacityCPU != 4 || resources.CapacityMemoryBytes != 4<<30 ||
		resources.FloorCPU != 4 || resources.FloorMemoryBytes != 1<<30 ||
		resources.StartupMemoryBytes != 2<<30 {
		t.Fatalf("snapshot-pinned resources = %+v", resources)
	}
}

func TestPinSnapshotAdmissionCapacityRejectsOversizedFloor(t *testing.T) {
	metadata := map[string]string{
		sandboxcfg.NsResource: `{"allocatable":{"cpu":5,"memory":"1GiB"}}`,
	}
	resources, err := sandboxcfg.ResolveResourcesWithDefaults(metadata, 8, 8<<30)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pinSnapshotAdmissionCapacity(metadata, resources, snapInfo{
		CapCPU: 4, CapMem: "4GiB", HasCapacity: true,
	}); err == nil {
		t.Fatal("allocatable CPU above the frozen capacity was accepted")
	}
}

func TestReadSnapshotConfigReturnsFrozenCapacity(t *testing.T) {
	dir := t.TempDir()
	sandboxCtl := filepath.Join(dir, "sandbox-ctl")
	if err := os.WriteFile(sandboxCtl, []byte("#!/bin/sh\nprintf '%s\\n' '{\"Resources\":{\"Capacity\":{\"CPU\":6,\"Memory\":\"12GiB\"}},\"Metadata\":{}}'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	o := testOrch(t)
	info, err := o.readSnapshotConfig(context.Background(), &types.Sandbox{
		ID: "sandbox-1", ManifestKey: "manifest-key",
	}, "manifest://snapshot")
	if err != nil {
		t.Fatal(err)
	}
	if !info.HasCapacity || info.CapCPU != 6 || info.CapMem != "12GiB" {
		t.Fatalf("snapshot info = %+v", info)
	}
}
