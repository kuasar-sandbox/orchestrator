package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const qosPublicConfig = `sandbox: {resources: {diff_cow: {discard: manual, cache_size: 32MiB, cache_idle_after: 10s, max_dirty_size: 0, read_bandwidth: 50M, write_bandwidth: 20M, read_iops: 0, write_iops: 500, max_dirty_size_per_disk: 64MiB, read_bandwidth_per_disk: 17M, write_bandwidth_per_disk: 9M, read_iops_per_disk: 0, write_iops_per_disk: 99}}}`

func TestDiffCOWQoSPublicRoundtripClone(t *testing.T) {
	cfg, err := DecodeConductor(strings.NewReader(qosPublicConfig))
	if err != nil {
		t.Fatal(err)
	}
	cow := cfg.Sandbox.Resources.DiffCOW
	if cow.ReadIOPS == nil || *cow.ReadIOPS != 0 || cow.MaxDirtySize == nil || *cow.MaxDirtySize != "0" {
		t.Fatal("public input lost explicit zeros")
	}
	for _, marshal := range []func(any) ([]byte, error){yaml.Marshal, json.Marshal} {
		data, err := marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		after, err := DecodeConductor(strings.NewReader(string(data)))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(after.Sandbox.Resources.DiffCOW, cow) {
			t.Fatalf("public roundtrip changed presence: %s", data)
		}
	}
	clone := cfg.Clone()
	*clone.Sandbox.Resources.DiffCOW.ReadIOPS = 77
	*clone.Sandbox.Resources.DiffCOW.ReadBandwidth = "0"
	if *cow.ReadIOPS != 0 || *cow.ReadBandwidth != "50M" {
		t.Fatal("Conductor.Clone shared QoS pointers")
	}
}

func TestDiffCOWQoSPublicOverlay(t *testing.T) {
	dir := t.TempDir()
	base, over := filepath.Join(dir, "base.yaml"), filepath.Join(dir, "over.yaml")
	if err := os.WriteFile(base, []byte(qosPublicConfig), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(over, []byte("sandbox: {resources: {diff_cow: {write_iops: 0, write_bandwidth: 0}}}"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConductor(base + ":" + over)
	if err != nil {
		t.Fatal(err)
	}
	cow := cfg.Sandbox.Resources.DiffCOW
	if cow.WriteIOPS == nil || *cow.WriteIOPS != 0 || cow.WriteBandwidth == nil || *cow.WriteBandwidth != "0" || cow.ReadBandwidth == nil || *cow.ReadBandwidth != "50M" {
		t.Fatalf("overlay zero/inherit failed: %+v", cow)
	}
}

func TestDiffCOWQoSPublicStrictIOPS(t *testing.T) {
	for _, v := range []string{"-1", "1.0", "'1'", "null", "true", "18446744073709551616"} {
		if _, err := DecodeConductor(strings.NewReader("sandbox: {resources: {diff_cow: {read_iops: " + v + "}}}")); err == nil {
			t.Fatalf("invalid IOPS accepted: %s", v)
		}
	}
	for _, v := range []string{"null", "-1", "1.0", "1e2", "\"1\""} {
		var cfg ResourceDiffCOW
		if err := json.Unmarshal([]byte(`{"read_iops":`+v+`}`), &cfg); err == nil {
			t.Fatalf("invalid JSON IOPS accepted: %s", v)
		}
	}
}

func TestDiffCOWQoSPublicOverlayNullUsesExistingDeletion(t *testing.T) {
	dir := t.TempDir()
	base, over := filepath.Join(dir, "base.yaml"), filepath.Join(dir, "over.yaml")
	if err := os.WriteFile(base, []byte(qosPublicConfig), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(over, []byte("sandbox: {resources: {diff_cow: {read_iops: null, read_bandwidth: null}}}"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConductor(base + ":" + over)
	if err != nil {
		t.Fatal(err)
	}
	cow := cfg.Sandbox.Resources.DiffCOW
	if cow.ReadIOPS != nil || cow.ReadBandwidth != nil || cow.WriteIOPS == nil || *cow.WriteIOPS != 500 {
		t.Fatalf("existing node YAML deletion semantics changed: %+v", cow)
	}
}

func TestDiffCOWQoSPublicScalarAliases(t *testing.T) {
	const raw = `sandbox:
  resources:
    diff_cow:
      cache_size: &size 32MiB
      max_dirty_size: *size
      max_dirty_size_per_disk: *size
      read_bandwidth: &rate 50MiB
      write_bandwidth: *rate
      read_iops: &count 0
      write_iops: *count
      read_iops_per_disk: *count
      write_iops_per_disk: *count
`
	cfg, err := DecodeConductor(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	cow := cfg.Sandbox.Resources.DiffCOW
	if *cow.MaxDirtySize != "32MiB" || *cow.MaxDirtySizePerDisk != "32MiB" || *cow.WriteBandwidth != "50MiB" || cow.WriteIOPS == nil || *cow.WriteIOPS != 0 || cow.WriteIOPSPerDisk == nil || *cow.WriteIOPSPerDisk != 0 {
		t.Fatalf("scalar aliases changed policy: %+v", cow)
	}
	for _, value := range []string{"1.0", "'1'", "-1"} {
		invalid := strings.Replace(raw, "&count 0", "&count "+value, 1)
		if _, err := DecodeConductor(strings.NewReader(invalid)); err == nil {
			t.Fatalf("invalid aliased IOPS accepted: %s", value)
		}
	}
}

func TestDiffCOWQoSPublicIOPSUint64Boundary(t *testing.T) {
	const input = `sandbox: {resources: {diff_cow: {read_iops: 18446744073709551615, write_iops_per_disk: 18446744073709551615}}}`
	cfg, err := DecodeConductor(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	again, err := DecodeConductor(strings.NewReader(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	cow := again.Sandbox.Resources.DiffCOW
	if cow.ReadIOPS == nil || *cow.ReadIOPS != ^uint64(0) || cow.WriteIOPSPerDisk == nil || *cow.WriteIOPSPerDisk != ^uint64(0) {
		t.Fatalf("IOPS lost integer precision: %+v", cow)
	}
}
