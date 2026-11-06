package config

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const twoPoolConfig = `
resource_listen:
  enabled: true
  pools:
    - {name: numa0, capacity: {cpu: 8, memory: 128GiB}, allocatable: {cpu: 8, memory: 32GiB}, watermarks: {cpu: {low_factor: 0}}}
    - {name: numa1, allocatable: {memory: 32GiB}}
units:
  runner_pools:
    - {name: r0, resource_pool: numa0, unit: r0@.service, size: 2, capacity: 32, priority: 100}
    - {name: r1, resource_pool: numa1, unit: r1@.service, size: 0}
  builder_pools:
    - {name: b0, resource_pool: numa0, unit: b0@.service, size: 0}
`

func TestResourcePoolRoundtripCloneAndDefaults(t *testing.T) {
	c, err := DecodeConductor(strings.NewReader(twoPoolConfig))
	if err != nil {
		t.Fatal(err)
	}
	for _, encode := range []func(any) ([]byte, error){yaml.Marshal, json.Marshal} {
		raw, err := encode(c)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeConductor(strings.NewReader(string(raw)))
		if err != nil {
			t.Fatalf("roundtrip: %v\n%s", err, raw)
		}
		if len(got.ResourceListen.Pools) != 2 || got.ResourceListen.Pools[0].Watermarks["cpu"].LowFactor == nil || *got.ResourceListen.Pools[0].Watermarks["cpu"].LowFactor != 0 {
			t.Fatal("lost explicit zero or pools")
		}
	}
	copy := c.Clone()
	*copy.ResourceListen.Pools[0].Capacity.CPU = 99
	*copy.ResourceListen.Pools[0].Watermarks["cpu"].LowFactor = .1
	*copy.Units.RunnerPools[0].Capacity = 1
	if *c.ResourceListen.Pools[0].Capacity.CPU != 8 || *c.ResourceListen.Pools[0].Watermarks["cpu"].LowFactor != 0 || *c.Units.RunnerPools[0].Capacity != 32 {
		t.Fatal("clone aliases original")
	}
	// Adding pools after defaults (Configure) must not manufacture legacy input.
	r := &ResourceListenConfig{}
	r.ApplyDefaults()
	r.Pools = cloneResourcePools(c.ResourceListen.Pools)
	if err := r.ValidatePools(); err != nil {
		t.Fatal(err)
	}
	r.Resources.PhysicalMemory = "64GiB"
	if err := r.ValidatePools(); err == nil {
		t.Fatal("Configure legacy conflict accepted")
	}
}

func TestResourcePoolRejectsInvalidConfiguration(t *testing.T) {
	for _, replacement := range []struct{ old, value string }{
		{"enabled: true", "enabled: true\n  resources: {physical_memory: auto}"},
		{"enabled: true", "enabled: true\n  watermarks: {operational_margin_factor: 0}"},
		{"name: numa1", "name: numa0"},
		{"name: numa1, allocatable: {memory: 32GiB}", "name: numa1"},
		{"resource_pool: numa1", "resource_pool: missing"},
		{"unit: r1@.service", "unit: r0@.service"},
		{"capacity: 32", "capacity: 1"},
		{"capacity: 32", "capacity: -1"},
		{"cpu: 8", "cpu: -1"},
		{"low_factor: 0", "high_factor: 0.99"},
		{"low_factor: 0", "typo: 0"},
	} {
		t.Run(replacement.value, func(t *testing.T) {
			if _, err := DecodeConductor(strings.NewReader(strings.Replace(twoPoolConfig, replacement.old, replacement.value, 1))); err == nil {
				t.Fatal("accepted invalid pools")
			}
		})
	}
}

func TestResourcePoolYAMLAnchorsAndLegacyNumericCPU(t *testing.T) {
	input := strings.Replace(twoPoolConfig, "capacity: {cpu: 8, memory: 128GiB}", "capacity: &capacity {cpu: 8, memory: 128GiB}", 1)
	input = strings.Replace(input, "name: numa1, allocatable:", "name: numa1, capacity: *capacity, allocatable:", 1)
	c, err := DecodeConductor(strings.NewReader(input))
	if err != nil || c.ResourceListen.Pools[1].Capacity.Memory == nil || *c.ResourceListen.Pools[1].Capacity.Memory != "128GiB" {
		t.Fatalf("resource aliases: %v", err)
	}
	legacy, err := DecodeConductor(strings.NewReader("resource_listen:\n  resources: {physical_cpu: 8}\n"))
	if err != nil || legacy.ResourceListen.Resources.PhysicalCPU != "8" {
		t.Fatalf("legacy numeric CPU: %v", err)
	}
}
