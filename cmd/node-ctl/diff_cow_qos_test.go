package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
)

func TestRenderConductorConfigPreservesDiffCOWQoSZeroPresence(t *testing.T) {
	const input = `
api: { domain: config.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
  resources:
    diff_cow:
      discard: manual
      cache_size: 32MiB
      cache_idle_after: 10s
      max_dirty_size: 0
      read_bandwidth: 0
      write_bandwidth: 0
      read_iops: 0
      write_iops: 0
      max_dirty_size_per_disk: 0
      read_bandwidth_per_disk: 0
      write_bandwidth_per_disk: 0
      read_iops_per_disk: 0
      write_iops_per_disk: 0
`
	before, err := config.DecodeConductor(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	path := writeConductorConfig(t, input)
	output, err := renderConductorConfig(false, false, path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := config.DecodeConductor(strings.NewReader(string(output)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Sandbox.Resources.DiffCOW, after.Sandbox.Resources.DiffCOW) {
		t.Fatalf("normalized config changed QoS zeros/presence:\n%s", output)
	}
}
