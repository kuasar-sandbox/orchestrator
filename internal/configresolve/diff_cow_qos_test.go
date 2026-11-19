package configresolve

import (
	"reflect"
	"strings"
	"testing"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
)

func TestDiffCOWQoSAdaptersPreserveAllFields(t *testing.T) {
	cfg, err := publicconfig.DecodeConductor(strings.NewReader(`sandbox: {resources: {diff_cow: {discard: manual, cache_size: 32MiB, cache_idle_after: 10s, max_dirty_size: 0, read_bandwidth: 50M, write_bandwidth: 20M, read_iops: 0, write_iops: 500, max_dirty_size_per_disk: 64MiB, read_bandwidth_per_disk: 17M, write_bandwidth_per_disk: 9M, read_iops_per_disk: 0, write_iops_per_disk: 99}}}`))
	if err != nil {
		t.Fatal(err)
	}
	before := cfg.Sandbox.Resources
	policy := SandboxResources(before)
	after := PublicSandboxResources(policy)
	if !reflect.DeepEqual(before.DiffCOW, after.DiffCOW) {
		t.Fatalf("adapter dropped fields: before=%+v after=%+v", before.DiffCOW, after.DiffCOW)
	}
	*policy.DiffCOW.ReadIOPS = 5
	*after.DiffCOW.ReadBandwidth = "0"
	if *before.DiffCOW.ReadIOPS != 0 || *before.DiffCOW.ReadBandwidth != "50M" {
		t.Fatal("adapter alias")
	}
	materialized := MaterializedSandboxResources(before)
	if !reflect.DeepEqual(materialized.DiffCOW, before.DiffCOW) {
		t.Fatal("materialization changed explicit zeros/presence")
	}
}
