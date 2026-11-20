package sandboxcfg

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

const qosFullResourcePatch = `{"diff_cow":{"cache_size":"32MiB","cache_idle_after":"10s","discard":"manual","max_dirty_size":"32MiB","read_bandwidth":"50M","write_bandwidth":"20M","read_iops":1000,"write_iops":500,"max_dirty_size_per_disk":"64MiB","read_bandwidth_per_disk":"17M","write_bandwidth_per_disk":"9M","read_iops_per_disk":201,"write_iops_per_disk":99}}`
const qosZeroResourcePatch = `{"diff_cow":{"max_dirty_size":"0","read_bandwidth":"0","write_bandwidth":"0","read_iops":0,"write_iops":0,"max_dirty_size_per_disk":"0","read_bandwidth_per_disk":"0","write_bandwidth_per_disk":"0","read_iops_per_disk":0,"write_iops_per_disk":0}}`

func TestDiffCOWQoSMetadataMergeAndResolve(t *testing.T) {
	base, err := ParseResourcePatch(qosFullResourcePatch)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := ParseResourcePatch(qosZeroResourcePatch)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := MergeMetadata(map[string]string{NsResource: qosFullResourcePatch}, map[string]string{NsResource: qosZeroResourcePatch})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := ParseResourcePatch(meta[NsResource])
	if err != nil {
		t.Fatal(err)
	}
	if *merged.DiffCOW.ReadIOPS != 0 || *merged.DiffCOW.CacheSize != "32MiB" || *merged.DiffCOW.Discard != "manual" {
		t.Fatalf("metadata merge failed: %s", meta[NsResource])
	}
	cloned, err := MergeResourcePatch(base, zero)
	if err != nil {
		t.Fatal(err)
	}
	*cloned.DiffCOW.ReadIOPS = 7
	*cloned.DiffCOW.WriteBandwidthPerDisk = "1MiB"
	if *base.DiffCOW.ReadIOPS != 1000 || *zero.DiffCOW.ReadIOPS != 0 || *zero.DiffCOW.WriteBandwidthPerDisk != "0" {
		t.Fatal("resource merge shared QoS leaf pointers")
	}
	for _, fromArtifact := range []bool{false, true} {
		r, err := ResolveResources(ResourceResolveInput{Node: NodeResourcePolicy{DiffCOW: base.DiffCOW}, Patch: zero, Restore: fromArtifact, ArtifactCapacity: &rtconfig.CapacityConfig{CPU: 2, Memory: "2GiB"}})
		if err != nil {
			t.Fatal(err)
		}
		limits, err := r.DiffCOW.Limits()
		if err != nil || limits != (rtconfig.DiffCOWLimits{}) {
			t.Fatalf("resolved zero policy=%+v, %v", limits, err)
		}
		if r.DiffCOW.ReadIOPS == nil || r.DiffCOW.ReadIOPSPerDisk == nil {
			t.Fatal("resolved explicit zeros became omitted")
		}
	}
	if *base.DiffCOW.ReadIOPS != 1000 || *base.DiffCOW.MaxDirtySize != "32MiB" {
		t.Fatal("resource resolve mutated node policy")
	}
	canonical, err := MarshalResourcePatch(zero)
	if err != nil {
		t.Fatal(err)
	}
	again, err := ParseResourcePatch(canonical)
	if err != nil || !reflect.DeepEqual(zero, again) {
		t.Fatalf("canonical metadata lost zero: %s, %v", canonical, err)
	}
	for _, raw := range []string{`{"diff_cow":{"read_iops":1.0}}`, `{"diff_cow":{"read_iops":-1}}`, `{"diff_cow":{"read_iops":null}}`, `{"diff_cow":{"read_iops":"1"}}`, `{"diff_cow":{"read_iops":0,"read_iops":1}}`, `{"diff_cow":{"read_bandwidth":"50M/s"}}`} {
		if _, err := ParseResourcePatch(raw); err == nil {
			t.Fatalf("bad metadata accepted: %s", raw)
		}
	}
}

func TestDiffCOWQoSResourceIOPSUint64Boundary(t *testing.T) {
	patch, err := ParseResourcePatch(`{"diff_cow":{"read_iops":18446744073709551615,"write_iops_per_disk":18446744073709551615}}`)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := MarshalResourcePatch(patch)
	if err != nil {
		t.Fatal(err)
	}
	again, err := ParseResourcePatch(canonical)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolveResources(ResourceResolveInput{Patch: again})
	if err != nil {
		t.Fatal(err)
	}
	limits, err := resolved.DiffCOW.Limits()
	if err != nil {
		t.Fatal(err)
	}
	if limits.Aggregate.ReadIOPS != ^uint64(0) || limits.PerDisk.WriteIOPS != ^uint64(0) {
		t.Fatalf("IOPS lost integer precision: %+v", limits)
	}
}

func TestDiffCOWQoSHostYAMLImageFromAndRestore(t *testing.T) {
	for _, raw := range []string{qosFullResourcePatch, qosZeroResourcePatch} {
		patch, err := ParseResourcePatch(raw)
		if err != nil {
			t.Fatal(err)
		}
		resources, err := ResolveResources(ResourceResolveInput{Patch: patch})
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []types.LaunchMode{types.LaunchImage, types.LaunchCold, types.LaunchMemory} {
			p := artifactRendererParams(mode)
			if mode == types.LaunchImage {
				p = baseParams(types.ProfileBare)
			}
			p.Resources = resources
			body, err := p.BuildYAML()
			if err != nil {
				t.Fatal(err)
			}
			host, presence, err := loadMergedHostConfig(t, body)
			if err != nil {
				t.Fatal(err)
			}
			want := resources.DiffCOW.Clone()
			if mode == types.LaunchImage {
				if !reflect.DeepEqual(host.Resources.DiffCOW, want) {
					t.Fatalf("image Host YAML changed QoS: %s", body)
				}
				continue
			}
			var runtime *rtconfig.SandboxConfig
			var c0 *rtconfig.PortableSandboxConfig
			if mode == types.LaunchMemory {
				want.Discard = ""
				runtime, c0, err = rtconfig.ApplyRestoreRules(rendererPortableConfig(), host, presence)
			} else {
				runtime, c0, err = rtconfig.ApplyFromRules(rendererPortableConfig(), host, presence, rtconfig.ApplyFromOptions{})
			}
			if err != nil {
				t.Fatalf("mode=%s: %v\n%s", mode, err, body)
			}
			if !reflect.DeepEqual(runtime.Resources.DiffCOW, want) {
				t.Fatalf("host roundtrip changed policy for %s:\n%s\ngot=%+v want=%+v", mode, body, runtime.Resources.DiffCOW, want)
			}
			*runtime.Resources.DiffCOW.ReadIOPS = 7
			if *host.Resources.DiffCOW.ReadIOPS == 7 {
				t.Fatal("ApplyFrom/Restore shared host pointers")
			}
			portable, err := rtconfig.MarshalPortableSandboxConfig(c0)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(portable, []byte("diff_cow")) {
				t.Fatal("QoS changed portable artifact")
			}
		}
	}
}

func TestDiffCOWQoSZeroClearsOnlySelectedLayer(t *testing.T) {
	base, err := ParseResourcePatch(qosFullResourcePatch)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, patch                                             string
		readBandwidth, readPerDisk, writeIOPS, writeIOPSPerDisk uint64
	}{
		{"aggregate", `{"diff_cow":{"read_bandwidth":"0","write_iops":0}}`, 0, 17 << 20, 0, 99},
		{"per disk", `{"diff_cow":{"read_bandwidth_per_disk":"0","write_iops_per_disk":0}}`, 50 << 20, 0, 500, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch, err := ParseResourcePatch(tc.patch)
			if err != nil {
				t.Fatal(err)
			}
			resolved, err := ResolveResources(ResourceResolveInput{Node: NodeResourcePolicy{DiffCOW: base.DiffCOW}, Patch: patch})
			if err != nil {
				t.Fatal(err)
			}
			limits, err := resolved.DiffCOW.Limits()
			if err != nil {
				t.Fatal(err)
			}
			if limits.Aggregate.ReadBandwidth != tc.readBandwidth || limits.PerDisk.ReadBandwidth != tc.readPerDisk || limits.Aggregate.WriteIOPS != tc.writeIOPS || limits.PerDisk.WriteIOPS != tc.writeIOPSPerDisk {
				t.Fatalf("zero cleared another policy layer: %+v", limits)
			}
		})
	}
}
