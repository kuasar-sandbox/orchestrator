package sandboxcfg

import (
	"errors"
	"strings"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestParseResourcePatchStrictSchema(t *testing.T) {
	valid := []string{
		`{}`,
		`{"capacity":{}}`,
		`{"capacity":{"cpu":2}}`,
		`{"capacity":{"memory":"2GiB"}}`,
		`{"allocatable":{"cpu":0.5,"memory":"256MiB"}}`,
		`{"startup":{"memory":"1GiB"}}`,
		`{"allocatable":{"memory":"1GiB"},"startup":{"memory":"512MiB"}}`,
		`{"capacity":{"cpu":4,"memory":"8GiB"},"allocatable":{"cpu":1.25,"memory":"512MiB"},"startup":{"memory":"2GiB"}}`,
	}
	for _, raw := range valid {
		if _, err := ParseResourcePatch(raw); err != nil {
			t.Errorf("ParseResourcePatch(%s): %v", raw, err)
		}
	}

	invalid := map[string]struct {
		raw  string
		path string
	}{
		"empty":               {``, NsResource},
		"null":                {`null`, NsResource},
		"array":               {`[]`, NsResource},
		"scalar":              {`true`, NsResource},
		"trailing":            {`{} {}`, NsResource},
		"duplicate root":      {`{"capacity":{},"capacity":{"cpu":2}}`, NsResource + " must be a valid JSON object"},
		"escaped duplicate":   {`{"capacity":{},"\u0063apacity":{"cpu":2}}`, NsResource + " must be a valid JSON object"},
		"duplicate leaf":      {`{"capacity":{"cpu":1,"cpu":2}}`, NsResource + " must be a valid JSON object"},
		"unknown root":        {`{"future":{}}`, NsResource},
		"control":             {`{"control":{}}`, NsResource + ".control is node-managed"},
		"overhead":            {`{"overhead":{"memory":"1MiB"}}`, NsResource + ".overhead is not request-configurable"},
		"watermark":           {`{"watermark_high":{}}`, NsResource + ".watermark_high is node-managed"},
		"sensor":              {`{"sensor":{}}`, NsResource + ".sensor is node-managed"},
		"deflate":             {`{"allocatable":{"deflate_on_oom":false}}`, NsResource + ".allocatable.deflate_on_oom is node-managed"},
		"capacity null":       {`{"capacity":null}`, NsResource + ".capacity"},
		"capacity array":      {`{"capacity":[]}`, NsResource + ".capacity"},
		"capacity unknown":    {`{"capacity":{"future":1}}`, NsResource + ".capacity"},
		"capacity cpu zero":   {`{"capacity":{"cpu":0}}`, NsResource + ".capacity.cpu"},
		"capacity cpu neg":    {`{"capacity":{"cpu":-1}}`, NsResource + ".capacity.cpu"},
		"capacity cpu float":  {`{"capacity":{"cpu":1.5}}`, NsResource + ".capacity.cpu"},
		"capacity cpu null":   {`{"capacity":{"cpu":null}}`, NsResource + ".capacity.cpu"},
		"capacity mem empty":  {`{"capacity":{"memory":""}}`, NsResource + ".capacity.memory"},
		"capacity mem bad":    {`{"capacity":{"memory":"many"}}`, NsResource + ".capacity.memory"},
		"capacity mem zero":   {`{"capacity":{"memory":"0"}}`, NsResource + ".capacity.memory"},
		"capacity mem neg":    {`{"capacity":{"memory":"-1MiB"}}`, NsResource + ".capacity.memory"},
		"capacity mem null":   {`{"capacity":{"memory":null}}`, NsResource + ".capacity.memory"},
		"alloc cpu zero":      {`{"allocatable":{"cpu":0}}`, NsResource + ".allocatable.cpu"},
		"alloc cpu neg":       {`{"allocatable":{"cpu":-0.5}}`, NsResource + ".allocatable.cpu"},
		"alloc mem empty":     {`{"allocatable":{"memory":""}}`, NsResource + ".allocatable.memory"},
		"alloc cpu above cap": {`{"capacity":{"cpu":1},"allocatable":{"cpu":2}}`, NsResource + ".allocatable.cpu"},
		"alloc mem above cap": {`{"capacity":{"memory":"128MiB"},"allocatable":{"memory":"256MiB"}}`, NsResource + ".allocatable.memory"},
		"startup unknown":     {`{"startup":{"cpu":1}}`, NsResource + ".startup"},
		"startup mem zero":    {`{"startup":{"memory":"0MiB"}}`, NsResource + ".startup.memory"},
		"startup above cap":   {`{"capacity":{"memory":"1GiB"},"startup":{"memory":"2GiB"}}`, NsResource + ".startup.memory"},
	}
	for name, test := range invalid {
		t.Run(name, func(t *testing.T) {
			_, err := ParseResourcePatch(test.raw)
			if err == nil || !strings.Contains(err.Error(), test.path) {
				t.Fatalf("ParseResourcePatch(%s) error = %v, want path %q", test.raw, err, test.path)
			}
		})
	}
}

func TestResourceMetadataMergesLeavesAndCanonicalizes(t *testing.T) {
	base := map[string]string{
		NsResource: ` { "capacity" : { "memory" : "8GiB" }, "allocatable" : { "cpu" : 0.5 } } `,
		NsNetwork:  `{"hostname":"base"}`,
		"keep":     "base",
	}
	over := map[string]string{
		NsResource: `{"capacity":{"cpu":4},"allocatable":{"memory":"512MiB"},"startup":{"memory":"1GiB"}}`,
		NsNetwork:  `{"hostname":"over"}`,
	}
	merged, err := MergeMetadata(base, over)
	if err != nil {
		t.Fatal(err)
	}
	wantResource := `{"capacity":{"cpu":4,"memory":"8GiB"},"allocatable":{"cpu":0.5,"memory":"512MiB"},"startup":{"memory":"1GiB"}}`
	if merged[NsResource] != wantResource {
		t.Fatalf("resource merge = %s, want %s", merged[NsResource], wantResource)
	}
	if merged[NsNetwork] != over[NsNetwork] || merged["keep"] != "base" {
		t.Fatalf("ordinary namespaces changed semantics: %+v", merged)
	}
	if base[NsResource] == merged[NsResource] || over[NsResource] != `{"capacity":{"cpu":4},"allocatable":{"memory":"512MiB"},"startup":{"memory":"1GiB"}}` {
		t.Fatal("merge mutated an input map")
	}

	_, err = MergeMetadata(
		map[string]string{NsResource: `{"allocatable":{"deflate_on_oom":false}}`},
		map[string]string{NsResource: `{"capacity":{"cpu":8},"allocatable":{"cpu":8,"memory":"8GiB"}}`},
	)
	if err == nil || !strings.Contains(err.Error(), "deflate_on_oom") {
		t.Fatalf("valid high-priority patch hid invalid base: %v", err)
	}
}

func TestApplyCapacityPreservesResourceSiblings(t *testing.T) {
	meta := map[string]string{
		NsResource: `{"capacity":{"memory":"1GiB"},"allocatable":{"cpu":0.5,"memory":"256MiB"},"startup":{"memory":"512MiB"}}`,
		"keep":     "value",
	}
	got, err := ApplyCapacity(meta, 4, 8192)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"capacity":{"cpu":4,"memory":"8192MiB"},"allocatable":{"cpu":0.5,"memory":"256MiB"},"startup":{"memory":"512MiB"}}`
	if got[NsResource] != want || got["keep"] != "value" {
		t.Fatalf("ApplyCapacity = %+v, want resource %s", got, want)
	}
	if meta[NsResource] == got[NsResource] {
		t.Fatal("ApplyCapacity mutated its input")
	}
	for name, values := range map[string][2]int{
		"negative cpu":    {-1, 0},
		"negative memory": {0, -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ApplyCapacity(nil, values[0], values[1]); err == nil {
				t.Fatal("negative first-class capacity accepted")
			}
		})
	}
	if _, err := ApplyCapacity(map[string]string{NsResource: `{"control":{}}`}, 8, 8192); err == nil {
		t.Fatal("capacity overlay hid invalid existing resource")
	}
	canonicalEmpty, err := ApplyCapacity(map[string]string{NsResource: ` { } `}, 0, 0)
	if err != nil || canonicalEmpty[NsResource] != `{}` {
		t.Fatalf("empty resource canonicalization = %+v, %v", canonicalEmpty, err)
	}
}

func TestResolveResourcesDefaultsAndNormalization(t *testing.T) {
	resources, err := ResolveResources(ResourceResolveInput{})
	if err != nil {
		t.Fatal(err)
	}
	if resources.Capacity.CPU != 2 || resources.Capacity.Memory != "2GiB" ||
		resources.Allocatable.CPU != 2 || resources.Allocatable.Memory != "256MiB" {
		t.Fatalf("defaults = %+v", resources)
	}
	if resources.Overhead == nil || resources.Overhead.Memory != "32MiB" {
		t.Fatalf("overhead = %+v", resources.Overhead)
	}
	if resources.Allocatable.DeflateOnOOM == nil || !*resources.Allocatable.DeflateOnOOM {
		t.Fatalf("deflate_on_oom = %v", resources.Allocatable.DeflateOnOOM)
	}
	if resources.Startup == nil || resources.Startup.Memory != "2GiB" || resources.Control.Controller != "" ||
		resources.WatermarkHigh == nil || resources.WatermarkHigh.Ratio != 0.875 || resources.Control.Sensor != nil {
		t.Fatalf("static/default runtime-only fields = %+v", resources)
	}

	small, err := ResolveResources(ResourceResolveInput{Patch: mustResourcePatch(t, `{"capacity":{"cpu":1,"memory":"128MiB"}}`)})
	if err != nil {
		t.Fatal(err)
	}
	if small.Allocatable.CPU != 1 || small.Allocatable.Memory != "128MiB" {
		t.Fatalf("small inherited floor was not normalized: %+v", small)
	}
	if small.Allocatable.DeflateOnOOM != nil {
		t.Fatalf("no-balloon config rendered deflate_on_oom: %+v", small.Allocatable)
	}

	configuredCPU := 2.0
	inheritedCPU, err := ResolveResources(ResourceResolveInput{
		Node:  NodeResourcePolicy{Allocatable: NodeAllocatablePolicy{CPU: &configuredCPU}},
		Patch: mustResourcePatch(t, `{"capacity":{"cpu":1}}`),
	})
	if err != nil || inheritedCPU.Allocatable.CPU != 1 {
		t.Fatalf("inherited node CPU normalization = %+v, %v", inheritedCPU, err)
	}
	badCapacityCPU, badAllocatableCPU := 1, 2.0
	_, err = ResolveResources(ResourceResolveInput{Patch: ResourcePatch{
		Capacity: &CapacityPatch{CPU: &badCapacityCPU}, Allocatable: &AllocatablePatch{CPU: &badAllocatableCPU},
	}})
	if !errors.Is(err, ErrInvalidResourceRequest) {
		t.Fatalf("explicit allocatable CPU above capacity error = %v", err)
	}
	badCapacityMemory, badAllocatableMemory := "128MiB", "256MiB"
	_, err = ResolveResources(ResourceResolveInput{Patch: ResourcePatch{
		Capacity: &CapacityPatch{Memory: &badCapacityMemory}, Allocatable: &AllocatablePatch{Memory: &badAllocatableMemory},
	}})
	if !errors.Is(err, ErrInvalidResourceRequest) {
		t.Fatalf("explicit allocatable memory above capacity error = %v", err)
	}
}

func TestResolveResourcesDynamicStartupAndNodeOwnership(t *testing.T) {
	const identity = "/run/real/controller.sock"
	resources, err := ResolveResources(ResourceResolveInput{
		Patch:                    mustResourcePatch(t, `{"capacity":{"cpu":8,"memory":"8GiB"}}`),
		Dynamic:                  true,
		ControllerSocketIdentity: identity,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resources.Allocatable.CPU != 8 || resources.Startup == nil || resources.Startup.Memory != "8GiB" ||
		resources.Control.Controller != identity {
		t.Fatalf("dynamic defaults = %+v", resources)
	}
	if resources.Control.CgroupPath != "" || resources.Control.CgroupFD != 0 || resources.Control.Sensor != nil ||
		resources.WatermarkHigh == nil || resources.WatermarkHigh.Ratio != 0.875 {
		t.Fatalf("node runtime capability/defaults were rendered: %+v", resources)
	}

	node := NodeResourcePolicy{Startup: &NodeStartupPolicy{Memory: "512MiB"}}
	adjusted, err := ResolveResources(ResourceResolveInput{
		Node:                     node,
		Patch:                    mustResourcePatch(t, `{"allocatable":{"memory":"1GiB"}}`),
		Dynamic:                  true,
		ControllerSocketIdentity: identity,
	})
	if err != nil || adjusted.Startup == nil || adjusted.Startup.Memory != "512MiB" {
		t.Fatalf("independent node startup headroom = %+v, %v", adjusted, err)
	}
	adjusted, err = ResolveResources(ResourceResolveInput{
		Node:                     node,
		Patch:                    mustResourcePatch(t, `{"capacity":{"memory":"256MiB"}}`),
		Dynamic:                  true,
		ControllerSocketIdentity: identity,
	})
	if err != nil || adjusted.Startup == nil || adjusted.Startup.Memory != "256MiB" {
		t.Fatalf("node startup upper-bound adjustment = %+v, %v", adjusted, err)
	}

	static, err := ResolveResources(ResourceResolveInput{
		Patch: mustResourcePatch(t, `{"allocatable":{"memory":"1GiB"},"startup":{"memory":"128MiB"}}`),
	})
	if err != nil || static.Startup == nil || static.Startup.Memory != "128MiB" {
		t.Fatalf("static independent startup = %+v, %v", static, err)
	}

	for name, input := range map[string]ResourceResolveInput{
		"above capacity": {
			Patch: mustResourcePatch(t, `{"startup":{"memory":"4GiB"}}`), Dynamic: true, ControllerSocketIdentity: identity,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ResolveResources(input); !errors.Is(err, ErrInvalidResourceRequest) {
				t.Fatalf("error = %v, want request rejection", err)
			}
		})
	}
}

func TestResolveResourcesRestoreCapacityConstraint(t *testing.T) {
	snapshot := &rtconfig.CapacityConfig{CPU: 4, Memory: "4GiB"}
	resources, err := ResolveResources(ResourceResolveInput{
		Patch:            mustResourcePatch(t, `{"capacity":{"cpu":4,"memory":"4096MiB"},"allocatable":{"memory":"512MiB"}}`),
		Restore:          true,
		SnapshotCapacity: snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resources.Capacity != *snapshot || resources.Allocatable.CPU != 4 || resources.Allocatable.Memory != "512MiB" {
		t.Fatalf("restore resources = %+v", resources)
	}
	for name, raw := range map[string]string{
		"cpu mismatch":    `{"capacity":{"cpu":2}}`,
		"memory mismatch": `{"capacity":{"memory":"2GiB"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ResolveResources(ResourceResolveInput{Patch: mustResourcePatch(t, raw), Restore: true, SnapshotCapacity: snapshot})
			if !errors.Is(err, ErrInvalidResourceRequest) {
				t.Fatalf("restore mismatch error = %v", err)
			}
		})
	}
	if _, err := ResolveResources(ResourceResolveInput{Restore: true}); err == nil || errors.Is(err, ErrInvalidResourceRequest) {
		t.Fatalf("missing snapshot capacity error = %v", err)
	}
}

func TestValidateNodeResourcePolicy(t *testing.T) {
	if err := ValidateNodeResourcePolicy(NodeResourcePolicy{}, false); err != nil {
		t.Fatalf("default static policy: %v", err)
	}
	if err := ValidateNodeResourcePolicy(NodeResourcePolicy{Startup: &NodeStartupPolicy{Memory: "1GiB"}}, false); err != nil {
		t.Fatalf("static node policy rejected startup: %v", err)
	}
	if err := ValidateNodeResourcePolicy(NodeResourcePolicy{Startup: &NodeStartupPolicy{Memory: "1GiB"}}, true); err != nil {
		t.Fatalf("dynamic node policy rejected startup: %v", err)
	}
	badCPU := 3.0
	if err := ValidateNodeResourcePolicy(NodeResourcePolicy{Allocatable: NodeAllocatablePolicy{CPU: &badCPU}}, false); err == nil {
		t.Fatal("node allocatable CPU above capacity accepted")
	}
	zero, one := 0.0, 1.0
	for _, ratio := range []*float64{&zero, &one} {
		if err := ValidateNodeResourcePolicy(NodeResourcePolicy{
			WatermarkHigh: &NodeWatermarkHighPolicy{Ratio: ratio},
		}, false); err == nil {
			t.Fatalf("invalid watermark ratio %v accepted", *ratio)
		}
	}
}

func mustResourcePatch(t *testing.T, raw string) ResourcePatch {
	t.Helper()
	patch, err := ParseResourcePatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	return patch
}
