package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func writeYAMLLayers(t *testing.T, layers ...string) []string {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, len(layers))
	for i, layer := range layers {
		paths[i] = filepath.Join(dir, fmt.Sprintf("%d.yaml", i))
		if err := os.WriteFile(paths[i], []byte(layer), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return paths
}

func TestLoadConductorMultipleFiles(t *testing.T) {
	paths := writeYAMLLayers(t, `
units: {runner: old@.service, runner_pool_size: 2}
cluster: {labels: {retained: base, removed: old, changed: base}}
sandbox: {timeout_sec: 60, network: {dns: [1.1.1.1]}}
checkpoint: {merge_ref: true}
`, `
units:
  runner: null
  runner_pool_size: null
  runner_pools: [{unit: custom@.service, size: 0}]
cluster: {labels: {removed: null, changed: platform, empty: ""}}
sandbox: {timeout_sec: 0, network: {dns: []}}
checkpoint: {merge_ref: false}
`)
	got, err := LoadConductor(strings.Join(paths, ":"))
	if err != nil {
		t.Fatal(err)
	}
	want, err := DecodeConductor(strings.NewReader(`
units: {runner_pools: [{unit: custom@.service, size: 0}]}
cluster: {labels: {retained: base, changed: platform, empty: ""}}
sandbox: {timeout_sec: 0, network: {dns: []}}
checkpoint: {merge_ref: false}
`))
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("merged config differs from the equivalent native document: error=%v\ngot=%+v\nwant=%+v", err, got, want)
	}
	// Zero and empty arrays reached the native defaulting rules, rather than
	// silently retaining the base values. False and empty strings remain explicit.
	if got.Sandbox.TimeoutSec != 300 || !reflect.DeepEqual(got.Sandbox.Network.DNS, []string{"169.254.169.253"}) ||
		got.Checkpoint.MergeRef == nil || *got.Checkpoint.MergeRef || got.Cluster.Labels["empty"] != "" {
		t.Fatalf("native zero/default semantics changed: %+v", got)
	}
	conflict := writeYAMLLayers(t, "units: {runner_pool_size: 0}", "units: {runner_pools: [{unit: r@.service, size: 0}]}")
	if _, err := LoadConductor(strings.Join(conflict, ":")); err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("explicit legacy zero conflict was lost: %v", err)
	}
}

func TestLoadMultipleFilesOrderAndDeferredDefaults(t *testing.T) {
	paths := writeYAMLLayers(t, "cluster: {labels: {layer: base}}", "cluster: {labels: {layer: overlay}}")
	got, err := LoadConductor(strings.Join([]string{paths[0], paths[1], paths[0]}, ":"))
	if err != nil || got.Cluster.Labels["layer"] != "base" {
		t.Fatalf("path list was reordered or deduplicated: cfg=%+v err=%v", got, err)
	}
	proxyPaths := writeYAMLLayers(t, "workers: 5\npaths: {run_root: /run/sandbox}", "data_listen: ':3443'\nauth: off")
	proxy, err := LoadProxy(strings.Join(proxyPaths, ":"))
	if err != nil || proxy.Workers != 5 || proxy.DataListen != ":3443" || proxy.Auth != AuthOff {
		t.Fatalf("proxy layers/defaulting: cfg=%+v err=%v", proxy, err)
	}
	telemetryPaths := writeYAMLLayers(t, "query: {backend: local}", "local: {enabled: true}")
	telemetry, err := LoadTelemetry(strings.Join(telemetryPaths, ":"))
	if err != nil || !telemetry.Local.Enabled || telemetry.Query.Backend != "local" {
		t.Fatalf("incomplete telemetry layers were validated before merging: cfg=%+v err=%v", telemetry, err)
	}
}

func TestLoadMultipleFilesResourcePoolSwitch(t *testing.T) {
	paths := writeYAMLLayers(t, `
resource_listen:
  resources: {physical_cpu: 8, physical_memory: 16GiB}
  watermarks: {operational_margin_factor: 0.1}
`, `
resource_listen:
  resources: null
  watermarks: {operational_margin_factor: null}
  pools:
    - {name: default, allocatable: {memory: 4GiB}}
    - {name: second, allocatable: {memory: 4GiB}}
`)
	cfg, err := LoadConductor(strings.Join(paths, ":"))
	if err != nil || len(cfg.ResourceListen.Pools) != 2 || cfg.ResourceListen.hasLegacyPhysical() {
		t.Fatalf("legacy resource fields survived deletion: cfg=%+v err=%v", cfg, err)
	}
}

func TestLoadMultipleFilesFileLocalAnchorsAndMerges(t *testing.T) {
	t.Run("replaced anchor definition", func(t *testing.T) {
		paths := writeYAMLLayers(t, `
units:
  runner_pools: &pools [{unit: old@.service, size: 1}]
  builder_pools: *pools
`, `units: {runner_pools: [{unit: new@.service, size: 2}]}`)
		cfg, err := LoadConductor(strings.Join(paths, ":"))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Units.RunnerPools[0].Unit != "new@.service" || cfg.Units.BuilderPools[0].Unit != "old@.service" {
			t.Fatalf("replacement changed another reference: %+v", cfg.Units)
		}
	})
	t.Run("delete fields inherited by merge key", func(t *testing.T) {
		paths := writeYAMLLayers(t, "units: {<<: {runner: old@.service, runner_pool_size: 2}}", `
units:
  runner: null
  runner_pool_size: null
  runner_pools: [{unit: new@.service, size: 0}]
`)
		if _, err := LoadConductor(strings.Join(paths, ":")); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("merge sequence precedence", func(t *testing.T) {
		paths := writeYAMLLayers(t, "units: {runner_pools: [{<<: [{unit: first@.service, size: 1}, {unit: second@.service, size: 2}], size: 3}]}", "{}")
		cfg, err := LoadConductor(strings.Join(paths, ":"))
		if err != nil || cfg.Units.RunnerPools[0].Unit != "first@.service" || cfg.Units.RunnerPools[0].Size != 3 {
			t.Fatalf("native merge precedence changed: cfg=%+v err=%v", cfg, err)
		}
	})
	t.Run("reused anchor names are file local", func(t *testing.T) {
		paths := writeYAMLLayers(t, "paths: {run_root: &p /run/base}\nunits: {dir: *p}", "paths: {run_root: &p /run/next}")
		cfg, err := LoadConductor(strings.Join(paths, ":"))
		if err != nil || cfg.Units.Dir != "/run/base" || cfg.Paths.RunRoot != "/run/next" {
			t.Fatalf("cross-file anchor leakage: cfg=%+v err=%v", cfg, err)
		}
	})
}

func TestLoadMultipleFilesPreservesScalarTextAndCollections(t *testing.T) {
	paths := writeYAMLLayers(t, `
cluster:
  labels:
    precise: 18446744073709551617
    numeric_text: !!str 0007
    preserved_null: null
    removed: old
collector: false
`, `
cluster: {labels: {removed: null}}
collector: null
`)
	cfg, err := LoadConductor(strings.Join(paths, ":"))
	if err != nil || cfg.Cluster.Labels["precise"] != "18446744073709551617" || cfg.Cluster.Labels["numeric_text"] != "0007" {
		t.Fatalf("scalar text/tag changed: cfg=%+v err=%v", cfg, err)
	}
	// A first-layer null remains native input. It must not become deletion just
	// because the operator appends an otherwise empty overlay.
	nullPaths := writeYAMLLayers(t, "units: {runner_pools: null}", "{}")
	for _, list := range []string{nullPaths[0], strings.Join(nullPaths, ":")} {
		if _, err := LoadConductor(list); err == nil || !strings.Contains(err.Error(), "must not be null") {
			t.Fatalf("first-layer null semantics changed: %v", err)
		}
	}
	emptyPaths := writeYAMLLayers(t, "units: {runner_pools: [{unit: r@.service, size: 0}]}", "units: {runner_pools: []}")
	if _, err := LoadConductor(strings.Join(emptyPaths, ":")); err == nil || !strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("explicit empty sequence became absent: %v", err)
	}
}

func TestLoadMultipleFilesRejectsHiddenStructuralErrors(t *testing.T) {
	for name, bad := range map[string]string{
		"duplicate key":      "units: {runner_pool_size: 1, runner_pool_size: 2}",
		"nested duplicate":   "units: {runner_pools: [{unit: one@.service, unit: two@.service, size: 0}]}",
		"multiple documents": "{}\n---\n{}",
		"syntax":             "units: [",
		"bad merge":          "units: {<<: false}",
		"cyclic alias":       "cluster: &self {labels: *self}",
		"cross file alias":   "units: {dir: *external}",
	} {
		t.Run(name, func(t *testing.T) {
			paths := writeYAMLLayers(t, bad, "units: null\ncluster: null")
			_, err := LoadConductor(strings.Join(paths, ":"))
			if err == nil || !strings.Contains(err.Error(), paths[0]) {
				t.Fatalf("bad earlier file was hidden or unlocated: %v", err)
			}
		})
	}
	paths := writeYAMLLayers(t, "unknown: 1", "{}")
	_, err := LoadConductor(strings.Join(paths, ":"))
	if err == nil || !strings.Contains(err.Error(), "unknown") || !strings.Contains(err.Error(), strings.Join(paths, ":")) {
		t.Fatalf("final schema error lacks field/input list: %v", err)
	}
}

func TestLoadMultipleFilesPathErrorsAndSizeLimits(t *testing.T) {
	paths := writeYAMLLayers(t, "{}", "{}")
	for _, input := range []string{"", ":" + paths[0], paths[0] + ":", paths[0] + "::" + paths[1], paths[0] + ":" + t.TempDir(), paths[0] + ":" + paths[0] + ".missing"} {
		if _, err := LoadConductor(input); err == nil {
			t.Fatalf("invalid path list accepted: %q", input)
		}
	}
	// Comments count toward total input even though final normalization removes them.
	large := "#" + strings.Repeat("x", maxConfigBytes/2) + "\n{}"
	paths = writeYAMLLayers(t, large, large)
	if _, err := LoadConductor(strings.Join(paths, ":")); err == nil || !strings.Contains(err.Error(), "combined configuration exceeds") {
		t.Fatalf("split input bypassed total limit: %v", err)
	}
	// This small alias graph expands to more than 4 MiB without excessive node
	// aliasing. The bounded encoder must stop instead of allocating the full output.
	long := strings.Repeat("x", maxConfigBytes/2)
	paths = writeYAMLLayers(t, "cluster:\n  labels:\n    first: &long "+long+"\n    second: *long\n", "{}")
	if _, err := LoadConductor(strings.Join(paths, ":")); err == nil || !strings.Contains(err.Error(), "merged configuration exceeds") {
		t.Fatalf("expanded output bypassed final limit: %v", err)
	}
}

func TestLoadSingleFileKeepsNativeResult(t *testing.T) {
	for _, raw := range []string{"{}", "null", "cluster: {labels: {yes: yes, precise: 18446744073709551617}}", "units: {runner_pools: &p [{unit: r@.service, size: 0}], builder_pools: *p}"} {
		path := writeYAMLLayers(t, raw)[0]
		got, loadErr := LoadConductor(path)
		want, decodeErr := DecodeConductor(strings.NewReader(raw))
		if (loadErr == nil) != (decodeErr == nil) || !reflect.DeepEqual(got, want) {
			t.Fatalf("single file differs from native decode: load=%v decode=%v", loadErr, decodeErr)
		}
	}
}

func TestYAMLLayerKeepsNativeMappingKeyTypes(t *testing.T) {
	raw := "collector: {custom: {1: 9007199254740993, 2: !!str 0007}}"
	paths := writeYAMLLayers(t, raw, "{}")
	merged, err := mergeYAMLFiles(paths, reflect.TypeFor[Telemetry]())
	if err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err := yaml.NewDecoder(bytes.NewReader(merged)).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte(raw), &want); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("native key/value types changed: got=%#v want=%#v err=%v", got, want, err)
	}
}

func TestLoadMultipleFilesStringMappingKeys(t *testing.T) {
	for _, tc := range []struct {
		base, overlay string
		want          map[string]string
	}{
		{"cluster: {labels: {01: old}}", "cluster: {labels: {1: new}}", map[string]string{"01": "old", "1": "new"}},
		{"cluster: {labels: {1: old}}", `cluster: {labels: {"1": new}}`, map[string]string{"1": "new"}},
		{"cluster: {labels: {1: old}}", `cluster: {labels: {"1": null}}`, map[string]string{}},
	} {
		paths := writeYAMLLayers(t, tc.base, tc.overlay)
		cfg, err := LoadConductor(strings.Join(paths, ":"))
		if err != nil || !reflect.DeepEqual(cfg.Cluster.Labels, tc.want) {
			t.Fatalf("string key identity changed: got=%+v want=%+v err=%v", cfg, tc.want, err)
		}
	}
}

func TestLoadMultipleFilesKeepsNativeEffectiveMapping(t *testing.T) {
	// yaml.v3 has type-dependent << behavior for numeric-looking string keys.
	// An empty overlay must keep the native single-document result.
	raw := "cluster: {labels: {<<: {1: inherited}, 1: explicit}}"
	paths := writeYAMLLayers(t, raw, "{}")
	got, err := LoadConductor(strings.Join(paths, ":"))
	if err != nil {
		t.Fatal(err)
	}
	want, err := DecodeConductor(strings.NewReader(raw))
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("native merge behavior changed: got=%+v want=%+v err=%v", got.Cluster.Labels, want.Cluster.Labels, err)
	}
	// These two spellings are one native integer key in an untyped Collector
	// subtree. Deleting it must not reveal the overwritten earlier spelling.
	paths = writeYAMLLayers(t, "collector: {custom: {01: old, 1: new}}", "collector: {custom: {1: null}}")
	telemetry, err := LoadTelemetry(strings.Join(paths, ":"))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := DecodeTelemetry(strings.NewReader("collector: {custom: {}}"))
	if err != nil || !reflect.DeepEqual(telemetry, expected) {
		t.Fatalf("deleted Collector key revived an old value: got=%#v want=%#v err=%v", telemetry.Collector, expected.Collector, err)
	}
}
