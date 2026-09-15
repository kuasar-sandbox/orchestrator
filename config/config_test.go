package config

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestTerminalRetentionDefaultsOverridesAndValidation(t *testing.T) {
	header := `
api: { domain: example.test }
encryption_key: test-key
`
	base := header + `
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
`
	defaults, err := LoadConductor(writeConfig(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Sandbox.DeadTTL != "24h" || defaults.Sandbox.DeadTTLDur() != 24*time.Hour {
		t.Fatalf("sandbox dead TTL = %q (%s), want 24h", defaults.Sandbox.DeadTTL, defaults.Sandbox.DeadTTLDur())
	}
	if defaults.Builder.TerminalTTL != "24h" || defaults.Builder.TerminalTTLDur() != 24*time.Hour {
		t.Fatalf("builder terminal TTL = %q (%s), want 24h", defaults.Builder.TerminalTTL, defaults.Builder.TerminalTTLDur())
	}

	configured, err := LoadConductor(writeConfig(t, header+`
sandbox: { dead_ttl: 2h, boot: { kernel: /kernel, runtime: /runtime } }
builder: { terminal_ttl: 3h }
`))
	if err != nil {
		t.Fatal(err)
	}
	if configured.Sandbox.DeadTTLDur() != 2*time.Hour || configured.Builder.TerminalTTLDur() != 3*time.Hour {
		t.Fatalf("configured terminal TTLs = sandbox %s, builder %s", configured.Sandbox.DeadTTLDur(), configured.Builder.TerminalTTLDur())
	}

	for name, extra := range map[string]string{
		"zero sandbox":   "sandbox: { dead_ttl: 0s, boot: { kernel: /kernel, runtime: /runtime } }\n",
		"bad sandbox":    "sandbox: { dead_ttl: forever, boot: { kernel: /kernel, runtime: /runtime } }\n",
		"zero builder":   "sandbox: { boot: { kernel: /kernel, runtime: /runtime } }\nbuilder: { terminal_ttl: 0s }\n",
		"negative build": "sandbox: { boot: { kernel: /kernel, runtime: /runtime } }\nbuilder: { terminal_ttl: -1h }\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadConductor(writeConfig(t, header+extra)); err == nil || !strings.Contains(err.Error(), "must be a positive duration") {
				t.Fatalf("Load error = %v, want positive-duration rejection", err)
			}
		})
	}
}

func TestResourceListenDefaultsScanManagedRunnerSlice(t *testing.T) {
	var cfg ResourceListenConfig
	cfg.ApplyDefaults()
	want := []string{
		"/sys/fs/cgroup/sandbox.slice/sandbox-runner.slice",
		"/sys/fs/cgroup/sandbox.slice/sandbox-builder.slice",
	}
	if !reflect.DeepEqual(cfg.CgroupScanPaths, want) {
		t.Fatalf("cgroup scan paths = %v, want %v", cfg.CgroupScanPaths, want)
	}
}

func TestLoadNestedSandboxResourcePolicyAndDefaults(t *testing.T) {
	base := `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`
	defaults, err := LoadConductor(writeConfig(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Sandbox.Resources.Allocatable.Memory != nil {
		t.Fatalf("public inherited allocatable.memory = %v, want nil", *defaults.Sandbox.Resources.Allocatable.Memory)
	}
	policy := defaults.Sandbox.Resources.nodeResourcePolicy()
	policy.ApplyDefaults()
	if policy.Capacity.CPU != 2 || policy.Capacity.Memory != "2GiB" ||
		policy.Allocatable.CPU != nil || policy.Allocatable.Memory != nil ||
		policy.Startup != nil || policy.Overhead.Memory != "32MiB" || policy.WatermarkHigh == nil ||
		policy.WatermarkHigh.Ratio == nil || *policy.WatermarkHigh.Ratio != 0.875 {
		t.Fatalf("resource defaults = %+v", policy)
	}
	resolvedDefaults, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{Node: policy})
	if err != nil || resolvedDefaults.Allocatable.Memory != "256MiB" {
		t.Fatalf("runtime allocatable default = %+v, %v", resolvedDefaults.Allocatable, err)
	}

	configured, err := LoadConductor(writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
resource_listen:
  enabled: true
sandbox:
  resources:
    capacity:
      cpu: 4
      memory: 8GiB
    allocatable:
      cpu: 1.5
      memory: 512MiB
    startup:
      memory: 1GiB
    overhead:
      memory: 64MiB
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`))
	if err != nil {
		t.Fatal(err)
	}
	policy = configured.Sandbox.Resources.nodeResourcePolicy()
	if policy.Capacity.CPU != 4 || policy.Capacity.Memory != "8GiB" ||
		policy.Allocatable.CPU == nil || *policy.Allocatable.CPU != 1.5 ||
		policy.Allocatable.Memory == nil || *policy.Allocatable.Memory != "512MiB" || policy.Startup == nil ||
		policy.Startup.Memory != "1GiB" || policy.Overhead.Memory != "64MiB" {
		t.Fatalf("configured resource policy = %+v", policy)
	}
}

func TestBuilderAdmissionDefaultsAndExplicitIndependence(t *testing.T) {
	base := `
api: { domain: example.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
`
	defaults, err := LoadConductor(writeConfig(t, base))
	if err != nil {
		t.Fatal(err)
	}
	execution, err := defaults.Builder.executionLimit()
	if err != nil {
		t.Fatal(err)
	}
	registration, err := defaults.Builder.registrationLimit()
	if err != nil {
		t.Fatal(err)
	}
	if execution.MaxBuilds != 2 || registration != execution {
		t.Fatalf("default limits: registration=%+v execution=%+v", registration, execution)
	}

	configured, err := LoadConductor(writeConfig(t, base+`
builder:
  admission:
    execution:
      max_builds: 4
      resources: { cpu: 8, memory: 32GiB, storage: 1TiB }
    registration:
      resources: { memory: 64GiB }
`))
	if err != nil {
		t.Fatal(err)
	}
	registration, _ = configured.Builder.registrationLimit()
	execution, _ = configured.Builder.executionLimit()
	if registration.MaxBuilds != 0 || registration.Resources.CPU != 0 || registration.Resources.Storage != 0 || registration.Resources.Memory != 64<<30 {
		t.Fatalf("explicit registration inherited hidden fields: %+v", registration)
	}
	if execution.MaxBuilds != 4 || execution.Resources.CPU != 8000 || execution.Resources.Memory != 32<<30 || execution.Resources.Storage != 1<<40 {
		t.Fatalf("execution = %+v", execution)
	}
}

func TestBuilderAdmissionStrictValidation(t *testing.T) {
	base := `
api: { domain: example.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
builder:
  admission:
%s
`
	tests := map[string]struct {
		body string
		want string
	}{
		"null execution":       {"    execution: null", "builder.admission.execution must not be null"},
		"null resources":       {"    execution:\n      resources: null", "builder.admission.execution.resources must not be null"},
		"null leaf":            {"    execution:\n      resources: { cpu: null }", "builder.admission.execution.resources.cpu must not be null"},
		"zero max":             {"    execution: { max_builds: 0 }", "max_builds must be > 0"},
		"negative max":         {"    execution: { max_builds: -1 }", "max_builds must be > 0"},
		"zero cpu":             {"    execution:\n      resources: { cpu: 0 }", "resources.cpu must be a positive"},
		"NaN cpu":              {"    execution:\n      resources: { cpu: .nan }", "resources.cpu must be a positive"},
		"Inf cpu":              {"    execution:\n      resources: { cpu: .inf }", "resources.cpu must be a positive"},
		"empty memory":         {"    execution:\n      resources: { memory: \"\" }", "resources.memory must be a non-empty"},
		"unknown field":        {"    execution: { future: 1 }", "field future not found"},
		"duplicate resource":   {"    execution:\n      resources: { cpu: 1, cpu: 2 }", "mapping key \"cpu\" already defined"},
		"old max_concurrent":   {"    execution: { max_builds: 2 }\n  max_concurrent: 2", "field max_concurrent not found"},
		"old cpu_quota":        {"    execution: { max_builds: 2 }\n  cpu_quota: 2", "field cpu_quota not found"},
		"old memory_max":       {"    execution: { max_builds: 2 }\n  memory_max: 2GiB", "field memory_max not found"},
		"old vcpu":             {"    execution: { max_builds: 2 }\n  vcpu: 2", "field vcpu not found"},
		"old memory":           {"    execution: { max_builds: 2 }\n  memory: 2GiB", "field memory not found"},
		"registration smaller": {"    execution: { max_builds: 4 }\n    registration: { max_builds: 3 }", "registration.max_builds must be >="},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := LoadConductor(writeConfig(t, fmt.Sprintf(base, test.body)))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestBuilderIdlePoolAllowsFiniteExecutionResources(t *testing.T) {
	base := `
api: { domain: example.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
units:
  builder_pool_size: 1
builder:
  admission:
    execution:
      resources:
`
	for name, resource := range map[string]string{
		"cpu":    "        cpu: 2\n",
		"memory": "        memory: 2GiB\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadConductor(writeConfig(t, base+resource))
			if err != nil {
				t.Fatalf("finite execution admission with idle pool: %v", err)
			}
		})
	}

	if _, err := LoadConductor(writeConfig(t, base+"        storage: 10GiB\n")); err != nil {
		t.Fatalf("storage-only admission unexpectedly rejected idle pool: %v", err)
	}
}

func TestBuilderAdmissionCPUPreservesDecimalForConservativeRounding(t *testing.T) {
	cfg, err := LoadConductor(writeConfig(t, `
api: { domain: example.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
builder:
  admission:
    execution:
      resources: { cpu: 1.0000000000000001 }
`))
	if err != nil {
		t.Fatal(err)
	}
	limit, err := cfg.Builder.executionLimit()
	if err != nil {
		t.Fatal(err)
	}
	if limit.Resources.CPU != 1001 {
		t.Fatalf("conservatively rounded CPU = %d, want 1001 milli-CPU", limit.Resources.CPU)
	}
}

func TestLoadRejectsOldAndRuntimeOwnedSandboxResourceFields(t *testing.T) {
	base := `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  resources:
%s
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`
	tests := map[string]string{
		"vcpu":                 "    vcpu: 2",
		"memory":               "    memory: 2GiB",
		"control_socket":       "    control_socket: /run/controller.sock",
		"control":              "    control: {}",
		"old watermark memory": "    watermark_high:\n      memory: 1GiB",
		"sensor":               "    sensor: {}",
		"deflate":              "    allocatable:\n      deflate_on_oom: false",
		"unknown":              "    future: {}",
	}
	for name, fields := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := LoadConductor(writeConfig(t, fmt.Sprintf(base, fields)))
			if err == nil {
				t.Fatalf("Load error = %v", err)
			}
		})
	}
}

func TestLoadValidatesSandboxResourcePolicyBeforeServe(t *testing.T) {
	base := `
api:
  domain: example.test
encryption_key: test-key
%s
sandbox:
  resources:
%s
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`
	tests := map[string]struct {
		resourceListen string
		resources      string
		path           string
	}{
		"bad capacity memory": {resources: "    capacity:\n      memory: nope", path: "capacity.memory"},
		"empty alloc memory":  {resources: "    allocatable:\n      memory: \"\"", path: "allocatable.memory"},
		"zero alloc memory":   {resources: "    allocatable:\n      memory: 0", path: "allocatable.memory"},
		"alloc memory above capacity": {
			resources: "    capacity:\n      memory: 128MiB\n    allocatable:\n      memory: 256MiB", path: "allocatable.memory",
		},
		"alloc cpu above capacity": {
			resources: "    capacity:\n      cpu: 1\n    allocatable:\n      cpu: 2", path: "allocatable.cpu",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := LoadConductor(writeConfig(t, fmt.Sprintf(base, test.resourceListen, test.resources)))
			if err == nil || !strings.Contains(err.Error(), test.path) {
				t.Fatalf("Load error = %v, want %q", err, test.path)
			}
		})
	}
}

func TestLoadAllowsNodeStartupToBeNormalizedPerFinalSandbox(t *testing.T) {
	for name, startup := range map[string]string{"below headroom": "512MiB"} {
		t.Run(name, func(t *testing.T) {
			loaded, err := LoadConductor(writeConfig(t, `
api: { domain: example.test }
encryption_key: test-key
resource_listen: { enabled: true }
sandbox:
  resources:
    allocatable: { memory: 1GiB }
    startup: { memory: `+startup+` }
  boot: { kernel: /kernel, runtime: /runtime }
`))
			if err != nil {
				t.Fatal(err)
			}
			resources, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
				Node: loaded.Sandbox.Resources.nodeResourcePolicy(), Dynamic: true,
				ControllerSocketIdentity: "/run/resource.sock",
			})
			if err != nil {
				t.Fatal(err)
			}
			want := "512MiB"
			if resources.Startup == nil || resources.Startup.Memory != want {
				t.Fatalf("normalized startup = %+v, want %s", resources.Startup, want)
			}
		})
	}
}

func TestLoadDistinguishesInheritedAndExplicitFloorAboveNodeCapacity(t *testing.T) {
	base := `
api: { domain: example.test }
encryption_key: test-key
sandbox:
  resources:
    capacity: { cpu: 1, memory: 128MiB }
%s
  boot: { kernel: /kernel, runtime: /runtime }
`
	inherited, err := LoadConductor(writeConfig(t, fmt.Sprintf(base, "")))
	if err != nil {
		t.Fatalf("omitted inherited floor: %v", err)
	}
	resolved, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node: inherited.Sandbox.Resources.nodeResourcePolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Allocatable.Memory != "128MiB" {
		t.Fatalf("inherited headroom = %s, want capacity 128MiB", resolved.Allocatable.Memory)
	}
	patch, err := sandboxcfg.ParseResourcePatch(`{"capacity":{"memory":"512MiB"}}`)
	if err != nil {
		t.Fatal(err)
	}
	raised, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node: inherited.Sandbox.Resources.nodeResourcePolicy(), Patch: patch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if raised.Allocatable.Memory != "256MiB" {
		t.Fatalf("inherited default was permanently clamped: %s", raised.Allocatable.Memory)
	}

	_, err = LoadConductor(writeConfig(t, fmt.Sprintf(base, "    allocatable: { memory: 256MiB }")))
	if err == nil || !strings.Contains(err.Error(), "allocatable.memory") {
		t.Fatalf("explicit floor above node capacity error = %v", err)
	}
}

func TestInheritAllocatableMemoryRestoresDefaultResolution(t *testing.T) {
	cfg, err := LoadConductor(writeConfig(t, `
api: { domain: example.test }
encryption_key: test-key
sandbox:
  resources:
    capacity: { cpu: 1, memory: 128MiB }
    allocatable: { memory: 64MiB }
  boot: { kernel: /kernel, runtime: /runtime }
`))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Sandbox.Resources.Allocatable.InheritMemory()
	if err := ValidateConductorFinal(cfg); err != nil {
		t.Fatalf("inherited default final validation: %v", err)
	}
	resolved, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node: cfg.Sandbox.Resources.nodeResourcePolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Allocatable.Memory != "128MiB" {
		t.Fatalf("inherited runtime memory = %q, want capacity clamp", resolved.Allocatable.Memory)
	}
}

func TestCheckpointRefLocationURI(t *testing.T) {
	c := CheckpointConfig{Remote: CheckpointRemoteConfig{RefLocationParent: "file:///mnt/shared/snapshots"}}
	// Publication names are bare entity ids; the SHA fan-out below the parent
	// bounds directory size and no date segment appears.
	name := "0198f7a1-1234-7234-9abc-0123456789ab"
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	want := "file:///mnt/shared/snapshots/" + digest[:2] + "/" + digest[2:4] + "/" + name
	got, err := c.RefLocationURI(name)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("RefLocationURI() = %q, want %q", got, want)
	}
}

func TestCheckpointRefLocationURIRejectsInvalidLocationNames(t *testing.T) {
	c := CheckpointConfig{Remote: CheckpointRemoteConfig{RefLocationParent: "file:///mnt/shared/snapshots"}}
	for _, name := range []string{
		"",        // empty
		"a/b",     // path separator
		".",       // dot
		".entity", // dot-prefixed
		"-entity", // leading hyphen
		"enti ty", // space
	} {
		if got, err := c.RefLocationURI(name); err == nil {
			t.Fatalf("RefLocationURI(%q) = %q, want error", name, got)
		}
	}
}

func TestLoadRejectsInvalidCheckpointRefLocationParent(t *testing.T) {
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
checkpoint:
  remote:
    ref_location_parent: https://example.test/snapshots
`)
	if _, err := LoadConductor(path); err == nil || !strings.Contains(err.Error(), "ref_location_parent") {
		t.Fatalf("Load error = %v", err)
	}
}

func TestCheckpointRemoteManifestPolicy(t *testing.T) {
	base := `
api: { domain: example.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
`

	omitted, err := LoadConductor(writeConfig(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if omitted.Checkpoint.Remote.Manifest {
		t.Fatal("omitted checkpoint.remote.manifest did not default to false")
	}

	explicitFalse, err := LoadConductor(writeConfig(t, base+`
checkpoint:
  remote:
    ref_location_parent: file:///mnt/shared/snapshots
    manifest: false
`))
	if err != nil {
		t.Fatal(err)
	}
	if explicitFalse.Checkpoint.Remote.Manifest {
		t.Fatal("explicit checkpoint.remote.manifest=false loaded as true")
	}

	explicitTrue, err := LoadConductor(writeConfig(t, base+`
checkpoint:
  remote:
    ref_location_parent: file:///mnt/shared/snapshots
    manifest: true
`))
	if err != nil {
		t.Fatal(err)
	}
	if !explicitTrue.Checkpoint.Remote.Manifest {
		t.Fatal("explicit checkpoint.remote.manifest=true was not preserved")
	}

	_, err = LoadConductor(writeConfig(t, base+`
checkpoint:
  remote:
    manifest: true
`))
	if err == nil || !strings.Contains(err.Error(), "manifest=true requires") {
		t.Fatalf("manifest=true without parent error = %v", err)
	}

	_, err = LoadConductor(writeConfig(t, base+`
checkpoint:
  remote:
    ref_location_parent: file:///mnt/shared/snapshots
    manifested: true
`))
	if err == nil || !strings.Contains(err.Error(), "field manifested not found") {
		t.Fatalf("unknown checkpoint remote YAML field error = %v", err)
	}

	_, err = DecodeConductor(strings.NewReader(`{
  "api":{"domain":"example.test"},
  "encryption_key":"test-key",
  "sandbox":{"boot":{"kernel":"/kernel","runtime":"/runtime"}},
  "checkpoint":{"remote":{"ref_location_parent":"file:///mnt/shared/snapshots","manifest":true}}
}`))
	if err != nil {
		t.Fatalf("strict JSON manifest policy: %v", err)
	}

	_, err = DecodeConductor(strings.NewReader(`{
  "api":{"domain":"example.test"},
  "encryption_key":"test-key",
  "sandbox":{"boot":{"kernel":"/kernel","runtime":"/runtime"}},
  "checkpoint":{"remote":{"ref_location_parent":"file:///mnt/shared/snapshots","manifests":true}}
}`))
	if err == nil || !strings.Contains(err.Error(), "field manifests not found") {
		t.Fatalf("unknown checkpoint remote JSON field error = %v", err)
	}
}

func TestLoadSnapshotPolicyTriState(t *testing.T) {
	base := `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`

	omitted, err := LoadConductor(writeConfig(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if omitted.Checkpoint.Mode != CheckpointLocal || omitted.Checkpoint.MergeRef != nil || omitted.Checkpoint.DropCaches != nil {
		t.Fatalf("omitted checkpoint policy = %+v", omitted.Checkpoint)
	}

	nulls, err := LoadConductor(writeConfig(t, base+`
checkpoint:
  mode: bundle
  merge_ref: null
  drop_caches: null
`))
	if err != nil {
		t.Fatal(err)
	}
	if nulls.Checkpoint.MergeRef != nil || nulls.Checkpoint.DropCaches != nil {
		t.Fatalf("YAML null did not remain nil: %+v", nulls.Checkpoint)
	}

	for _, mode := range []string{CheckpointLocal, CheckpointBundle} {
		for _, mergeRef := range []bool{false, true} {
			for _, dropCaches := range []bool{false, true} {
				name := fmt.Sprintf("%s_merge_%t_drop_%t", mode, mergeRef, dropCaches)
				t.Run(name, func(t *testing.T) {
					cfg, err := LoadConductor(writeConfig(t, base+fmt.Sprintf(`
checkpoint:
  mode: %s
  merge_ref: %t
  drop_caches: %t
`, mode, mergeRef, dropCaches)))
					if err != nil {
						t.Fatal(err)
					}
					if cfg.Checkpoint.Mode != mode {
						t.Fatalf("loaded checkpoint mode = %q, want %q", cfg.Checkpoint.Mode, mode)
					}
					if cfg.Checkpoint.MergeRef == nil || *cfg.Checkpoint.MergeRef != mergeRef ||
						cfg.Checkpoint.DropCaches == nil || *cfg.Checkpoint.DropCaches != dropCaches {
						t.Fatalf("loaded checkpoint policy = %+v", cfg.Checkpoint)
					}
				})
			}
		}
	}
}

func TestLoadRejectsRemovedCheckpointLocalDir(t *testing.T) {
	_, err := LoadConductor(writeConfig(t, `
api: { domain: example.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
checkpoint:
  mode: local
  local_dir: /var/lib/legacy-checkpoints
`))
	if err == nil || !strings.Contains(err.Error(), "field local_dir not found") {
		t.Fatalf("checkpoint.local_dir error = %v", err)
	}
}

func TestLoadRejectsRunRootTooLongForObjectSockets(t *testing.T) {
	_, err := DecodeConductor(strings.NewReader(fmt.Sprintf("paths: { run_root: /%s }\n", strings.Repeat("r", 80))))
	if err == nil || !strings.Contains(err.Error(), "maximum Sandbox socket path") {
		t.Fatalf("overlong paths.run_root error = %v", err)
	}
}

func TestLoadRejectsUnsupportedCheckpointModes(t *testing.T) {
	base := `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
checkpoint:
  mode: %s
`
	for _, mode := range []string{"remote", "archive"} {
		t.Run(mode, func(t *testing.T) {
			_, err := LoadConductor(writeConfig(t, fmt.Sprintf(base, mode)))
			if err == nil || !strings.Contains(err.Error(), "want local|bundle") {
				t.Fatalf("Load error = %v", err)
			}
		})
	}
}

func TestLoadRejectsOldRuntimeFields(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime_e2b: /opt/sandbox/runtime-e2b.erofs
`)

	_, err := LoadConductor(path)
	if err == nil {
		t.Fatal("Load succeeded with obsolete runtime_e2b field")
	}
	if !strings.Contains(err.Error(), "runtime_e2b") {
		t.Fatalf("error %q does not mention obsolete field", err)
	}
}

func TestLoadDefersRequiredRuntimeToFinalValidation(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
`)

	cfg, err := LoadConductor(path)
	if err != nil {
		t.Fatalf("declarative Load failed without final runtime: %v", err)
	}
	err = ValidateConductorFinal(cfg)
	if err == nil || !strings.Contains(err.Error(), "sandbox.boot.runtime") {
		t.Fatalf("error %q does not mention sandbox.boot.runtime", err)
	}
}

func TestLoadAcceptsSingleRuntime(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`)

	cfg, err := LoadConductor(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got := cfg.Sandbox.Boot.Runtime; got != "/opt/sandbox/sandbox-runtime.bundle" {
		t.Fatalf("runtime = %q", got)
	}
}

func TestLoadRejectsNonPositivePoolWaitTimeout(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	for _, value := range []string{"0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
units:
  pool_wait_timeout: `+value+`
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`)
			_, err := LoadConductor(path)
			if err == nil || !strings.Contains(err.Error(), "units.pool_wait_timeout") {
				t.Fatalf("Load error = %v, want units.pool_wait_timeout validation", err)
			}
		})
	}
}

func TestLoadBuilderRefererDefaults(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
builder:
  referer:
    enabled: true
    desc: acme-prod
`)

	cfg, err := LoadConductor(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !cfg.Builder.Referer.FallbackEnabled() || !cfg.Builder.Referer.WritebackEnabled() {
		t.Fatalf("referer defaults not enabled: %+v", cfg.Builder.Referer)
	}
	if cfg.Builder.Referer.Key != "acme-prod" {
		t.Fatalf("referer key default = %q", cfg.Builder.Referer.Key)
	}
}

func TestLoadRejectsBuilderRefererWithoutDesc(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
builder:
  referer:
    enabled: true
`)

	_, err := LoadConductor(path)
	if err == nil {
		t.Fatal("Load succeeded with referer enabled but no desc")
	}
	if !strings.Contains(err.Error(), "builder.referer.desc") {
		t.Fatalf("error %q does not mention builder.referer.desc", err)
	}
}

func TestLoadRejectsBuilderRefererInvalidValidity(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	for _, validity := range []string{"soon", "0s", "-1h"} {
		t.Run(validity, func(t *testing.T) {
			path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
builder:
  referer:
    validity: `+validity+`
`)

			_, err := LoadConductor(path)
			if err == nil {
				t.Fatal("Load succeeded with invalid referer validity")
			}
			if !strings.Contains(err.Error(), "builder.referer.validity") {
				t.Fatalf("error %q does not mention builder.referer.validity", err)
			}
		})
	}
}

func TestLoadAcceptsTapFDSocket(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  network:
    tapfd_socket: /run/kuasar/connector/sw0/tapfd.sock
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`)

	cfg, err := LoadConductor(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got := cfg.Sandbox.Network.TapFDSocket; got != "/run/kuasar/connector/sw0/tapfd.sock" {
		t.Fatalf("tapfd_socket = %q", got)
	}
}

func TestLoadRejectsRelativeTapFDSocket(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  network:
    tapfd_socket: tapfd.sock
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`)

	_, err := LoadConductor(path)
	if err == nil {
		t.Fatal("Load succeeded with relative tapfd_socket")
	}
	if !strings.Contains(err.Error(), "tapfd_socket") {
		t.Fatalf("error %q does not mention tapfd_socket", err)
	}
}

func TestLoadConductorRejectsRemovedProxyFields(t *testing.T) {
	for _, field := range []string{"mode: internal", "data_listen: 127.0.0.1:8443", "proxy_netns: sw0_mgmt"} {
		t.Run(strings.Fields(field)[0], func(t *testing.T) {
			t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
			path := writeConfig(t, "proxy:\n  "+field+"\n")
			if _, err := LoadConductor(path); err == nil {
				t.Fatalf("LoadConductor accepted removed proxy field %q", field)
			}
		})
	}
}

func TestClusterAdvertisedEndpointsAreExplicitAndDistinctFromBindDefaults(t *testing.T) {
	base := `
api:
  domain: example.test
  listen: ":443"
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
cluster:
  node_link: { endpoint: registry.test:7700 }
`
	for _, test := range []struct {
		name   string
		fields string
		want   string
	}{
		{name: "missing both", want: "cluster.api_endpoint"},
		{name: "missing data", fields: "  api_endpoint: node-api.test:8443\n", want: "cluster.data_endpoint"},
		{name: "missing API", fields: "  data_endpoint: node-data.test:9443\n", want: "cluster.api_endpoint"},
		{name: "API URL", fields: "  api_endpoint: http://node.test:8443\n  data_endpoint: node-data.test:9443\n", want: "cluster.api_endpoint"},
		{name: "data bind address", fields: "  api_endpoint: node-api.test:8443\n  data_endpoint: :9443\n", want: "cluster.data_endpoint"},
		{name: "valid", fields: "  api_endpoint: node-api.test:8443\n  data_endpoint: node-data.test:9443\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := LoadConductor(writeConfig(t, base+test.fields))
			if err == nil {
				err = ValidateConductorFinal(cfg)
			}
			if test.want != "" {
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("LoadConductor error=%v, want %s", err, test.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Cluster.APIEndpoint != "node-api.test:8443" || cfg.Cluster.DataEndpoint != "node-data.test:9443" {
				t.Fatalf("cluster endpoints = API %q Data %q", cfg.Cluster.APIEndpoint, cfg.Cluster.DataEndpoint)
			}
		})
	}
}

func TestLoadProxyAcceptsProxyNetNS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("paths: { run_root: /run/sandbox }\nproxy_netns: sw0_mgmt\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadProxy(path)
	if err != nil {
		t.Fatalf("LoadProxy failed: %v", err)
	}
	if got := cfg.ProxyNetNS; got != "sw0_mgmt" {
		t.Fatalf("proxy_netns = %q", got)
	}
	if got := cfg.Paths.RunRoot; got != "/run/sandbox" {
		t.Fatalf("paths.run_root = %q", got)
	}
	if got := cfg.StatsSocket; got != "/run/sandbox/proxy-stats.sock" {
		t.Fatalf("default stats_socket = %q", got)
	}
}

func TestLoadProxyStatsSocketValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "explicit",
			body: "paths: { run_root: /run/sandbox }\nconfig_socket: /tmp/config.sock\nstats_socket: /tmp/stats.sock\n",
			want: "/tmp/stats.sock",
		},
		{
			name: "relative",
			body: "paths: { run_root: /run/sandbox }\nstats_socket: stats.sock\n",
		},
		{
			name: "config conflict",
			body: "paths: { run_root: /run/sandbox }\nconfig_socket: /tmp/same.sock\nstats_socket: /tmp/same.sock\n",
		},
		{
			name: "shm conflict",
			body: "paths: { run_root: /run/sandbox }\nshm_path: /tmp/same.sock\nstats_socket: /tmp/same.sock\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "proxy.yaml")
			if err := os.WriteFile(path, []byte(test.body), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadProxy(path)
			if test.want == "" {
				if err == nil {
					t.Fatalf("LoadProxy accepted invalid stats_socket: %+v", cfg)
				}
				return
			}
			if err != nil || cfg.StatsSocket != test.want {
				t.Fatalf("LoadProxy stats_socket=%q err=%v, want %q", cfg.StatsSocket, err, test.want)
			}
		})
	}
}

func TestLoadProxyRejectsRemovedProxySocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("proxy_socket: /tmp/proxy.sock\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProxy(path); err == nil || !strings.Contains(err.Error(), "proxy_socket") {
		t.Fatalf("LoadProxy removed proxy_socket error = %v", err)
	}
}

func TestLoadProxyDefersRunRootToFinalValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("proxy_netns: sw0_mgmt\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadProxy(path)
	if err != nil {
		t.Fatalf("declarative LoadProxy failed without run_root: %v", err)
	}
	err = ValidateProxyFinal(cfg)
	if err == nil || !strings.Contains(err.Error(), "paths.run_root is required") {
		t.Fatalf("error %q does not report required run_root", err)
	}
}

func TestLoadProxyRejectsTopLevelRunRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("run_root: /run/sandbox\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadProxy(path)
	if err == nil {
		t.Fatal("LoadProxy accepted the removed top-level run_root field")
	}
	if !strings.Contains(err.Error(), "field run_root not found") {
		t.Fatalf("error %q does not reject top-level run_root at schema decode", err)
	}
}

func TestLoadMMDSRoutesAndConductorServiceRegistry(t *testing.T) {
	cfg, err := LoadConductor(writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/runtime.erofs
mmds:
  enabled: true
  listen: 127.0.0.1:19254
  routes:
    enabled: true
    max_routes_per_sandbox: 7
    max_namespace_bytes: 8192
    max_static_body_bytes: 1024
    max_secret_value_bytes: 2048
    reserved_path_prefixes: [/latest/api/, /internal/]
  services:
    external-mmds:
      endpoint: unix:///run/kuasar/mmds/external.sock
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.MMDS.Routes.Enabled || cfg.MMDS.Routes.MaxRoutesPerSandbox != 7 ||
		cfg.MMDS.Services["external-mmds"].Endpoint != "unix:///run/kuasar/mmds/external.sock" {
		t.Fatalf("MMDS config = %+v", cfg.MMDS)
	}
}

func TestLoadMMDSRouteDefaults(t *testing.T) {
	cfg, err := LoadConductor(writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/runtime.erofs
`))
	if err != nil {
		t.Fatal(err)
	}
	routes := cfg.MMDS.Routes
	if routes.MaxRoutesPerSandbox != 32 || routes.MaxNamespaceBytes != 65536 ||
		routes.MaxStaticBodyBytes != 16384 || routes.MaxSecretValueBytes != 16384 ||
		len(routes.ReservedPathPrefixes) != 2 {
		t.Fatalf("MMDS route defaults = %+v", routes)
	}
}

func TestLoadRejectsInvalidMMDSServiceEndpoint(t *testing.T) {
	_, err := LoadConductor(writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/runtime.erofs
mmds:
  services:
    remote:
      endpoint: https://example.test/metadata
`))
	if err == nil || !strings.Contains(err.Error(), "mmds.services") {
		t.Fatalf("Load error = %v", err)
	}
}

func TestLoadProxyRejectsRemovedMMDSConfiguration(t *testing.T) {
	for name, body := range map[string]string{
		"mmds_listen": "paths: { run_root: /run/sandbox }\nmmds_listen: 127.0.0.1:19254\n",
		"services":    "paths: { run_root: /run/sandbox }\nservices: {}\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "proxy.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadProxy(path); err == nil {
				t.Fatalf("LoadProxy accepted removed %s configuration", name)
			}
		})
	}
}

func TestResourceStatePathRemainsParseOnlyCompatibility(t *testing.T) {
	cfg, err := LoadConductor(writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/runtime.erofs
resource_listen:
  enabled: true
  state_path: /run/legacy-resource-state.json
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ResourceListen == nil || cfg.ResourceListen.StatePath != "/run/legacy-resource-state.json" {
		t.Fatalf("deprecated state_path did not remain parse-compatible: %+v", cfg.ResourceListen)
	}
	wantCgroupRoots := []string{
		"/sys/fs/cgroup/sandbox.slice/sandbox-runner.slice",
		"/sys/fs/cgroup/sandbox.slice/sandbox-builder.slice",
	}
	if got := cfg.ResourceListen.CgroupScanPaths; !reflect.DeepEqual(got, wantCgroupRoots) {
		t.Fatalf("cgroup recovery defaults = %v", got)
	}

	var omitted ResourceListenConfig
	omitted.ApplyDefaults()
	if omitted.StatePath != "" {
		t.Fatalf("state_path still has an active default: %q", omitted.StatePath)
	}
}

func TestLoadRejectsRemovedResourceAuditField(t *testing.T) {
	_, err := LoadConductor(writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/runtime.erofs
resource_listen:
  enabled: true
  audit_path: /run/node-ctl/audit.log
`))
	if err == nil || !strings.Contains(err.Error(), "field audit_path not found") {
		t.Fatalf("removed resource_listen.audit_path error = %v", err)
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
