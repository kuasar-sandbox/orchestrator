// Package configresolve converts the public declarative configuration into
// internal runtime values. Public config types deliberately do not expose
// sandboxcfg or types values in their API.
package configresolve

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Executables is process bootstrap state derived from the exact node-ctl path.
// It is deliberately separate from the serializable public configuration.
type Executables struct {
	NodeCtl string
	dir     string
}

// CurrentExecutables derives helper lookup from the currently running node-ctl.
// Custom conductor bootstrap replaces this source with the exact path supplied
// by node-ctl rather than deriving it from xconductor.
func CurrentExecutables() (Executables, error) {
	executable, err := os.Executable()
	if err != nil {
		return Executables{}, fmt.Errorf("resolve node-ctl executable: %w", err)
	}
	return ExecutablesForNodeCtl(executable), nil
}

// ExecutablesForNodeCtl freezes the exact node-ctl path and its distribution
// directory for helper discovery.
func ExecutablesForNodeCtl(nodeCtl string) Executables {
	return Executables{NodeCtl: nodeCtl, dir: filepath.Dir(nodeCtl)}
}

func (e Executables) binary(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	if e.dir == "" {
		return filepath.Base(name)
	}
	candidate := filepath.Join(e.dir, filepath.Base(name))
	if _, err := os.Stat(candidate); err == nil || !os.IsNotExist(err) {
		return candidate
	}
	return filepath.Base(name)
}

func (e Executables) SandboxCtl() string   { return e.binary("sandbox-ctl") }
func (e Executables) ManifestCtl() string  { return e.binary("manifest-ctl") }
func (e Executables) ConnectorCtl() string { return e.binary("connector-ctl") }
func (e Executables) FlattenCtl() string   { return e.binary("flatten-ctl") }
func (e Executables) OrchestratorCtl() string {
	if e.NodeCtl != "" {
		return e.NodeCtl
	}
	return e.binary("node-ctl")
}

// EncryptionKeySpec resolves the built-in conductor's legacy environment
// override. Runtime providers introduced by the conductor App are resolved at
// a later lifecycle stage and do not become part of the public Config value.
func EncryptionKeySpec(cfg *publicconfig.Conductor) string {
	if value := os.Getenv("NODE_CONFIG_ENCRYPTION_KEY"); value != "" {
		return value
	}
	if cfg == nil {
		return ""
	}
	return cfg.EncryptionKey
}

// SandboxResources converts the public resource policy into the internal
// resolver input while preserving the parser's omitted-default presence bit.
func SandboxResources(in publicconfig.ResourcesConfig) sandboxcfg.NodeResourcePolicy {
	out := sandboxcfg.NodeResourcePolicy{
		Capacity: sandboxcfg.NodeCapacityPolicy{
			CPU: in.Capacity.CPU, Memory: in.Capacity.Memory,
		},
		Allocatable: sandboxcfg.NodeAllocatablePolicy{
			CPU: clonePtr(in.Allocatable.CPU), Memory: in.Allocatable.Memory,
		},
		Overhead: sandboxcfg.NodeOverheadPolicy{Memory: in.Overhead.Memory},
	}
	if in.Startup != nil {
		out.Startup = &sandboxcfg.NodeStartupPolicy{Memory: in.Startup.Memory}
	}
	if in.WatermarkHigh != nil {
		out.WatermarkHigh = &sandboxcfg.NodeWatermarkHighPolicy{Ratio: clonePtr(in.WatermarkHigh.Ratio)}
	}
	out.SetAllocatableMemoryInherited(in.AllocatableMemoryInherited())
	return out
}

// PublicSandboxResources converts an internal policy for internal tests and
// diagnostic adapters. Runtime code should normally flow in the opposite
// direction from the public schema into SandboxResources.
func PublicSandboxResources(policy sandboxcfg.NodeResourcePolicy) publicconfig.ResourcesConfig {
	out := publicconfig.ResourcesConfig{
		Capacity: publicconfig.ResourceCapacity{
			CPU: policy.Capacity.CPU, Memory: policy.Capacity.Memory,
		},
		Allocatable: publicconfig.ResourceAllocatable{
			CPU: clonePtr(policy.Allocatable.CPU), Memory: policy.Allocatable.Memory,
		},
		Overhead: publicconfig.ResourceOverhead{Memory: policy.Overhead.Memory},
	}
	if policy.Startup != nil {
		out.Startup = &publicconfig.ResourceStartup{Memory: policy.Startup.Memory}
	}
	if policy.WatermarkHigh != nil {
		out.WatermarkHigh = &publicconfig.ResourceWatermarkHigh{Ratio: clonePtr(policy.WatermarkHigh.Ratio)}
	}
	return out
}

// MaterializedSandboxResources returns a normalized public policy suitable for
// config diagnostics without turning an inherited clamp into an explicit
// operator value.
func MaterializedSandboxResources(in publicconfig.ResourcesConfig) publicconfig.ResourcesConfig {
	policy := sandboxcfg.MaterializedNodeResourcePolicy(SandboxResources(in))
	return PublicSandboxResources(policy)
}

// BuilderRegistrationLimit resolves the public aggregate admission schema.
func BuilderRegistrationLimit(in publicconfig.BuilderConfig) (types.BuildAdmissionLimit, error) {
	if in.Admission.Registration == nil {
		return types.BuildAdmissionLimit{}, fmt.Errorf("builder.admission.registration is unresolved")
	}
	return buildAdmissionLimit(*in.Admission.Registration, "builder.admission.registration")
}

// BuilderExecutionLimit resolves the public aggregate admission schema.
func BuilderExecutionLimit(in publicconfig.BuilderConfig) (types.BuildAdmissionLimit, error) {
	if in.Admission.Execution == nil {
		return types.BuildAdmissionLimit{}, fmt.Errorf("builder.admission.execution is unresolved")
	}
	return buildAdmissionLimit(*in.Admission.Execution, "builder.admission.execution")
}

func buildAdmissionLimit(in publicconfig.BuildAdmissionLimitConfig, path string) (types.BuildAdmissionLimit, error) {
	var out types.BuildAdmissionLimit
	if in.MaxBuilds != nil {
		if *in.MaxBuilds <= 0 {
			return out, fmt.Errorf("%s.max_builds must be > 0", path)
		}
		out.MaxBuilds = *in.MaxBuilds
	}
	if in.Resources.CPU != nil {
		value, err := buildcfg.CPUCoresToMilli(path+".resources.cpu", json.Number(*in.Resources.CPU))
		if err != nil {
			return out, err
		}
		out.Resources.CPU = value
	}
	for _, size := range []struct {
		name string
		raw  *string
		dst  *int64
	}{
		{name: "memory", raw: in.Resources.Memory, dst: &out.Resources.Memory},
		{name: "storage", raw: in.Resources.Storage, dst: &out.Resources.Storage},
	} {
		if size.raw == nil {
			continue
		}
		value, err := buildcfg.SizeBytes(path+".resources."+size.name, *size.raw)
		if err != nil {
			return out, err
		}
		*size.dst = value
	}
	return out, nil
}

func clonePtr[T any](in *T) *T {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
