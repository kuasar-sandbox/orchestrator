package sandboxcfg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	rtutil "github.com/kuasar-sandbox/sandboxer/pkg/util"
)

const resourceFieldPath = NsResource

// ErrInvalidResourceRequest marks a syntactically valid resource patch whose
// explicit request values cannot be satisfied by the selected node or restore
// snapshot. HTTP and cluster boundaries map it to a request rejection.
var ErrInvalidResourceRequest = errors.New("invalid sandbox resource request")

// ResourcePatch is the complete tenant-configurable resource surface. Every
// leaf is a pointer so an omitted value remains distinguishable from an
// explicitly invalid zero or empty value.
type ResourcePatch struct {
	Capacity    *CapacityPatch    `json:"capacity,omitempty" yaml:"capacity,omitempty"`
	Allocatable *AllocatablePatch `json:"allocatable,omitempty" yaml:"allocatable,omitempty"`
	Startup     *StartupPatch     `json:"startup,omitempty" yaml:"startup,omitempty"`
}

type CapacityPatch struct {
	CPU    *int    `json:"cpu,omitempty" yaml:"cpu,omitempty"`
	Memory *string `json:"memory,omitempty" yaml:"memory,omitempty"`
}

type AllocatablePatch struct {
	CPU    *float64 `json:"cpu,omitempty" yaml:"cpu,omitempty"`
	Memory *string  `json:"memory,omitempty" yaml:"memory,omitempty"`
}

type StartupPatch struct {
	Memory *string `json:"memory,omitempty" yaml:"memory,omitempty"`
}

// NodeResourcePolicy is conductor-owned policy, deliberately narrower than
// sandboxer's runtime ResourcesConfig. Tenant metadata never sees Overhead or
// WatermarkHigh, and neither request surface exposes control, sensor, or
// deflate policy.
type NodeResourcePolicy struct {
	Capacity      NodeCapacityPolicy       `yaml:"capacity" json:"capacity"`
	Allocatable   NodeAllocatablePolicy    `yaml:"allocatable" json:"allocatable"`
	Startup       *NodeStartupPolicy       `yaml:"startup,omitempty" json:"startup,omitempty"`
	Overhead      NodeOverheadPolicy       `yaml:"overhead" json:"overhead"`
	WatermarkHigh *NodeWatermarkHighPolicy `yaml:"watermark_high,omitempty" json:"watermark_high,omitempty"`
}

type NodeCapacityPolicy struct {
	CPU    int    `yaml:"cpu" json:"cpu"`
	Memory string `yaml:"memory" json:"memory"`
}

type NodeAllocatablePolicy struct {
	// A nil CPU follows the final per-request capacity CPU. A configured value
	// is still inherited node policy and may be clamped if a request lowers
	// capacity; an explicit request allocatable value is never clamped.
	CPU    *float64 `yaml:"cpu,omitempty" json:"cpu,omitempty"`
	Memory string   `yaml:"memory,omitempty" json:"memory,omitempty"`
	// memoryInherited survives conductor defaulting so validation and the final
	// resolver can distinguish an omitted 256MiB default (safe to clamp) from an
	// operator's explicit value (which must satisfy the node policy as written).
	memoryInherited bool
}

// SetAllocatableMemoryInherited carries the conductor parser's distinction
// between an omitted default and an explicitly configured value into the
// internal resolver. It is intentionally an internal-package API; public
// configuration callers never need to reference NodeResourcePolicy.
func (p *NodeResourcePolicy) SetAllocatableMemoryInherited(inherited bool) {
	p.Allocatable.memoryInherited = inherited
}

// AllocatableMemoryInherited reports the parser presence bit so normalized
// public configuration can preserve the same semantics across a clone or
// serialization boundary.
func (p NodeResourcePolicy) AllocatableMemoryInherited() bool {
	return p.Allocatable.memoryInherited
}

type NodeStartupPolicy struct {
	Memory string `yaml:"memory" json:"memory"`
}

type NodeOverheadPolicy struct {
	Memory string `yaml:"memory" json:"memory"`
}

type NodeWatermarkHighPolicy struct {
	// Pointer preserves explicit ratio: 0 so validation rejects it instead of
	// silently treating it as an omitted default.
	Ratio *float64 `yaml:"ratio,omitempty" json:"ratio,omitempty"`
}

// ApplyDefaults fills conductor-owned resource defaults without manufacturing
// an allocatable CPU presence bit: omitted CPU must continue to follow the
// final capacity after request overlays.
func (p *NodeResourcePolicy) ApplyDefaults() {
	if p.Capacity.CPU == 0 {
		p.Capacity.CPU = 2
	}
	if p.Capacity.Memory == "" {
		p.Capacity.Memory = "2GiB"
	}
	if p.Allocatable.Memory == "" {
		p.Allocatable.Memory = "256MiB"
		p.Allocatable.memoryInherited = true
	}
	if p.Overhead.Memory == "" {
		p.Overhead.Memory = "32MiB"
	}
	if p.WatermarkHigh == nil {
		p.WatermarkHigh = &NodeWatermarkHighPolicy{}
	}
	if p.WatermarkHigh.Ratio == nil {
		ratio := 0.875
		p.WatermarkHigh.Ratio = &ratio
	}
}

// MaterializedNodeResourcePolicy returns a self-contained policy suitable for
// normalized config output. The ordinary 256MiB default is materialized. When
// it currently exceeds node capacity, however, the leaf remains omitted so a
// reload preserves its per-sandbox min(default, final capacity) semantics.
func MaterializedNodeResourcePolicy(policy NodeResourcePolicy) NodeResourcePolicy {
	policy.ApplyDefaults()
	if policy.Allocatable.memoryInherited {
		capMem, capErr := positiveSize("sandbox.resources.capacity.memory", policy.Capacity.Memory)
		allocMem, allocErr := positiveSize("sandbox.resources.allocatable.memory", policy.Allocatable.Memory)
		if capErr == nil && allocErr == nil && allocMem > capMem {
			policy.Allocatable.Memory = ""
		}
	}
	policy.Allocatable.memoryInherited = false
	return policy
}

// ValidateNodeResourcePolicy rejects malformed conductor policy before the
// daemon creates sockets, cgroups, networks, or runners.
func ValidateNodeResourcePolicy(policy NodeResourcePolicy, dynamic bool) error {
	policy.ApplyDefaults()
	if policy.Capacity.CPU <= 0 {
		return errors.New("sandbox.resources.capacity.cpu must be > 0")
	}
	capMem, err := positiveSize("sandbox.resources.capacity.memory", policy.Capacity.Memory)
	if err != nil {
		return err
	}
	if policy.Allocatable.CPU != nil {
		if !positiveFinite(*policy.Allocatable.CPU) {
			return errors.New("sandbox.resources.allocatable.cpu must be > 0")
		}
		if *policy.Allocatable.CPU > float64(policy.Capacity.CPU) {
			return errors.New("sandbox.resources.allocatable.cpu must be <= sandbox.resources.capacity.cpu")
		}
	}
	allocMem, err := positiveSize("sandbox.resources.allocatable.memory", policy.Allocatable.Memory)
	if err != nil {
		return err
	}
	if allocMem > capMem && !policy.Allocatable.memoryInherited {
		return errors.New("sandbox.resources.allocatable.memory must be <= sandbox.resources.capacity.memory")
	}
	if _, err := positiveSize("sandbox.resources.overhead.memory", policy.Overhead.Memory); err != nil {
		return err
	}
	if policy.WatermarkHigh == nil || policy.WatermarkHigh.Ratio == nil ||
		!positiveFinite(*policy.WatermarkHigh.Ratio) || *policy.WatermarkHigh.Ratio >= 1 {
		return errors.New("sandbox.resources.watermark_high.ratio must be > 0 and < 1")
	}
	if policy.Startup != nil {
		startupMem, err := positiveSize("sandbox.resources.startup.memory", policy.Startup.Memory)
		if err != nil {
			return err
		}
		if startupMem > capMem {
			return errors.New("sandbox.resources.startup.memory must be <= sandbox.resources.capacity.memory")
		}
	}
	_ = dynamic // controller presence does not change startup/headroom semantics.
	return nil
}

// ParseResourcePatch strictly parses one request/template resource object.
// Runtime-only fields receive ownership-specific errors instead of being
// silently ignored by the YAML decoder.
func ParseResourcePatch(raw string) (ResourcePatch, error) {
	var patch ResourcePatch
	if err := rejectDuplicateJSONKeys([]byte(raw)); err != nil {
		return patch, fmt.Errorf("%s must be a valid JSON object: %w", resourceFieldPath, err)
	}
	object, err := decodeJSONObject(resourceFieldPath, []byte(raw))
	if err != nil {
		return patch, err
	}
	for field, value := range object {
		path := resourceFieldPath + "." + field
		switch field {
		case "capacity":
			parsed, err := parseCapacityPatch(path, value)
			if err != nil {
				return patch, err
			}
			patch.Capacity = parsed
		case "allocatable":
			parsed, err := parseAllocatablePatch(path, value)
			if err != nil {
				return patch, err
			}
			patch.Allocatable = parsed
		case "startup":
			parsed, err := parseStartupPatch(path, value)
			if err != nil {
				return patch, err
			}
			patch.Startup = parsed
		case "control":
			return patch, fmt.Errorf("%s is node-managed", path)
		case "overhead":
			return patch, fmt.Errorf("%s is not request-configurable", path)
		case "watermark_high":
			return patch, fmt.Errorf("%s is node-managed", path)
		case "sensor":
			return patch, fmt.Errorf("%s is node-managed", path)
		default:
			return patch, fmt.Errorf("%s contains unknown field %q", resourceFieldPath, field)
		}
	}
	if err := ValidateResourcePatch(patch); err != nil {
		return ResourcePatch{}, err
	}
	return patch, nil
}

func parseCapacityPatch(path string, raw json.RawMessage) (*CapacityPatch, error) {
	object, err := decodeJSONObject(path, raw)
	if err != nil {
		return nil, err
	}
	out := &CapacityPatch{}
	for field, value := range object {
		fieldPath := path + "." + field
		switch field {
		case "cpu":
			var cpu int
			if err := decodeLeaf(fieldPath, value, &cpu); err != nil {
				return nil, err
			}
			if cpu <= 0 {
				return nil, fmt.Errorf("%s must be > 0", fieldPath)
			}
			out.CPU = &cpu
		case "memory":
			memory, err := parseMemoryLeaf(fieldPath, value)
			if err != nil {
				return nil, err
			}
			out.Memory = &memory
		default:
			return nil, fmt.Errorf("%s contains unknown field %q", path, field)
		}
	}
	return out, nil
}

func parseAllocatablePatch(path string, raw json.RawMessage) (*AllocatablePatch, error) {
	object, err := decodeJSONObject(path, raw)
	if err != nil {
		return nil, err
	}
	out := &AllocatablePatch{}
	for field, value := range object {
		fieldPath := path + "." + field
		switch field {
		case "cpu":
			var cpu float64
			if err := decodeLeaf(fieldPath, value, &cpu); err != nil {
				return nil, err
			}
			if !positiveFinite(cpu) {
				return nil, fmt.Errorf("%s must be > 0", fieldPath)
			}
			out.CPU = &cpu
		case "memory":
			memory, err := parseMemoryLeaf(fieldPath, value)
			if err != nil {
				return nil, err
			}
			out.Memory = &memory
		case "deflate_on_oom":
			return nil, fmt.Errorf("%s is node-managed", fieldPath)
		default:
			return nil, fmt.Errorf("%s contains unknown field %q", path, field)
		}
	}
	return out, nil
}

func parseStartupPatch(path string, raw json.RawMessage) (*StartupPatch, error) {
	object, err := decodeJSONObject(path, raw)
	if err != nil {
		return nil, err
	}
	out := &StartupPatch{}
	for field, value := range object {
		fieldPath := path + "." + field
		switch field {
		case "memory":
			memory, err := parseMemoryLeaf(fieldPath, value)
			if err != nil {
				return nil, err
			}
			out.Memory = &memory
		default:
			return nil, fmt.Errorf("%s contains unknown field %q", path, field)
		}
	}
	return out, nil
}

func decodeJSONObject(path string, raw []byte) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, fmt.Errorf("%s must be a JSON object", path)
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	var object map[string]json.RawMessage
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("%s must be a valid JSON object: %w", path, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("%s must contain exactly one JSON object", path)
		}
		return nil, fmt.Errorf("%s must contain exactly one JSON object: %w", path, err)
	}
	if object == nil {
		return nil, fmt.Errorf("%s must be a JSON object", path)
	}
	return object, nil
}

func decodeLeaf(path string, raw json.RawMessage, out any) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("%s must not be null", path)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%s has invalid value: %w", path, err)
	}
	return nil
}

func parseMemoryLeaf(path string, raw json.RawMessage) (string, error) {
	var memory string
	if err := decodeLeaf(path, raw, &memory); err != nil {
		return "", err
	}
	if _, err := positiveSize(path, memory); err != nil {
		return "", err
	}
	return memory, nil
}

func positiveSize(path, value string) (uint64, error) {
	if value == "" {
		return 0, fmt.Errorf("%s must be a non-empty positive size", path)
	}
	parsed, err := rtutil.ParseSize(value)
	if err != nil {
		return 0, fmt.Errorf("%s is invalid: %w", path, err)
	}
	if parsed == 0 {
		return 0, fmt.Errorf("%s must be > 0", path)
	}
	return parsed, nil
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}

// ValidateResourcePatch checks only relationships that are fully determined by
// explicit portable leaves. It deliberately does not apply node defaults: a
// template may be built with a snapshot capacity that differs from the node's
// cold-boot default, and restore validation owns that final constraint.
func ValidateResourcePatch(patch ResourcePatch) error {
	var capacityMemory, allocatableMemory, startupMemory uint64
	var hasCapacityMemory, hasAllocatableMemory, hasStartupMemory bool
	if patch.Capacity != nil {
		if patch.Capacity.CPU != nil && *patch.Capacity.CPU <= 0 {
			return fmt.Errorf("%s.capacity.cpu must be > 0", resourceFieldPath)
		}
		if patch.Capacity.Memory != nil {
			parsed, err := positiveSize(resourceFieldPath+".capacity.memory", *patch.Capacity.Memory)
			if err != nil {
				return err
			}
			capacityMemory, hasCapacityMemory = parsed, true
		}
	}
	if patch.Allocatable != nil {
		if patch.Allocatable.CPU != nil {
			if !positiveFinite(*patch.Allocatable.CPU) {
				return fmt.Errorf("%s.allocatable.cpu must be > 0", resourceFieldPath)
			}
			if patch.Capacity != nil && patch.Capacity.CPU != nil &&
				*patch.Allocatable.CPU > float64(*patch.Capacity.CPU) {
				return fmt.Errorf("%s.allocatable.cpu must be <= %s.capacity.cpu", resourceFieldPath, resourceFieldPath)
			}
		}
		if patch.Allocatable.Memory != nil {
			parsed, err := positiveSize(resourceFieldPath+".allocatable.memory", *patch.Allocatable.Memory)
			if err != nil {
				return err
			}
			allocatableMemory, hasAllocatableMemory = parsed, true
		}
	}
	if patch.Startup != nil && patch.Startup.Memory != nil {
		parsed, err := positiveSize(resourceFieldPath+".startup.memory", *patch.Startup.Memory)
		if err != nil {
			return err
		}
		startupMemory, hasStartupMemory = parsed, true
	}
	if hasCapacityMemory && hasAllocatableMemory && allocatableMemory > capacityMemory {
		return fmt.Errorf("%s.allocatable.memory must be <= %s.capacity.memory", resourceFieldPath, resourceFieldPath)
	}
	if hasStartupMemory && hasCapacityMemory && startupMemory > capacityMemory {
		return fmt.Errorf("%s.startup.memory must be <= %s.capacity.memory", resourceFieldPath, resourceFieldPath)
	}
	return nil
}

// MarshalResourcePatch emits the stable compact JSON representation stored in
// metadata and transported through cluster and migration paths.
func MarshalResourcePatch(patch ResourcePatch) (string, error) {
	if err := ValidateResourcePatch(patch); err != nil {
		return "", err
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return "", fmt.Errorf("marshal %s: %w", resourceFieldPath, err)
	}
	return string(raw), nil
}

// MergeResourcePatch overlays only leaves present in over onto base and checks
// relationships introduced across layers.
func MergeResourcePatch(base, over ResourcePatch) (ResourcePatch, error) {
	out := cloneResourcePatch(base)
	if over.Capacity != nil {
		if out.Capacity == nil {
			out.Capacity = &CapacityPatch{}
		}
		if over.Capacity.CPU != nil {
			out.Capacity.CPU = intPtr(*over.Capacity.CPU)
		}
		if over.Capacity.Memory != nil {
			out.Capacity.Memory = stringPtr(*over.Capacity.Memory)
		}
	}
	if over.Allocatable != nil {
		if out.Allocatable == nil {
			out.Allocatable = &AllocatablePatch{}
		}
		if over.Allocatable.CPU != nil {
			out.Allocatable.CPU = floatPtr(*over.Allocatable.CPU)
		}
		if over.Allocatable.Memory != nil {
			out.Allocatable.Memory = stringPtr(*over.Allocatable.Memory)
		}
	}
	if over.Startup != nil {
		if out.Startup == nil {
			out.Startup = &StartupPatch{}
		}
		if over.Startup.Memory != nil {
			out.Startup.Memory = stringPtr(*over.Startup.Memory)
		}
	}
	if err := ValidateResourcePatch(out); err != nil {
		return ResourcePatch{}, err
	}
	return out, nil
}

func cloneResourcePatch(in ResourcePatch) ResourcePatch {
	var out ResourcePatch
	if in.Capacity != nil {
		out.Capacity = &CapacityPatch{}
		if in.Capacity.CPU != nil {
			out.Capacity.CPU = intPtr(*in.Capacity.CPU)
		}
		if in.Capacity.Memory != nil {
			out.Capacity.Memory = stringPtr(*in.Capacity.Memory)
		}
	}
	if in.Allocatable != nil {
		out.Allocatable = &AllocatablePatch{}
		if in.Allocatable.CPU != nil {
			out.Allocatable.CPU = floatPtr(*in.Allocatable.CPU)
		}
		if in.Allocatable.Memory != nil {
			out.Allocatable.Memory = stringPtr(*in.Allocatable.Memory)
		}
	}
	if in.Startup != nil {
		out.Startup = &StartupPatch{}
		if in.Startup.Memory != nil {
			out.Startup.Memory = stringPtr(*in.Startup.Memory)
		}
	}
	return out
}

func intPtr(v int) *int           { return &v }
func floatPtr(v float64) *float64 { return &v }
func stringPtr(v string) *string  { return &v }
func boolPtr(v bool) *bool        { return &v }

// NormalizeResourceMetadata validates and canonicalizes the resource namespace
// without changing any other metadata namespace.
func NormalizeResourceMetadata(meta map[string]string) (map[string]string, error) {
	raw, present := meta[NsResource]
	if !present {
		return meta, nil
	}
	patch, err := ParseResourcePatch(raw)
	if err != nil {
		return nil, err
	}
	canonical, err := MarshalResourcePatch(patch)
	if err != nil {
		return nil, err
	}
	out := cloneStringMap(meta)
	out[NsResource] = canonical
	return out, nil
}

// ApplyCapacity overlays E2B first-class cpuCount/memoryMB leaves while
// preserving every allocatable/startup sibling. Zero means absent; negatives
// are explicit invalid requests.
func ApplyCapacity(meta map[string]string, cpu, memoryMiB int) (map[string]string, error) {
	if cpu < 0 {
		return nil, errors.New("cpuCount must be >= 0")
	}
	if memoryMiB < 0 {
		return nil, errors.New("memoryMB must be >= 0")
	}
	if memoryMiB > 0 {
		if _, err := positiveSize("memoryMB", fmt.Sprintf("%dMiB", memoryMiB)); err != nil {
			return nil, err
		}
	}
	var patch ResourcePatch
	rawResource, resourcePresent := meta[NsResource]
	if resourcePresent {
		parsed, err := ParseResourcePatch(rawResource)
		if err != nil {
			return nil, err
		}
		patch = parsed
	}
	if cpu > 0 || memoryMiB > 0 {
		overlay := ResourcePatch{Capacity: &CapacityPatch{}}
		if cpu > 0 {
			overlay.Capacity.CPU = intPtr(cpu)
		}
		if memoryMiB > 0 {
			overlay.Capacity.Memory = stringPtr(fmt.Sprintf("%dMiB", memoryMiB))
		}
		merged, err := MergeResourcePatch(patch, overlay)
		if err != nil {
			return nil, err
		}
		patch = merged
	}
	if !resourcePresent && patch.Capacity == nil && patch.Allocatable == nil && patch.Startup == nil {
		return cloneStringMap(meta), nil
	}
	canonical, err := MarshalResourcePatch(patch)
	if err != nil {
		return nil, err
	}
	out := cloneStringMap(meta)
	if out == nil {
		out = map[string]string{}
	}
	out[NsResource] = canonical
	return out, nil
}

// ResourceResolveInput supplies the complete pure inputs for one final runtime
// resource document. Restore=true requires SnapshotCapacity and treats any
// explicit request capacity leaves as equality assertions.
type ResourceResolveInput struct {
	Node                     NodeResourcePolicy
	Patch                    ResourcePatch
	Restore                  bool
	SnapshotCapacity         *rtconfig.CapacityConfig
	Dynamic                  bool
	ControllerSocketIdentity string
}

// ResolveResources applies node defaults, tenant leaves, restore constraints,
// and node-only runtime policy in one place.
func ResolveResources(input ResourceResolveInput) (rtconfig.ResourcesConfig, error) {
	policy := input.Node
	policy.ApplyDefaults()
	if err := ValidateNodeResourcePolicy(policy, input.Dynamic); err != nil {
		return rtconfig.ResourcesConfig{}, err
	}
	if err := ValidateResourcePatch(input.Patch); err != nil {
		return rtconfig.ResourcesConfig{}, fmt.Errorf("%w: %v", ErrInvalidResourceRequest, err)
	}

	capacity := rtconfig.CapacityConfig{CPU: policy.Capacity.CPU, Memory: policy.Capacity.Memory}
	if !input.Restore && input.Patch.Capacity != nil {
		if input.Patch.Capacity.CPU != nil {
			capacity.CPU = *input.Patch.Capacity.CPU
		}
		if input.Patch.Capacity.Memory != nil {
			capacity.Memory = *input.Patch.Capacity.Memory
		}
	}
	if input.Restore {
		if input.SnapshotCapacity == nil {
			return rtconfig.ResourcesConfig{}, errors.New("restore snapshot capacity is required")
		}
		snapshot := *input.SnapshotCapacity
		if snapshot.CPU <= 0 {
			return rtconfig.ResourcesConfig{}, errors.New("restore snapshot resources.capacity.cpu must be > 0")
		}
		snapshotMemory, err := positiveSize("restore snapshot resources.capacity.memory", snapshot.Memory)
		if err != nil {
			return rtconfig.ResourcesConfig{}, err
		}
		if requested := input.Patch.Capacity; requested != nil {
			if requested.CPU != nil && *requested.CPU != snapshot.CPU {
				return rtconfig.ResourcesConfig{}, fmt.Errorf("%w: %s.capacity.cpu=%d conflicts with restore snapshot capacity.cpu=%d",
					ErrInvalidResourceRequest, resourceFieldPath, *requested.CPU, snapshot.CPU)
			}
			if requested.Memory != nil {
				requestedMemory, _ := positiveSize(resourceFieldPath+".capacity.memory", *requested.Memory)
				if requestedMemory != snapshotMemory {
					return rtconfig.ResourcesConfig{}, fmt.Errorf("%w: %s.capacity.memory=%q conflicts with restore snapshot capacity.memory=%q",
						ErrInvalidResourceRequest, resourceFieldPath, *requested.Memory, snapshot.Memory)
				}
			}
		}
		capacity = snapshot
	}

	capMem, err := positiveSize("resources.capacity.memory", capacity.Memory)
	if err != nil {
		return rtconfig.ResourcesConfig{}, err
	}
	if capacity.CPU <= 0 {
		return rtconfig.ResourcesConfig{}, errors.New("resources.capacity.cpu must be > 0")
	}

	allocCPU := float64(capacity.CPU)
	if input.Patch.Allocatable != nil && input.Patch.Allocatable.CPU != nil {
		allocCPU = *input.Patch.Allocatable.CPU
		if allocCPU > float64(capacity.CPU) {
			return rtconfig.ResourcesConfig{}, fmt.Errorf("%w: %s.allocatable.cpu must be <= capacity.cpu",
				ErrInvalidResourceRequest, resourceFieldPath)
		}
	} else if policy.Allocatable.CPU != nil {
		allocCPU = min(*policy.Allocatable.CPU, float64(capacity.CPU))
	}

	allocMemory := policy.Allocatable.Memory
	allocMem, err := positiveSize("sandbox.resources.allocatable.memory", allocMemory)
	if err != nil {
		return rtconfig.ResourcesConfig{}, err
	}
	if input.Patch.Allocatable != nil && input.Patch.Allocatable.Memory != nil {
		allocMemory = *input.Patch.Allocatable.Memory
		allocMem, _ = positiveSize(resourceFieldPath+".allocatable.memory", allocMemory)
		if allocMem > capMem {
			return rtconfig.ResourcesConfig{}, fmt.Errorf("%w: %s.allocatable.memory must be <= capacity.memory",
				ErrInvalidResourceRequest, resourceFieldPath)
		}
	} else if allocMem > capMem {
		allocMemory, allocMem = capacity.Memory, capMem
	}

	resources := rtconfig.ResourcesConfig{
		Capacity: capacity,
		Allocatable: rtconfig.AllocatableConfig{
			CPU: allocCPU, Memory: allocMemory,
		},
		Overhead: &rtconfig.OverheadConfig{Memory: policy.Overhead.Memory},
		WatermarkHigh: &rtconfig.WatermarkHighConfig{
			Ratio: *policy.WatermarkHigh.Ratio,
		},
	}
	if allocMem < capMem {
		resources.Allocatable.DeflateOnOOM = boolPtr(true)
	}

	if input.Dynamic {
		if input.ControllerSocketIdentity == "" {
			return rtconfig.ResourcesConfig{}, errors.New("resource_listen canonical socket identity is required in dynamic mode")
		}
		resources.Control.Controller = input.ControllerSocketIdentity
	}
	switch {
	case input.Patch.Startup != nil && input.Patch.Startup.Memory != nil:
		startup := *input.Patch.Startup.Memory
		startupMem, _ := positiveSize(resourceFieldPath+".startup.memory", startup)
		if startupMem > capMem {
			return rtconfig.ResourcesConfig{}, fmt.Errorf("%w: %s.startup.memory must be <= capacity.memory",
				ErrInvalidResourceRequest, resourceFieldPath)
		}
		resources.Startup = &rtconfig.StartupConfig{Memory: startup}
	case policy.Startup != nil:
		startup := policy.Startup.Memory
		startupMem, _ := positiveSize("sandbox.resources.startup.memory", startup)
		if startupMem > capMem {
			startup = capacity.Memory
		}
		resources.Startup = &rtconfig.StartupConfig{Memory: startup}
	default:
		resources.Startup = &rtconfig.StartupConfig{Memory: capacity.Memory}
	}

	if err := ValidateResolvedResources(resources, input.Dynamic); err != nil {
		return rtconfig.ResourcesConfig{}, err
	}
	return resources, nil
}

// ValidateResolvedResources checks the pure resource relationships without
// requiring the node-owned cgroup FD, which run-sandbox injects later.
func ValidateResolvedResources(resources rtconfig.ResourcesConfig, dynamic bool) error {
	if resources.Capacity.CPU <= 0 {
		return errors.New("resources.capacity.cpu must be > 0")
	}
	capMem, err := positiveSize("resources.capacity.memory", resources.Capacity.Memory)
	if err != nil {
		return err
	}
	if !positiveFinite(resources.Allocatable.CPU) {
		return errors.New("resources.allocatable.cpu must be > 0")
	}
	if resources.Allocatable.CPU > float64(resources.Capacity.CPU) {
		return errors.New("resources.allocatable.cpu must be <= resources.capacity.cpu")
	}
	allocMem, err := positiveSize("resources.allocatable.memory", resources.Allocatable.Memory)
	if err != nil {
		return err
	}
	if allocMem > capMem {
		return errors.New("resources.allocatable.memory must be <= resources.capacity.memory")
	}
	if resources.Overhead == nil {
		return errors.New("resources.overhead.memory is required")
	}
	if _, err := positiveSize("resources.overhead.memory", resources.Overhead.Memory); err != nil {
		return err
	}
	if resources.WatermarkHigh == nil || !positiveFinite(resources.WatermarkHigh.Ratio) || resources.WatermarkHigh.Ratio >= 1 {
		return errors.New("resources.watermark_high.ratio must be > 0 and < 1")
	}
	if resources.Control.Sensor != nil {
		return errors.New("resources.control.sensor must remain omitted")
	}
	if resources.Control.CgroupPath != "" || resources.Control.CgroupFD != 0 {
		return errors.New("resources.control cgroup capability is injected by run-sandbox")
	}
	if dynamic {
		if resources.Control.Controller == "" {
			return errors.New("resources.control.controller is required in dynamic mode")
		}
	} else {
		if resources.Control.Controller != "" {
			return errors.New("resources.control.controller must be omitted in static mode")
		}
	}
	if resources.Startup == nil {
		return errors.New("resources.startup is required")
	}
	startupMem, err := positiveSize("resources.startup.memory", resources.Startup.Memory)
	if err != nil {
		return err
	}
	if startupMem > capMem {
		return errors.New("resources.startup.memory must be <= resources.capacity.memory")
	}
	if allocMem < capMem && (resources.Allocatable.DeflateOnOOM == nil || !*resources.Allocatable.DeflateOnOOM) {
		return errors.New("resources.allocatable.deflate_on_oom must be true when ballooning is enabled")
	}
	return nil
}
