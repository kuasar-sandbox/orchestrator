package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/util"
	"gopkg.in/yaml.v3"
)

const DefaultResourcePool = "default"

// Omission means unlimited for capacity. For allocatable, omission inherits
// capacity (memory instead uses the legacy single-pool formula).
type ResourcePoolConfig struct {
	Name        string                                 `yaml:"name" json:"name"`
	Capacity    ResourceAllocatable                    `yaml:"capacity,omitempty" json:"capacity,omitempty"`
	Allocatable ResourceAllocatable                    `yaml:"allocatable,omitempty" json:"allocatable,omitempty"`
	Watermarks  map[string]ResourceDimensionWatermarks `yaml:"watermarks,omitempty" json:"watermarks,omitempty"`
	UnitDropins []string                               `yaml:"unit_dropins,omitempty" json:"unit_dropins,omitempty"`
}

// Pointers preserve explicit zero thresholds and partial dimension overrides.
type ResourceDimensionWatermarks struct {
	LowFactor       *float64 `yaml:"low_factor,omitempty" json:"low_factor,omitempty"`
	HighFactor      *float64 `yaml:"high_factor,omitempty" json:"high_factor,omitempty"`
	EmergencyFactor *float64 `yaml:"emergency_factor,omitempty" json:"emergency_factor,omitempty"`
}

func (w ResourceDimensionWatermarks) Resolve(base ResourceWatermarksConfig) ResourceWatermarksConfig {
	if w.LowFactor != nil {
		base.LowFactor = *w.LowFactor
	}
	if w.HighFactor != nil {
		base.HighFactor = *w.HighFactor
	}
	if w.EmergencyFactor != nil {
		base.EmergencyFactor = *w.EmergencyFactor
	}
	return base
}

func cloneResourcePools(in []ResourcePoolConfig) []ResourcePoolConfig {
	out := slices.Clone(in)
	for i := range out {
		p := &out[i]
		p.Capacity.CPU, p.Capacity.Memory = clonePtr(p.Capacity.CPU), clonePtr(p.Capacity.Memory)
		p.Allocatable.CPU, p.Allocatable.Memory = clonePtr(p.Allocatable.CPU), clonePtr(p.Allocatable.Memory)
		p.UnitDropins = slices.Clone(p.UnitDropins)
		if p.Watermarks != nil {
			p.Watermarks = make(map[string]ResourceDimensionWatermarks, len(in[i].Watermarks))
			for k, w := range in[i].Watermarks {
				w.LowFactor, w.HighFactor, w.EmergencyFactor = clonePtr(w.LowFactor), clonePtr(w.HighFactor), clonePtr(w.EmergencyFactor)
				p.Watermarks[k] = w
			}
		}
	}
	return out
}

func (r *ResourceListenConfig) ResourcePoolConfigs() []ResourcePoolConfig {
	if r == nil || r.Pools == nil {
		return []ResourcePoolConfig{{Name: DefaultResourcePool}}
	}
	return r.Pools
}

func (p RunPoolConfig) PoolName(kind string, index int) string {
	if p.Name != "" {
		return p.Name
	}
	return fmt.Sprintf("%s-%d", kind, index)
}

func (p RunPoolConfig) ResourcePoolName() string {
	if p.ResourcePool == "" {
		return DefaultResourcePool
	}
	return p.ResourcePool
}

func (r *ResourceListenConfig) trackLegacyDefaults() {
	if !r.legacyDefaulted {
		r.legacyExplicit = r.legacyExplicit || r.Resources != (ResourceHostConfig{}) || r.Watermarks.OperationalMarginFactor != 0
	} else if r.Resources != r.legacyDefaults || r.Watermarks.OperationalMarginFactor != r.marginDefault {
		r.legacyExplicit = true
	}
}

func (r *ResourceListenConfig) finishLegacyDefaults() {
	r.legacyDefaulted = true
	r.legacyDefaults, r.marginDefault = r.Resources, r.Watermarks.OperationalMarginFactor
}

func (r *ResourceListenConfig) hasLegacyPhysical() bool {
	if r.legacyExplicit {
		return true
	}
	if r.legacyDefaulted {
		return r.Resources != r.legacyDefaults || r.Watermarks.OperationalMarginFactor != r.marginDefault
	}
	return r.Resources != (ResourceHostConfig{}) || r.Watermarks.OperationalMarginFactor != 0
}

var poolNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

func (r *ResourceListenConfig) ValidatePools() error {
	if r == nil {
		return nil
	}
	if r.Pools != nil && len(r.Pools) == 0 {
		return fmt.Errorf("config: resource_listen.pools must not be empty")
	}
	if len(r.Pools) > 1 && r.hasLegacyPhysical() {
		return fmt.Errorf("config: multiple resource pools cannot use explicit physical_memory, physical_cpu, host_reserved or operational_margin_factor")
	}
	seen := map[string]bool{}
	for _, p := range r.Pools {
		if !poolNamePattern.MatchString(p.Name) || seen[p.Name] {
			return fmt.Errorf("config: invalid or duplicate resource pool name %q", p.Name)
		}
		seen[p.Name] = true
		if len(r.Pools) > 1 && p.Allocatable.Memory == nil {
			return fmt.Errorf("config: resource pool %s requires allocatable.memory", p.Name)
		}
		for _, quantity := range []ResourceAllocatable{p.Capacity, p.Allocatable} {
			if quantity.CPU != nil && (*quantity.CPU <= 0 || math.IsNaN(*quantity.CPU) || math.IsInf(*quantity.CPU, 0) || *quantity.CPU >= float64(math.MaxUint64)/1000) {
				return fmt.Errorf("config: resource pool %s CPU must be finite, positive and representable", p.Name)
			}
			if quantity.Memory != nil {
				n, err := util.ParseSize(*quantity.Memory)
				if err != nil || n == 0 {
					return fmt.Errorf("config: resource pool %s memory must be a positive size", p.Name)
				}
			}
		}
		defaults := *r
		defaults.ApplyDefaults()
		for dimension, override := range p.Watermarks {
			if dimension != "cpu" && dimension != "memory" {
				return fmt.Errorf("config: unsupported resource pool watermark dimension %q", dimension)
			}
			w := override.Resolve(defaults.Watermarks)
			if math.IsNaN(w.LowFactor+w.HighFactor+w.EmergencyFactor) || w.LowFactor < 0 || w.LowFactor >= w.HighFactor || w.EmergencyFactor < 0 || w.HighFactor >= 1-w.EmergencyFactor {
				return fmt.Errorf("config: resource pool %s %s thresholds require 0 <= low < high < 1-emergency <= 1", p.Name, dimension)
			}
		}
		for _, path := range p.UnitDropins {
			if !filepath.IsAbs(path) {
				return fmt.Errorf("config: resource pool %s unit_dropins must use absolute paths", p.Name)
			}
		}
	}
	return nil
}

// ValidateResourcePools is called after Configure as well as during loading.
func (c *Conductor) ValidateResourcePools() error {
	if err := c.ResourceListen.ValidatePools(); err != nil {
		return err
	}
	resources := map[string]bool{}
	for _, p := range c.ResourceListen.ResourcePoolConfigs() {
		resources[p.Name] = true
	}
	templates := map[string]string{}
	for _, kind := range []struct {
		name  string
		pools []RunPoolConfig
	}{{"runner", c.Units.RunnerPoolConfigs()}, {"builder", c.Units.BuilderPoolConfigs()}} {
		names := map[string]bool{}
		for i, p := range kind.pools {
			name, resource := p.PoolName(kind.name, i), p.ResourcePoolName()
			if !poolNamePattern.MatchString(name) || names[name] {
				return fmt.Errorf("config: invalid or duplicate %s pool name %q", kind.name, name)
			}
			names[name] = true
			if !resources[resource] {
				return fmt.Errorf("config: %s pool %s references unknown resource pool %q", kind.name, name, resource)
			}
			if prior, ok := templates[p.Unit]; ok && prior != resource {
				return fmt.Errorf("config: unit template %s cannot span resource pools %s and %s", p.Unit, prior, resource)
			}
			templates[p.Unit] = resource
		}
	}
	return nil
}

func (r *ResourceListenConfig) UnmarshalJSON(raw []byte) error {
	type plain ResourceListenConfig
	var value plain
	if err := strictjson.Decode(raw, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if v, ok := fields["pools"]; ok && string(v) == "null" {
		return fmt.Errorf("config: resource_listen.pools must not be null")
	}
	var resources, watermarks map[string]json.RawMessage
	if v := fields["resources"]; v != nil {
		if err := json.Unmarshal(v, &resources); err != nil {
			return err
		}
	}
	if v := fields["watermarks"]; v != nil {
		if err := json.Unmarshal(v, &watermarks); err != nil {
			return err
		}
	}
	*r = ResourceListenConfig(value)
	_, margin := watermarks["operational_margin_factor"]
	r.legacyExplicit = len(resources) > 0 || margin
	return nil
}

func (r *ResourceListenConfig) UnmarshalYAML(node *yaml.Node) error {
	// Decode a plain alias with KnownFields, preserving legacy YAML scalar
	// coercions (for example physical_cpu: 8) and merge keys.
	var fields map[string]any
	if err := node.Decode(&fields); err != nil {
		return err
	}
	for key := range fields {
		switch key {
		case "pools", "pressure", "enabled", "socket", "state_path", "cgroup_scan_paths", "resources", "watermarks", "rate_limits", "admission", "log_level":
		default:
			return fmt.Errorf("field %s not found in type config.ResourceListenConfig", key)
		}
	}
	raw, err := yaml.Marshal(fields)
	if err != nil {
		return err
	}
	type plain ResourceListenConfig
	var value plain
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	if pools, ok := fields["pools"]; ok && pools == nil {
		return fmt.Errorf("config: resource_listen.pools must not be null")
	}
	*r = ResourceListenConfig(value)
	if resources, ok := fields["resources"].(map[string]any); ok && len(resources) > 0 {
		r.legacyExplicit = true
	}
	if watermarks, ok := fields["watermarks"].(map[string]any); ok {
		_, explicit := watermarks["operational_margin_factor"]
		r.legacyExplicit = r.legacyExplicit || explicit
	}
	return nil
}

func (r ResourceListenConfig) output() (map[string]any, error) {
	type plain ResourceListenConfig
	if r.CgroupScanPaths == nil {
		r.CgroupScanPaths = []string{}
	}
	raw, err := json.Marshal(plain(r))
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	// Defaults are not explicit legacy input. Preserve this across config dumps.
	if !r.hasLegacyPhysical() {
		delete(fields, "resources")
		delete(fields["watermarks"].(map[string]any), "operational_margin_factor")
	}
	return fields, nil
}
func (r ResourceListenConfig) MarshalYAML() (any, error) { return r.output() }
func (r ResourceListenConfig) MarshalJSON() ([]byte, error) {
	v, err := r.output()
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
