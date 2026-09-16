package config

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"gopkg.in/yaml.v3"
)

// RunPoolConfig is one independent pool. Size is an idle target, not execution
// capacity or selection weight. Repeated entries are intentional.
type RunPoolConfig struct {
	Unit string `yaml:"unit" json:"unit"`
	Size int    `yaml:"size" json:"size"`
}

type runPoolInput struct {
	Unit *string `yaml:"unit" json:"unit"`
	Size *int    `yaml:"size" json:"size"`
}

func (p *RunPoolConfig) decode(in runPoolInput) error {
	if in.Unit == nil || in.Size == nil {
		return fmt.Errorf("config: pool requires unit and size")
	}
	p.Unit, p.Size = *in.Unit, *in.Size
	return nil
}

func (p *RunPoolConfig) UnmarshalYAML(node *yaml.Node) error {
	var fields map[string]yaml.Node
	if err := node.Decode(&fields); err != nil {
		return err
	}
	for key := range fields {
		if key != "unit" && key != "size" {
			return fmt.Errorf("config: pool contains unknown field %q", key)
		}
	}
	if size, present := fields["size"]; present && poolYAMLValue(&size).Tag != "!!int" {
		return fmt.Errorf("config: pool size must be an integer")
	}
	var in runPoolInput
	if err := node.Decode(&in); err != nil {
		return err
	}
	return p.decode(in)
}

// Inspect the referenced value while retaining the original document's anchors.
func poolYAMLValue(node *yaml.Node) *yaml.Node {
	for node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	return node
}

func (p *RunPoolConfig) UnmarshalJSON(raw []byte) error {
	var in runPoolInput
	if err := strictjson.Decode(raw, &in); err != nil {
		return err
	}
	return p.decode(in)
}

const defaultRunnerUnit = "sandbox-runner@.service"
const defaultBuilderUnit = "sandbox-builder@.service"

var serviceTemplate = regexp.MustCompile(`^[a-zA-Z0-9_.:\\-]+@\.service$`)

func (u UnitsConfig) RunnerPoolConfigs() []RunPoolConfig {
	if u.RunnerPools != nil {
		return u.RunnerPools
	}
	unit := u.Runner
	if unit == "" {
		unit = defaultRunnerUnit
	}
	return []RunPoolConfig{{Unit: unit, Size: u.RunnerPoolSize}}
}

func (u UnitsConfig) BuilderPoolConfigs() []RunPoolConfig {
	if u.BuilderPools != nil {
		return u.BuilderPools
	}
	unit := u.Builder
	if unit == "" {
		unit = defaultBuilderUnit
	}
	return []RunPoolConfig{{Unit: unit, Size: u.BuilderPoolSize}}
}

func (u *UnitsConfig) applyDefaults() {
	if u.RunnerPools == nil && u.Runner == "" {
		u.Runner, u.runnerDefaulted = defaultRunnerUnit, true
	}
	if u.BuilderPools == nil && u.Builder == "" {
		u.Builder, u.builderDefaulted = defaultBuilderUnit, true
	}
}

func (u UnitsConfig) validate() error {
	for _, kind := range []struct {
		name                string
		pools               []RunPoolConfig
		legacy              string
		size                int
		explicit, defaulted bool
		defaultUnit         string
	}{
		{"runner", u.RunnerPools, u.Runner, u.RunnerPoolSize, u.runnerExplicit, u.runnerDefaulted, defaultRunnerUnit},
		{"builder", u.BuilderPools, u.Builder, u.BuilderPoolSize, u.builderExplicit, u.builderDefaulted, defaultBuilderUnit},
	} {
		if kind.size < 0 {
			return fmt.Errorf("config: units.%s_pool_size must be >= 0", kind.name)
		}
		if kind.pools == nil {
			continue
		}
		if kind.explicit || kind.size != 0 || (kind.legacy != "" && !(kind.defaulted && kind.legacy == kind.defaultUnit)) {
			return fmt.Errorf("config: units.%s_pools cannot be combined with explicit %s or %s_pool_size", kind.name, kind.name, kind.name)
		}
		if len(kind.pools) == 0 {
			return fmt.Errorf("config: units.%s_pools must not be empty", kind.name)
		}
		for i, pool := range kind.pools {
			if !serviceTemplate.MatchString(pool.Unit) {
				return fmt.Errorf("config: units.%s_pools[%d].unit must be a service template name (name@.service)", kind.name, i)
			}
			if pool.Size < 0 {
				return fmt.Errorf("config: units.%s_pools[%d].size must be >= 0", kind.name, i)
			}
		}
	}
	return nil
}

func (u *UnitsConfig) UnmarshalYAML(node *yaml.Node) error {
	// Decode the original node: locally re-encoding it loses anchors defined
	// outside units. Mapping decode resolves merges for presence/field checks;
	// explicit allowlists preserve strictness inside these custom decoders.
	var fields map[string]yaml.Node
	if err := node.Decode(&fields); err != nil {
		return err
	}
	for key := range fields {
		switch key {
		case "dir", "install", "pool_wait_timeout", "runner", "runner_pool_size", "builder", "builder_pool_size", "runner_pools", "builder_pools":
		default:
			return fmt.Errorf("config: units contains unknown field %q", key)
		}
	}
	for _, key := range []string{"runner_pools", "builder_pools"} {
		if value, present := fields[key]; present && poolYAMLValue(&value).Tag == "!!null" {
			return fmt.Errorf("config: units.%s must not be null", key)
		}
	}
	type plain UnitsConfig
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*u = UnitsConfig(decoded)
	_, runner := fields["runner"]
	_, runnerSize := fields["runner_pool_size"]
	_, builder := fields["builder"]
	_, builderSize := fields["builder_pool_size"]
	u.runnerExplicit, u.builderExplicit = runner || runnerSize, builder || builderSize
	return u.validate()
}

func (u *UnitsConfig) UnmarshalJSON(raw []byte) error {
	type plain UnitsConfig
	var decoded plain
	if err := strictjson.Decode(raw, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, key := range []string{"runner_pools", "builder_pools"} {
		if value, present := fields[key]; present && string(value) == "null" {
			return fmt.Errorf("config: units.%s must not be null", key)
		}
	}
	*u = UnitsConfig(decoded)
	_, runner := fields["runner"]
	_, runnerSize := fields["runner_pool_size"]
	_, builder := fields["builder"]
	_, builderSize := fields["builder_pool_size"]
	u.runnerExplicit, u.builderExplicit = runner || runnerSize, builder || builderSize
	return u.validate()
}

// Output only the selected input form, so this binary can read its own cloned
// and defaulted configuration without manufacturing mixed old/new input.
func (u UnitsConfig) output() (map[string]any, error) {
	if err := u.validate(); err != nil {
		return nil, err
	}
	out := map[string]any{"dir": u.Dir, "install": u.Install, "pool_wait_timeout": u.PoolWaitTimeout}
	if u.RunnerPools != nil {
		out["runner_pools"] = u.RunnerPools
	} else {
		out["runner"], out["runner_pool_size"] = u.Runner, u.RunnerPoolSize
	}
	if u.BuilderPools != nil {
		out["builder_pools"] = u.BuilderPools
	} else {
		out["builder"], out["builder_pool_size"] = u.Builder, u.BuilderPoolSize
	}
	return out, nil
}

func (u UnitsConfig) MarshalYAML() (any, error) { return u.output() }
func (u UnitsConfig) MarshalJSON() ([]byte, error) {
	out, err := u.output()
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}
