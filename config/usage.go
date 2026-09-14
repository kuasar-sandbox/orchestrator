package config

import (
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

// UsageConfig is node-local policy for each new runtime invocation. It does not
// enter sandbox metadata or portable artifacts and is independent of telemetry.
type UsageConfig struct {
	Enabled        bool   `yaml:"enabled" json:"enabled"`
	SampleInterval string `yaml:"sample_interval" json:"sample_interval"`
	FlushInterval  string `yaml:"flush_interval" json:"flush_interval"`
}

// Runtime adapts the public config to sandboxer's existing policy/validation.
func (u UsageConfig) Runtime() rtconfig.UsageConfig {
	return rtconfig.UsageConfig{Enabled: u.Enabled, SampleInterval: u.SampleInterval, FlushInterval: u.FlushInterval}
}

func (u *UsageConfig) UnmarshalJSON(raw []byte) error {
	type wire UsageConfig
	var decoded wire
	if err := strictjson.Decode(raw, &decoded); err != nil {
		return fmt.Errorf("sandbox.usage: %w", err)
	}
	*u = UsageConfig(decoded)
	return nil
}

func (u *UsageConfig) UnmarshalYAML(node *yaml.Node) error {
	var native rtconfig.UsageConfig
	if err := native.UnmarshalYAML(node); err != nil {
		return fmt.Errorf("sandbox.%w", err)
	}
	*u = UsageConfig{Enabled: native.Enabled, SampleInterval: native.SampleInterval, FlushInterval: native.FlushInterval}
	return nil
}

// Match the native config's null/merge validation before yaml.v3 can turn an
// explicit null into an omitted value without invoking UnmarshalYAML.
func validateSandboxUsageYAML(raw []byte) error {
	var root map[string]yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return err
	}
	sandbox, exists := root["sandbox"]
	if !exists {
		return nil
	}
	var fields map[string]yaml.Node
	if err := sandbox.Decode(&fields); err != nil {
		return err
	}
	if node, exists := fields["usage"]; exists {
		var checked UsageConfig
		return checked.UnmarshalYAML(&node)
	}
	return nil
}
