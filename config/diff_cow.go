package config

import (
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"gopkg.in/yaml.v3"
)

func (c *ResourceDiffCOW) UnmarshalJSON(raw []byte) error {
	type wire ResourceDiffCOW
	var decoded wire
	if err := strictjson.Decode(raw, &decoded); err != nil {
		return fmt.Errorf("sandbox.resources.diff_cow: %w", err)
	}
	*c = ResourceDiffCOW(decoded)
	return nil
}

// Preserve YAML merge handling while keeping the owned nested schema strict.
// In particular, a null discard must not become an omitted/default policy.
func (c *ResourceDiffCOW) UnmarshalYAML(node *yaml.Node) error {
	var fields map[string]yaml.Node
	if err := node.Decode(&fields); err != nil {
		return fmt.Errorf("sandbox.resources.diff_cow: %w", err)
	}
	for field, value := range fields {
		switch field {
		case "cache_size", "max_dirty_size":
		case "discard":
			var decoded any
			if err := value.Decode(&decoded); err != nil {
				return fmt.Errorf("sandbox.resources.diff_cow.discard: %w", err)
			}
			if _, ok := decoded.(string); !ok {
				return fmt.Errorf("sandbox.resources.diff_cow.discard must be auto or manual")
			}
		default:
			return fmt.Errorf("sandbox.resources.diff_cow contains unknown field %q", field)
		}
	}
	type wire ResourceDiffCOW
	var decoded wire
	if err := node.Decode(&decoded); err != nil {
		return fmt.Errorf("sandbox.resources.diff_cow: %w", err)
	}
	*c = ResourceDiffCOW(decoded)
	return nil
}
