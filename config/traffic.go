package config

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"gopkg.in/yaml.v3"
)

// ProxyTrafficConfig defines node-local defaults for per-Sandbox traffic
// admission. These values are resolved by the target Proxy master and are not
// copied into durable Sandbox metadata.
type ProxyTrafficConfig struct {
	MaxInflight MaxInflight `yaml:"max_inflight" json:"max_inflight"`
}

// MaxInflight contains the Proxy-wide per-Sandbox inflight limits. Zero means
// unlimited. Service limits are independent from Total.
type MaxInflight struct {
	Total              uint32 `yaml:"total" json:"total"`
	Forward            uint32 `yaml:"forward" json:"forward"`
	E2BEnvd            uint32 `yaml:"e2b:envd" json:"e2b:envd"`
	E2BCodeInterpreter uint32 `yaml:"e2b:code-interpreter" json:"e2b:code-interpreter"`
	Exec               uint32 `yaml:"exec" json:"exec"`
}

// Unlimited reports whether admission can use the existing no-arena fast path.
func (m MaxInflight) Unlimited() bool {
	return m.Total == 0 && m.Forward == 0 && m.E2BEnvd == 0 &&
		m.E2BCodeInterpreter == 0 && m.Exec == 0
}

func (t *ProxyTrafficConfig) UnmarshalJSON(raw []byte) error {
	type wire struct {
		MaxInflight MaxInflight `json:"max_inflight"`
	}
	var decoded wire
	if err := strictjson.Decode(raw, &decoded); err != nil {
		return fmt.Errorf("proxy traffic: %w", err)
	}
	t.MaxInflight = decoded.MaxInflight
	return nil
}

func (t *ProxyTrafficConfig) UnmarshalYAML(node *yaml.Node) error {
	if err := requireYAMLMapping("proxy traffic", node); err != nil {
		return err
	}
	*t = ProxyTrafficConfig{}
	seen := map[string]struct{}{}
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if _, duplicate := seen[key.Value]; duplicate {
			return fmt.Errorf("proxy traffic contains duplicate field %q", key.Value)
		}
		seen[key.Value] = struct{}{}
		if key.Value != "max_inflight" {
			return fmt.Errorf("proxy traffic contains unknown field %q", key.Value)
		}
		if value.Tag == "!!null" {
			return fmt.Errorf("traffic.max_inflight must not be null")
		}
		if err := value.Decode(&t.MaxInflight); err != nil {
			return err
		}
	}
	return nil
}

func (m *MaxInflight) UnmarshalJSON(raw []byte) error {
	type wire MaxInflight
	var decoded wire
	if err := strictjson.Decode(raw, &decoded); err != nil {
		return fmt.Errorf("traffic.max_inflight: %w", err)
	}
	*m = MaxInflight(decoded)
	return nil
}

func (m *MaxInflight) UnmarshalYAML(node *yaml.Node) error {
	if err := requireYAMLMapping("traffic.max_inflight", node); err != nil {
		return err
	}
	*m = MaxInflight{}
	seen := map[string]struct{}{}
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if _, duplicate := seen[key.Value]; duplicate {
			return fmt.Errorf("traffic.max_inflight contains duplicate field %q", key.Value)
		}
		seen[key.Value] = struct{}{}
		var target *uint32
		switch key.Value {
		case "total":
			target = &m.Total
		case "forward":
			target = &m.Forward
		case "e2b:envd":
			target = &m.E2BEnvd
		case "e2b:code-interpreter":
			target = &m.E2BCodeInterpreter
		case "exec":
			target = &m.Exec
		default:
			return fmt.Errorf("traffic.max_inflight contains unknown field %q", key.Value)
		}
		if value.Kind != yaml.ScalarNode || value.Tag != "!!int" {
			return fmt.Errorf("traffic.max_inflight.%s must be an integer", key.Value)
		}
		parsed, err := strconv.ParseUint(value.Value, 0, 32)
		if err != nil {
			return fmt.Errorf("traffic.max_inflight.%s has invalid value: %w", key.Value, err)
		}
		*target = uint32(parsed)
	}
	return nil
}

// yaml.v3 does not call a value's UnmarshalYAML method for an explicit null.
// Inspect the owned top-level field before decoding so null cannot alias an
// omitted traffic policy.
func validateProxyTrafficYAML(raw []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return err
	}
	if len(document.Content) == 0 {
		return nil
	}
	root := document.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value == "traffic" && root.Content[i+1].Tag == "!!null" {
			return fmt.Errorf("proxy traffic must not be null")
		}
	}
	return nil
}

func requireYAMLMapping(path string, node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode || node.Tag == "!!null" {
		return fmt.Errorf("%s must be an object", path)
	}
	return nil
}

// MarshalJSON is declared explicitly so MaxInflight's strict decoder does not
// affect its stable effective-config representation.
func (m MaxInflight) MarshalJSON() ([]byte, error) {
	type wire MaxInflight
	return json.Marshal(wire(m))
}
