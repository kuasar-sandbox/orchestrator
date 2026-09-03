package buildcfg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const NsBuilder = "kuasar-sandbox.builder"

// Extract removes the build-only builder namespace from meta and decodes it as
// BuildOptions. The returned metadata is safe to persist as template sandbox
// defaults.
func Extract(meta map[string]string) (map[string]string, types.BuildOptions, error) {
	rawValue, present := meta[NsBuilder]
	if !present {
		return meta, types.BuildOptions{}, nil
	}
	raw := strings.TrimSpace(rawValue)
	if raw == "" {
		return nil, types.BuildOptions{}, fmt.Errorf("buildcfg: metadata[%q] must be a non-empty JSON object", NsBuilder)
	}
	clean := make(map[string]string, len(meta))
	for k, v := range meta {
		if k != NsBuilder {
			clean[k] = v
		}
	}
	opts, err := parseOptions([]byte(raw))
	if err != nil {
		return nil, types.BuildOptions{}, fmt.Errorf("buildcfg: metadata[%q] is not valid JSON: %w", NsBuilder, err)
	}
	return clean, opts, nil
}

// Marshal returns canonical JSON for build-only options. An empty definition
// is encoded as an empty object so cluster transport never depends on original
// request whitespace or key ordering.
func Marshal(opts types.BuildOptions) (string, error) {
	raw, err := json.Marshal(opts)
	if err != nil {
		return "", fmt.Errorf("buildcfg: marshal: %w", err)
	}
	return string(raw), nil
}

type optionsInput struct {
	Target    json.RawMessage `json:"target"`
	Resources json.RawMessage `json:"resources"`
	Referer   json.RawMessage `json:"referer"`
	Registry  json.RawMessage `json:"registry"`
}

func parseOptions(raw []byte) (types.BuildOptions, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return types.BuildOptions{}, fmt.Errorf("builder configuration must not be null")
	}
	var input optionsInput
	if err := strictjson.Decode(raw, &input); err != nil {
		return types.BuildOptions{}, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return types.BuildOptions{}, fmt.Errorf("builder configuration must be a JSON object")
	}
	var out types.BuildOptions
	if len(input.Target) != 0 {
		if bytes.Equal(bytes.TrimSpace(input.Target), []byte("null")) {
			return types.BuildOptions{}, fmt.Errorf("%s.target must not be null", NsBuilder)
		}
		var target types.BuildTarget
		if err := strictjson.Decode(input.Target, &target); err != nil {
			return types.BuildOptions{}, fmt.Errorf("%s.target: %w", NsBuilder, err)
		}
		if err := target.Validate(); err != nil {
			return types.BuildOptions{}, fmt.Errorf("%s.target: %w", NsBuilder, err)
		}
		out.Target = &target
	}
	if len(input.Resources) != 0 {
		patch, err := ParseResourceObject(NsBuilder+".resources", input.Resources)
		if err != nil {
			return types.BuildOptions{}, err
		}
		resources := types.BuildResources{}
		if patch.CPU != nil {
			resources.CPU = *patch.CPU
		}
		if patch.Memory != nil {
			resources.Memory = *patch.Memory
		}
		if patch.Storage != nil {
			resources.Storage = *patch.Storage
		}
		out.Resources = &resources
	}
	if len(input.Referer) != 0 {
		if bytes.Equal(bytes.TrimSpace(input.Referer), []byte("null")) {
			return types.BuildOptions{}, fmt.Errorf("%s.referer must not be null", NsBuilder)
		}
		var referer types.BuildRefererOptions
		if err := strictjson.Decode(input.Referer, &referer); err != nil {
			return types.BuildOptions{}, fmt.Errorf("%s.referer: %w", NsBuilder, err)
		}
		out.Referer = &referer
	}
	if len(input.Registry) != 0 {
		if bytes.Equal(bytes.TrimSpace(input.Registry), []byte("null")) {
			return types.BuildOptions{}, fmt.Errorf("%s.registry must not be null", NsBuilder)
		}
		var registry types.BuildRegistryOptions
		if err := strictjson.Decode(input.Registry, &registry); err != nil {
			return types.BuildOptions{}, fmt.Errorf("%s.registry: %w", NsBuilder, err)
		}
		out.Registry = &registry
	}
	return out, nil
}
