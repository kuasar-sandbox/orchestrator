package buildcfg

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"gopkg.in/yaml.v3"
)

const NsBuilder = "kuasar-sandbox.builder"

// Extract removes the build-only builder namespace from meta and decodes it as
// BuildOptions. The returned metadata is safe to persist as template sandbox
// defaults.
func Extract(meta map[string]string) (map[string]string, types.BuildOptions, error) {
	raw := strings.TrimSpace(meta[NsBuilder])
	if raw == "" {
		return meta, types.BuildOptions{}, nil
	}
	clean := make(map[string]string, len(meta))
	for k, v := range meta {
		if k != NsBuilder {
			clean[k] = v
		}
	}
	var opts types.BuildOptions
	dec := yaml.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&opts); err != nil {
		return nil, types.BuildOptions{}, fmt.Errorf("buildcfg: metadata[%q] is not valid JSON: %w", NsBuilder, err)
	}
	return clean, opts, nil
}

// Merge overlays trigger-time options over register-time options. Nil pointers
// mean "not specified", so only explicit trigger fields override.
func Merge(base, over types.BuildOptions) types.BuildOptions {
	out := clone(base)
	if over.Referer == nil {
		return out
	}
	if out.Referer == nil {
		out.Referer = &types.BuildRefererOptions{}
	}
	if over.Referer.Enabled != nil {
		out.Referer.Enabled = boolPtr(*over.Referer.Enabled)
	}
	if over.Referer.Writeback != nil {
		out.Referer.Writeback = boolPtr(*over.Referer.Writeback)
	}
	return out
}

func clone(in types.BuildOptions) types.BuildOptions {
	var out types.BuildOptions
	if in.Referer != nil {
		out.Referer = &types.BuildRefererOptions{}
		if in.Referer.Enabled != nil {
			out.Referer.Enabled = boolPtr(*in.Referer.Enabled)
		}
		if in.Referer.Writeback != nil {
			out.Referer.Writeback = boolPtr(*in.Referer.Writeback)
		}
	}
	return out
}

func boolPtr(v bool) *bool { return &v }
