package sandboxcfg

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// CheckpointPolicy controls one local checkpoint capture. Nil means that the
// current source does not specify the field, so a lower-priority source (or the
// sandbox-ctl default) remains in effect.
type CheckpointPolicy struct {
	MergeRef   *bool `json:"merge_ref,omitempty"`
	DropCaches *bool `json:"drop_caches,omitempty"`
	// Memory requests a disk-only capture when false (sandboxer#120). While
	// node runtime support is absent, an explicit Pause fails fast and the
	// reaper downgrades to a memory-bearing capture.
	Memory *bool `json:"memory,omitempty"`
}

// Empty reports whether neither checkpoint field is specified.
func (p CheckpointPolicy) Empty() bool {
	return p.MergeRef == nil && p.DropCaches == nil && p.Memory == nil
}

// CloneBool returns an independent copy of v.
func CloneBool(v *bool) *bool {
	if v == nil {
		return nil
	}
	clone := *v
	return &clone
}

// CloneCheckpointPolicy returns a policy without sharing mutable bool pointers.
func CloneCheckpointPolicy(p CheckpointPolicy) CheckpointPolicy {
	return CheckpointPolicy{
		MergeRef:   CloneBool(p.MergeRef),
		DropCaches: CloneBool(p.DropCaches),
		Memory:     CloneBool(p.Memory),
	}
}

// OverlayCheckpointPolicy overlays higher onto base one field at a time. Nil
// fields in higher inherit base; concrete true and false values replace it.
func OverlayCheckpointPolicy(base, higher CheckpointPolicy) CheckpointPolicy {
	out := CloneCheckpointPolicy(base)
	if higher.MergeRef != nil {
		out.MergeRef = CloneBool(higher.MergeRef)
	}
	if higher.DropCaches != nil {
		out.DropCaches = CloneBool(higher.DropCaches)
	}
	if higher.Memory != nil {
		out.Memory = CloneBool(higher.Memory)
	}
	return out
}

// ParseCheckpointPolicyJSON strictly parses the checkpoint metadata/header
// representation. The top level must be exactly one object containing only
// merge_ref, drop_caches, and memory, whose values may be true, false, or null.
func ParseCheckpointPolicyJSON(raw string) (CheckpointPolicy, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed[0] != '{' {
		return CheckpointPolicy{}, fmt.Errorf("checkpoint policy must be a JSON object")
	}

	dec := json.NewDecoder(strings.NewReader(trimmed))
	var object map[string]json.RawMessage
	if err := dec.Decode(&object); err != nil {
		return CheckpointPolicy{}, fmt.Errorf("invalid checkpoint policy: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return CheckpointPolicy{}, fmt.Errorf("checkpoint policy must contain exactly one JSON object")
		}
		return CheckpointPolicy{}, fmt.Errorf("invalid trailing checkpoint policy data: %w", err)
	}

	var policy CheckpointPolicy
	for field, value := range object {
		var target **bool
		switch field {
		case "merge_ref":
			target = &policy.MergeRef
		case "drop_caches":
			target = &policy.DropCaches
		case "memory":
			target = &policy.Memory
		default:
			return CheckpointPolicy{}, fmt.Errorf("checkpoint policy contains unknown field %q", field)
		}
		if err := json.Unmarshal(value, target); err != nil {
			return CheckpointPolicy{}, fmt.Errorf("checkpoint policy field %q must be true, false, or null: %w", field, err)
		}
	}
	return policy, nil
}

// MarshalCheckpointPolicyJSON returns the canonical checkpoint JSON. Nil
// fields are omitted, so an empty policy marshals as {}.
func MarshalCheckpointPolicyJSON(policy CheckpointPolicy) (string, error) {
	body, err := json.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("marshal checkpoint policy: %w", err)
	}
	return string(body), nil
}

// NormalizeCheckpointMetadata validates and canonicalizes the host-only
// checkpoint namespace without mutating the input. An empty policy removes the
// namespace entirely.
func NormalizeCheckpointMetadata(meta map[string]string) (map[string]string, error) {
	out := cloneMetadata(meta)
	raw, ok := meta[NsCheckpoint]
	if !ok {
		return out, nil
	}
	policy, err := ParseCheckpointPolicyJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("sandboxcfg: metadata[%q]: %w", NsCheckpoint, err)
	}
	if policy.Empty() {
		delete(out, NsCheckpoint)
		return out, nil
	}
	canonical, err := MarshalCheckpointPolicyJSON(policy)
	if err != nil {
		return nil, fmt.Errorf("sandboxcfg: metadata[%q]: %w", NsCheckpoint, err)
	}
	out[NsCheckpoint] = canonical
	return out, nil
}

func cloneMetadata(meta map[string]string) map[string]string {
	if meta == nil {
		return nil
	}
	out := make(map[string]string, len(meta))
	for key, value := range meta {
		out[key] = value
	}
	return out
}
