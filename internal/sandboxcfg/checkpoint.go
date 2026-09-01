package sandboxcfg

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// SnapshotPolicy controls one local checkpoint capture. Nil means that the
// current source does not specify the field, so a lower-priority source (or the
// sandbox-ctl default) remains in effect.
type SnapshotPolicy struct {
	MergeRef   *bool `json:"merge_ref,omitempty"`
	DropCaches *bool `json:"drop_caches,omitempty"`
}

type CaptureRequest struct {
	Kind           types.CaptureKind
	SnapshotPolicy SnapshotPolicy
}

type CaptureResult struct {
	Source types.ResumeSource
}

// Empty reports whether neither checkpoint field is specified.
func (p SnapshotPolicy) Empty() bool {
	return p.MergeRef == nil && p.DropCaches == nil
}

// CloneBool returns an independent copy of v.
func CloneBool(v *bool) *bool {
	if v == nil {
		return nil
	}
	clone := *v
	return &clone
}

// CloneSnapshotPolicy returns a policy without sharing mutable bool pointers.
func CloneSnapshotPolicy(p SnapshotPolicy) SnapshotPolicy {
	return SnapshotPolicy{
		MergeRef:   CloneBool(p.MergeRef),
		DropCaches: CloneBool(p.DropCaches),
	}
}

// OverlaySnapshotPolicy overlays higher onto base one field at a time. Nil
// fields in higher inherit base; concrete true and false values replace it.
func OverlaySnapshotPolicy(base, higher SnapshotPolicy) SnapshotPolicy {
	out := CloneSnapshotPolicy(base)
	if higher.MergeRef != nil {
		out.MergeRef = CloneBool(higher.MergeRef)
	}
	if higher.DropCaches != nil {
		out.DropCaches = CloneBool(higher.DropCaches)
	}
	return out
}

// ParseSnapshotPolicyJSON strictly parses the checkpoint metadata/header
// representation. The top level must be exactly one object containing only
// merge_ref and drop_caches, whose values may be true, false, or null.
func ParseSnapshotPolicyJSON(raw string) (SnapshotPolicy, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed[0] != '{' {
		return SnapshotPolicy{}, fmt.Errorf("checkpoint policy must be a JSON object")
	}

	if err := strictjson.RejectDuplicateKeys([]byte(trimmed)); err != nil {
		return SnapshotPolicy{}, fmt.Errorf("invalid checkpoint policy: %w", err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &object); err != nil {
		return SnapshotPolicy{}, fmt.Errorf("invalid checkpoint policy: %w", err)
	}

	var policy SnapshotPolicy
	for field, value := range object {
		var target **bool
		switch field {
		case "merge_ref":
			target = &policy.MergeRef
		case "drop_caches":
			target = &policy.DropCaches
		default:
			return SnapshotPolicy{}, fmt.Errorf("checkpoint policy contains unknown field %q", field)
		}
		if err := json.Unmarshal(value, target); err != nil {
			return SnapshotPolicy{}, fmt.Errorf("checkpoint policy field %q must be true, false, or null: %w", field, err)
		}
	}
	return policy, nil
}

// MarshalSnapshotPolicyJSON returns the canonical checkpoint JSON. Nil
// fields are omitted, so an empty policy marshals as {}.
func MarshalSnapshotPolicyJSON(policy SnapshotPolicy) (string, error) {
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
	policy, err := ParseSnapshotPolicyJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("sandboxcfg: metadata[%q]: %w", NsCheckpoint, err)
	}
	if policy.Empty() {
		delete(out, NsCheckpoint)
		return out, nil
	}
	canonical, err := MarshalSnapshotPolicyJSON(policy)
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
