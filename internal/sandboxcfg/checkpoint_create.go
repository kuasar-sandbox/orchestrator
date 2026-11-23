package sandboxcfg

import (
	"encoding/json"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// CreateCheckpointPolicy adds the retained-checkpoint quota to the snapshot
// defaults accepted during Create. The quota is extracted into typed instance
// state; it is not part of a Pause action or a Build snapshot policy.
type CreateCheckpointPolicy struct {
	SnapshotPolicy
	MaxCheckpoints *int `json:"max_checkpoints,omitempty"`
}

func (p CreateCheckpointPolicy) Empty() bool {
	return p.SnapshotPolicy.Empty() && p.MaxCheckpoints == nil
}

func CloneCreateCheckpointPolicy(p CreateCheckpointPolicy) CreateCheckpointPolicy {
	out := CreateCheckpointPolicy{SnapshotPolicy: CloneSnapshotPolicy(p.SnapshotPolicy)}
	if p.MaxCheckpoints != nil {
		value := *p.MaxCheckpoints
		out.MaxCheckpoints = &value
	}
	return out
}

// OverlayCreateCheckpointPolicy applies non-null fields from higher to base.
// Missing and null values inherit; neither source shares mutable pointers with
// the result.
func OverlayCreateCheckpointPolicy(base, higher CreateCheckpointPolicy) CreateCheckpointPolicy {
	out := CloneCreateCheckpointPolicy(base)
	out.SnapshotPolicy = OverlaySnapshotPolicy(base.SnapshotPolicy, higher.SnapshotPolicy)
	if higher.MaxCheckpoints != nil {
		value := *higher.MaxCheckpoints
		out.MaxCheckpoints = &value
	}
	return out
}

// ParseCreateCheckpointPolicyJSON uses the same strict object and snapshot-field
// validation as action policies, additionally accepting a positive integer quota.
// A missing or null quota remains unset until instance admission applies default 1.
func ParseCreateCheckpointPolicyJSON(raw string) (CreateCheckpointPolicy, error) {
	object, err := parseCheckpointPolicyObject(raw)
	if err != nil {
		return CreateCheckpointPolicy{}, err
	}
	var policy CreateCheckpointPolicy
	if value, present := object["max_checkpoints"]; present {
		if err := json.Unmarshal(value, &policy.MaxCheckpoints); err != nil {
			return CreateCheckpointPolicy{}, fmt.Errorf("checkpoint policy field %q must be a positive integer or null: %w", "max_checkpoints", err)
		}
		if _, err := types.ResolveMaxCheckpoints(policy.MaxCheckpoints); err != nil {
			return CreateCheckpointPolicy{}, err
		}
		delete(object, "max_checkpoints")
	}
	policy.SnapshotPolicy, err = parseSnapshotPolicyFields(object)
	if err != nil {
		return CreateCheckpointPolicy{}, err
	}
	return policy, nil
}

func MarshalCreateCheckpointPolicyJSON(policy CreateCheckpointPolicy) (string, error) {
	body, err := json.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("marshal Create checkpoint policy: %w", err)
	}
	return string(body), nil
}

// ExtractCreateCheckpointPolicy removes the quota from the metadata carrier,
// preserving only canonical snapshot defaults for subsequent Pause operations.
// The caller owns quota admission and persistence; neither input map nor values
// are changed.
func ExtractCreateCheckpointPolicy(meta map[string]string) (CreateCheckpointPolicy, map[string]string, error) {
	out := cloneMetadata(meta)
	raw, present := meta[NsCheckpoint]
	if !present {
		return CreateCheckpointPolicy{}, out, nil
	}
	policy, err := ParseCreateCheckpointPolicyJSON(raw)
	if err != nil {
		return CreateCheckpointPolicy{}, nil, fmt.Errorf("sandboxcfg: metadata[%q]: %w", NsCheckpoint, err)
	}
	if policy.SnapshotPolicy.Empty() {
		delete(out, NsCheckpoint)
		return policy, out, nil
	}
	canonical, err := MarshalSnapshotPolicyJSON(policy.SnapshotPolicy)
	if err != nil {
		return CreateCheckpointPolicy{}, nil, err
	}
	out[NsCheckpoint] = canonical
	return policy, out, nil
}
