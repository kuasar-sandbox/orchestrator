package sandboxcfg

import (
	"encoding/json"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const trafficFieldPath = NsTraffic

// TrafficPatch is the portable, presence-aware Sandbox traffic policy. Every
// leaf remains a pointer so explicit zero can clear a lower-priority or target
// node default without materializing omitted defaults in durable metadata.
type TrafficPatch struct {
	MaxInflight *MaxInflightPatch `json:"max_inflight,omitempty" yaml:"max_inflight,omitempty"`
}

// MaxInflightPatch carries only explicitly supplied service leaves.
type MaxInflightPatch struct {
	Total              *uint32 `json:"total,omitempty" yaml:"total,omitempty"`
	Forward            *uint32 `json:"forward,omitempty" yaml:"forward,omitempty"`
	E2BEnvd            *uint32 `json:"e2b:envd,omitempty" yaml:"e2b:envd,omitempty"`
	E2BCodeInterpreter *uint32 `json:"e2b:code-interpreter,omitempty" yaml:"e2b:code-interpreter,omitempty"`
	Exec               *uint32 `json:"exec,omitempty" yaml:"exec,omitempty"`
}

// ParseTrafficPatch strictly parses one portable traffic patch.
func ParseTrafficPatch(raw string) (TrafficPatch, error) {
	var patch TrafficPatch
	if err := rejectDuplicateJSONKeys([]byte(raw)); err != nil {
		return patch, fmt.Errorf("%s must be a valid JSON object: %w", trafficFieldPath, err)
	}
	object, err := decodeJSONObject(trafficFieldPath, []byte(raw))
	if err != nil {
		return patch, err
	}
	for field, value := range object {
		switch field {
		case "max_inflight":
			parsed, err := parseMaxInflightPatch(trafficFieldPath+".max_inflight", value)
			if err != nil {
				return patch, err
			}
			patch.MaxInflight = parsed
		default:
			return patch, fmt.Errorf("%s contains unknown field %q", trafficFieldPath, field)
		}
	}
	return patch, nil
}

func parseMaxInflightPatch(path string, raw json.RawMessage) (*MaxInflightPatch, error) {
	object, err := decodeJSONObject(path, raw)
	if err != nil {
		return nil, err
	}
	out := &MaxInflightPatch{}
	for field, value := range object {
		fieldPath := path + "." + field
		var target **uint32
		switch field {
		case "total":
			target = &out.Total
		case "forward":
			target = &out.Forward
		case "e2b:envd":
			target = &out.E2BEnvd
		case "e2b:code-interpreter":
			target = &out.E2BCodeInterpreter
		case "exec":
			target = &out.Exec
		default:
			return nil, fmt.Errorf("%s contains unknown field %q", path, field)
		}
		var parsed uint32
		if err := decodeLeaf(fieldPath, value, &parsed); err != nil {
			return nil, err
		}
		*target = uint32Ptr(parsed)
	}
	return out, nil
}

// MarshalTrafficPatch returns the stable compact representation stored in
// metadata and carried unchanged by migration tokens.
func MarshalTrafficPatch(patch TrafficPatch) (string, error) {
	raw, err := json.Marshal(patch)
	if err != nil {
		return "", fmt.Errorf("marshal %s: %w", trafficFieldPath, err)
	}
	return string(raw), nil
}

// MergeTrafficPatch overlays only explicitly present leaves.
func MergeTrafficPatch(base, over TrafficPatch) TrafficPatch {
	out := cloneTrafficPatch(base)
	if over.MaxInflight == nil {
		return out
	}
	if out.MaxInflight == nil {
		out.MaxInflight = &MaxInflightPatch{}
	}
	mergeUint32 := func(dst **uint32, src *uint32) {
		if src != nil {
			*dst = uint32Ptr(*src)
		}
	}
	mergeUint32(&out.MaxInflight.Total, over.MaxInflight.Total)
	mergeUint32(&out.MaxInflight.Forward, over.MaxInflight.Forward)
	mergeUint32(&out.MaxInflight.E2BEnvd, over.MaxInflight.E2BEnvd)
	mergeUint32(&out.MaxInflight.E2BCodeInterpreter, over.MaxInflight.E2BCodeInterpreter)
	mergeUint32(&out.MaxInflight.Exec, over.MaxInflight.Exec)
	return out
}

// NormalizeTrafficMetadata validates and canonicalizes only the traffic
// namespace. An absent namespace remains absent.
func NormalizeTrafficMetadata(meta map[string]string) (map[string]string, error) {
	raw, present := meta[NsTraffic]
	if !present {
		return meta, nil
	}
	patch, err := ParseTrafficPatch(raw)
	if err != nil {
		return nil, err
	}
	canonical, err := MarshalTrafficPatch(patch)
	if err != nil {
		return nil, err
	}
	out := cloneStringMap(meta)
	out[NsTraffic] = canonical
	return out, nil
}

// ValidateTrafficForProfile rejects E2B-only service declarations on bare
// Sandboxes. Node defaults may contain every service; filtering those defaults
// is owned by the target Proxy master and is not checked here.
func ValidateTrafficForProfile(profile types.Profile, patch TrafficPatch) error {
	if profile != types.ProfileBare || patch.MaxInflight == nil {
		return nil
	}
	if patch.MaxInflight.E2BEnvd != nil {
		return fmt.Errorf("%s.max_inflight.e2b:envd is not applicable to bare profile", trafficFieldPath)
	}
	if patch.MaxInflight.E2BCodeInterpreter != nil {
		return fmt.Errorf("%s.max_inflight.e2b:code-interpreter is not applicable to bare profile", trafficFieldPath)
	}
	return nil
}

func cloneTrafficPatch(in TrafficPatch) TrafficPatch {
	if in.MaxInflight == nil {
		return TrafficPatch{}
	}
	copyPtr := func(in *uint32) *uint32 {
		if in == nil {
			return nil
		}
		return uint32Ptr(*in)
	}
	return TrafficPatch{MaxInflight: &MaxInflightPatch{
		Total:              copyPtr(in.MaxInflight.Total),
		Forward:            copyPtr(in.MaxInflight.Forward),
		E2BEnvd:            copyPtr(in.MaxInflight.E2BEnvd),
		E2BCodeInterpreter: copyPtr(in.MaxInflight.E2BCodeInterpreter),
		Exec:               copyPtr(in.MaxInflight.Exec),
	}}
}

func uint32Ptr(value uint32) *uint32 { return &value }
