package sandboxcfg

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// NsIdentity is a request-scoped standalone Create input, never a portable
// default or a second durable source of sandbox identity.
const NsIdentity = "kuasar-sandbox.identity"

// Identity selects a node-local ID and optionally an independent stable ID.
// Empty fields are unspecified. Allocation and fallback belong to the core.
type Identity struct {
	ID       string `json:"id,omitempty"`
	StableID string `json:"stable_id,omitempty"`
}

// ParseIdentity accepts exactly the owned, case-sensitive JSON field names.
// Both new Create inputs use the existing local sandbox ID format; this does
// not redefine the broader historical migration-token StableID contract.
func ParseIdentity(raw string) (Identity, error) {
	var identity Identity
	trimmed := strings.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return identity, fmt.Errorf("sandboxcfg: metadata[%q] must be a JSON object", NsIdentity)
	}
	var fields map[string]json.RawMessage
	if err := strictjson.Decode([]byte(trimmed), &fields); err != nil {
		return identity, fmt.Errorf("sandboxcfg: metadata[%q]: %w", NsIdentity, err)
	}
	for name, rawValue := range fields {
		var value *string
		switch name {
		case "id":
			value = &identity.ID
		case "stable_id":
			value = &identity.StableID
		default:
			return Identity{}, fmt.Errorf("sandboxcfg: metadata[%q] contains unknown field %q", NsIdentity, name)
		}
		if err := json.Unmarshal(rawValue, value); err != nil {
			return Identity{}, fmt.Errorf("sandboxcfg: metadata[%q].%s must be a string", NsIdentity, name)
		}
		if *value != "" && !types.ValidLocalSandboxID(*value) {
			return Identity{}, fmt.Errorf("sandboxcfg: metadata[%q].%s must be a 1..57-byte lowercase sandbox ID", NsIdentity, name)
		}
	}
	return identity, nil
}

// ExtractIdentity strictly parses and removes the input namespace without
// changing the caller's map. The selected identity is materialized into the
// Sandbox's dedicated fields before credential generation or launch.
func ExtractIdentity(meta map[string]string) (identity Identity, cleaned map[string]string, err error) {
	raw, present := meta[NsIdentity]
	if !present {
		return Identity{}, meta, nil
	}
	identity, err = ParseIdentity(raw)
	if err != nil {
		return Identity{}, nil, err
	}
	cleaned = make(map[string]string, len(meta)-1)
	for key, value := range meta {
		if key != NsIdentity {
			cleaned[key] = value
		}
	}
	return identity, cleaned, nil
}
