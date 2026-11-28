package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// SDKs can share one header set between Register and Trigger. Only these two
// non-secret definitions may be repeated, as assertions against the registered
// row. All other configuration remains registration-only.
func parseBuildTriggerAssertions(h http.Header) (*sandboxcfg.NetworkSpec, *types.BuildTarget, error) {
	var network *sandboxcfg.NetworkSpec
	if raw, err := singleOptionalHeader(h, "X-Kuasar-Sandbox-Network"); err != nil {
		return nil, nil, err
	} else if raw != nil {
		// Validate the assertion's complete owned schema before reusing the
		// registration parser's network-format validation. Unknown fields,
		// duplicate keys and null cannot disappear into an equal zero value.
		var input sandboxcfg.NetworkSpec
		if err := strictjson.Decode([]byte(*raw), &input); err != nil {
			return nil, nil, fmt.Errorf("trigger network assertion: %w", err)
		}
		// encoding/json accepts case-insensitive struct field names. Reject
		// those aliases explicitly: the registration YAML parser has different
		// field-name rules and must not silently drop an asserted input.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(*raw), &fields); err != nil {
			return nil, nil, fmt.Errorf("trigger network assertion: %w", err)
		}
		for name := range fields {
			switch name {
			case "hostname", "dns", "inner_ip", "nexthop", "transit_gateway_ip", "transit_geneve_vni", "transit_mac":
			default:
				return nil, nil, fmt.Errorf("trigger network assertion: unknown field %q", name)
			}
		}
		canonical, err := json.Marshal(input)
		if err != nil {
			return nil, nil, fmt.Errorf("trigger network assertion: %w", err)
		}
		if _, err := sandboxcfg.ParseSpec(map[string]string{sandboxcfg.NsNetwork: string(canonical)}); err != nil {
			return nil, nil, fmt.Errorf("trigger network assertion: %w", err)
		}
		network = &input
	}
	var target *types.BuildTarget
	if raw, err := singleOptionalHeader(h, builderHeader); err != nil {
		return nil, nil, err
	} else if raw != nil {
		_, options, err := buildcfg.Extract(map[string]string{buildcfg.NsBuilder: *raw})
		if err != nil {
			return nil, nil, err
		}
		if options.Target == nil || options.Resources != nil || options.Referer != nil || options.Registry != nil {
			return nil, nil, fmt.Errorf("trigger-time builder configuration is deprecated; only a repeated registered target is accepted")
		}
		// Require canonical keys so case aliases cannot replace an earlier
		// assertion value during encoding/json's struct decoding.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(*raw), &fields); err != nil {
			return nil, nil, fmt.Errorf("trigger builder assertion: %w", err)
		}
		for name := range fields {
			if name != "target" {
				return nil, nil, fmt.Errorf("trigger builder assertion: unknown field %q", name)
			}
		}
		var targetFields map[string]json.RawMessage
		if err := json.Unmarshal(fields["target"], &targetFields); err != nil {
			return nil, nil, fmt.Errorf("trigger target assertion: %w", err)
		}
		for name := range targetFields {
			if name != "kind" && name != "memory" {
				return nil, nil, fmt.Errorf("trigger target assertion: unknown field %q", name)
			}
		}
		target = options.Target
	}
	return network, target, nil
}
