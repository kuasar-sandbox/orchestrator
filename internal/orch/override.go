package orch

// Per-instance sandbox config overrides. A tenant supplies them on create via the
// reserved metadata key "kuasar-sandbox/config" whose value is a JSON object — no
// e2b SDK/API change needed (metadata values are strings). The sandbox API is the
// full-capability surface (the platform itself manages sandboxes through it), so
// there is no allow-list gate: any field a sandbox can carry is overridable. Values
// are format-validated and re-parsed on resume (metadata is persisted), so an
// override applies identically across the sandbox's life.

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
)

// overrideMetaKey is the reserved metadata key carrying the override JSON.
const overrideMetaKey = "kuasar-sandbox/config"

// sandboxOverride is the decoded "kuasar-sandbox/config" metadata value. Empty
// fields fall back to the orchestrator/profile defaults.
type sandboxOverride struct {
	Hostname         string   `json:"hostname,omitempty"`
	DNS              []string `json:"dns,omitempty"`
	InnerIP          string   `json:"inner_ip,omitempty"`           // CIDR
	Nexthop          string   `json:"nexthop,omitempty"`            // default-route gateway
	TransitGatewayIP string   `json:"transit_gateway_ip,omitempty"` // GENEVE gateway
	TransitGeneveVNI uint32   `json:"transit_geneve_vni,omitempty"` // GENEVE VNI
	TransitMAC       string   `json:"transit_mac,omitempty"`        // transit dest MAC
}

// parseOverrides decodes meta["kuasar-sandbox/config"] (absent => zero value, all
// defaults) and validates the field formats.
func parseOverrides(meta map[string]string) (sandboxOverride, error) {
	var ov sandboxOverride
	raw, ok := meta[overrideMetaKey]
	if !ok || strings.TrimSpace(raw) == "" {
		return ov, nil
	}
	if err := json.Unmarshal([]byte(raw), &ov); err != nil {
		return ov, fmt.Errorf("orch: metadata[%q] is not valid JSON: %w", overrideMetaKey, err)
	}
	if err := ov.validate(); err != nil {
		return ov, err
	}
	return ov, nil
}

func (ov sandboxOverride) validate() error {
	if ov.InnerIP != "" {
		if _, _, err := net.ParseCIDR(ov.InnerIP); err != nil {
			return fmt.Errorf("orch: override inner_ip %q is not a CIDR: %w", ov.InnerIP, err)
		}
	}
	if ov.Nexthop != "" && net.ParseIP(ov.Nexthop) == nil {
		return fmt.Errorf("orch: override nexthop %q is not an IP", ov.Nexthop)
	}
	if ov.TransitGatewayIP != "" && net.ParseIP(ov.TransitGatewayIP) == nil {
		return fmt.Errorf("orch: override transit_gateway_ip %q is not an IP", ov.TransitGatewayIP)
	}
	if ov.TransitMAC != "" {
		if _, err := net.ParseMAC(ov.TransitMAC); err != nil {
			return fmt.Errorf("orch: override transit_mac %q is not a MAC: %w", ov.TransitMAC, err)
		}
	}
	return nil
}

// firstNonEmpty returns a if non-empty, else b (override-over-default helper).
func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
