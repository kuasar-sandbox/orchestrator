package orch

import "testing"

func TestParseOverrides(t *testing.T) {
	// absent / unrelated metadata => zero value, no error.
	if ov, err := parseOverrides(nil); err != nil || ov.Hostname != "" || ov.InnerIP != "" {
		t.Fatalf("absent: %+v %v", ov, err)
	}
	if ov, err := parseOverrides(map[string]string{"foo": "bar"}); err != nil || ov.Hostname != "" {
		t.Fatalf("unrelated key: %+v %v", ov, err)
	}

	// full valid override round-trips.
	js := `{"hostname":"h1","dns":["1.1.1.1","8.8.8.8"],"inner_ip":"10.0.0.5/30",` +
		`"nexthop":"10.0.0.4","transit_gateway_ip":"172.16.0.1","transit_geneve_vni":4242,"transit_mac":"02:00:00:00:00:09"}`
	ov, err := parseOverrides(map[string]string{overrideMetaKey: js})
	if err != nil {
		t.Fatalf("valid: %v", err)
	}
	if ov.Hostname != "h1" || len(ov.DNS) != 2 || ov.InnerIP != "10.0.0.5/30" || ov.Nexthop != "10.0.0.4" ||
		ov.TransitGatewayIP != "172.16.0.1" || ov.TransitGeneveVNI != 4242 || ov.TransitMAC != "02:00:00:00:00:09" {
		t.Fatalf("parsed wrong: %+v", ov)
	}

	// invalid field formats are rejected (no allow-list gate — only format validation).
	for name, bad := range map[string]string{
		"bad-json":    `{not json`,
		"bad-cidr":    `{"inner_ip":"10.0.0.5"}`, // a plain IP, not a CIDR
		"bad-nexthop": `{"nexthop":"nope"}`,
		"bad-gateway": `{"transit_gateway_ip":"999.1.1.1"}`,
		"bad-mac":     `{"transit_mac":"zz:zz"}`,
	} {
		if _, err := parseOverrides(map[string]string{overrideMetaKey: bad}); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}
