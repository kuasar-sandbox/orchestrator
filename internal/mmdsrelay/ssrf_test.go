package mmdsrelay

import (
	"net"
	"testing"
)

func TestDisallowedIP(t *testing.T) {
	cases := []struct {
		name    string
		ip      string
		blocked bool
	}{
		{"loopback v4", "127.0.0.1", true},
		{"loopback v6", "::1", true},
		{"rfc1918 10", "10.0.0.1", true},
		{"rfc1918 172.16", "172.16.5.1", true},
		{"rfc1918 192.168", "192.168.1.1", true},
		{"link-local incl metadata", "169.254.169.254", true},
		{"link-local generic", "169.254.1.1", true},
		{"cgnat", "100.64.0.1", true},
		{"documentation test-net-1", "192.0.2.1", true},
		{"documentation test-net-2", "198.51.100.1", true},
		{"documentation test-net-3", "203.0.113.1", true},
		{"benchmarking", "198.18.0.1", true},
		{"multicast", "224.0.0.1", true},
		{"unspecified v4", "0.0.0.0", true},
		{"unspecified v6", "::", true},
		{"unique-local v6", "fc00::1", true}, // net.IP.IsPrivate covers fc00::/7
		{"link-local v6", "fe80::1", true},
		{"public v4 a", "8.8.8.8", false},
		{"public v4 b", "1.1.1.1", false},
		{"public v4 c", "93.184.216.34", false},
		{"public v6", "2606:4700:4700::1111", false},
		{"ipv4-mapped loopback", "::ffff:127.0.0.1", true},
		{"ipv4-mapped private", "::ffff:10.0.0.1", true},
		{"ipv4-mapped public", "::ffff:8.8.8.8", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("test bug: %q does not parse as an IP", tc.ip)
			}
			reason, blocked := disallowedIP(ip)
			if blocked != tc.blocked {
				t.Fatalf("disallowedIP(%s) = (%q, %t), want blocked=%t", tc.ip, reason, blocked, tc.blocked)
			}
			if blocked && reason == "" {
				t.Fatalf("disallowedIP(%s) blocked with no reason", tc.ip)
			}
		})
	}
}

func TestDisallowedIPNil(t *testing.T) {
	if _, blocked := disallowedIP(nil); !blocked {
		t.Fatal("disallowedIP(nil) should be blocked")
	}
}
