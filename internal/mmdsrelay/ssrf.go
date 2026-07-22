package mmdsrelay

import "net"

// blockedCIDRs covers ranges Go's stdlib net.IP predicates (IsLoopback,
// IsPrivate, IsMulticast, IsLinkLocalUnicast, IsLinkLocalMulticast,
// IsUnspecified) do NOT classify: carrier-grade NAT, the three RFC 5737
// documentation ranges, and the RFC 2544 benchmarking range. 169.254.0.0/16
// (which covers the cloud metadata address 169.254.169.254) is already
// caught by IsLinkLocalUnicast, but it must always be rejected, so it is
// also listed here as a second, independent check — belt-and-suspenders
// against a future stdlib behavior change.
var blockedCIDRs = mustParseCIDRs(
	"100.64.0.0/10",   // CGNAT (RFC 6598)
	"192.0.2.0/24",    // TEST-NET-1 (RFC 5737)
	"198.51.100.0/24", // TEST-NET-2 (RFC 5737)
	"203.0.113.0/24",  // TEST-NET-3 (RFC 5737)
	"198.18.0.0/15",   // benchmarking (RFC 2544)
	"169.254.0.0/16",  // link-local, incl. the cloud metadata address
)

func mustParseCIDRs(cidrs ...string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic("mmdsrelay: bad CIDR literal " + c + ": " + err.Error())
		}
		out = append(out, n)
	}
	return out
}

// disallowedIP reports whether ip must never be dialed as a relay upstream:
// reject loopback, unspecified, multicast, link-local, private, CGNAT,
// documentation, benchmark, and any other non-global address — always
// including 169.254.0.0/16.
func disallowedIP(ip net.IP) (reason string, blocked bool) {
	if ip == nil {
		return "invalid", true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4 // normalize IPv4-mapped IPv6 (::ffff:a.b.c.d) to plain v4 before classifying
	}
	switch {
	case ip.IsLoopback():
		return "loopback", true
	case ip.IsUnspecified():
		return "unspecified", true
	case ip.IsMulticast():
		return "multicast", true
	case ip.IsLinkLocalMulticast():
		return "link-local-multicast", true
	case ip.IsLinkLocalUnicast():
		return "link-local", true
	case ip.IsPrivate():
		return "private", true
	}
	for _, cidr := range blockedCIDRs {
		if cidr.Contains(ip) {
			return "blocked-range", true
		}
	}
	return "", false
}
