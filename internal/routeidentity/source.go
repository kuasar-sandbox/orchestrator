// Package routeidentity contains the FloatingIP identity rules shared by MMDS
// and telemetry. Reverse indexes are hints: the primary current route is always
// revalidated before accepting the candidate identity.
package routeidentity

import (
	"encoding/binary"
	"net/netip"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func IPv4(raw string) (uint32, bool) {
	address, err := netip.ParseAddr(raw)
	if err != nil {
		return 0, false
	}
	address = address.Unmap()
	if !address.Is4() {
		return 0, false
	}
	b := address.As4()
	return binary.BigEndian.Uint32(b[:]), true
}

func Active(state string) bool {
	return state == routesync.StateStarting || state == routesync.StateRunning
}

func Matches(route routesync.RouteEntry, ipv4 uint32) bool {
	address, ok := IPv4(route.FloatingIP)
	return ok && address == ipv4 && Active(route.State) && route.SandboxID != ""
}
