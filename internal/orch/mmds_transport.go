package orch

import (
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// validateMMDSRouteEntryTransport confirms entry will actually fit the
// routesync wire frame once published. sandboxcfg.ExtractMMDS's own
// canonical-form re-check only bounds the raw kuasar-sandbox.mmds string in
// isolation (maxMMDSCanonicalBytes) -- but that string is embedded a second
// time as entry.MMDSRoutes and JSON-string-escaped again when the whole
// RouteEntry is marshaled for the wire. A raw static-route body with enough
// backslashes doubles in size on this second pass (each already-escaped
// `\\` becomes `\\\\`), so a specification that comfortably clears the
// single-escape canonical check can still overflow the frame once actually
// published: WriteMsg then fails deep inside route publication instead of at
// admission, the route never reaches the registry or proxy, and every
// subsequent reconcile/resync hits the identical failure again. Checked
// against the real encoded RouteEntry (mirroring what WriteMsg itself does),
// not an estimated expansion factor, so it stays correct regardless of which
// escape sequence turns out to be worst-case.
func validateMMDSRouteEntryTransport(entry routesync.RouteEntry) error {
	err := routesync.ValidateMessage(&routesync.Msg{Type: routesync.TypeUpsert, Route: &entry})
	if err == nil {
		return nil
	}
	if errors.Is(err, routesync.ErrMessageTooLarge) {
		return errors.New("MMDS metadata: specified routes are too large to publish once encoded for route sync")
	}
	return err
}
