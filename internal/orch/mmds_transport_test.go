package orch

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// TestValidateMMDSRouteEntryTransportRejectsSecondPassEscapeOverflow proves the
// check catches an entry that only overflows the frame after RouteEntry's own
// JSON-string escaping doubles an already-escaped backslash-heavy payload --
// not just a raw entry that is already too large before that second pass.
func TestValidateMMDSRouteEntryTransportRejectsSecondPassEscapeOverflow(t *testing.T) {
	// 600,000 raw backslashes: comfortably under 1 MiB on their own, but each
	// becomes "\\\\" (4 bytes) once embedded as a RouteEntry field and the
	// whole struct is marshaled -- 2.4MB, well past routesync's 1 MiB frame.
	entry := routesync.RouteEntry{
		SandboxID:  "sbx-1",
		Profile:    "bare",
		State:      "starting",
		MMDSRoutes: strings.Repeat(`\`, 600_000),
	}
	err := validateMMDSRouteEntryTransport(entry)
	if err == nil {
		t.Fatal("expected the second-pass escape overflow to be rejected")
	}
	if !strings.Contains(err.Error(), "too large to publish") {
		t.Fatalf("error = %v, want the transport-overflow detail", err)
	}
}

func TestValidateMMDSRouteEntryTransportAcceptsOrdinaryEntry(t *testing.T) {
	entry := routesync.RouteEntry{
		SandboxID:  "sbx-1",
		Profile:    "bare",
		State:      "starting",
		MMDSRoutes: `{"version":1,"routes":[{"path":"/x","type":"static","content_type":"text/plain","data":"hello"}]}`,
	}
	if err := validateMMDSRouteEntryTransport(entry); err != nil {
		t.Fatalf("ordinary entry rejected: %v", err)
	}
}
