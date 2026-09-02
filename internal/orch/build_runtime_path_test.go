package orch

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestBuildRuntimeDirUsesCompleteOpaqueIdentityAndBoundsPhaseSockets(t *testing.T) {
	first := buildRuntimeDir("/run", "same-prefix-build-one")
	second := buildRuntimeDir("/run", "same-prefix-build-two")
	if first == second {
		t.Fatalf("distinct Build IDs produced the same runtime directory %q", first)
	}
	if got := buildRuntimeDir("/run", "same-prefix-build-one"); got != first {
		t.Fatalf("runtime directory is not deterministic: got %q want %q", got, first)
	}
	if strings.Contains(first, "same-prefix-build-one") {
		t.Fatalf("runtime directory exposed the opaque BuildID: %q", first)
	}
	if got := buildRuntimeDir("/run", "../../escape"); filepath.Dir(got) != "/run" {
		t.Fatalf("opaque BuildID escaped run root: %q", got)
	}

	// This mirrors the longest independent-Proxy E2E root and sandbox launch
	// socket. The previous full-BuildID directory exceeded Linux sun_path even
	// after phase Sandbox IDs were compacted.
	runRoot := "/tmp/e2e-orch-proxy-XXXXXX/run"
	phaseSID := "bp-a-" + strings.Repeat("0", 20)
	socket := filepath.Join(buildRuntimeDir(runRoot, strings.Repeat("b", 36)), "run", phaseSID, "vsock.sock_5000")
	if len(socket) >= len(unix.RawSockaddrUnix{}.Path) {
		t.Fatalf("phase runtime socket path is %d bytes, want less than sun_path %d: %s",
			len(socket), len(unix.RawSockaddrUnix{}.Path), socket)
	}
}
