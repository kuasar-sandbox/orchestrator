package orch

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Export fixtures model artifact interpretation in the tenant tool separately
// from publication. Reject other commands and malformed cleanup argv so a
// missing fixture never silently substitutes for the publisher or disk API.
func installExportCheckpointCleanup(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	const script = `#!/bin/sh
set -eu
if [ "$1" = check-snapshot-runtime ]; then
  [ "${#MANIFEST_KEY}" = 64 ]
  /bin/cat >/dev/null
  printf '%s\n' '{"status":"compatible"}'
  exit 0
fi
[ "$1" = checkpoint-cleanup ] || exit 64
shift
sid= pathid= base= from=
while [ "$#" -gt 0 ]; do
  [ "$#" -ge 2 ] || exit 64
  case "$1" in
    --sandbox-id) sid=$2 ;;
    --path-id) pathid=$2 ;;
    --base-root) base=$2 ;;
    --from) from=$2 ;;
    --restore|--manifest-config|--ref-location-parent) ;;
    *) exit 64 ;;
  esac
  shift 2
done
[ -n "$sid" ] && [ "$pathid" = "$sid" ] && [ -n "$from" ]
[ -d "$base/$sid/checkpoint" ]
[ "${#MANIFEST_KEY}" = 64 ]
`
	if err := os.WriteFile(filepath.Join(dir, "node-ctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func preparedExportSource(sb *types.Sandbox) *types.Sandbox {
	prepared := cloneSandbox(sb)
	prepared.RunID, prepared.RunDir, prepared.EnvdUDS, prepared.CiUDS = "", "", "", ""
	prepared.VswitchPort, prepared.FloatingIP, prepared.InnerIP, prepared.PortMAC = "", "", "", ""
	return prepared
}

func assertExportDeletingOwnership(t *testing.T, fixture sandboxFinalizerFixture) *types.Sandbox {
	t.Helper()
	fixture.sb = preparedExportSource(fixture.sb)
	return assertDeletingOwnershipState(t, fixture, false)
}
