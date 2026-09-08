package orch

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestBuildMMDSIdentityCannotAliasLocalSandbox(t *testing.T) {
	for _, buildFirst := range []bool{false, true} {
		name := "sandbox-first"
		if buildFirst {
			name = "build-first"
		}
		t.Run(name, func(t *testing.T) {
			o := testOrch(t)
			o.cfg.MMDS.Enabled = true
			build := &types.Build{
				BuildID: "same-id", Profile: types.ProfileE2B,
				TemplateID: "transient-same-id", RunID: "builder-run",
				RuntimeFloatingIP: "192.0.2.10", RuntimeEnvdAccessToken: "build-token",
			}
			local := &types.Sandbox{
				ID: "build-" + build.BuildID, Profile: types.ProfileBare,
				State: types.StateRunning, Metadata: map[string]string{"owner": "sandbox"},
			}
			if !types.ValidLocalSandboxID(local.ID) || types.ValidLocalSandboxID(buildMMDSID(build.BuildID)) {
				t.Fatal("internal Build MMDS namespace overlaps legal local sandbox IDs")
			}
			var row *types.Sandbox
			if buildFirst {
				row = o.publishRecoveredBuildMMDS(build)
				o.cache(local)
			} else {
				o.cache(local)
				row = o.publishRecoveredBuildMMDS(build)
			}
			if row == nil || row.ID == local.ID || row.ID != buildMMDSID(build.BuildID) {
				t.Fatal("Build route did not use its separate internal identity")
			}
			if current := o.lookup(local.ID); current == nil || current.Metadata["owner"] != "sandbox" {
				t.Fatal("Build publication replaced the real sandbox route")
			}
			if current := o.lookup(row.ID); current == nil || current.FloatingIP != build.RuntimeFloatingIP {
				t.Fatal("sandbox publication replaced the Build route")
			}
			kind, owner := o.mmdsRouteSecretOwner(local.ID)
			if kind != store.MMDSRouteSecretOwnerSandbox || owner != local.ID {
				t.Fatal("real sandbox inherited Build MMDS secret ownership")
			}
			kind, owner = o.mmdsRouteSecretOwner(row.ID)
			if kind != store.MMDSRouteSecretOwnerBuild || owner != build.BuildID {
				t.Fatal("Build lost its MMDS secret ownership")
			}
			// This is the same cache/route cleanup used by live and recovered Builds.
			o.uncache(row.ID)
			o.publishDelete(row.ID)
			o.setMMDSBuildOwner(row.ID, "")
			if current := o.lookup(local.ID); current == nil || current.Metadata["owner"] != "sandbox" {
				t.Fatal("Build cleanup removed the real sandbox route")
			}
		})
	}
}
