package orch

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestE2BSystemdPermissionChecksAtCreateAdmission(t *testing.T) {
	o := testOrch(t)
	for _, mode := range []string{"off", "auto", "runtime", "provided"} {
		raw := "{\"systemd\":\"" + mode + "\"}"
		if _, err := o.normalizeSandboxCreateConfig(types.ProfileE2B, map[string]string{sandboxcfg.NsLaunch: raw}); err != nil {
			t.Fatalf("mode %s: %v", mode, err)
		}
	}
	for _, raw := range []string{
		"{\"systemd\":\"auto\",\"exec\":\"\"}",
		"{\"systemd\":\"auto\",\"args\":[]}",
		"{\"systemd\":\"auto\",\"plugin\":[]}",
		"{\"systemd\":\"auto\",\"cgroup_control\":false}",
	} {
		if _, err := o.normalizeSandboxCreateConfig(types.ProfileE2B, map[string]string{sandboxcfg.NsLaunch: raw}); !errors.Is(err, api.ErrBadRequest) {
			t.Fatalf("accepted E2B product field %s: %v", raw, err)
		}
	}
}

func TestSnapshotSystemdOverrideRejectedBeforeCreateOwnership(t *testing.T) {
	o := testOrch(t)
	for _, mode := range []string{"off", "auto", "runtime", "provided"} {
		id := "snapshot-mode-" + mode
		_, _, err := o.acceptSandboxCreate(context.Background(), sandboxCreateSpec{
			ID:       id,
			Template: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: "manifest://source"},
			Config:   sandboxCreateConfig{Metadata: map[string]string{sandboxcfg.NsLaunch: "{\"systemd\":\"" + mode + "\"}"}},
		})
		if !errors.Is(err, api.ErrBadRequest) {
			t.Fatalf("mode %s: %v", mode, err)
		}
		stored, err := o.st.Get(context.Background(), id)
		if err != nil || stored != nil {
			t.Fatalf("rejected override allocated owner: %+v, %v", stored, err)
		}
	}
}
