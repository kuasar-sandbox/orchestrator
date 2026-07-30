package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func materializeTestSandboxCredentials(t *testing.T, sb *types.Sandbox) {
	t.Helper()
	if err := materializeSandboxCredentials(sb, sandboxcfg.Credentials{}); err != nil {
		t.Fatal(err)
	}
}

func TestMaterializeSandboxCredentials(t *testing.T) {
	apiSecret := strings.Repeat("1", 64)
	for _, profile := range []types.Profile{types.ProfileE2B, types.ProfileBare} {
		t.Run(string(profile), func(t *testing.T) {
			sb := &types.Sandbox{ID: "local-id", Profile: profile, APISecret: apiSecret}
			if err := materializeSandboxCredentials(sb, sandboxcfg.Credentials{}); err != nil {
				t.Fatal(err)
			}
			wantService, err := keys.DeriveServiceSecret(apiSecret, sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			if sb.ServiceSecret != wantService {
				t.Fatalf("ServiceSecret did not use AuthSandboxID fallback")
			}
			if err := keys.VerifyForwardAccessToken(sb.ForwardAccessToken, sb.ServiceSecret, sb.ID); err != nil {
				t.Fatalf("ForwardAccessToken = invalid: %v", err)
			}
			if profile == types.ProfileE2B {
				if sb.EnvdAccessToken == "" || sb.TrafficAccessToken == "" {
					t.Fatalf("e2b credentials are incomplete: %+v", sb)
				}
			} else if sb.EnvdAccessToken != "" || sb.TrafficAccessToken != "" {
				t.Fatalf("bare sandbox received e2b-only credentials: %+v", sb)
			}
		})
	}
}

func TestMaterializeSandboxCredentialsUsesOverridesAndStableSubject(t *testing.T) {
	serviceSecret := strings.Repeat("2", 64)
	sb := &types.Sandbox{
		ID: "node-id", Profile: types.ProfileE2B, APISecret: strings.Repeat("1", 64),
		AuthSandboxIDValue: "stable-id",
	}
	credentials := sandboxcfg.Credentials{
		ServiceSecret: serviceSecret, EnvdAccessToken: "envd-override", TrafficAccessToken: "traffic-override",
	}
	if err := materializeSandboxCredentials(sb, credentials); err != nil {
		t.Fatal(err)
	}
	if sb.ServiceSecret != serviceSecret || sb.EnvdAccessToken != "envd-override" || sb.TrafficAccessToken != "traffic-override" {
		t.Fatalf("overrides were not preserved: %+v", sb)
	}
	if err := keys.VerifyForwardAccessToken(sb.ForwardAccessToken, serviceSecret, "stable-id"); err != nil {
		t.Fatalf("ForwardAccessToken is not bound to stable AuthSandboxID: %v", err)
	}
	if err := keys.VerifyForwardAccessToken(sb.ForwardAccessToken, serviceSecret, sb.ID); err == nil {
		t.Fatal("ForwardAccessToken accepted the node-local ID")
	}
}

func TestValidateSandboxCredentialOverridesRejectsE2BOnlyBareFields(t *testing.T) {
	for _, credentials := range []sandboxcfg.Credentials{
		{EnvdAccessToken: "envd"},
		{TrafficAccessToken: "traffic"},
	} {
		if err := validateSandboxCredentialOverrides(types.ProfileBare, credentials); err == nil {
			t.Fatalf("bare credentials accepted: %+v", credentials)
		}
	}
}

func TestCreateRejectsInvalidCredentialsBeforeLaunchSideEffects(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	cfg.Paths.BaseRoot = filepath.Join(root, "base")
	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	for name, raw := range map[string]string{
		"malformed":         `{"service_secret":"short"}`,
		"e2b only for bare": `{"envd_access_token":"envd"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := o.Create(context.Background(), api.CreateReq{
				APIKey: apiKey, TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(), TimeoutSec: 60,
				Metadata: map[string]string{sandboxcfg.NsCredentials: raw},
			})
			if !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("Create error = %v, want ErrBadRequest", err)
			}
		})
	}
	for _, path := range []string{cfg.Paths.RunRoot, cfg.Paths.BaseRoot} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("invalid credentials created %s: stat error = %v", path, err)
		}
	}
}
