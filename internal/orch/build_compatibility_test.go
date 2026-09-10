package orch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestBareAutoRejectsSandboxOptionsBeforeRegistrationAdmission(t *testing.T) {
	for _, input := range []struct {
		name  string
		body  map[string]any
		field string
	}{
		{"env", map[string]any{"envVars": map[string]string{"APP_MODE": "production"}}, "envVars"},
		{"secure", map[string]any{"secure": true}, "secure"},
		{"resource", map[string]any{"metadata": map[string]string{sandboxcfg.NsResource: `{"allocatable":{"memory":"16GiB"}}`}}, sandboxcfg.NsResource},
		{"files", map[string]any{"metadata": map[string]string{sandboxcfg.NsFiles: `[]`}}, sandboxcfg.NsFiles},
	} {
		t.Run(input.name, func(t *testing.T) {
			o := testOrch(t)
			key, _, _ := allowlistedBuildIdentity(t, o)
			input.body["profile"] = "bare"
			body, err := json.Marshal(input.body)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v3/templates", strings.NewReader(string(body)))
			req.Header.Set("X-API-KEY", key)
			req.Header.Set("X-Kuasar-Sandbox-Builder", `{"resources":{"cpu":2,"memory":"2GiB","storage":"4GiB"}}`)
			rec := httptest.NewRecorder()
			api.New(o, "example.test", api.Resources{}, o.log).Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), input.field) {
				t.Fatalf("registration = %d %s", rec.Code, rec.Body.String())
			}
			usage, err := o.st.BuildUsage(context.Background())
			if err != nil || usage.RegistrationBuilds != 0 {
				t.Fatalf("rejected registration consumed admission: %+v, %v", usage, err)
			}
		})
	}
	t.Run("cluster", func(t *testing.T) {
		o := testOrch(t)
		_, _, fingerprint := allowlistedBuildIdentity(t, o)
		cmd := clusterBuildRegisterCommand("bare-auto-env", fingerprint)
		cmd.Profile = "bare"
		cmd.BuildEnv = map[string]string{"APP_MODE": "production"}
		if err := o.registerClusterBuild(context.Background(), cmd); !errors.Is(err, api.ErrBadRequest) || !strings.Contains(err.Error(), "envVars") {
			t.Fatalf("cluster register = %v", err)
		}
		if b, err := o.st.GetBuild(context.Background(), cmd.BuildID); err != nil || b != nil {
			t.Fatalf("rejected cluster row = %+v, %v", b, err)
		}
	})
}

func TestAutoTriggerValidatesKnownTargetBeforeQueueing(t *testing.T) {
	ref := "manifest://" + strings.Repeat("a", 64)
	for _, source := range []string{"image", "img", "sbx", "snp", "alias-img", "alias-sbx", "hook-image"} {
		t.Run(source, func(t *testing.T) {
			o := testOrch(t)
			key, _, _ := allowlistedBuildIdentity(t, o)
			ctx := context.Background()
			b, err := o.RegisterBuild(ctx, key, api.RegisterSpec{Profile: types.ProfileE2B, Resources: testBuildResources(), EnvVars: map[string]string{"APP_MODE": "production"}, Secure: true})
			if err != nil {
				t.Fatal(err)
			}
			trigger := api.TriggerSpec{FromImage: "registry.test/image:latest"}
			if source != "image" {
				kind := types.Kind(strings.TrimPrefix(source, "alias-"))
				if source == "hook-image" {
					kind = types.KindSbx
				}
				trigger = api.TriggerSpec{FromTemplate: types.TemplateID{Profile: types.ProfileE2B, Kind: kind, Ref: ref}.String()}
				if strings.HasPrefix(source, "alias-") {
					ready := &types.Build{BuildID: "source-" + source, TemplateID: "source-template", APISecret: b.APISecret, ManifestKey: b.ManifestKey,
						Status: types.BuildReady, Profile: types.ProfileE2B, Kind: kind, PersistID: trigger.FromTemplate, Aliases: []string{"source-alias"}, Resources: testBuildResources()}
					if err := o.st.PutBuild(ctx, ready); err != nil {
						t.Fatal(err)
					}
					trigger.FromTemplate = "source-alias"
				}
			}
			if source == "hook-image" {
				o.SetExtensionHooks(nil, buildHookFunc(func(_ context.Context, op *conductorextension.BuildOperation) error {
					op.Trigger.FromTemplate = ""
					op.Trigger.FromImage = "registry.test/hook-final:latest"
					return nil
				}))
			}
			err = o.TriggerBuild(ctx, key, b.TemplateID, b.BuildID, trigger, api.BuildAuth{})
			unknown := source == "sbx" || source == "snp" || source == "alias-sbx"
			stored, getErr := o.st.GetBuild(ctx, b.BuildID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if unknown {
				if err != nil || stored.Status != types.BuildWaiting {
					t.Fatalf("unknown target rejected: %+v, %v", stored, err)
				}
			} else {
				if !errors.Is(err, api.ErrBadRequest) || !strings.Contains(err.Error(), "envVars") {
					t.Fatalf("known image trigger = %v", err)
				}
				if stored.Status != types.BuildRegistered || stored.WaitingUnix != 0 || stored.ExecutionClaimed || stored.FromTemplate != "" {
					t.Fatalf("rejected trigger persisted work: %+v", stored)
				}
				// A rejected trigger is retryable with commands selecting memory C.
				trigger.StartCmd = "serve"
				if err = o.TriggerBuild(ctx, key, b.TemplateID, b.BuildID, trigger, api.BuildAuth{}); err != nil {
					t.Fatalf("retry with supported target: %v", err)
				}
			}
			if !reflect.DeepEqual(stored.Env, b.Env) || stored.Builder.Target != nil || !stored.Secure {
				t.Fatalf("registration was rewritten: %+v", stored)
			}
		})
	}
}

func TestAcceptedBuildResultIgnoresUnsupportedOptions(t *testing.T) {
	ref := "manifest://" + strings.Repeat("a", 64)
	for _, target := range []types.BuildTarget{{Kind: types.BuildTargetImage}, {Kind: types.BuildTargetSandbox}} {
		b := &types.Build{Profile: types.ProfileE2B, Env: map[string]string{"APP_MODE": "production"}, Secure: true,
			Metadata: map[string]string{sandboxcfg.NsFiles: `[]`, sandboxcfg.NsTraffic: `{"max_inflight":{"total":1}}`},
			Builder:  types.BuildOptions{Target: &target}}
		result := types.BuildResult{Target: target}
		if target.Kind == types.BuildTargetImage {
			result.ImageRef = ref
		} else {
			result.SandboxRef = ref
		}
		if err := validateBuildResult(b, result); err != nil {
			t.Fatalf("accepted %s result: %v", target.Kind, err)
		}
		if target.Kind == types.BuildTargetImage {
			b.Builder.Target = nil
			b.FromTemplate = types.TemplateID{Profile: b.Profile, Kind: types.KindImg, Ref: ref}.String()
			if direct, handled, err := directImageBuildResult(b, false); err != nil || !handled || direct == nil || direct.ImageRef != ref {
				t.Fatalf("accepted direct image: %+v, %t, %v", direct, handled, err)
			}
		}
	}
}

func TestRecoveredAutoPreparesOnlyResolvedTarget(t *testing.T) {
	for _, commands := range []bool{false, true} {
		for _, option := range []string{"resource", "checkpoint"} {
			t.Run(option+"/commands="+map[bool]string{false: "false", true: "true"}[commands], func(t *testing.T) {
				o := testOrchCfg(t, buildNetworkTestConfig())
				o.cfg.Builder.TotalTimeoutSec = 60
				stop := errors.New("reached network attach")
				o.vs = failingCreateVS{err: stop}
				metadata := map[string]string{sandboxcfg.NsResource: `{"allocatable":{"memory":"16GiB"}}`}
				if option == "checkpoint" {
					metadata = map[string]string{sandboxcfg.NsCheckpoint: `{"drop_caches":true}`}
					o.cfg.Checkpoint.Mode = "unsupported-after-admission"
				}
				b := &types.Build{BuildID: "accepted-auto", Profile: types.ProfileE2B, Resources: testBuildResources(), Metadata: metadata, Env: map[string]string{"APP_MODE": "production"}, Secure: true, ExecutionClaimedUnix: time.Now().Unix()}
				spec, resources, _, err := o.resolveBuildRequestInputs(b, true)
				if err != nil {
					t.Fatal(err)
				}
				pend := &pendingBuild{build: b, spec: spec, resources: resources, sourceTemplate: true, handoff: newBuildTaskHandoff(true, ""), result: make(chan configsock.BuildResult, 1)}
				summary := validBuildPrepareSummary()
				summary.HasBuildCommands = commands
				if _, err = pend.handoff.Submit(summary); err != nil {
					t.Fatal(err)
				}
				_, _, _, _, err = o.continueRecoveredBuildPreparation(context.Background(), b, pend, "unused.service")
				if commands {
					if err == nil || errors.Is(err, stop) {
						t.Fatalf("memory target failed to validate supported %s: %v", option, err)
					}
				} else {
					if !errors.Is(err, stop) {
						t.Fatalf("image prepared inapplicable %s: %v", option, err)
					}
					if pend.sandboxResources != (rtconfig.ResourcesConfig{}) || !reflect.DeepEqual(pend.checkpointPolicy, sandboxcfg.SnapshotPolicy{}) || pend.sourceHasBuildCommands {
						t.Fatalf("image acquired Sandbox runtime: %+v", pend)
					}
				}
				if b.Metadata[sandboxcfg.NsResource] != metadata[sandboxcfg.NsResource] || !b.Secure || b.Env["APP_MODE"] != "production" {
					t.Fatal("preparation mutated registration")
				}
			})
		}
	}
}

func TestBuildPrepareReplayBindsCommandPresence(t *testing.T) {
	h := newBuildTaskHandoff(true, "")
	summary := validBuildPrepareSummary()
	if _, err := h.Submit(summary); err != nil {
		t.Fatal(err)
	}
	summary.HasBuildCommands = true
	if _, err := h.Submit(summary); err == nil {
		t.Fatal("changed command presence accepted as identical replay")
	}
}
