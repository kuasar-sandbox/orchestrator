package orch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/builder"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestImageBuildNetworkFromRegistrationToResult(t *testing.T) {
	for _, profile := range []types.Profile{types.ProfileE2B, types.ProfileBare} {
		for _, explicit := range []bool{false, true} {
			for _, carrier := range []string{"header", "metadata"} {
				for _, network := range []struct{ name, json string }{
					{"configured", `{"hostname":"image-builder","dns":["192.0.2.53"],"transit_gateway_ip":"192.0.2.1","transit_geneve_vni":9,"transit_mac":"aa:bb:cc:dd:ee:ff"}`},
					{"empty", `{}`},
				} {
					targetName := "auto"
					if explicit {
						targetName = "explicit"
					}
					t.Run(string(profile)+"/"+targetName+"/"+carrier+"/"+network.name, func(t *testing.T) {
						ctx := context.Background()
						cfg := buildNetworkTestConfig()
						cfg.ManifestConfig = filepath.Join(t.TempDir(), "manifest.yaml")
						// The worker reuses an existing Manifest IMG, so publication
						// must not need a Store RPC or a running VM.
						if err := os.WriteFile(cfg.ManifestConfig, []byte("store:\n  endpoint: 127.0.0.1:1\n"), 0o600); err != nil {
							t.Fatal(err)
						}
						o := testOrchCfg(t, cfg)
						apiKey, _, _ := allowlistedBuildIdentity(t, o)
						body := map[string]any{"name": "network-image", "profile": profile}
						if carrier == "metadata" {
							body["metadata"] = map[string]string{sandboxcfg.NsNetwork: network.json}
						}
						encoded, err := json.Marshal(body)
						if err != nil {
							t.Fatal(err)
						}
						req := httptest.NewRequest(http.MethodPost, "/v3/templates", strings.NewReader(string(encoded)))
						req.Header.Set("X-API-KEY", apiKey)
						req.Header.Set("Content-Type", "application/json")
						buildOptions := `{"resources":{"cpu":2,"memory":"2GiB","storage":"4GiB"}`
						if explicit {
							buildOptions += `,"target":{"kind":"image"}`
						}
						req.Header.Set("X-Kuasar-Sandbox-Builder", buildOptions+"}")
						if carrier == "header" {
							req.Header.Set("X-Kuasar-Sandbox-Network", network.json)
						}
						response := httptest.NewRecorder()
						api.New(o, "example.test", api.Resources{}, o.log).Handler().ServeHTTP(response, req)
						if response.Code != http.StatusAccepted {
							t.Fatalf("Register HTTP %d: %s", response.Code, response.Body.String())
						}
						var registered struct {
							BuildID string `json:"buildID"`
						}
						if err := json.Unmarshal(response.Body.Bytes(), &registered); err != nil {
							t.Fatal(err)
						}
						b, err := o.st.GetBuild(ctx, registered.BuildID)
						if err != nil || b == nil {
							t.Fatalf("registered Build missing: %v", err)
						}
						ref := "manifest://" + strings.Repeat("a", 64)
						b.FromTemplate = types.TemplateID{Profile: profile, Kind: types.KindImg, Ref: ref}.String()
						spec, resources, targetResources, err := o.resolveBuildRequestInputs(b, false)
						if err != nil {
							t.Fatal(err)
						}
						buildNet, templateNet, err := o.resolveBuildNetworks(profile, sandboxcfg.NetworkSpec{}, spec.Network, "build-image")
						if err != nil {
							t.Fatal(err)
						}
						vs := &capturingNetworkVS{}
						o.vs = vs
						if _, err := o.attachNetwork(ctx, buildNet); err != nil {
							t.Fatal(err)
						}
						pend := &pendingBuild{build: b, spec: spec, resources: resources, sandboxResources: targetResources, network: buildNet, templateNetwork: templateNet}
						task, err := o.buildSpecForPending(ctx, pend)
						if err != nil {
							t.Fatal(err)
						}
						if network.name == "configured" && (task.Net.Hostname != "image-builder" || !reflect.DeepEqual(task.Net.DNS, []string{"192.0.2.53"}) || vs.req.TransitGatewayIP != "192.0.2.1" || vs.req.TransitGeneveVNI != 9 || vs.req.TransitMAC != "aa:bb:cc:dd:ee:ff") {
							t.Fatalf("request network lost: guest=%+v attach=%+v", task.Net, vs.req)
						}
						task.Env = buildTaskEnv(b)
						result := builder.Run(ctx, task, nil, func(phase, _, _ string) error {
							t.Fatalf("unchanged IMG started phase %s", phase)
							return nil
						}, o.log)
						if result.Error != "" || result.ImageRef != ref || result.Target.Kind != types.BuildTargetImage {
							t.Fatalf("worker result = %+v", result)
						}
						if err := validateBuildResult(b, types.BuildResult{Target: result.Target, ImageRef: result.ImageRef}); err != nil {
							t.Fatalf("image result rejected: %v", err)
						}
						if direct, handled, err := directImageBuildResult(b, false); err != nil || !handled || direct == nil || direct.ImageRef != ref {
							t.Fatalf("direct IMG reuse: handled=%t err=%v", handled, err)
						}
					})
				}
			}
		}
	}
}
