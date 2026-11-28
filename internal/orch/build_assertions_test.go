package orch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const registeredTestNetwork = `{"dns":["192.0.2.53"],"transit_gateway_ip":"192.0.2.10","transit_geneve_vni":7}`

func buildAssertionRequest(handler http.Handler, key, method, path, body string, headers http.Header) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header = headers.Clone()
	if r.Header == nil {
		r.Header = make(http.Header)
	}
	r.Header.Set("X-API-KEY", key)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

// Exercise the shared-header Register -> Trigger -> Status sequence used by
// E2B Template.build against the real API and durable Node store. Worker/VM
// execution is outside this compatibility boundary.
func TestBuildRepeatedSDKHeadersRegisterTriggerStatus(t *testing.T) {
	for _, test := range []struct{ name, target, network, repeatedNetwork string }{
		{name: "auto with network", network: registeredTestNetwork},
		{name: "image", target: `{"kind":"image"}`, network: registeredTestNetwork},
		{name: "cold sandbox", target: `{"kind":"sandbox","memory":false}`, network: registeredTestNetwork},
		{name: "memory snapshot", target: `{"kind":"sandbox","memory":true}`, network: registeredTestNetwork,
			repeatedNetwork: `{ "transit_geneve_vni": 7, "transit_gateway_ip":"192.0.2.10", "dns":["192.0.2.53"] }`},
		{name: "empty network", network: `{}`},
		{name: "empty DNS declaration", network: `{"dns":[]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testOrch(t)
			ctx := context.Background()
			key, _, _ := allowlistedBuildIdentity(t, o)
			handler := api.New(o, "example.test", api.Resources{}, o.log).Handler()
			headers := make(http.Header)
			headers.Set("X-Kuasar-Sandbox-Network", test.network)
			if test.target != "" {
				headers.Set("X-Kuasar-Sandbox-Builder", `{"target":`+test.target+`}`)
			}
			response := buildAssertionRequest(handler, key, http.MethodPost, "/v3/templates",
				`{"name":"sdk-header-reuse","cpuCount":2,"memoryMB":2048}`, headers)
			if response.Code != http.StatusAccepted {
				t.Fatalf("Register = %d %s", response.Code, response.Body.String())
			}
			var handles struct {
				TemplateID string `json:"templateID"`
				BuildID    string `json:"buildID"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &handles); err != nil {
				t.Fatal(err)
			}
			before, err := o.st.GetBuild(ctx, handles.BuildID)
			if err != nil || before == nil {
				t.Fatalf("registered Build = %+v, %v", before, err)
			}
			hookCalls := 0
			o.SetExtensionHooks(nil, buildHookFunc(func(_ context.Context, operation *conductorextension.BuildOperation) error {
				hookCalls++
				operation.Trigger.FromImage = "registry.test/sdk-hook:latest"
				return nil
			}))
			// Whitespace and object key order are not immutable network values.
			if test.repeatedNetwork != "" {
				headers.Set("X-Kuasar-Sandbox-Network", test.repeatedNetwork)
			}
			path := "/v2/templates/" + handles.TemplateID + "/builds/" + handles.BuildID
			response = buildAssertionRequest(handler, key, http.MethodPost, path,
				`{"fromImage":"registry.test/sdk:latest","cpuCount":2,"memoryMB":2048}`, headers)
			if response.Code != http.StatusAccepted || hookCalls != 1 {
				t.Fatalf("Trigger = %d %s; hook calls %d", response.Code, response.Body.String(), hookCalls)
			}
			after, err := o.st.GetBuild(ctx, handles.BuildID)
			if err != nil || after.Status != types.BuildWaiting || after.FromImage != "registry.test/sdk-hook:latest" ||
				!reflect.DeepEqual(after.Metadata, before.Metadata) || !reflect.DeepEqual(after.Builder, before.Builder) || after.Resources != before.Resources {
				t.Fatalf("Trigger changed registration or failed to queue: %+v, %v", after, err)
			}
			statusPath := "/templates/" + handles.TemplateID + "/builds/" + handles.BuildID + "/status"
			response = buildAssertionRequest(handler, key, http.MethodGet, statusPath, "", headers)
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"building"`) ||
				!strings.Contains(response.Body.String(), handles.BuildID) {
				t.Fatalf("Status = %d %s", response.Code, response.Body.String())
			}
			response = buildAssertionRequest(handler, key, http.MethodPost, path, `{"fromImage":"registry.test/sdk:latest"}`, headers)
			if response.Code != http.StatusConflict || hookCalls != 1 {
				t.Fatalf("repeated Trigger = %d %s; hook calls %d", response.Code, response.Body.String(), hookCalls)
			}
		})
	}
}

func TestBuildTriggerConfigAssertionRejectsBeforeSideEffects(t *testing.T) {
	for _, test := range []struct {
		name, network, target string
		omitNetwork           bool
		omitTarget            bool
	}{
		{name: "changed Area", network: `{"dns":["192.0.2.53"],"transit_gateway_ip":"192.0.2.10","transit_geneve_vni":8}`},
		{name: "added network field", network: `{"dns":["192.0.2.53"],"transit_gateway_ip":"192.0.2.10","transit_geneve_vni":7,"hostname":"new"}`},
		{name: "network absent at registration", network: registeredTestNetwork, omitNetwork: true},
		{name: "different target", target: `{"target":{"kind":"image"}}`},
		{name: "different memory", target: `{"target":{"kind":"sandbox","memory":true}}`},
		{name: "target absent at registration", target: `{"target":{"kind":"sandbox"}}`, omitTarget: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testOrch(t)
			ctx := context.Background()
			key, _, _ := allowlistedBuildIdentity(t, o)
			spec := api.RegisterSpec{Profile: types.ProfileE2B, Resources: testBuildResources()}
			if !test.omitNetwork {
				spec.Metadata = map[string]string{sandboxcfg.NsNetwork: registeredTestNetwork}
			}
			if !test.omitTarget {
				spec.Builder.Target = &types.BuildTarget{Kind: types.BuildTargetSandbox}
			}
			b, err := o.RegisterBuild(ctx, key, spec)
			if err != nil {
				t.Fatal(err)
			}
			before, err := o.st.GetBuild(ctx, b.BuildID)
			if err != nil {
				t.Fatal(err)
			}
			o.SetExtensionHooks(nil, buildHookFunc(func(context.Context, *conductorextension.BuildOperation) error {
				t.Fatal("rejected immutable assertion reached the Hook")
				return nil
			}))
			handler := api.New(o, "example.test", api.Resources{}, o.log).Handler()
			headers := make(http.Header)
			if test.network != "" {
				headers.Set("X-Kuasar-Sandbox-Network", test.network)
			}
			if test.target != "" {
				headers.Set("X-Kuasar-Sandbox-Builder", test.target)
			}
			headers.Set(api.PullTokenHeader, "invalid-pull-token-must-not-be-resolved")
			path := "/v2/templates/" + b.TemplateID + "/builds/" + b.BuildID
			// Without an assertion rejection this unconfigured COPY fails with
			// 501, and the invalid pull token would fail credential resolution.
			body := `{"fromImage":"registry.test/changed:latest","steps":[{"type":"COPY","filesHash":"missing-context"}]}`
			response := buildAssertionRequest(handler, key, http.MethodPost, path, body, headers)
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "trigger ") {
				t.Fatalf("Trigger = %d %s", response.Code, response.Body.String())
			}
			after, err := o.st.GetBuild(ctx, b.BuildID)
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("rejection changed durable Build: before=%+v after=%+v, %v", before, after, err)
			}
			_, otherKey := defaultTestCredentials(t, strings.Repeat("6", 64))
			response = buildAssertionRequest(handler, otherKey, http.MethodPost, path, body, headers)
			if response.Code != http.StatusNotFound {
				t.Fatalf("assertion leaked another tenant's definition: %d %s", response.Code, response.Body.String())
			}
		})
	}
}
