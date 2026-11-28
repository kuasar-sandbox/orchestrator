package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestBuildTriggerAssertionScopeAndStrictInput(t *testing.T) {
	core := &buildMMDSCoreStub{trigger: func(context.Context, string, string, string, TriggerSpec, BuildAuth) error {
		t.Fatal("invalid assertion reached Core")
		return nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	for _, test := range []struct {
		name, value string
		header      string
		duplicate   bool
	}{
		{name: "unknown network field", header: "X-Kuasar-Sandbox-Network", value: `{"new_field":7}`},
		{name: "noncanonical network field", header: "X-Kuasar-Sandbox-Network", value: `{"Hostname":"new"}`},
		{name: "noncanonical Area field", header: "X-Kuasar-Sandbox-Network", value: `{"TRANSIT_GENEVE_VNI":7}`},
		{name: "network case collision", header: "X-Kuasar-Sandbox-Network", value: `{"hostname":"same","Hostname":"new"}`},
		{name: "duplicate network key", header: "X-Kuasar-Sandbox-Network", value: `{"transit_geneve_vni":7,"transit_geneve_vni":8}`},
		{name: "network null", header: "X-Kuasar-Sandbox-Network", value: `null`},
		{name: "network array", header: "X-Kuasar-Sandbox-Network", value: `[]`},
		{name: "network empty", header: "X-Kuasar-Sandbox-Network", value: ``},
		{name: "invalid gateway", header: "X-Kuasar-Sandbox-Network", value: `{"transit_gateway_ip":"bad"}`},
		{name: "multiple network headers", header: "X-Kuasar-Sandbox-Network", value: `{}`, duplicate: true},
		{name: "target plus resources", header: builderHeader, value: `{"target":{"kind":"sandbox"},"resources":{"cpu":2}}`},
		{name: "target plus referer", header: builderHeader, value: `{"target":{"kind":"sandbox"},"referer":{"enabled":false}}`},
		{name: "target plus registry", header: builderHeader, value: `{"target":{"kind":"sandbox"},"registry":{"tls":{"insecure_skip_verify":true}}}`},
		{name: "target absent", header: builderHeader, value: `{}`},
		{name: "target null", header: builderHeader, value: `{"target":null}`},
		{name: "target unknown field", header: builderHeader, value: `{"target":{"kind":"sandbox","unknown":true}}`},
		{name: "noncanonical target field", header: builderHeader, value: `{"TARGET":{"kind":"sandbox"}}`},
		{name: "noncanonical kind field", header: builderHeader, value: `{"target":{"Kind":"sandbox"}}`},
		{name: "noncanonical memory field", header: builderHeader, value: `{"target":{"kind":"sandbox","MEMORY":false}}`},
		{name: "target case collision", header: builderHeader, value: `{"target":{"kind":"image"},"Target":{"kind":"sandbox"}}`},
		{name: "kind case collision", header: builderHeader, value: `{"target":{"kind":"image","Kind":"sandbox"}}`},
		{name: "memory case collision", header: builderHeader, value: `{"target":{"kind":"sandbox","memory":true,"Memory":false}}`},
		{name: "multiple Builder headers", header: builderHeader, value: `{"target":{"kind":"sandbox"}}`, duplicate: true},
		{name: "MMDS remains forbidden", header: mmdsHeader, value: `{"routes":[]}`},
		{name: "credentials remain forbidden", header: credentialsHeader, value: `{}`},
		{name: "workspace remains forbidden", header: "X-Kuasar-Sandbox-Workspace", value: `[]`},
		{name: "resource remains forbidden", header: "X-Kuasar-Sandbox-Resource", value: `{}`},
		{name: "launch remains forbidden", header: "X-Kuasar-Sandbox-Launch", value: `{"systemd":"auto"}`},
		{name: "future header remains forbidden", header: "X-Kuasar-Sandbox-Future", value: `{}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			headers := header(test.header, test.value)
			if test.duplicate {
				headers.Add(test.header, test.value)
			}
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v2/templates/template/builds/build", strings.NewReader(`{}`), headers)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("Trigger = %d %s", response.Code, response.Body.String())
			}
		})
	}
}
