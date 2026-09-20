package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRequestedBuildProfile(t *testing.T) {
	tests := []struct {
		raw     string
		want    types.Profile
		wantErr bool
	}{
		{raw: "", want: types.ProfileE2B},
		{raw: "e2b", want: types.ProfileE2B},
		{raw: "bare", want: types.ProfileBare},
		{raw: "unknown", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := requestedBuildProfile(tt.raw)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("requestedBuildProfile(%q) = %q, %v; want %q, error=%t", tt.raw, got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestMergeConfigHeaders(t *testing.T) {
	// No headers => no allocation.
	if m := mustMergeConfigHeaders(t, nil, http.Header{}); m != nil {
		t.Fatalf("no headers should not allocate: %v", m)
	}

	// Headers populate the matching namespace keys.
	h := http.Header{}
	h.Set("X-Kuasar-Sandbox-Network", `{"hostname":"h1"}`)
	h.Set("X-Kuasar-Sandbox-Resource", `{"capacity":{"cpu":4}}`)
	h.Set("X-Kuasar-Sandbox-Restore", `{"prefetch":"memory"}`)
	h.Set("X-Kuasar-Sandbox-Credentials", `{"envd_access_token":"envd"}`)
	m := mustMergeConfigHeaders(t, nil, h)
	if m[sandboxcfg.NsNetwork] != `{"hostname":"h1"}` || m[sandboxcfg.NsResource] != `{"capacity":{"cpu":4}}` {
		t.Fatalf("headers not normalized: %+v", m)
	}
	if _, ok := m[sandboxcfg.NsRestore]; ok {
		t.Fatalf("generic/template headers admitted request-scoped restore: %+v", m)
	}
	if got := mustMergeCreateConfigHeaders(t, nil, h); got[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("create restore header not normalized: %+v", got)
	}
	if got := mustMergeCreateConfigHeaders(t, nil, h); got[sandboxcfg.NsCredentials] != `{"envd_access_token":"envd"}` {
		t.Fatalf("create credentials header not normalized: %+v", got)
	}

	// Header wins over an e2b metadata key of the same namespace.
	meta := map[string]string{sandboxcfg.NsNetwork: `{"hostname":"from-metadata"}`}
	got := mustMergeConfigHeaders(t, meta, header("X-Kuasar-Sandbox-Network", `{"hostname":"from-header"}`))
	if got[sandboxcfg.NsNetwork] != `{"hostname":"from-header"}` {
		t.Fatalf("header should win over metadata: %+v", got)
	}
	meta = map[string]string{sandboxcfg.NsRestore: `{"prefetch":"memory"}`}
	got = mustMergeCreateConfigHeaders(t, meta, header("X-Kuasar-Sandbox-Restore", `{"prefetch":"off"}`))
	if got[sandboxcfg.NsRestore] != `{"prefetch":"off"}` {
		t.Fatalf("restore header should win over metadata: %+v", got)
	}
	emptyRestore := http.Header{}
	emptyRestore.Set("X-Kuasar-Sandbox-Restore", "")
	got = mustMergeCreateConfigHeaders(t, nil, emptyRestore)
	if _, ok := got[sandboxcfg.NsRestore]; !ok {
		t.Fatalf("present empty restore header must reach strict validation: %+v", got)
	}
	emptyCredentials := http.Header{}
	emptyCredentials.Set("X-Kuasar-Sandbox-Credentials", "")
	got = mustMergeCreateConfigHeaders(t, nil, emptyCredentials)
	if _, ok := got[sandboxcfg.NsCredentials]; !ok {
		t.Fatalf("present empty credentials header must reach strict validation: %+v", got)
	}
	metadataCredentials := map[string]string{sandboxcfg.NsCredentials: `{"service_secret":"metadata"}`}
	got = mustMergeCreateConfigHeaders(t, metadataCredentials, header("X-Kuasar-Sandbox-Credentials", `{"envd_access_token":"header"}`))
	if got[sandboxcfg.NsCredentials] != `{"envd_access_token":"header"}` {
		t.Fatalf("credentials header should replace the metadata object: %+v", got)
	}

	// Builder is build-only and is not folded by the generic sandbox header path.
	got = mustMergeConfigHeaders(t, nil, header("X-Kuasar-Sandbox-Builder", `{"referer":{"enabled":false}}`))
	if got != nil {
		t.Fatalf("builder header should not enter sandbox metadata: %+v", got)
	}
}

func TestMergeConfigHeadersMergesOnlyResourceLeaves(t *testing.T) {
	metadata := map[string]string{
		sandboxcfg.NsResource: `{"capacity":{"memory":"8GiB"},"allocatable":{"cpu":0.5,"memory":"256MiB"},"startup":{"memory":"1GiB"}}`,
		sandboxcfg.NsNetwork:  `{"hostname":"metadata"}`,
	}
	headers := http.Header{}
	headers.Set("X-Kuasar-Sandbox-Resource", `{"capacity":{"cpu":4},"allocatable":{"memory":"512MiB"}}`)
	headers.Set("X-Kuasar-Sandbox-Network", `{"hostname":"header"}`)
	got := mustMergeConfigHeaders(t, metadata, headers)
	want := `{"capacity":{"cpu":4,"memory":"8GiB"},"allocatable":{"cpu":0.5,"memory":"512MiB"},"startup":{"memory":"1GiB"}}`
	if got[sandboxcfg.NsResource] != want {
		t.Fatalf("resource header merge = %s, want %s", got[sandboxcfg.NsResource], want)
	}
	if got[sandboxcfg.NsNetwork] != `{"hostname":"header"}` {
		t.Fatalf("non-resource header was not whole-namespace: %+v", got)
	}
	if _, err := mergeConfigHeaders(
		map[string]string{sandboxcfg.NsResource: `{"control":{}}`},
		header("X-Kuasar-Sandbox-Resource", `{"capacity":{"cpu":4}}`),
	); err == nil || !strings.Contains(err.Error(), "control") {
		t.Fatalf("valid header hid invalid metadata resource: %v", err)
	}
}

func TestMergeConfigHeadersMergesTrafficLeavesStrictly(t *testing.T) {
	metadata := map[string]string{
		sandboxcfg.NsTraffic: `{"max_inflight":{"total":32,"exec":4,"forward":7}}`,
	}
	headers := http.Header{}
	headers.Set("X-Kuasar-Sandbox-Traffic", `{"max_inflight":{"exec":0,"forward":2}}`)
	got := mustMergeConfigHeaders(t, metadata, headers)
	want := `{"max_inflight":{"total":32,"forward":2,"exec":0}}`
	if got[sandboxcfg.NsTraffic] != want {
		t.Fatalf("traffic header merge = %s, want %s", got[sandboxcfg.NsTraffic], want)
	}

	for name, values := range map[string][]string{
		"duplicate": {`{"max_inflight":{"total":1}}`, `{"max_inflight":{"total":2}}`},
		"empty":     {""},
		"null":      {`{"max_inflight":{"total":null}}`},
		"unknown":   {`{"max_inflight":{"future":1}}`},
	} {
		t.Run(name, func(t *testing.T) {
			h := http.Header{"X-Kuasar-Sandbox-Traffic": values}
			if _, err := mergeCreateConfigHeaders(metadata, h); err == nil {
				t.Fatalf("traffic header %s was accepted", name)
			}
		})
	}
}

func mustMergeConfigHeaders(t *testing.T, meta map[string]string, headers http.Header) map[string]string {
	t.Helper()
	got, err := mergeConfigHeaders(meta, headers)
	if err != nil {
		t.Fatalf("mergeConfigHeaders: %v", err)
	}
	return got
}

func mustMergeCreateConfigHeaders(t *testing.T, meta map[string]string, headers http.Header) map[string]string {
	t.Helper()
	got, err := mergeCreateConfigHeaders(meta, headers)
	if err != nil {
		t.Fatalf("mergeCreateConfigHeaders: %v", err)
	}
	return got
}

func TestMergeBuildConfigHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("X-Kuasar-Sandbox-Builder", `{"referer":{"enabled":false}}`)
	h.Set("X-Kuasar-Sandbox-Network", `{"hostname":"build"}`)
	h.Set("X-Kuasar-Sandbox-Credentials", `{"envd_access_token":"must-not-enter-build"}`)
	h.Set(checkpointHeader, `{"merge_ref":false}`)
	got, err := mergeBuildConfigHeaders(nil, h)
	if err != nil {
		t.Fatal(err)
	}
	if got[buildcfg.NsBuilder] != `{"referer":{"enabled":false}}` {
		t.Fatalf("builder header not normalized: %+v", got)
	}
	if got[sandboxcfg.NsNetwork] != `{"hostname":"build"}` {
		t.Fatalf("sandbox build header not normalized: %+v", got)
	}
	if got[sandboxcfg.NsCredentials] != `{"envd_access_token":"must-not-enter-build"}` {
		t.Fatalf("credentials header was not forwarded to core extraction: %+v", got)
	}
	if got[sandboxcfg.NsCheckpoint] != `{"merge_ref":false}` {
		t.Fatalf("checkpoint header was not normalized for build registration: %+v", got)
	}
	if _, err := mergeBuildConfigHeaders(
		map[string]string{buildcfg.NsBuilder: `{"resources":{"cpu":2},"future":true}`},
		header(builderHeader, `{"resources":{"cpu":2,"memory":"2GiB"}}`),
	); err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("valid Builder header hid invalid metadata definition: %v", err)
	}
}

func TestBuildRegisterCarriesMMDSHeaderSeparatelyFromMetadata(t *testing.T) {
	var got RegisterSpec
	core := &buildMMDSCoreStub{register: func(_ context.Context, _ string, spec RegisterSpec) (*types.Build, error) {
		got = spec
		return &types.Build{TemplateID: "transient-template", BuildID: "build", Profile: types.ProfileE2B}, nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	body := `{"name":"mmds","cpuCount":2,"memoryMB":2048,"metadata":{"kuasar-sandbox.mmds":"{\"routes\":[{\"path\":\"/data\",\"data\":\"value\"}]}"}}`
	headers := http.Header{}
	headers.Set(mmdsHeader, `{"secrets":{}}`)
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v3/templates", strings.NewReader(body), headers)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d", response.Code)
	}
	if got.MMDSHeader == nil || *got.MMDSHeader != `{"secrets":{}}` || got.Metadata[sandboxcfg.NsMMDS] == "" {
		t.Fatal("Build Register did not preserve separate MMDS carriers")
	}
}

func TestBuildRegisterResourcesRemainIndependentFromSandboxResourceHeader(t *testing.T) {
	var got RegisterSpec
	core := &buildMMDSCoreStub{register: func(_ context.Context, _ string, spec RegisterSpec) (*types.Build, error) {
		got = spec
		return &types.Build{TemplateID: "transient-template", BuildID: "build", Profile: types.ProfileE2B}, nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	headers := http.Header{}
	headers.Set("X-Kuasar-Sandbox-Resource", `{"capacity":{"memory":"1GiB"},"allocatable":{"memory":"256MiB"},"startup":{"memory":"512MiB"}}`)
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v3/templates",
		strings.NewReader(`{"name":"resources","cpuCount":4,"memoryMB":8192,"metadata":{"kuasar-sandbox.resource":"{\"capacity\":{\"cpu\":2},\"allocatable\":{\"cpu\":0.5}}"}}`), headers)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	want := `{"capacity":{"cpu":2,"memory":"1GiB"},"allocatable":{"cpu":0.5,"memory":"256MiB"},"startup":{"memory":"512MiB"}}`
	if got.Metadata[sandboxcfg.NsResource] != want {
		t.Fatalf("registered resource = %s, want %s", got.Metadata[sandboxcfg.NsResource], want)
	}
	if got.Resources.CPU != 4000 || got.Resources.Memory != 8192<<20 {
		t.Fatalf("build resources = %+v", got.Resources)
	}
}

func TestBuildRegisterCanonicalizesAliasesAndAssertsBuilderHeader(t *testing.T) {
	var got RegisterSpec
	core := &buildMMDSCoreStub{register: func(_ context.Context, _ string, spec RegisterSpec) (*types.Build, error) {
		got = spec
		return &types.Build{
			TemplateID: "transient-template", BuildID: "build", Profile: types.ProfileE2B,
			Builder: spec.Builder,
		}, nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	headers := header(builderHeader, `{"target":{"kind":"sandbox","memory":false},"resources":{"cpu":2,"memory":"2GiB","storage":"64GiB"},"referer":{"enabled":false}}`)
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v3/templates",
		strings.NewReader(`{"cpuCount":2,"cpu_count":2.0,"memoryMB":2048,"memory_mb":2048,"extension":null}`), headers)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if got.Resources != (types.BuildResources{CPU: 2000, Memory: 2 << 30, Storage: 64 << 30}) {
		t.Fatalf("canonical resources = %+v", got.Resources)
	}
	if got.Builder.Resources != nil || got.Builder.Referer == nil || got.Builder.Target == nil ||
		got.Builder.Target.Kind != types.BuildTargetSandbox || got.Builder.Target.Memory {
		t.Fatalf("builder resources were not extracted from build-only options: %+v", got.Builder)
	}
	if _, exists := got.Metadata[buildcfg.NsBuilder]; exists {
		t.Fatalf("builder namespace leaked into template metadata: %+v", got.Metadata)
	}
	var responseBody struct {
		Target *types.BuildTarget `json:"target"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &responseBody); err != nil {
		t.Fatal(err)
	}
	if responseBody.Target == nil || *responseBody.Target != *got.Builder.Target {
		t.Fatalf("register response target = %+v, want %+v", responseBody.Target, got.Builder.Target)
	}
}

func TestBuildRegisterStrictResourceBoundaryRejectsBeforeCore(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		headers http.Header
	}{
		{name: "duplicate body key", body: `{"cpuCount":2,"cpuCount":3,"memoryMB":2048}`},
		{name: "null body leaf", body: `{"cpuCount":null,"memoryMB":2048}`},
		{name: "array body", body: `[]`},
		{name: "scalar body", body: `1`},
		{name: "trailing body", body: `{"cpuCount":2,"memoryMB":2048} {}`},
		{name: "zero body CPU", body: `{"cpuCount":0,"memoryMB":2048}`},
		{name: "zero body memory", body: `{"cpuCount":2,"memoryMB":0}`},
		{name: "alias CPU conflict", body: `{"cpuCount":2,"cpu_count":3,"memoryMB":2048}`},
		{name: "alias memory conflict", body: `{"cpuCount":2,"memoryMB":2048,"memory_mb":1024}`},
		{name: "body header CPU conflict", body: `{"cpuCount":2,"memoryMB":2048}`, headers: header(builderHeader, `{"resources":{"cpu":3}}`)},
		{name: "body header memory conflict", body: `{"cpuCount":2,"memoryMB":2048}`, headers: header(builderHeader, `{"resources":{"memory":"1GiB"}}`)},
		{name: "empty builder header", body: `{"cpuCount":2,"memoryMB":2048}`, headers: header(builderHeader, ``)},
		{name: "null builder header", body: `{"cpuCount":2,"memoryMB":2048}`, headers: header(builderHeader, `null`)},
		{name: "unknown builder field", body: `{"cpuCount":2,"memoryMB":2048}`, headers: header(builderHeader, `{"future":true}`)},
		{name: "duplicate builder leaf", body: `{"cpuCount":2,"memoryMB":2048}`, headers: header(builderHeader, `{"resources":{"cpu":2,"cpu":2}}`)},
		{name: "null builder leaf", body: `{"cpuCount":2,"memoryMB":2048}`, headers: header(builderHeader, `{"resources":{"storage":null}}`)},
		{name: "nonfinite JSON CPU", body: `{"cpuCount":NaN,"memoryMB":2048}`},
		{name: "overflow CPU", body: `{"cpuCount":1e9999,"memoryMB":2048}`},
		{name: "overflow memory", body: `{"cpuCount":2,"memoryMB":999999999999999999999999999999}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			core := &buildMMDSCoreStub{register: func(context.Context, string, RegisterSpec) (*types.Build, error) {
				called = true
				return &types.Build{}, nil
			}}
			handler, apiKey := newMigrationTestHandler(t, core)
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v3/templates",
				strings.NewReader(test.body), test.headers)
			if response.Code != http.StatusBadRequest || called {
				t.Fatalf("status=%d called=%t body=%s", response.Code, called, response.Body.String())
			}
		})
	}
}

func TestBuildRegisterPreservesEstablishedUnknownEnvelopeCompatibility(t *testing.T) {
	called := false
	core := &buildMMDSCoreStub{register: func(context.Context, string, RegisterSpec) (*types.Build, error) {
		called = true
		return &types.Build{TemplateID: "transient-template", BuildID: "build", Profile: types.ProfileE2B}, nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v3/templates",
		strings.NewReader(`{"cpuCount":2,"memoryMB":2048,"team_id":"existing-sdk-envelope"}`), nil)
	if response.Code != http.StatusAccepted || !called {
		t.Fatalf("status=%d called=%t body=%s", response.Code, called, response.Body.String())
	}
}

func TestBuildRegisterDoesNotDropMalformedResourceMetadata(t *testing.T) {
	called := false
	core := &buildMMDSCoreStub{register: func(context.Context, string, RegisterSpec) (*types.Build, error) {
		called = true
		return &types.Build{}, nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v3/templates",
		strings.NewReader(`{"metadata":{"kuasar-sandbox.resource":[]}}`), nil)
	if response.Code != http.StatusBadRequest || called {
		t.Fatalf("status=%d called=%t body=%s", response.Code, called, response.Body.String())
	}
}

func TestBuildFirstClassCapacityRejectsNegativeValues(t *testing.T) {
	for _, endpoint := range []struct {
		name string
		path string
		body string
	}{
		{name: "register cpu", path: "/v3/templates", body: `{"name":"bad","cpuCount":-1}`},
		{name: "register memory", path: "/v3/templates", body: `{"name":"bad","memoryMB":-1}`},
		{name: "trigger cpu", path: "/v2/templates/template/builds/build", body: `{"cpuCount":-1}`},
		{name: "trigger memory", path: "/v2/templates/template/builds/build", body: `{"memoryMB":-1}`},
	} {
		t.Run(endpoint.name, func(t *testing.T) {
			called := false
			core := &buildMMDSCoreStub{
				register: func(context.Context, string, RegisterSpec) (*types.Build, error) {
					called = true
					return &types.Build{}, nil
				},
				trigger: func(context.Context, string, string, string, TriggerSpec, BuildAuth) error {
					called = true
					return nil
				},
			}
			handler, apiKey := newMigrationTestHandler(t, core)
			response := migrationRequest(t, handler, apiKey, http.MethodPost, endpoint.path, strings.NewReader(endpoint.body), nil)
			if response.Code != http.StatusBadRequest || called {
				t.Fatalf("status=%d called=%t body=%s", response.Code, called, response.Body.String())
			}
		})
	}
}

func TestBuildTriggerRejectsMMDSBeforeCore(t *testing.T) {
	called := false
	core := &buildMMDSCoreStub{trigger: func(context.Context, string, string, string, TriggerSpec, BuildAuth) error {
		called = true
		return nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	for _, test := range []struct {
		name    string
		body    string
		headers http.Header
	}{
		{name: "header", body: `{}`, headers: header(mmdsHeader, "")},
		{name: "metadata", body: `{"metadata":{"kuasar-sandbox.mmds":"{\"routes\":[]}"}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v2/templates/template/builds/build", strings.NewReader(test.body), test.headers)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d", response.Code)
			}
		})
	}
	if called {
		t.Fatal("Build Trigger MMDS override reached Core")
	}
}

func TestBuildTriggerRejectsGenericConfigButKeepsResourceAssertions(t *testing.T) {
	called := false
	var got TriggerSpec
	core := &buildMMDSCoreStub{trigger: func(_ context.Context, _ string, _ string, _ string, spec TriggerSpec, _ BuildAuth) error {
		called = true
		got = spec
		return nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	for _, test := range []struct {
		name    string
		body    string
		headers http.Header
	}{
		{name: "body metadata", body: `{"metadata":{"kuasar-sandbox.resource":"{}"}}`},
		{name: "resource header", body: `{}`, headers: header("X-Kuasar-Sandbox-Resource", `{}`)},
		{name: "builder header", body: `{}`, headers: header(builderHeader, `{"referer":{"enabled":false}}`)},
		{name: "restore header", body: `{}`, headers: header(restoreHeader, `{}`)},
		{name: "unknown sandbox config header", body: `{}`, headers: header("X-Kuasar-Sandbox-Future", `{}`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v2/templates/template/builds/build", strings.NewReader(test.body), test.headers)
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "deprecated") {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
			}
		})
	}
	if called {
		t.Fatal("deprecated trigger config reached Core")
	}

	headers := make(http.Header)
	headers.Set(clusterGroupHeader, "/g")
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/v2/templates/template/builds/build",
		strings.NewReader(`{"cpuCount":4,"memoryMB":8192}`), headers)
	if response.Code != http.StatusAccepted || !called {
		t.Fatalf("compatible trigger status = %d, called=%t, body=%s", response.Code, called, response.Body.String())
	}
	if got.ResourceAssertion.CPU == nil || *got.ResourceAssertion.CPU != 4000 ||
		got.ResourceAssertion.Memory == nil || *got.ResourceAssertion.Memory != 8192<<20 {
		t.Fatalf("compatible trigger assertion = %+v", got.ResourceAssertion)
	}
}

func TestBuildTriggerStateConflictResponse(t *testing.T) {
	states := []types.BuildState{
		types.BuildWaiting,
		types.BuildBuilding,
		types.BuildReady,
		types.BuildError,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			called := false
			core := &buildMMDSCoreStub{trigger: func(context.Context, string, string, string, TriggerSpec, BuildAuth) error {
				called = true
				return &BuildStateConflictError{State: state}
			}}
			handler, apiKey := newMigrationTestHandler(t, core)
			response := migrationRequest(t, handler, apiKey, http.MethodPost,
				"/v2/templates/template/builds/build", strings.NewReader(`{"force":true}`), nil)
			if !called {
				t.Fatal("trigger did not reach Core")
			}
			if response.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409; body=%s", response.Code, response.Body.String())
			}
			if got := response.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", got)
			}
			var body map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			wantMessage := "build cannot be triggered from state " + string(state)
			if len(body) != 2 || body["message"] != wantMessage || body["state"] != string(state) {
				t.Fatalf("response = %#v, want message=%q state=%q", body, wantMessage, state)
			}
		})
	}
}

func TestCreateCheckpointHeaderOverlaysMetadataPerField(t *testing.T) {
	var got CreateReq
	core := &checkpointCoreStub{create: func(_ context.Context, req CreateReq) (*types.Sandbox, error) {
		got = req
		return &types.Sandbox{ID: "created", Profile: types.ProfileBare, State: types.StateStarting}, nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	body := `{"templateID":"bare:img:manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","metadata":{"kuasar-sandbox.checkpoint":"{\"merge_ref\":true,\"drop_caches\":false}"}}`
	headers := http.Header{}
	headers.Set(checkpointHeader, `{"merge_ref":false,"drop_caches":null}`)
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes", strings.NewReader(body), headers)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	if got.Metadata[sandboxcfg.NsCheckpoint] != `{"merge_ref":false,"drop_caches":false}` {
		t.Fatalf("merged checkpoint metadata = %+v", got.Metadata)
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if _, exposed := payload["state"]; exposed {
		t.Fatalf("Create response exposed internal starting state: %+v", payload)
	}
}

func TestCreateCheckpointHeaderValidation(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		header string
	}{
		{name: "empty header", body: `{}`, header: ""},
		{name: "unknown header field", body: `{}`, header: `{"unknown":true}`},
		{name: "wrong header type", body: `{}`, header: `{"merge_ref":"false"}`},
		{name: "trailing header value", body: `{}`, header: `{} {}`},
		{name: "malformed body policy", body: `{"metadata":{"kuasar-sandbox.checkpoint":"not-json"}}`, header: `{}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			core := &checkpointCoreStub{create: func(context.Context, CreateReq) (*types.Sandbox, error) {
				called = true
				return &types.Sandbox{}, nil
			}}
			handler, apiKey := newMigrationTestHandler(t, core)
			headers := http.Header{}
			headers.Set(checkpointHeader, tc.header)
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes", strings.NewReader(tc.body), headers)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
			}
			if called {
				t.Fatal("invalid checkpoint header reached Core.Create")
			}
		})
	}
}

func TestCreateCheckpointHeaderAbsentAndEmptyPolicy(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		headers     http.Header
		wantPresent bool
		wantRaw     string
	}{
		{name: "absent", body: `{}`, headers: http.Header{}},
		{name: "body canonicalized", body: `{"metadata":{"kuasar-sandbox.checkpoint":" { \"merge_ref\" : false } "}}`,
			headers: http.Header{}, wantPresent: true, wantRaw: `{"merge_ref":false}`},
		{name: "body all null removed", body: `{"metadata":{"kuasar-sandbox.checkpoint":"{\"merge_ref\":null}"}}`, headers: http.Header{}},
		{name: "empty object", body: `{}`, headers: header(checkpointHeader, `{}`)},
		{name: "all null", body: `{}`, headers: header(checkpointHeader, `{"merge_ref":null,"drop_caches":null}`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			core := &checkpointCoreStub{create: func(_ context.Context, req CreateReq) (*types.Sandbox, error) {
				raw, present := req.Metadata[sandboxcfg.NsCheckpoint]
				if present != tc.wantPresent {
					t.Fatalf("checkpoint namespace present=%t, want %t: %+v", present, tc.wantPresent, req.Metadata)
				}
				if raw != tc.wantRaw {
					t.Fatalf("checkpoint metadata = %q, want %q", raw, tc.wantRaw)
				}
				return &types.Sandbox{ID: "created", Profile: types.ProfileBare}, nil
			}}
			handler, apiKey := newMigrationTestHandler(t, core)
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes", strings.NewReader(tc.body), tc.headers)
			if response.Code != http.StatusCreated {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestCreateCheckpointBodyValidationBeforeCore(t *testing.T) {
	called := false
	core := &checkpointCoreStub{create: func(context.Context, CreateReq) (*types.Sandbox, error) {
		called = true
		return &types.Sandbox{}, nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes",
		strings.NewReader(`{"metadata":{"kuasar-sandbox.checkpoint":"{\"unknown\":true}"}}`), nil)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
	}
	if called {
		t.Fatal("invalid checkpoint body metadata reached Core.Create")
	}
}

func TestCreatePreservesAutoPauseMemoryPresence(t *testing.T) {
	for _, test := range []struct {
		name        string
		body        string
		wantPresent bool
		wantValue   bool
	}{
		{name: "missing", body: `{}`},
		{name: "null", body: `{"autoPauseMemory":null}`},
		{name: "true", body: `{"autoPauseMemory":true}`, wantPresent: true, wantValue: true},
		{name: "false", body: `{"autoPauseMemory":false}`, wantPresent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			core := &checkpointCoreStub{create: func(_ context.Context, req CreateReq) (*types.Sandbox, error) {
				if (req.AutoPauseMemory != nil) != test.wantPresent {
					t.Fatalf("AutoPauseMemory presence = %t, want %t", req.AutoPauseMemory != nil, test.wantPresent)
				}
				if req.AutoPauseMemory != nil && *req.AutoPauseMemory != test.wantValue {
					t.Fatalf("AutoPauseMemory = %t, want %t", *req.AutoPauseMemory, test.wantValue)
				}
				return &types.Sandbox{ID: "created", Profile: types.ProfileBare}, nil
			}}
			handler, apiKey := newMigrationTestHandler(t, core)
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes", strings.NewReader(test.body), nil)
			if response.Code != http.StatusCreated {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestCreateStrictlyBoundsOwnedFieldsAndAllowsUpstreamExtensions(t *testing.T) {
	calls := 0
	core := &checkpointCoreStub{create: func(_ context.Context, req CreateReq) (*types.Sandbox, error) {
		calls++
		return &types.Sandbox{ID: "created", Profile: types.ProfileBare}, nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)

	accepted := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes", strings.NewReader(
		`{"templateID":"template","autoPause":true,"autoResume":{"enabled":false},"network":{},"iam":null}`,
	), nil)
	if accepted.Code != http.StatusCreated || calls != 1 {
		t.Fatalf("upstream extension body = %d %q calls=%d", accepted.Code, accepted.Body.String(), calls)
	}

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "duplicate autoPauseMemory", body: `{"autoPauseMemory":true,"autoPauseMemory":false}`},
		{name: "malformed autoPauseMemory", body: `{"autoPauseMemory":"false"}`},
		{name: "trailing value", body: `{ } { }`},
		{name: "array", body: `[]`},
		{name: "top-level null", body: `null`},
		{name: "empty", body: ``},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := calls
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes", strings.NewReader(test.body), nil)
			if response.Code != http.StatusBadRequest || calls != before {
				t.Fatalf("response=%d %q calls=%d, want 400/no Core call", response.Code, response.Body.String(), calls)
			}
		})
	}

	oversized := strings.Repeat(" ", maxCreateRequestBytes+1)
	request := httptest.NewRequest(http.MethodPost, "/sandboxes", nil)
	request.Body = io.NopCloser(strings.NewReader(oversized))
	request.ContentLength = -1
	request.Header.Set("X-API-KEY", apiKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge || calls != 1 {
		t.Fatalf("chunked oversized response=%d %q calls=%d", response.Code, response.Body.String(), calls)
	}
}

func TestPauseRequestPolicyAndStatus(t *testing.T) {
	boolPtr := func(value bool) *bool { return &value }
	tests := []struct {
		name       string
		body       string
		header     *string
		want       sandboxcfg.CaptureRequest
		coreErr    error
		wantStatus int
		wantCalls  int
	}{
		{name: "empty body", body: "", want: sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "top-level null", body: `null`, want: sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "empty object", body: `{}`, want: sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "memory null", body: `{"memory":null}`, want: sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "memory true", body: `{"memory":true}`, want: sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "memory false", body: `{"memory":false}`, want: sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "body values", body: `{"checkpoint_merge_ref":false,"checkpoint_drop_caches":true}`,
			want: sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, SnapshotPolicy: sandboxcfg.SnapshotPolicy{MergeRef: boolPtr(false), DropCaches: boolPtr(true)}}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "body null inherits", body: `{"checkpoint_merge_ref":null,"checkpoint_drop_caches":false}`,
			want: sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, SnapshotPolicy: sandboxcfg.SnapshotPolicy{DropCaches: boolPtr(false)}}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "already paused", body: `{}`, want: sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, coreErr: ErrAlreadyPaused, wantStatus: http.StatusConflict, wantCalls: 1},
		{name: "starting", body: `{}`, want: sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, coreErr: ErrSandboxStarting, wantStatus: http.StatusConflict, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			core := &checkpointCoreStub{pause: func(_ context.Context, _, _ string, got sandboxcfg.CaptureRequest) error {
				calls++
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("Pause override = %+v, want %+v", got, tc.want)
				}
				return tc.coreErr
			}}
			handler, apiKey := newMigrationTestHandler(t, core)
			headers := http.Header{}
			if tc.header != nil {
				headers.Set(checkpointHeader, *tc.header)
			}
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes/sid/pause", strings.NewReader(tc.body), headers)
			if response.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, tc.wantStatus, response.Body.String())
			}
			if calls != tc.wantCalls {
				t.Fatalf("Core.Pause calls = %d, want %d", calls, tc.wantCalls)
			}
		})
	}
}

func TestPauseCheckpointHeaderOverlaysBodyPerField(t *testing.T) {
	headers := http.Header{}
	headers.Set(checkpointHeader, `{"merge_ref":false,"drop_caches":null}`)
	core := &checkpointCoreStub{pause: func(_ context.Context, _, _ string, got sandboxcfg.CaptureRequest) error {
		if got.Kind != types.CaptureSnapshot || got.SnapshotPolicy.MergeRef == nil || *got.SnapshotPolicy.MergeRef ||
			got.SnapshotPolicy.DropCaches == nil || *got.SnapshotPolicy.DropCaches {
			t.Fatalf("merged action override = %+v, want merge=false drop=false", got)
		}
		return nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes/sid/pause",
		strings.NewReader(`{"checkpoint_merge_ref":true,"checkpoint_drop_caches":false}`), headers)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
}

func TestPauseRejectsInvalidRequestsBeforeCore(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		header *string
	}{
		{name: "memory false with merge", body: `{"memory":false,"checkpoint_merge_ref":true}`},
		{name: "memory false with drop", body: `{"memory":false,"checkpoint_drop_caches":false}`},
		{name: "memory false with null merge", body: `{"memory":false,"checkpoint_merge_ref":null}`},
		{name: "memory false with null drop", body: `{"memory":false,"checkpoint_drop_caches":null}`},
		{name: "memory false with null header field", body: `{"memory":false}`, header: stringPtr(`{"merge_ref":null}`)},
		{name: "malformed body", body: `{`},
		{name: "unknown body field", body: `{"future":true}`},
		{name: "wrong memory type", body: `{"memory":"true"}`},
		{name: "wrong policy type", body: `{"checkpoint_merge_ref":0}`},
		{name: "second body value", body: `{} {}`},
		{name: "empty header", body: `{}`, header: stringPtr("")},
		{name: "unknown header field", body: `{}`, header: stringPtr(`{"unknown":true}`)},
		{name: "wrong header type", body: `{}`, header: stringPtr(`{"drop_caches":[]}`)},
		{name: "second header value", body: `{}`, header: stringPtr(`{} {}`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			core := &checkpointCoreStub{pause: func(context.Context, string, string, sandboxcfg.CaptureRequest) error {
				calls++
				return nil
			}}
			handler, apiKey := newMigrationTestHandler(t, core)
			headers := http.Header{}
			if tc.header != nil {
				headers.Set(checkpointHeader, *tc.header)
			}
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes/sid/pause", strings.NewReader(tc.body), headers)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
			}
			if calls != 0 {
				t.Fatalf("invalid request called Core.Pause %d times", calls)
			}
		})
	}
}

func TestPauseRejectsOversizedBodyBeforeCore(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		contentLength int64
	}{
		{name: "declared length", body: `{}`, contentLength: maxPauseRequestBytes + 1},
		{name: "streamed", body: `{}` + strings.Repeat(" ", maxPauseRequestBytes-1), contentLength: -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			core := &checkpointCoreStub{pause: func(context.Context, string, string, sandboxcfg.CaptureRequest) error {
				calls++
				return nil
			}}
			handler, apiKey := newMigrationTestHandler(t, core)
			request := httptest.NewRequest(http.MethodPost, "/sandboxes/sid/pause", strings.NewReader(tc.body))
			request.Header.Set("X-API-KEY", apiKey)
			request.ContentLength = tc.contentLength
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d, want 413; body=%s", response.Code, response.Body.String())
			}
			if calls != 0 {
				t.Fatalf("oversized request called Core.Pause %d times", calls)
			}
		})
	}
}

func stringPtr(value string) *string { return &value }

func TestSandboxResponseUsesProfileSpecificCredentials(t *testing.T) {
	a := &API{domain: "example.test"}
	sandbox := &types.Sandbox{
		ID: "e2b", Profile: types.ProfileE2B, EnvdAccessToken: "envd",
		TrafficAccessToken: "traffic", ForwardAccessToken: "forward",
	}
	e2b := a.sandboxResp(sandbox)
	if e2b["envdAccessToken"] != "envd" || e2b["trafficAccessToken"] != "traffic" || e2b["forwardAccessToken"] != "forward" {
		t.Fatalf("e2b response credentials = %+v", e2b)
	}
	detail := a.sandboxDetail(sandbox)
	for _, field := range []string{"envdAccessToken", "trafficAccessToken", "forwardAccessToken"} {
		if _, found := detail[field]; found {
			t.Fatalf("sandbox detail exposed %s: %+v", field, detail)
		}
	}
	bare := a.sandboxResp(&types.Sandbox{ID: "bare", Profile: types.ProfileBare, ForwardAccessToken: "forward"})
	if bare["forwardAccessToken"] != "forward" {
		t.Fatalf("bare response forward credential = %+v", bare)
	}
	if _, ok := bare["envdAccessToken"]; ok {
		t.Fatalf("bare response exposed envdAccessToken: %+v", bare)
	}
	if _, ok := bare["trafficAccessToken"]; ok {
		t.Fatalf("bare response exposed trafficAccessToken: %+v", bare)
	}
}

func TestSandboxDetailUsesISO8601Timestamps(t *testing.T) {
	a := &API{domain: "example.test", res: Resources{VCPU: 2, MemoryMB: 512, DiskMB: 2048}}
	created := int64(1_700_000_000)
	sandbox := &types.Sandbox{ID: "sandbox", CreatedUnix: created}

	detail := a.sandboxDetail(sandbox)
	for field, want := range map[string]int{
		"cpuCount":   2,
		"memoryMB":   512,
		"diskSizeMB": 2048,
	} {
		if got := detail[field]; got != want {
			t.Errorf("detail %s = %v, want %d", field, got, want)
		}
	}
	startedAt, ok := detail["startedAt"].(string)
	if !ok {
		t.Fatalf("startedAt type = %T, want string", detail["startedAt"])
	}
	endAt, ok := detail["endAt"].(string)
	if !ok {
		t.Fatalf("endAt type = %T, want string", detail["endAt"])
	}

	want := time.Unix(created, 0).UTC().Format(time.RFC3339)
	if startedAt != want || endAt != want {
		t.Fatalf("detail timestamps = startedAt %q, endAt %q; want %q for both", startedAt, endAt, want)
	}

	deadline := created + 300
	sandbox.DeadlineUnix = deadline
	detail = a.sandboxDetail(sandbox)
	wantEnd := time.Unix(deadline, 0).UTC().Format(time.RFC3339)
	if got := detail["endAt"]; got != wantEnd {
		t.Fatalf("detail endAt = %v, want %q", got, wantEnd)
	}
}

func TestListedSandboxIncludesE2BDetailFields(t *testing.T) {
	a := &API{res: Resources{VCPU: 4, MemoryMB: 1024, DiskMB: 4096}}
	sandbox := &types.Sandbox{ID: "sandbox", CreatedUnix: 1_700_000_000, DeadlineUnix: 1_700_000_300}

	listed := a.listed(sandbox)
	for field, want := range map[string]int{
		"cpuCount":   4,
		"memoryMB":   1024,
		"diskSizeMB": 4096,
	} {
		if got := listed[field]; got != want {
			t.Errorf("listed %s = %v, want %d", field, got, want)
		}
	}
	for _, field := range []string{"startedAt", "endAt"} {
		value, ok := listed[field].(string)
		if !ok {
			t.Fatalf("listed %s type = %T, want string", field, listed[field])
		}
		if _, err := time.Parse(time.RFC3339, value); err != nil {
			t.Errorf("listed %s = %q is not RFC3339: %v", field, value, err)
		}
	}
}

func TestSandboxDetailAndListHTTPContract(t *testing.T) {
	created := int64(1_700_000_000)
	for _, deadlineCase := range []struct {
		name     string
		deadline int64
	}{
		{name: "without deadline", deadline: 0},
		{name: "with deadline", deadline: created + 300},
	} {
		t.Run(deadlineCase.name, func(t *testing.T) {
			sandbox := &types.Sandbox{
				ID: "sandbox", TemplateID: "e2b-img-template", Profile: types.ProfileE2B,
				State: types.StateRunning, CreatedUnix: created, DeadlineUnix: deadlineCase.deadline,
			}
			h, apiKey := newSandboxContractHandler(t, &sandboxContractCoreStub{sandbox: sandbox}, Resources{VCPU: 2, MemoryMB: 512, DiskMB: 2048})

			for _, endpoint := range []struct {
				name string
				path string
			}{
				{name: "detail", path: "/sandboxes/sandbox"},
				{name: "list", path: "/v2/sandboxes"},
			} {
				t.Run(endpoint.name, func(t *testing.T) {
					response := migrationRequest(t, h, apiKey, http.MethodGet, endpoint.path, nil, nil)
					if response.Code != http.StatusOK {
						t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
					}

					var body map[string]any
					if endpoint.name == "list" {
						var items []map[string]any
						if err := json.Unmarshal(response.Body.Bytes(), &items); err != nil {
							t.Fatal(err)
						}
						if len(items) != 1 {
							t.Fatalf("list returned %d items, want 1", len(items))
						}
						body = items[0]
					} else if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
						t.Fatal(err)
					}

					for field, want := range map[string]float64{
						"cpuCount":   2,
						"memoryMB":   512,
						"diskSizeMB": 2048,
					} {
						if got := body[field]; got != want {
							t.Errorf("%s = %v, want %v", field, got, want)
						}
					}
					for _, field := range []string{"startedAt", "endAt"} {
						value, ok := body[field].(string)
						if !ok {
							t.Fatalf("%s type = %T, want string", field, body[field])
						}
						if _, err := time.Parse(time.RFC3339, value); err != nil {
							t.Errorf("%s = %q is not RFC3339: %v", field, value, err)
						}
					}
					if deadlineCase.deadline == 0 && body["startedAt"] != body["endAt"] {
						t.Errorf("no-deadline endAt = %v, want startedAt %v", body["endAt"], body["startedAt"])
					}
					if deadlineCase.deadline != 0 {
						wantEnd := time.Unix(deadlineCase.deadline, 0).UTC().Format(time.RFC3339)
						if body["endAt"] != wantEnd {
							t.Errorf("deadline endAt = %v, want %s", body["endAt"], wantEnd)
						}
					}
				})
			}
		})
	}
}

func TestImportSandboxPassesOptionalTargetID(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantTarget string
	}{
		{name: "omitted", body: `{"token":"kmt1.token"}`},
		{name: "explicit", body: `{"token":"kmt1.token","sandboxID":"target-sandbox"}`, wantTarget: "target-sandbox"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotToken, gotTarget string
			core := &migrationCoreStub{importSandbox: func(_ context.Context, _, token, targetID string) (string, error) {
				gotToken, gotTarget = token, targetID
				return "imported-sandbox", nil
			}}
			h, apiKey := newMigrationTestHandler(t, core)
			response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/import", strings.NewReader(tt.body), nil)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
			}
			if gotToken != "kmt1.token" || gotTarget != tt.wantTarget {
				t.Fatalf("ImportSandbox token=%q targetID=%q, want token=%q targetID=%q", gotToken, gotTarget, "kmt1.token", tt.wantTarget)
			}
			var body map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body["sandboxID"] != "imported-sandbox" {
				t.Fatalf("response sandboxID = %q, want imported-sandbox", body["sandboxID"])
			}
		})
	}
}

func TestImportSandboxAcceptsMaximumTokenAndTarget(t *testing.T) {
	token := strings.Repeat("a", migrationtoken.MaxWireSize)
	targetID := strings.Repeat("a", types.MaxLocalSandboxIDBytes)
	// Match common serializer output rather than relying on compact JSON: spaces
	// around separators must not reduce either field's documented maximum.
	payload := []byte(fmt.Sprintf(`{"token": %q, "sandboxID": %q}`, token, targetID))
	if len(payload) > maxImportRequestBytes {
		t.Fatalf("maximum contract request is %d bytes, handler limit is %d", len(payload), maxImportRequestBytes)
	}

	called := false
	core := &migrationCoreStub{importSandbox: func(_ context.Context, _, gotToken, gotTarget string) (string, error) {
		called = true
		if gotToken != token || gotTarget != targetID {
			t.Fatal("ImportSandbox did not receive the maximum contract unchanged")
		}
		return targetID, nil
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/import", bytes.NewReader(payload), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	if !called {
		t.Fatal("ImportSandbox was not called")
	}
}

func TestExportSandboxMapsTokenTooLarge(t *testing.T) {
	core := &migrationCoreStub{exportSandbox: func(context.Context, string, string, bool, bool) (types.ExportResult, error) {
		return types.ExportResult{}, fmt.Errorf("private export detail: %w", migrationtoken.ErrTokenTooLarge)
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/sandbox/export", strings.NewReader(`{}`), nil)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusRequestEntityTooLarge, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "private export detail") {
		t.Fatalf("response exposed internal detail: %s", response.Body.String())
	}
}

func TestExportSandboxMapsPreemptedResumeToConflict(t *testing.T) {
	core := &migrationCoreStub{exportSandbox: func(context.Context, string, string, bool, bool) (types.ExportResult, error) {
		return types.ExportResult{}, fmt.Errorf("private export detail: %w", ErrExportPreempted)
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/sandbox/export", strings.NewReader(`{}`), nil)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusConflict, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), ErrExportPreempted.Error()) || strings.Contains(response.Body.String(), "private export detail") {
		t.Fatalf("preempted export response was not stable and redacted: %s", response.Body.String())
	}
}

func TestImportSandboxMapsAlreadyExistsToConflict(t *testing.T) {
	core := &migrationCoreStub{importSandbox: func(context.Context, string, string, string) (string, error) {
		return "", fmt.Errorf("target occupied: %w", ErrAlreadyExists)
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/import", strings.NewReader(`{"token":"kmt1.token","sandboxID":"target"}`), nil)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusConflict, response.Body.String())
	}
}

func TestImportSandboxMapsIncompatibleTargetToConflict(t *testing.T) {
	core := &migrationCoreStub{importSandbox: func(context.Context, string, string, string) (string, error) {
		return "", fmt.Errorf("target runtime: %w", migrationtoken.ErrIncompatible)
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/import", strings.NewReader(`{"token":"kmt1.token"}`), nil)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusConflict, response.Body.String())
	}
}

func TestMigrationErrorsUseTypedStatusAndRedactedBodies(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{name: "malformed token", err: fmt.Errorf("secret-token-fragment: %w", migrationtoken.ErrMalformedToken), wantStatus: http.StatusBadRequest, wantBody: "invalid migration token"},
		{name: "invalid payload", err: fmt.Errorf("secret-token-fragment: %w", migrationtoken.ErrInvalidPayload), wantStatus: http.StatusBadRequest, wantBody: "invalid migration token"},
		{name: "authentication", err: fmt.Errorf("secret-token-fragment: %w", migrationtoken.ErrAuthentication), wantStatus: http.StatusForbidden, wantBody: "migration credential not allowed"},
		{name: "fingerprint", err: fmt.Errorf("secret-token-fragment: %w", migrationtoken.ErrCredentialMismatch), wantStatus: http.StatusForbidden, wantBody: "migration credential not allowed"},
		{name: "allowlist", err: fmt.Errorf("private-store-detail: %w", ErrNotAllowed), wantStatus: http.StatusForbidden, wantBody: "migration credential not allowed"},
		{name: "incompatible", err: fmt.Errorf("private-runtime-path: %w", migrationtoken.ErrIncompatible), wantStatus: http.StatusConflict, wantBody: "target environment incompatible"},
		{name: "exists", err: fmt.Errorf("private-target-detail: %w", ErrAlreadyExists), wantStatus: http.StatusConflict, wantBody: "target sandbox already exists"},
		{name: "not found", err: fmt.Errorf("private-owner-detail: %w", ErrNotFound), wantStatus: http.StatusNotFound, wantBody: "not found"},
		{name: "bad request", err: fmt.Errorf("invalid target sandbox ID: %w", ErrBadRequest), wantStatus: http.StatusBadRequest, wantBody: "invalid target sandbox ID"},
		{name: "invalid local key", err: fmt.Errorf("private-key-material: %w", migrationtoken.ErrInvalidKeyMaterial), wantStatus: http.StatusInternalServerError, wantBody: "internal error"},
		{name: "store failure", err: errors.New("private-database-path"), wantStatus: http.StatusInternalServerError, wantBody: "internal error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core := &migrationCoreStub{importSandbox: func(context.Context, string, string, string) (string, error) {
				return "", tt.err
			}}
			h, apiKey := newMigrationTestHandler(t, core)
			response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/import", strings.NewReader(`{"token":"kmt1.token"}`), nil)
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, tt.wantStatus, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), tt.wantBody) {
				t.Fatalf("body = %q, want %q", response.Body.String(), tt.wantBody)
			}
			for _, secret := range []string{"secret-token-fragment", "private-store-detail", "private-runtime-path", "private-target-detail", "private-owner-detail", "private-key-material", "private-database-path"} {
				if strings.Contains(response.Body.String(), secret) {
					t.Fatalf("response exposed internal detail %q: %s", secret, response.Body.String())
				}
			}
		})
	}
}

func TestImportSandboxRejectsOversizedMigrationToken(t *testing.T) {
	called := false
	core := &migrationCoreStub{importSandbox: func(context.Context, string, string, string) (string, error) {
		called = true
		return "", nil
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	payload, err := json.Marshal(map[string]string{"token": strings.Repeat("a", migrationtoken.MaxWireSize+1)})
	if err != nil {
		t.Fatal(err)
	}
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/import", bytes.NewReader(payload), nil)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusRequestEntityTooLarge, response.Body.String())
	}
	if called {
		t.Fatal("oversized token reached Core.ImportSandbox")
	}
}

func TestImportSandboxMapsCoreTokenTooLarge(t *testing.T) {
	core := &migrationCoreStub{importSandbox: func(context.Context, string, string, string) (string, error) {
		return "", fmt.Errorf("open token: %w", migrationtoken.ErrTokenTooLarge)
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/import", strings.NewReader(`{"token":"kmt1.token"}`), nil)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusRequestEntityTooLarge, response.Body.String())
	}
}

func TestImportSandboxRejectsOversizedRequestBody(t *testing.T) {
	called := false
	core := &migrationCoreStub{importSandbox: func(context.Context, string, string, string) (string, error) {
		called = true
		return "", nil
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	body := `{"token":"kmt1.token"}` + strings.Repeat(" ", maxImportRequestBytes)
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/import", strings.NewReader(body), nil)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusRequestEntityTooLarge, response.Body.String())
	}
	if called {
		t.Fatal("oversized request body reached Core.ImportSandbox")
	}
}

func TestConnectRejectsOversizedMigrationTokenHeader(t *testing.T) {
	called := false
	core := &migrationCoreStub{connect: func(context.Context, string, string, string, ConnectOptions) (*types.Sandbox, error) {
		called = true
		return nil, nil
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	headers := http.Header{MigrationTokenHeader: []string{strings.Repeat("a", migrationtoken.MaxWireSize+1)}}
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/sandbox/connect", strings.NewReader(`{}`), headers)
	if response.Code != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusRequestHeaderFieldsTooLarge, response.Body.String())
	}
	if called {
		t.Fatal("oversized migration token header reached Core.Connect")
	}
}

func TestConnectPreservesMemoryPresence(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantPresent bool
		wantMemory  bool
		wantTimeout int
	}{
		{name: "empty body"},
		{name: "top-level null", body: `null`},
		{name: "empty object", body: `{}`},
		{name: "memory null", body: `{"memory":null}`},
		{name: "memory true", body: `{"memory":true}`, wantPresent: true, wantMemory: true},
		{name: "memory false", body: `{"memory":false}`, wantPresent: true},
		{name: "timeout and memory", body: `{"timeout":19,"memory":false}`, wantPresent: true, wantTimeout: 19},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			core := &migrationCoreStub{connect: func(_ context.Context, id, _ string, _ string, options ConnectOptions) (*types.Sandbox, error) {
				calls++
				if options.TimeoutSec != test.wantTimeout {
					t.Fatalf("TimeoutSec = %d, want %d", options.TimeoutSec, test.wantTimeout)
				}
				if (options.Memory != nil) != test.wantPresent {
					t.Fatalf("Memory presence = %t, want %t", options.Memory != nil, test.wantPresent)
				}
				if options.Memory != nil && *options.Memory != test.wantMemory {
					t.Fatalf("Memory = %t, want %t", *options.Memory, test.wantMemory)
				}
				return &types.Sandbox{ID: id, Profile: types.ProfileE2B}, nil
			}}
			h, apiKey := newMigrationTestHandler(t, core)
			response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/sandbox/connect", strings.NewReader(test.body), nil)
			if response.Code != http.StatusOK || calls != 1 {
				t.Fatalf("status = %d, calls = %d, body=%s", response.Code, calls, response.Body.String())
			}
		})
	}
}

func TestConnectRejectsAmbiguousOrMalformedBody(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"future":true}`},
		{name: "duplicate memory", body: `{"memory":true,"memory":false}`},
		{name: "wrong memory type", body: `{"memory":"false"}`},
		{name: "malformed", body: `{"memory":`},
		{name: "trailing object", body: `{} {}`},
		{name: "null then trailing", body: `null {}`},
		{name: "array", body: `[]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			core := &migrationCoreStub{connect: func(context.Context, string, string, string, ConnectOptions) (*types.Sandbox, error) {
				called = true
				return nil, nil
			}}
			h, apiKey := newMigrationTestHandler(t, core)
			response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/sandbox/connect", strings.NewReader(test.body), nil)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusBadRequest, response.Body.String())
			}
			if called {
				t.Fatal("invalid body reached Core.Connect")
			}
		})
	}
}

func TestConnectRejectsOversizedRequestBody(t *testing.T) {
	called := false
	core := &migrationCoreStub{connect: func(context.Context, string, string, string, ConnectOptions) (*types.Sandbox, error) {
		called = true
		return nil, nil
	}}
	h, apiKey := newMigrationTestHandler(t, core)
	body := `{}` + strings.Repeat(" ", maxConnectRequestBytes)
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/sandbox/connect", strings.NewReader(body), nil)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusRequestEntityTooLarge, response.Body.String())
	}
	if called {
		t.Fatal("oversized body reached Core.Connect")
	}
}

func TestConnectModeConflictsAreConflictWithOrWithoutMigration(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		token string
	}{
		{name: "memory unavailable", err: types.ErrMemoryUnavailable},
		{name: "launch mode conflict", err: types.ErrLaunchModeConflict},
		{name: "memory unavailable after import", err: types.ErrMemoryUnavailable, token: "kmt1.token"},
		{name: "launch mode conflict after import", err: types.ErrLaunchModeConflict, token: "kmt1.token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			core := &migrationCoreStub{connect: func(context.Context, string, string, string, ConnectOptions) (*types.Sandbox, error) {
				return nil, test.err
			}}
			h, apiKey := newMigrationTestHandler(t, core)
			headers := http.Header{}
			if test.token != "" {
				headers.Set(MigrationTokenHeader, test.token)
			}
			response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/sandbox/connect", strings.NewReader(`{}`), headers)
			if response.Code != http.StatusConflict {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusConflict, response.Body.String())
			}
		})
	}
}

func TestStandaloneConnectExistingTargetIgnoresOversizedMigrationHeader(t *testing.T) {
	called := false
	core := &connectMMDSCoreStub{
		migrationCoreStub: migrationCoreStub{connect: func(context.Context, string, string, string, ConnectOptions) (*types.Sandbox, error) {
			t.Fatal("legacy Connect unexpectedly called")
			return nil, nil
		}},
		connectMMDS: func(_ context.Context, id, _ string, token string, _ ConnectOptions, _ map[string]string, header *string) (*types.Sandbox, error) {
			called = true
			if len(token) <= migrationtoken.MaxWireSize || header == nil || *header != "not-json" {
				t.Fatal("standalone input was changed before the existence-aware Core check")
			}
			return &types.Sandbox{ID: id, Profile: types.ProfileE2B}, nil
		},
	}
	h, apiKey := newMigrationTestHandler(t, core)
	headers := http.Header{}
	headers.Set(MigrationTokenHeader, strings.Repeat("a", migrationtoken.MaxWireSize+1))
	headers.Set(mmdsHeader, "not-json")
	response := migrationRequest(t, h, apiKey, http.MethodPost, "/sandboxes/existing/connect", strings.NewReader(`{}`), headers)
	if response.Code != http.StatusOK || !called {
		t.Fatalf("status = %d, core called=%t", response.Code, called)
	}
}

func TestCreateExecSessionPassesStrictRequestAndReturnsOnlyToken(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		wantTTL        int64
		wantConditions []string
	}{
		{name: "empty"},
		{name: "object", body: `{}`},
		{name: "ttl", body: `{"ttlSeconds":37}`, wantTTL: 37},
		{name: "conditions empty", body: `{"conditions":[]}`},
		{name: "conditions", body: `{"conditions":[{"expr":"request.cwd == '/'"},{"expr":"!request.stdio.tty"}]}`,
			wantConditions: []string{"request.cwd == '/'", "!request.stdio.tty"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var gotID, gotAPIKey, gotMigrationToken string
			var gotTTL int64
			var gotConditions []string
			core := &execSessionCoreStub{execSession: func(_ context.Context, id, apiKey, migrationToken string, ttlSeconds int64, conditions []string) (string, error) {
				gotID, gotAPIKey, gotMigrationToken, gotTTL = id, apiKey, migrationToken, ttlSeconds
				gotConditions = append([]string(nil), conditions...)
				return "kat1.exec", nil
			}}
			handler, apiKey := newMigrationTestHandler(t, core)
			headers := header(MigrationTokenHeader, "kmt1.opaque")
			response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes/stable/exec-sessions", strings.NewReader(test.body), headers)
			if response.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201; body=%s", response.Code, response.Body.String())
			}
			if gotID != "stable" || gotAPIKey != apiKey || gotMigrationToken != "kmt1.opaque" || gotTTL != test.wantTTL {
				t.Fatalf("ExecSession args = id=%q apiKey=%q migration=%q ttl=%d", gotID, gotAPIKey, gotMigrationToken, gotTTL)
			}
			if !reflect.DeepEqual(gotConditions, test.wantConditions) {
				t.Fatalf("ExecSession conditions = %q, want %q", gotConditions, test.wantConditions)
			}
			if got := response.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", got)
			}
			var payload map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if len(payload) != 1 || payload["execAccessToken"] != "kat1.exec" {
				t.Fatalf("response = %#v", payload)
			}
		})
	}
}

func TestCreateExecSessionRejectsBodyBeforeCore(t *testing.T) {
	called := false
	core := &execSessionCoreStub{execSession: func(context.Context, string, string, string, int64, []string) (string, error) {
		called = true
		return "", nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "unknown", body: `{"unknown":true}`, wantStatus: http.StatusBadRequest},
		{name: "negative ttl", body: `{"ttlSeconds":-1}`, wantStatus: http.StatusBadRequest},
		{name: "second value", body: `{} {}`, wantStatus: http.StatusBadRequest},
		{name: "conditions null", body: `{"conditions":null}`, wantStatus: http.StatusBadRequest},
		{name: "condition unknown", body: `{"conditions":[{"expr":"true","other":1}]}`, wantStatus: http.StatusBadRequest},
		{name: "condition empty", body: `{"conditions":[{"expr":""}]}`, wantStatus: http.StatusBadRequest},
		{name: "oversized streamed", body: `{}` + strings.Repeat(" ", 64<<10-1), wantStatus: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/sandboxes/stable/exec-sessions", strings.NewReader(test.body))
			request.ContentLength = -1
			request.Header.Set("X-API-KEY", apiKey)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, test.wantStatus, response.Body.String())
			}
		})
	}
	if called {
		t.Fatal("invalid exec-session request reached Core")
	}
}

func TestCreateExecSessionRejectsOversizedMigrationTokenBeforeCore(t *testing.T) {
	called := false
	core := &execSessionCoreStub{execSession: func(context.Context, string, string, string, int64, []string) (string, error) {
		called = true
		return "", nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes/stable/exec-sessions", strings.NewReader("{}"),
		header(MigrationTokenHeader, strings.Repeat("x", migrationtoken.MaxWireSize+1)))
	if response.Code != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status = %d, want 431; body=%s", response.Code, response.Body.String())
	}
	if called {
		t.Fatal("oversized migration token reached Core.ExecSession")
	}
}

func TestCreateExecSessionRequiresExplicitAPIKeyHeader(t *testing.T) {
	called := false
	core := &execSessionCoreStub{execSession: func(context.Context, string, string, string, int64, []string) (string, error) {
		called = true
		return "kat1.exec", nil
	}}
	handler, apiKey := newMigrationTestHandler(t, core)
	request := httptest.NewRequest(http.MethodPost, "/sandboxes/stable/exec-sessions", strings.NewReader("{}"))
	request.Header.Set("Authorization", "Bearer "+apiKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("Bearer-only status = %d, want 401; body=%s", response.Code, response.Body.String())
	}
	if called {
		t.Fatal("Bearer-only exec-session request reached Core")
	}
}

func TestCreateExecSessionSanitizesOperationalFailuresAsUnavailable(t *testing.T) {
	const internalDetail = "node stable-g7 failed at /private/run/ctl.sock"
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "operation failure", err: errors.New(internalDetail)},
		{name: "incompatible import", err: fmt.Errorf("%s: %w", internalDetail, migrationtoken.ErrIncompatible)},
		{name: "occupied import target", err: fmt.Errorf("%s: %w", internalDetail, ErrAlreadyExists)},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, migrationToken := range []string{"", "kmt1.opaque"} {
				core := &execSessionCoreStub{execSession: func(context.Context, string, string, string, int64, []string) (string, error) {
					return "", test.err
				}}
				handler, apiKey := newMigrationTestHandler(t, core)
				headers := http.Header{}
				if migrationToken != "" {
					headers.Set(MigrationTokenHeader, migrationToken)
				}
				response := migrationRequest(t, handler, apiKey, http.MethodPost, "/sandboxes/stable/exec-sessions", strings.NewReader("{}"), headers)
				if response.Code != http.StatusServiceUnavailable {
					t.Fatalf("migration=%t status = %d, want 503; body=%s", migrationToken != "", response.Code, response.Body.String())
				}
				if got := response.Body.String(); got != "{\"message\":\"exec session unavailable\"}\n" {
					t.Fatalf("public response = %q, want fixed unavailable error", got)
				}
			}
		})
	}
}

type execSessionCoreStub struct {
	Core
	execSession func(context.Context, string, string, string, int64, []string) (string, error)
}

type checkpointCoreStub struct {
	Core
	create func(context.Context, CreateReq) (*types.Sandbox, error)
	pause  func(context.Context, string, string, sandboxcfg.CaptureRequest) error
}

type buildMMDSCoreStub struct {
	Core
	register func(context.Context, string, RegisterSpec) (*types.Build, error)
	trigger  func(context.Context, string, string, string, TriggerSpec, BuildAuth) error
}

type buildStatusCoreStub struct {
	Core
	build *types.Build
	logs  []BuildLogEntry
	list  []*types.Build
}

func (c *buildStatusCoreStub) BuildStatus(context.Context, string, string, string) (*types.Build, error) {
	return c.build, nil
}

func (c *buildStatusCoreStub) BuildLogs(context.Context, string, string, string, int) ([]BuildLogEntry, error) {
	return c.logs, nil
}

func (c *buildStatusCoreStub) ListTemplates(context.Context, string) ([]*types.Build, error) {
	return c.list, nil
}

func (c *buildMMDSCoreStub) RegisterBuild(ctx context.Context, apiKey string, spec RegisterSpec) (*types.Build, error) {
	return c.register(ctx, apiKey, spec)
}

func (c *buildMMDSCoreStub) TriggerBuild(ctx context.Context, apiKey, templateID, buildID string, spec TriggerSpec, auth BuildAuth) error {
	return c.trigger(ctx, apiKey, templateID, buildID, spec, auth)
}

func TestBuildStatusReportsAdmissionAndPhaseState(t *testing.T) {
	handler, apiKey := newMigrationTestHandler(t, &buildStatusCoreStub{build: &types.Build{
		BuildID: "build-observed", TemplateID: "template-observed", Profile: types.ProfileE2B,
		Builder: types.BuildOptions{Target: &types.BuildTarget{
			Kind: types.BuildTargetSandbox, Memory: true,
		}},
		Status: types.BuildBuilding, Resources: types.BuildResources{
			CPU: 2500, Memory: 3 << 30, Storage: 64 << 30,
		},
		ExecutionClaimed: true, RunID: "br-observed",
		Phase: "b", PhaseSandboxID: "build-build-observed-b",
	}})
	response := migrationRequest(t, handler, apiKey, http.MethodGet,
		"/templates/template-observed/builds/build-observed/status", nil, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Resources struct {
			CPUMilli     int64 `json:"cpuMilli"`
			MemoryBytes  int64 `json:"memoryBytes"`
			StorageBytes int64 `json:"storageBytes"`
		} `json:"resources"`
		ExecutionClaimed bool   `json:"executionClaimed"`
		RunID            string `json:"runID"`

		StorageEnforcement string            `json:"storageEnforcement"`
		Target             types.BuildTarget `json:"target"`
		Kind               types.Kind        `json:"kind"`
		Phase              struct {
			Name      string `json:"name"`
			SandboxID string `json:"sandboxID"`
		} `json:"phase"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["systemdEnforcement"]; present {
		t.Fatalf("obsolete systemdEnforcement exposed: %s", response.Body.String())
	}
	if _, present := fields["kind"]; present {
		t.Fatalf("nonterminal build status exposed terminal kind: %s", response.Body.String())
	}
	if body.Resources.CPUMilli != 2500 || body.Resources.MemoryBytes != 3<<30 ||
		body.Resources.StorageBytes != 64<<30 || !body.ExecutionClaimed ||
		body.RunID != "br-observed" ||
		body.StorageEnforcement != "admission-only" || body.Phase.Name != "b" ||
		body.Phase.SandboxID != "build-build-observed-b" || body.Kind != "" ||
		body.Target != (types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}) {
		t.Fatalf("build status observation = %+v; body=%s", body, response.Body.String())
	}
}

func TestListTemplatesReportsRequestedTargetAndFinalKind(t *testing.T) {
	target := &types.BuildTarget{Kind: types.BuildTargetSandbox}
	core := &buildStatusCoreStub{list: []*types.Build{{
		BuildID: "build-list", PersistID: "e2b-sbx-portable", Profile: types.ProfileE2B,
		Kind: types.KindSbx, Status: types.BuildReady, Builder: types.BuildOptions{Target: target},
	}}}
	handler, apiKey := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, apiKey, http.MethodGet, "/templates", nil, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	var items []struct {
		Target types.BuildTarget `json:"target"`
		Kind   types.Kind        `json:"kind"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Target != *target || items[0].Kind != types.KindSbx {
		t.Fatalf("list response = %+v", items)
	}
}

func (c *checkpointCoreStub) Create(ctx context.Context, req CreateReq) (*types.Sandbox, error) {
	return c.create(ctx, req)
}

func (c *checkpointCoreStub) Pause(ctx context.Context, id, apiKey string, request sandboxcfg.CaptureRequest) error {
	return c.pause(ctx, id, apiKey, request)
}

func (c *execSessionCoreStub) ExecSession(ctx context.Context, id, apiKey, migrationToken string, ttlSeconds int64, conditions []string) (string, error) {
	return c.execSession(ctx, id, apiKey, migrationToken, ttlSeconds, conditions)
}

type migrationCoreStub struct {
	Core
	importSandbox func(context.Context, string, string, string) (string, error)
	exportSandbox func(context.Context, string, string, bool, bool) (types.ExportResult, error)
	connect       func(context.Context, string, string, string, ConnectOptions) (*types.Sandbox, error)
}

type connectMMDSCoreStub struct {
	migrationCoreStub
	connectMMDS func(context.Context, string, string, string, ConnectOptions, map[string]string, *string) (*types.Sandbox, error)
}

func (c *connectMMDSCoreStub) ConnectWithMMDS(ctx context.Context, id, apiKey, token string, options ConnectOptions, metadata map[string]string, header *string) (*types.Sandbox, error) {
	return c.connectMMDS(ctx, id, apiKey, token, options, metadata, header)
}

type sandboxContractCoreStub struct {
	Core
	sandbox *types.Sandbox
}

func (c *sandboxContractCoreStub) Get(context.Context, string, string) (*types.Sandbox, error) {
	return c.sandbox, nil
}

func (c *sandboxContractCoreStub) List(context.Context, string, string, int, string) ([]*types.Sandbox, string, error) {
	return []*types.Sandbox{c.sandbox}, "", nil
}

func newSandboxContractHandler(t *testing.T, core Core, resources Resources) (http.Handler, string) {
	t.Helper()
	apiKey, err := apikey.Mint(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(core, "example.test", resources, logger).Handler(), apiKey
}

type resourceStatsCoreStub struct {
	Core
	stats *ResourceStats
	err   error
}

func (c *resourceStatsCoreStub) ResourceStats(context.Context, string, string) (*ResourceStats, error) {
	return c.stats, c.err
}

type trafficStatsCoreStub struct {
	Core
	stats *TrafficStats
	err   error
}

func (c *trafficStatsCoreStub) TrafficStats(context.Context, string, string) (*TrafficStats, error) {
	return c.stats, c.err
}

func TestResourceStatsSparseJSONAndStatusMapping(t *testing.T) {
	cpu := 2.0
	handler, apiKey := newSandboxContractHandler(t, &resourceStatsCoreStub{
		stats: &ResourceStats{CPUCapacity: &cpu},
	}, Resources{})
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/s1/stats/resource", nil)
	req.Header.Set("X-API-KEY", apiKey)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK || resp.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response status=%d cache=%q", resp.Code, resp.Header().Get("Cache-Control"))
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 1 || body["cpuCapacity"] == nil {
		t.Fatalf("sparse resource JSON = %s", resp.Body.String())
	}

	for _, tc := range []struct {
		err  error
		want int
	}{
		{ErrNotFound, http.StatusNotFound},
		{ErrStatsUnsupported, http.StatusNotImplemented},
		{ErrStatsConflict, http.StatusConflict},
		{ErrStatsUnavailable, http.StatusServiceUnavailable},
	} {
		t.Run(http.StatusText(tc.want), func(t *testing.T) {
			handler, apiKey := newSandboxContractHandler(t, &resourceStatsCoreStub{err: tc.err}, Resources{})
			req := httptest.NewRequest(http.MethodGet, "/sandboxes/s1/stats/resource", nil)
			req.Header.Set("X-API-KEY", apiKey)
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			if resp.Code != tc.want || resp.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d cache=%q, want %d/no-store", resp.Code, resp.Header().Get("Cache-Control"), tc.want)
			}
		})
	}
}

func TestTrafficStatsCompactJSONAndStatusMapping(t *testing.T) {
	idle := time.Date(2026, time.August, 12, 14, 3, 21, 123456789, time.UTC)
	handler, apiKey := newSandboxContractHandler(t, &trafficStatsCoreStub{stats: &TrafficStats{
		State:       string(types.StateRunning),
		MaxInflight: publicconfig.MaxInflight{Total: 32, Forward: 8, Exec: 2},
		Inflight:    TrafficInflight{},
		IdleSince:   &idle,
		Services: map[string]ServiceTrafficStats{
			"forward": {IdleSince: &idle},
			"exec":    {Parking: 1},
		},
	}}, Resources{})
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/s1/stats/traffic", nil)
	req.Header.Set("X-API-KEY", apiKey)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK || resp.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response status=%d cache=%q", resp.Code, resp.Header().Get("Cache-Control"))
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"idle", "idleForSeconds", "lastOpened", "lastClosed", "connectionsTotal", "requestsTotal", "workerID", "workerCount"} {
		if _, present := body[forbidden]; present {
			t.Fatalf("traffic response exposed %q: %s", forbidden, resp.Body.String())
		}
	}
	var services map[string]map[string]json.RawMessage
	if err := json.Unmarshal(body["services"], &services); err != nil {
		t.Fatal(err)
	}
	var maxInflight publicconfig.MaxInflight
	if err := json.Unmarshal(body["maxInflight"], &maxInflight); err != nil {
		t.Fatal(err)
	}
	if maxInflight != (publicconfig.MaxInflight{Total: 32, Forward: 8, Exec: 2}) {
		t.Fatalf("maxInflight = %+v", maxInflight)
	}
	if len(services["forward"]) != 3 || services["forward"]["idleSince"] == nil ||
		len(services["exec"]) != 2 || services["exec"]["idleSince"] != nil {
		t.Fatalf("compact service JSON = %s", resp.Body.String())
	}

	for _, tc := range []struct {
		err  error
		want int
	}{
		{ErrNotFound, http.StatusNotFound},
		{ErrStatsUnsupported, http.StatusNotImplemented},
		{ErrStatsConflict, http.StatusConflict},
		{ErrStatsUnavailable, http.StatusServiceUnavailable},
	} {
		t.Run(http.StatusText(tc.want), func(t *testing.T) {
			handler, apiKey := newSandboxContractHandler(t, &trafficStatsCoreStub{err: tc.err}, Resources{})
			req := httptest.NewRequest(http.MethodGet, "/sandboxes/s1/stats/traffic", nil)
			req.Header.Set("X-API-KEY", apiKey)
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			if resp.Code != tc.want || resp.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d cache=%q, want %d/no-store", resp.Code, resp.Header().Get("Cache-Control"), tc.want)
			}
		})
	}
}

func TestFailMapsProxyUnavailableToServiceUnavailable(t *testing.T) {
	a := &API{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	response := httptest.NewRecorder()
	a.fail(response, errors.Join(ErrProxyUnavailable, errors.New("private route session detail")))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.Code)
	}
	if got := response.Body.String(); got != "{\"message\":\"proxy temporarily unavailable\"}\n" {
		t.Fatalf("public response = %q", got)
	}
}

func TestFailMapsExtensionErrorsWithoutLeakingDetails(t *testing.T) {
	a := &API{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, test := range []struct {
		name   string
		err    error
		status int
		body   string
	}{
		{name: "rejected", err: errors.Join(conductorextension.ErrRejected, errors.New("private policy detail")), status: http.StatusForbidden, body: conductorextension.ErrRejected.Error()},
		{name: "unavailable", err: errors.Join(ErrExtensionUnavailable, errors.New("private outage detail")), status: http.StatusServiceUnavailable, body: ErrExtensionUnavailable.Error()},
		{name: "sandbox changed", err: ErrSandboxChanged, status: http.StatusConflict, body: ErrSandboxChanged.Error()},
		{name: "build changed", err: ErrBuildChanged, status: http.StatusConflict, body: ErrBuildChanged.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			a.fail(response, test.err)
			if response.Code != test.status || response.Body.String() != "{\"message\":\""+test.body+"\"}\n" {
				t.Fatalf("response = %d %q", response.Code, response.Body.String())
			}
		})
	}
}

func TestFailExecSessionMapsStaleExtensionResult(t *testing.T) {
	a := &API{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	response := httptest.NewRecorder()
	a.failExecSession(response, errors.Join(ErrSandboxChanged, errors.New("private incarnation detail")))
	if response.Code != http.StatusConflict || response.Body.String() != "{\"message\":\""+ErrSandboxChanged.Error()+"\"}\n" {
		t.Fatalf("response = %d %q", response.Code, response.Body.String())
	}
}

func (c *migrationCoreStub) ImportSandbox(ctx context.Context, apiKey, token, targetID string) (string, error) {
	return c.importSandbox(ctx, apiKey, token, targetID)
}

func (c *migrationCoreStub) ExportSandbox(ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) (types.ExportResult, error) {
	return c.exportSandbox(ctx, apiKey, sid, toTemplate, keepSource)
}

func (c *migrationCoreStub) Connect(ctx context.Context, id, apiKey, token string, options ConnectOptions) (*types.Sandbox, error) {
	return c.connect(ctx, id, apiKey, token, options)
}

func newMigrationTestHandler(t *testing.T, core Core) (http.Handler, string) {
	t.Helper()
	apiKey, err := apikey.Mint(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(core, "example.test", Resources{}, logger).Handler(), apiKey
}

func migrationRequest(t *testing.T, handler http.Handler, apiKey, method, path string, body io.Reader, headers http.Header) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, body)
	request.Header.Set("X-API-KEY", apiKey)
	for name, values := range headers {
		request.Header[name] = append([]string(nil), values...)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func header(k, v string) http.Header {
	h := http.Header{}
	h.Set(k, v)
	return h
}
