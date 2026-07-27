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
	if m := mergeConfigHeaders(nil, http.Header{}); m != nil {
		t.Fatalf("no headers should not allocate: %v", m)
	}

	// Headers populate the matching namespace keys.
	h := http.Header{}
	h.Set("X-Kuasar-Sandbox-Network", `{"hostname":"h1"}`)
	h.Set("X-Kuasar-Sandbox-Resource", `{"capacity":{"cpu":4}}`)
	h.Set("X-Kuasar-Sandbox-Restore", `{"prefetch":"memory"}`)
	h.Set("X-Kuasar-Sandbox-Credentials", `{"envd_access_token":"envd"}`)
	m := mergeConfigHeaders(nil, h)
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
	got := mergeConfigHeaders(meta, header("X-Kuasar-Sandbox-Network", `{"hostname":"from-header"}`))
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
	got = mergeConfigHeaders(nil, header("X-Kuasar-Sandbox-Builder", `{"referer":{"enabled":false}}`))
	if got != nil {
		t.Fatalf("builder header should not enter sandbox metadata: %+v", got)
	}
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
	got := mergeBuildConfigHeaders(nil, h)
	if got[buildcfg.NsBuilder] != `{"referer":{"enabled":false}}` {
		t.Fatalf("builder header not normalized: %+v", got)
	}
	if got[sandboxcfg.NsNetwork] != `{"hostname":"build"}` {
		t.Fatalf("sandbox build header not normalized: %+v", got)
	}
	if _, ok := got[sandboxcfg.NsCredentials]; ok {
		t.Fatalf("credentials header entered build metadata: %+v", got)
	}
	if _, ok := got[sandboxcfg.NsCheckpoint]; ok {
		t.Fatalf("checkpoint header entered build metadata: %+v", got)
	}
}

func TestCreateCheckpointHeaderOverlaysMetadataPerField(t *testing.T) {
	var got CreateReq
	core := &checkpointCoreStub{create: func(_ context.Context, req CreateReq) (*types.Sandbox, error) {
		got = req
		return &types.Sandbox{ID: "created", Profile: types.ProfileBare}, nil
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

func TestPauseRequestPolicyAndStatus(t *testing.T) {
	boolPtr := func(value bool) *bool { return &value }
	tests := []struct {
		name       string
		body       string
		header     *string
		want       sandboxcfg.CheckpointPolicy
		coreErr    error
		wantStatus int
		wantCalls  int
	}{
		{name: "empty body", body: "", wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "empty object", body: `{}`, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "memory null", body: `{"memory":null}`, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "memory true", body: `{"memory":true}`, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "body values", body: `{"checkpoint_merge_ref":false,"checkpoint_drop_caches":true}`,
			want: sandboxcfg.CheckpointPolicy{MergeRef: boolPtr(false), DropCaches: boolPtr(true)}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "body null inherits", body: `{"checkpoint_merge_ref":null,"checkpoint_drop_caches":false}`,
			want: sandboxcfg.CheckpointPolicy{DropCaches: boolPtr(false)}, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "unknown body field remains accepted", body: `{"future":true}`, wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "already paused", body: `{}`, coreErr: ErrAlreadyPaused, wantStatus: http.StatusConflict, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			core := &checkpointCoreStub{pause: func(_ context.Context, _, _ string, got sandboxcfg.CheckpointPolicy) error {
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
	core := &checkpointCoreStub{pause: func(_ context.Context, _, _ string, got sandboxcfg.CheckpointPolicy) error {
		if got.MergeRef == nil || *got.MergeRef || got.DropCaches == nil || *got.DropCaches {
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
		{name: "memory false", body: `{"memory":false}`},
		{name: "malformed body", body: `{`},
		{name: "top-level null", body: `null`},
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
			core := &checkpointCoreStub{pause: func(context.Context, string, string, sandboxcfg.CheckpointPolicy) error {
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
			core := &checkpointCoreStub{pause: func(context.Context, string, string, sandboxcfg.CheckpointPolicy) error {
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
	a := &API{domain: "example.test"}
	created := int64(1_700_000_000)
	sandbox := &types.Sandbox{ID: "sandbox", CreatedUnix: created}

	detail := a.sandboxDetail(sandbox)
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
	core := &migrationCoreStub{exportSandbox: func(context.Context, string, string, bool, bool) (string, error) {
		return "", fmt.Errorf("private export detail: %w", migrationtoken.ErrTokenTooLarge)
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
	core := &migrationCoreStub{connect: func(context.Context, string, string, string, int) (*types.Sandbox, error) {
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

func TestCreateExecSessionPassesStrictRequestAndReturnsOnlyToken(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantTTL int64
	}{
		{name: "empty"},
		{name: "object", body: `{}`},
		{name: "ttl", body: `{"ttlSeconds":37}`, wantTTL: 37},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var gotID, gotAPIKey, gotMigrationToken string
			var gotTTL int64
			core := &execSessionCoreStub{execSession: func(_ context.Context, id, apiKey, migrationToken string, ttlSeconds int64) (string, error) {
				gotID, gotAPIKey, gotMigrationToken, gotTTL = id, apiKey, migrationToken, ttlSeconds
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
	core := &execSessionCoreStub{execSession: func(context.Context, string, string, string, int64) (string, error) {
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
	core := &execSessionCoreStub{execSession: func(context.Context, string, string, string, int64) (string, error) {
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
	core := &execSessionCoreStub{execSession: func(context.Context, string, string, string, int64) (string, error) {
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
				core := &execSessionCoreStub{execSession: func(context.Context, string, string, string, int64) (string, error) {
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
	execSession func(context.Context, string, string, string, int64) (string, error)
}

type checkpointCoreStub struct {
	Core
	create func(context.Context, CreateReq) (*types.Sandbox, error)
	pause  func(context.Context, string, string, sandboxcfg.CheckpointPolicy) error
}

func (c *checkpointCoreStub) Create(ctx context.Context, req CreateReq) (*types.Sandbox, error) {
	return c.create(ctx, req)
}

func (c *checkpointCoreStub) Pause(ctx context.Context, id, apiKey string, override sandboxcfg.CheckpointPolicy) error {
	return c.pause(ctx, id, apiKey, override)
}

func (c *execSessionCoreStub) ExecSession(ctx context.Context, id, apiKey, migrationToken string, ttlSeconds int64) (string, error) {
	return c.execSession(ctx, id, apiKey, migrationToken, ttlSeconds)
}

type migrationCoreStub struct {
	Core
	importSandbox func(context.Context, string, string, string) (string, error)
	exportSandbox func(context.Context, string, string, bool, bool) (string, error)
	connect       func(context.Context, string, string, string, int) (*types.Sandbox, error)
}

func (c *migrationCoreStub) ImportSandbox(ctx context.Context, apiKey, token, targetID string) (string, error) {
	return c.importSandbox(ctx, apiKey, token, targetID)
}

func (c *migrationCoreStub) ExportSandbox(ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) (string, error) {
	return c.exportSandbox(ctx, apiKey, sid, toTemplate, keepSource)
}

func (c *migrationCoreStub) Connect(ctx context.Context, id, apiKey, token string, timeout int) (*types.Sandbox, error) {
	return c.connect(ctx, id, apiKey, token, timeout)
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
