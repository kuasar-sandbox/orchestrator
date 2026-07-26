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
	"strings"
	"testing"

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
	if got := mergeCreateConfigHeaders(nil, h); got[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("create restore header not normalized: %+v", got)
	}
	if got := mergeCreateConfigHeaders(nil, h); got[sandboxcfg.NsCredentials] != `{"envd_access_token":"envd"}` {
		t.Fatalf("create credentials header not normalized: %+v", got)
	}

	// Header wins over an e2b metadata key of the same namespace.
	meta := map[string]string{sandboxcfg.NsNetwork: `{"hostname":"from-metadata"}`}
	got := mergeConfigHeaders(meta, header("X-Kuasar-Sandbox-Network", `{"hostname":"from-header"}`))
	if got[sandboxcfg.NsNetwork] != `{"hostname":"from-header"}` {
		t.Fatalf("header should win over metadata: %+v", got)
	}
	meta = map[string]string{sandboxcfg.NsRestore: `{"prefetch":"memory"}`}
	got = mergeCreateConfigHeaders(meta, header("X-Kuasar-Sandbox-Restore", `{"prefetch":"off"}`))
	if got[sandboxcfg.NsRestore] != `{"prefetch":"off"}` {
		t.Fatalf("restore header should win over metadata: %+v", got)
	}
	emptyRestore := http.Header{}
	emptyRestore.Set("X-Kuasar-Sandbox-Restore", "")
	got = mergeCreateConfigHeaders(nil, emptyRestore)
	if _, ok := got[sandboxcfg.NsRestore]; !ok {
		t.Fatalf("present empty restore header must reach strict validation: %+v", got)
	}
	emptyCredentials := http.Header{}
	emptyCredentials.Set("X-Kuasar-Sandbox-Credentials", "")
	got = mergeCreateConfigHeaders(nil, emptyCredentials)
	if _, ok := got[sandboxcfg.NsCredentials]; !ok {
		t.Fatalf("present empty credentials header must reach strict validation: %+v", got)
	}
	metadataCredentials := map[string]string{sandboxcfg.NsCredentials: `{"service_secret":"metadata"}`}
	got = mergeCreateConfigHeaders(metadataCredentials, header("X-Kuasar-Sandbox-Credentials", `{"envd_access_token":"header"}`))
	if got[sandboxcfg.NsCredentials] != `{"envd_access_token":"header"}` {
		t.Fatalf("credentials header should replace the metadata object: %+v", got)
	}

	// Builder is build-only and is not folded by the generic sandbox header path.
	got = mergeConfigHeaders(nil, header("X-Kuasar-Sandbox-Builder", `{"referer":{"enabled":false}}`))
	if got != nil {
		t.Fatalf("builder header should not enter sandbox metadata: %+v", got)
	}
}

func TestMergeBuildConfigHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("X-Kuasar-Sandbox-Builder", `{"referer":{"enabled":false}}`)
	h.Set("X-Kuasar-Sandbox-Network", `{"hostname":"build"}`)
	h.Set("X-Kuasar-Sandbox-Credentials", `{"envd_access_token":"must-not-enter-build"}`)
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
}

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
	payload, err := json.Marshal(struct {
		Token     string `json:"token"`
		SandboxID string `json:"sandboxID"`
	}{Token: token, SandboxID: targetID})
	if err != nil {
		t.Fatal(err)
	}
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

type migrationCoreStub struct {
	Core
	importSandbox func(context.Context, string, string, string) (string, error)
	connect       func(context.Context, string, string, string, int) (*types.Sandbox, error)
}

func (c *migrationCoreStub) ImportSandbox(ctx context.Context, apiKey, token, targetID string) (string, error) {
	return c.importSandbox(ctx, apiKey, token, targetID)
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
