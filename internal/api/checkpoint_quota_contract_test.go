package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func checkpointQuotaRequestBody(policy string) string {
	if policy == "" {
		return `{"metadata":{"caller":"contract"}}`
	}
	return fmt.Sprintf(`{"metadata":{"caller":"contract",%q:%q}}`, sandboxcfg.NsCheckpoint, policy)
}

func TestCreateCheckpointQuotaSources(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata string
		header   string
		want     string
	}{
		{name: "omitted"},
		{name: "metadata", metadata: `{"max_checkpoints":2}`, want: `{"max_checkpoints":2}`},
		{name: "header", header: `{"max_checkpoints":3}`, want: `{"max_checkpoints":3}`},
		{
			name:     "header overlays quota and one snapshot field",
			metadata: `{"max_checkpoints":2,"merge_ref":true,"drop_caches":false}`,
			header:   `{"max_checkpoints":4,"merge_ref":false}`,
			want:     `{"merge_ref":false,"drop_caches":false,"max_checkpoints":4}`,
		},
		{
			name:     "omitted header quota inherits metadata",
			metadata: `{"max_checkpoints":2,"merge_ref":true}`,
			header:   `{"drop_caches":false}`,
			want:     `{"merge_ref":true,"drop_caches":false,"max_checkpoints":2}`,
		},
		{
			name:     "null header quota inherits metadata",
			metadata: `{"max_checkpoints":2,"drop_caches":false}`,
			header:   `{"max_checkpoints":null,"merge_ref":false}`,
			want:     `{"merge_ref":false,"drop_caches":false,"max_checkpoints":2}`,
		},
		{name: "null metadata quota", metadata: `{"max_checkpoints":null}`},
		{name: "null header quota", header: `{"max_checkpoints":null}`},
		{
			name:     "header sets null metadata quota",
			metadata: `{"max_checkpoints":null,"merge_ref":false}`,
			header:   `{"max_checkpoints":3}`,
			want:     `{"merge_ref":false,"max_checkpoints":3}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			var got CreateReq
			core := &checkpointCoreStub{create: func(_ context.Context, req CreateReq) (*types.Sandbox, error) {
				calls++
				got = req
				return &types.Sandbox{ID: "created", Profile: types.ProfileBare}, nil
			}}
			handler, key := newMigrationTestHandler(t, core)
			var headers http.Header
			if test.header != "" {
				headers = header(checkpointHeader, test.header)
			}
			response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes",
				strings.NewReader(checkpointQuotaRequestBody(test.metadata)), headers)
			if response.Code != http.StatusCreated || calls != 1 {
				t.Fatalf("Create response = %d %s, calls=%d", response.Code, response.Body.String(), calls)
			}
			raw, present := got.Metadata[sandboxcfg.NsCheckpoint]
			if raw != test.want || present != (test.want != "") || got.Metadata["caller"] != "contract" {
				t.Fatalf("checkpoint metadata = %#v, want policy %q and caller retained", got.Metadata, test.want)
			}
			if got.MaxCheckpoints != nil {
				t.Fatalf("public Create bypassed the namespace carrier: MaxCheckpoints=%d", *got.MaxCheckpoints)
			}
		})
	}
}

func TestCreateRejectsTopLevelCheckpointQuota(t *testing.T) {
	for _, field := range []string{"max_checkpoints", "Max_Checkpoints", "MAX_CHECKPOINTS"} {
		for _, value := range []string{"3", "null"} {
			for _, withHeader := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/header=%t", field, value, withHeader), func(t *testing.T) {
					called := false
					core := &checkpointCoreStub{create: func(context.Context, CreateReq) (*types.Sandbox, error) {
						called = true
						return &types.Sandbox{}, nil
					}}
					handler, key := newMigrationTestHandler(t, core)
					var headers http.Header
					if withHeader {
						headers = header(checkpointHeader, `{"max_checkpoints":3}`)
					}
					response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes",
						strings.NewReader(fmt.Sprintf(`{%q:%s}`, field, value)), headers)
					if response.Code != http.StatusBadRequest || called ||
						!strings.Contains(response.Body.String(), "max_checkpoints must be set") {
						t.Fatalf("removed field response = %d %s, called=%t", response.Code, response.Body.String(), called)
					}
				})
			}
		}
	}
}

func TestCreateRejectsInvalidCheckpointQuotaBeforeCore(t *testing.T) {
	for _, policy := range []string{
		`{"max_checkpoints":0}`,
		`{"max_checkpoints":-1}`,
		`{"max_checkpoints":1.5}`,
		`{"max_checkpoints":"3"}`,
		`{"max_checkpoints":999999999999999999999999}`,
		`{"Max_Checkpoints":3}`,
		`{"max_checkpoints":2,"max_checkpoints":3}`,
	} {
		for _, source := range []string{"header", "metadata", "metadata with valid header"} {
			t.Run(source+"/"+policy, func(t *testing.T) {
				called := false
				core := &checkpointCoreStub{create: func(context.Context, CreateReq) (*types.Sandbox, error) {
					called = true
					return &types.Sandbox{}, nil
				}}
				handler, key := newMigrationTestHandler(t, core)
				body := checkpointQuotaRequestBody(policy)
				var headers http.Header
				switch source {
				case "header":
					body = `{}`
					headers = header(checkpointHeader, policy)
				case "metadata with valid header":
					headers = header(checkpointHeader, `{"max_checkpoints":3}`)
				}
				response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes", strings.NewReader(body), headers)
				if response.Code != http.StatusBadRequest || called {
					t.Fatalf("invalid quota response = %d %s, called=%t", response.Code, response.Body.String(), called)
				}
			})
		}
	}
}

func TestBuildRejectsCreateCheckpointQuota(t *testing.T) {
	for _, value := range []string{"3", "null"} {
		policy := fmt.Sprintf(`{"max_checkpoints":%s}`, value)
		for _, source := range []string{"header", "metadata", "metadata with valid header"} {
			t.Run(source+"/"+value, func(t *testing.T) {
				called := false
				core := &buildMMDSCoreStub{register: func(context.Context, string, RegisterSpec) (*types.Build, error) {
					called = true
					return &types.Build{TemplateID: "template", BuildID: "build", Profile: types.ProfileE2B}, nil
				}}
				handler, key := newMigrationTestHandler(t, core)
				body := checkpointQuotaRequestBody(policy)
				var headers http.Header
				switch source {
				case "header":
					body = `{}`
					headers = header(checkpointHeader, policy)
				case "metadata with valid header":
					headers = header(checkpointHeader, `{"merge_ref":false}`)
				}
				response := migrationRequest(t, handler, key, http.MethodPost, "/v3/templates", strings.NewReader(body), headers)
				if response.Code != http.StatusBadRequest || called || !strings.Contains(response.Body.String(), "max_checkpoints") {
					t.Fatalf("Build quota response = %d %s, called=%t", response.Code, response.Body.String(), called)
				}
			})
		}
	}
}

func TestPauseRejectsCreateCheckpointQuota(t *testing.T) {
	for _, value := range []string{"3", "null"} {
		policy := fmt.Sprintf(`{"max_checkpoints":%s}`, value)
		for _, source := range []string{"header", "body"} {
			t.Run(source+"/"+value, func(t *testing.T) {
				called := false
				core := &checkpointCoreStub{pause: func(context.Context, string, string, sandboxcfg.CaptureRequest) error {
					called = true
					return nil
				}}
				handler, key := newMigrationTestHandler(t, core)
				body := policy
				var headers http.Header
				if source == "header" {
					body = `{}`
					headers = header(checkpointHeader, policy)
				}
				response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/sid/pause", strings.NewReader(body), headers)
				if response.Code != http.StatusBadRequest || called {
					t.Fatalf("Pause quota response = %d %s, called=%t", response.Code, response.Body.String(), called)
				}
			})
		}
	}
}

func TestBuildRejectsRemovedSecureOption(t *testing.T) {
	for _, field := range []string{"secure", "Secure", "SECURE"} {
		for _, value := range []string{"true", "false", "null"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				called := false
				core := &buildMMDSCoreStub{register: func(context.Context, string, RegisterSpec) (*types.Build, error) {
					called = true
					return &types.Build{TemplateID: "template", BuildID: "build", Profile: types.ProfileE2B}, nil
				}}
				handler, key := newMigrationTestHandler(t, core)
				response := migrationRequest(t, handler, key, http.MethodPost, "/v3/templates",
					strings.NewReader(fmt.Sprintf(`{%q:%s}`, field, value)), nil)
				if response.Code != http.StatusBadRequest || called || !strings.Contains(response.Body.String(), "secure") {
					t.Fatalf("removed secure response = %d %s, called=%t", response.Code, response.Body.String(), called)
				}
			})
		}
	}
}
