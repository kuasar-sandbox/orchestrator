package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type resetCoreStub struct {
	Core
	reset func(context.Context, string, string) error
}

func (c *resetCoreStub) Reset(ctx context.Context, sid, key string) error {
	return c.reset(ctx, sid, key)
}

func TestResetHTTPCommitAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"committed", nil, http.StatusNoContent},
		{"conflict", ErrSandboxChanged, http.StatusConflict},
		{"wrong-owner", ErrNotFound, http.StatusNotFound},
		{"unavailable-disks", fmt.Errorf("%w: current disk is not ready", ErrResetFailed), http.StatusUnprocessableEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			core := &resetCoreStub{reset: func(_ context.Context, sid, key string) error {
				called = true
				if sid != "s1" || key == "" {
					t.Fatalf("Reset identity = %q, %q", sid, key)
				}
				return tc.err
			}}
			h, key := newMigrationTestHandler(t, core)
			r := migrationRequest(t, h, key, http.MethodPost, "/sandboxes/s1/reset", nil, nil)
			if !called || r.Code != tc.status {
				t.Fatalf("Reset = %d %s, called=%v", r.Code, r.Body.String(), called)
			}
			if tc.err == nil && r.Body.Len() != 0 {
				t.Fatal("204 contains a body")
			}
			called = false
			r = migrationRequest(t, h, "", http.MethodPost, "/sandboxes/s1/reset", nil, nil)
			if called || r.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated Reset = %d, called=%v", r.Code, called)
			}
		})
	}
}

func TestResetCreateOptionAndTerminalQuery(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		var got bool
		core := &checkpointCoreStub{create: func(_ context.Context, req CreateReq) (*types.Sandbox, error) {
			got = req.AutoReset
			return &types.Sandbox{ID: "s1", State: types.StateStarting}, nil
		}}
		h, key := newMigrationTestHandler(t, core)
		body := `{"templateID":"template"}`
		if enabled {
			body = `{"templateID":"template","autoReset":true}`
		}
		r := migrationRequest(t, h, key, http.MethodPost, "/sandboxes", strings.NewReader(body), nil)
		if r.Code != http.StatusCreated || got != enabled {
			t.Fatalf("create autoReset=%v: %d %s got=%v", enabled, r.Code, r.Body.String(), got)
		}
	}
	sb := &types.Sandbox{ID: "s1", State: types.StateDead, AutoReset: true, CreatedUnix: 100, DeadUnix: 200, DeadlineUnix: 300, AutoResetError: "current disk is not ready"}
	h, key := newSandboxContractHandler(t, &sandboxContractCoreStub{sandbox: sb}, Resources{})
	for _, path := range []string{"/sandboxes/s1", "/v2/sandboxes?state=dead"} {
		r := migrationRequest(t, h, key, http.MethodGet, path, nil, nil)
		if r.Code != 200 {
			t.Fatalf("query: %d %s", r.Code, r.Body.String())
		}
		var raw any
		if err := json.Unmarshal(r.Body.Bytes(), &raw); err != nil {
			t.Fatal(err)
		}
		var item map[string]any
		switch v := raw.(type) {
		case map[string]any:
			item = v
		case []any:
			item = v[0].(map[string]any)
		}
		if item["autoReset"] != true || item["deadAt"] != isoUnix(200) || item["endAt"] != isoUnix(300) || item["autoResetError"] != sb.AutoResetError {
			t.Fatalf("terminal query=%v", item)
		}
		if _, ok := item["executionResult"]; ok {
			t.Fatal("unknown execution result must be absent")
		}
	}
}
