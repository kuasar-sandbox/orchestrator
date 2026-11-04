package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestIdleTimeoutAPIValues(t *testing.T) {
	for _, tc := range []struct {
		body    string
		status  int
		present bool
		value   int64
	}{
		{`{}`, 201, false, 0}, {`{"idleTimeout":null}`, 201, false, 0}, {`{"idleTimeout":0}`, 201, true, 0}, {`{"idleTimeout":5}`, 201, true, 5},
		{`{"idleTimeout":-1}`, 400, false, 0}, {`{"idleTimeout":0.5}`, 400, false, 0}, {`{"idleTimeout":1e2}`, 400, false, 0}, {`{"idleTimeout":9223372037}`, 400, false, 0}, {`{"idleTimeout":9223372036854775808}`, 400, false, 0}} {
		t.Run(tc.body, func(t *testing.T) {
			called := false
			core := &checkpointCoreStub{create: func(_ context.Context, r CreateReq) (*types.Sandbox, error) {
				called = true
				if (r.IdleTimeout != nil) != tc.present || (r.IdleTimeout != nil && *r.IdleTimeout != tc.value) {
					t.Fatal(r.IdleTimeout)
				}
				return &types.Sandbox{ID: "s", Profile: types.ProfileBare, IdleTimeout: r.IdleTimeout}, nil
			}}
			handler, key := newMigrationTestHandler(t, core)
			resp := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes", strings.NewReader(tc.body), nil)
			if resp.Code != tc.status || (tc.status == 400 && called) {
				t.Fatal(resp.Code, resp.Body.String())
			}
			if tc.status == 201 {
				var view map[string]any
				if err := json.Unmarshal(resp.Body.Bytes(), &view); err != nil {
					t.Fatal(err)
				}
				value, exists := view["idleTimeout"]
				if !exists || (tc.present && value != float64(tc.value)) || (!tc.present && value != nil) {
					t.Fatal("management response lost nullable value", view)
				}
			}

		})
	}
}
