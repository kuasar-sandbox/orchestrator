package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIdlePolicyRouterValidation(t *testing.T) {
	for _, tc := range []struct {
		body    string
		valid   bool
		present bool
		value   int64
	}{
		{`{}`, true, false, 0}, {`{"idleTimeout":null}`, true, false, 0}, {`{"idleTimeout":0}`, true, true, 0}, {`{"idleTimeout":5}`, true, true, 5},
		{`{"idleTimeout":-1}`, false, false, 0}, {`{"idleTimeout":1.5}`, false, false, 0}, {`{"idleTimeout":9223372037}`, false, false, 0}, {`{"idleTimeout":0,"idleTimeout":5}`, false, false, 0},
	} {
		t.Run(tc.body, func(t *testing.T) {
			var v *int64
			_, err := createSandboxPolicies(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(tc.body)), nil, &v)
			if (err == nil) != tc.valid {
				t.Fatal(err)
			}
			if tc.valid && ((v != nil) != tc.present || (v != nil && *v != tc.value)) {
				t.Fatal(v)
			}
		})
	}
}
