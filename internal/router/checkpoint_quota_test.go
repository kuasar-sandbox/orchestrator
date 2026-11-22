package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateCheckpointQuotaPresenceAndValidation(t *testing.T) {
	for _, tc := range []struct {
		body           string
		valid, present bool
		value          int
	}{
		{`{}`, true, false, 0}, {`{"max_checkpoints":1}`, true, true, 1}, {`{"max_checkpoints":17}`, true, true, 17},
		{`{"max_checkpoints":0}`, false, true, 0}, {`{"max_checkpoints":-1}`, false, true, -1},
		{`{"max_checkpoints":1.5}`, false, false, 0}, {`{"max_checkpoints":"3"}`, false, false, 0},
		{`{"max_checkpoints":1,"max_checkpoints":3}`, false, false, 0},
	} {
		t.Run(tc.body, func(t *testing.T) {
			var quota *int
			_, err := createSandboxPoliciesWithQuota(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(tc.body)), nil, nil, &quota)
			if (err == nil) != tc.valid {
				t.Fatalf("validation: %v", err)
			}
			if tc.valid && ((quota != nil) != tc.present || quota != nil && *quota != tc.value) {
				t.Fatalf("quota value/presence lost: %v", quota)
			}
		})
	}
}
