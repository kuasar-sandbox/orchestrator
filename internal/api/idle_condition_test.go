package api

import (
	"strings"
	"testing"
)

func TestDecodeIdleSince(t *testing.T) {
	for _, tc := range []struct {
		body           string
		valid, present bool
	}{
		{`{}`, true, false},
		{`{"ifIdle":true,"IfIdleSince":"2026-11-01T1:00:00Z"}`, false, false},
		{`{"ifIdle":true,"ifIdleSince":"2026-11-01T01:00:00Z","IFIDLESINCE":"2026-11-01T1:00:00Z"}`, false, false},
		{`{"ifIdle":true,"ifIdleSince":"2026-11-01T1:00:00Z"}`, false, false},
		{`{"ifIdle":true,"ifIdleSince":"2026-11-01T01:00:00,5Z"}`, false, false},
		{`{"ifIdle":true,"ifIdleSince":"2026-11-01T01:00:00+24:00"}`, false, false}, {`{"ifIdleSince":null}`, true, false},
		{`{"ifIdle":true,"ifIdleSince":"2026-11-01T00:00:00.123456789+08:00"}`, true, true},
		{`{"ifIdleSince":"2026-11-01T00:00:00Z"}`, false, false},
		{`{"ifIdle":true,"ifIdleSince":"2026-11-01T00:00:00"}`, false, false},
		{`{"ifIdle":true,"ifIdleSince":4}`, false, false},
	} {
		req, _, err := decodePauseRequest(strings.NewReader(tc.body))
		if (err == nil) != tc.valid || (err == nil && (req.IfIdleSince != nil) != tc.present) {
			t.Fatalf("%s: %+v %v", tc.body, req, err)
		}
	}
}
