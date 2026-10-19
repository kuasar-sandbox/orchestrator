package proxyapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"syscall"
	"testing"
)

// SIGKILL of the candidate can close a connection before Transport records an
// expected response. net/http then replaces EOF with an unexported sentinel,
// which has no Unwrap method. Match only that exact leaf error, only while the
// test's deliberate fault is active; ordinary traffic still permits no losses.
func fixtureExpectedConnectionLoss(err error, duringInjectedFailure bool) bool {
	if !duringInjectedFailure || err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	for errors.Unwrap(err) != nil {
		err = errors.Unwrap(err)
	}
	return err.Error() == "http: server closed idle connection"
}

func TestFixtureExpectedConnectionLoss(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"EOF", io.EOF, true},
		{"reset", syscall.ECONNRESET, true},
		{"idle close", errors.New("http: server closed idle connection"), true},
		{"timeout", context.DeadlineExceeded, false},
		{"HTTP failure", errors.New("business 503 unavailable"), false},
		{"unrelated close", errors.New("http: server closed idle connection: other failure"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			variants := []error{test.err}
			if test.err != nil {
				variants = append(variants, &url.Error{Op: "Get", URL: "http://127.0.0.1/fixture/id", Err: fmt.Errorf("wrapped: %w", test.err)})
			}
			for _, err := range variants {
				if got := fixtureExpectedConnectionLoss(err, true); got != test.want {
					t.Errorf("injected error %v: got %v, want %v", err, got, test.want)
				}
				if fixtureExpectedConnectionLoss(err, false) {
					t.Errorf("accepted error outside injected failure: %v", err)
				}
			}
		})
	}
}
