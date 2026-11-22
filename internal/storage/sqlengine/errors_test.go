package sqlengine

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

func TestPostgresCommitOutcome(t *testing.T) {
	for _, code := range []string{"40001", "40P01"} {
		if !errors.Is(classify(&pgconn.PgError{Code: code}, true), storage.ErrRetry) {
			t.Fatalf("known abort %s was not retryable", code)
		}
	}
	for _, err := range []error{&pgconn.PgError{Severity: "FATAL", Code: "57P01"}, context.DeadlineExceeded, errors.New("connection reset")} {
		if !errors.Is(classify(err, true), storage.ErrUnknown) {
			t.Fatalf("commit ambiguity discarded: %v", err)
		}
	}
}
