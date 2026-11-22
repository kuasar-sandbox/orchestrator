//go:build fdb && cgo

package fdbdriver

import (
	"context"
	"errors"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

func TestCommitClassification(t *testing.T) {
	for _, test := range []struct {
		code  int
		known bool
	}{{1007, true}, {1020, true}, {1021, false}, {1031, false}, {1025, false}} {
		classified := engineError(context.Background(), fdb.Error{Code: test.code}, true)
		if errors.Is(classified, storage.ErrRetry) != test.known || errors.Is(classified, storage.ErrUnknown) == test.known {
			t.Fatalf("classification %d: %v", test.code, classified)
		}
		var native fdb.Error
		if errors.As(classified, &native) {
			t.Fatal("classification exposes native retry cause")
		}
	}
	if !errors.Is(engineError(context.Background(), context.DeadlineExceeded, true), storage.ErrUnknown) {
		t.Fatal("post-commit timeout must remain unknown")
	}
}
