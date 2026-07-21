package orch

import (
	"context"
	"errors"
	"testing"
)

func TestRetryBuildTerminalCommitRetainsCompletedResult(t *testing.T) {
	attempts := 0
	transient := errors.New("sqlite busy")
	err := retryBuildTerminalCommit(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return transient
		}
		return nil
	})
	if err != nil || attempts != 3 {
		t.Fatalf("terminal commit retry = %v after %d attempts", err, attempts)
	}
}

func TestRetryBuildTerminalCommitStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	want := errors.New("sqlite closed")
	err := retryBuildTerminalCommit(ctx, func() error { return want })
	if !errors.Is(err, context.Canceled) || !errors.Is(err, want) {
		t.Fatalf("terminal commit cancellation = %v", err)
	}
}
