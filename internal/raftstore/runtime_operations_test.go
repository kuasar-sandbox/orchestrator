package raftstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimeOperationContextAddsBoundAndPreservesShorterCallerDeadline(t *testing.T) {
	tuning := DefaultRuntimeTuning()
	tuning.OperationTimeoutMillis = 5000
	runtime := &Runtime{config: RuntimeConfig{Tuning: tuning}}
	started := time.Now()
	operation, cancel := runtime.operationContext(context.Background())
	defer cancel()
	deadline, ok := operation.Deadline()
	if !ok || deadline.Before(started.Add(4900*time.Millisecond)) || deadline.After(started.Add(5100*time.Millisecond)) {
		t.Fatalf("operation deadline = %v, started = %v", deadline, started)
	}

	caller, callerCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer callerCancel()
	callerDeadline, _ := caller.Deadline()
	bounded, boundedCancel := runtime.operationContext(caller)
	defer boundedCancel()
	boundedDeadline, ok := bounded.Deadline()
	if !ok || !boundedDeadline.Equal(callerDeadline) {
		t.Fatalf("short caller deadline changed from %v to %v", callerDeadline, boundedDeadline)
	}
}

func TestRuntimeStartupRetryRejectsPermanentErrors(t *testing.T) {
	tuning := DefaultRuntimeTuning()
	runtime := &Runtime{config: RuntimeConfig{Tuning: tuning}}
	permanent := errors.New("invalid bootstrap identity")
	attempts := 0
	err := runtime.retryStartupOperation(context.Background(), func(context.Context) (bool, error) {
		attempts++
		return false, permanent
	})
	if !errors.Is(err, permanent) || attempts != 1 {
		t.Fatalf("permanent startup error = %v after %d attempts", err, attempts)
	}
}
