package main

import (
	"errors"
	"testing"
)

func TestWaitRegistryExitPropagatesBackgroundFailure(t *testing.T) {
	serverErr := make(chan error, 1)
	backgroundErr := make(chan error, 1)
	want := errors.New("workflow failed")
	backgroundErr <- want
	serverErr <- nil
	stopped := false

	err := waitRegistryExit(func() { stopped = true }, serverErr, backgroundErr)
	if !errors.Is(err, want) || !stopped {
		t.Fatalf("registry exit = %v, stopped=%t", err, stopped)
	}
}

func TestWaitRegistryExitPreservesServerResult(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "clean shutdown"},
		{name: "listener failure", err: errors.New("listener failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			serverErr := make(chan error, 1)
			serverErr <- test.err
			if err := waitRegistryExit(func() {}, serverErr, make(chan error)); !errors.Is(err, test.err) {
				t.Fatalf("registry exit = %v, want %v", err, test.err)
			}
		})
	}
}
