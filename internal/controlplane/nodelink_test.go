package controlplane

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestInitialNodeRegistrationReadFollowsRequestCancellation(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err := readInitialNodeRegistration(ctx, reader)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("initial registration read error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("initial registration read ignored cancellation for %s", elapsed)
	}
}
