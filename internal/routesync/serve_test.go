package routesync

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestStreamAuthorityAllowsNilLoggerAtReadEnd(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		StreamAuthority(
			context.Background(),
			io.Discard,
			func() {},
			bytes.NewReader(nil),
			nil,
			Register{},
			nil,
			nil,
			nil,
		)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("authority did not stop after the reader reached EOF")
	}
}
