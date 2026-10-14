// Immutable source-open retry policy mirrored from sandboxer/internal/readretry
// at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0).
// Package readretry owns synchronous retries of immutable source reads.
// The caller retains its request and buffers until Do returns. Only the
// runtime owner decides whether a terminal mandatory read ends the VM.
package readretry

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
)

// terminal distinguishes a stopped source read from an independent writable
// diff error or an unsupported guest request. In particular EOF/EAGAIN
// compatibility paths must not consume it as a normal completion.
type terminal struct{ err error }

func (e *terminal) Error() string { return e.err.Error() }
func (e *terminal) Unwrap() error { return e.err }

// Terminal identifies a source result that the consuming flow must not
// publish as a guest completion. It carries no process-termination policy.
func Terminal(err error) error {
	if err == nil || IsTerminal(err) {
		return err
	}
	return &terminal{err}
}

// IsTerminal recognizes a source read that cannot complete in this operation.
func IsTerminal(err error) bool {
	if err == nil {
		return false
	}
	var e *terminal
	return errors.As(err, &e)
}

// Open retries a read-only open whose unsuccessful attempts already clean
// up their resources. A successful open concurrent with operation cancellation
// is closed here rather than escaping into an ended operation.
func Open[T io.Closer](ctx context.Context, open func() (T, error)) (T, error) {
	var value T
	opened := false
	err := Do(ctx, func() error {
		var err error
		value, err = open()
		opened = err == nil
		return err
	})
	if err != nil && opened {
		_ = value.Close()
		var zero T
		return zero, err
	}
	return value, err
}

// Do retries only the supplied read attempt. Unknown access failures and
// backend-local timeouts/cancellations remain retryable while ctx is alive.
// There is no attempt or elapsed-time limit. The healthy path needs neither
// a timer nor a goroutine; retries use one interruptible timer.
func Do(ctx context.Context, read func() error) error {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	wait := func(delay time.Duration) error {
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timer.C:
			return nil
		}
	}
	return do(ctx, read, wait)
}

// do keeps retry policy deterministic in package tests while Do owns the real
// interruptible timer used in production.
func do(ctx context.Context, read func() error, wait func(time.Duration) error) error {
	const initial, maximum = 10 * time.Millisecond, time.Second
	delay := initial
	for {
		if err := context.Cause(ctx); err != nil {
			return &terminal{err}
		}
		err := read()
		if cause := context.Cause(ctx); cause != nil {
			return &terminal{cause}
		}
		if err == nil {
			return nil
		}
		if IsTerminal(err) || readerr.IsPermanent(err) {
			return &terminal{err}
		}
		waitFor := delay/2 + time.Duration(rand.Int64N(int64(delay-delay/2)+1))
		if err := wait(waitFor); err != nil {
			return &terminal{err}
		}
		delay = min(delay*2, maximum)
	}
}
