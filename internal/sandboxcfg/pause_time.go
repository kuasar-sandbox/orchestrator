package sandboxcfg

import (
	"context"
	"time"
)

type pauseReceiptKey struct{}

func WithPauseReceipt(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, pauseReceiptKey{}, t)
}
func PauseReceipt(ctx context.Context) time.Time {
	if t, ok := ctx.Value(pauseReceiptKey{}).(time.Time); ok {
		return t
	}
	return time.Now()
}
