package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// TSDB's background compaction/reload errors are logged rather than returned
// from Appender. Surface them as component health errors instead of continuing
// to advertise a silently damaged primary reader. WAL repair is made visible
// as well; the underlying library owns its standard recovery procedure.
type storageLogHandler struct {
	slog.Handler
	fail func(error)
}

func (h *storageLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn || h.Handler.Enabled(ctx, level)
}
func (h *storageLogHandler) Handle(ctx context.Context, record slog.Record) error {
	message := strings.ToLower(record.Message)
	if record.Level >= slog.LevelError || (record.Level >= slog.LevelWarn && (strings.Contains(message, "corrupt") || strings.Contains(message, "wal read error"))) {
		var cause error
		record.Attrs(func(attr slog.Attr) bool {
			if value, ok := attr.Value.Any().(error); ok {
				cause = value
			}
			return true
		})
		if cause == nil {
			cause = fmt.Errorf("%s", record.Message)
		}
		h.fail(fmt.Errorf("telemetry TSDB %s: %w", record.Message, cause))
	}
	if h.Handler.Enabled(ctx, record.Level) {
		return h.Handler.Handle(ctx, record)
	}
	return nil
}
func (h *storageLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &storageLogHandler{Handler: h.Handler.WithAttrs(attrs), fail: h.fail}
}
func (h *storageLogHandler) WithGroup(name string) slog.Handler {
	return &storageLogHandler{Handler: h.Handler.WithGroup(name), fail: h.fail}
}
