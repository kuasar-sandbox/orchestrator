package telemetry

import (
	"context"
	"log/slog"

	"go.uber.org/zap/zapcore"
)

// Bridge Collector component diagnostics to the component's configured logger.
// No additional log endpoint or independently configured logging subsystem.
type slogCore struct {
	logger *slog.Logger
	fields []zapcore.Field
}

func (c *slogCore) Enabled(level zapcore.Level) bool {
	return c.logger.Enabled(context.Background(), slogLevel(level))
}
func (c *slogCore) With(fields []zapcore.Field) zapcore.Core {
	return &slogCore{logger: c.logger, fields: append(append([]zapcore.Field(nil), c.fields...), fields...)}
}
func (c *slogCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}
func (c *slogCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	encoder := zapcore.NewMapObjectEncoder()
	for _, field := range c.fields {
		field.AddTo(encoder)
	}
	for _, field := range fields {
		field.AddTo(encoder)
	}
	attrs := make([]slog.Attr, 0, len(encoder.Fields))
	for key, value := range encoder.Fields {
		attrs = append(attrs, slog.Any(key, value))
	}
	c.logger.LogAttrs(context.Background(), slogLevel(entry.Level), entry.Message, attrs...)
	return nil
}
func (*slogCore) Sync() error { return nil }
func slogLevel(level zapcore.Level) slog.Level {
	switch {
	case level < zapcore.InfoLevel:
		return slog.LevelDebug
	case level < zapcore.WarnLevel:
		return slog.LevelInfo
	case level < zapcore.ErrorLevel:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}
