package telemetry

import (
	"errors"
	"math"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

type pointKey struct {
	stamp int64
	field extension.Field
}
type pointBuckets struct {
	step   int64
	values map[pointKey]float64
}

func newPointBuckets(step time.Duration) (*pointBuckets, error) {
	if step.Milliseconds() <= 0 {
		return nil, errors.New("invalid query step")
	}
	return &pointBuckets{step: step.Milliseconds(), values: make(map[pointKey]float64)}, nil
}
func (b *pointBuckets) add(field extension.Field, stamp int64, value float64) error {
	if field >= extension.FieldCount || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || stamp < 0 {
		return ErrInvalidMetrics
	}
	key := pointKey{stamp / b.step * b.step, field}
	previous, exists := b.values[key]
	if !exists && len(b.values) >= 700000 {
		return errors.New("metrics result exceeds bucket limit")
	}
	if !exists || value > previous {
		b.values[key] = value
	}
	return nil
}
func (b *pointBuckets) points() []extension.Point {
	points := make([]extension.Point, 0, len(b.values))
	for key, value := range b.values {
		points = append(points, extension.Point{Timestamp: time.UnixMilli(key.stamp).UTC(), Field: key.field, Value: value})
	}
	return points
}
