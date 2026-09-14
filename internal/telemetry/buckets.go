package telemetry

import (
	"encoding/json"
	"errors"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

const maxQueryPoints = 700000

func validateSelection(selection extension.Selection) error {
	if selection.SandboxID == "" || len(selection.SandboxID) > 128 || !utf8.ValidString(selection.SandboxID) || len(selection.Metrics) > 64 || len(selection.Attributes) > 110 {
		return errors.New("invalid metrics selection")
	}
	for _, name := range selection.Metrics {
		if name == "" || len(name) > 128 || !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
			return errors.New("invalid metric name")
		}
	}
	for key, value := range selection.Attributes {
		if key == "" || len(key) > 160 || len(value) > 256 || !utf8.ValidString(key) || !utf8.ValidString(value) || key == "__name__" || key == SandboxIDAttribute && value != selection.SandboxID {
			return errors.New("invalid metric attribute or sandbox scope")
		}
	}
	return nil
}

func selectedSeries(selection extension.Selection, name string, attributes map[string]string) bool {
	if attributes[SandboxIDAttribute] != selection.SandboxID || len(selection.Metrics) != 0 && !slices.Contains(selection.Metrics, name) {
		return false
	}
	for key, value := range selection.Attributes {
		if actual, present := attributes[key]; !present || actual != value {
			return false
		}
	}
	return true
}

// Prometheus equality with an empty value also retrieves absent labels. Filter
// those physical matches, while still rejecting any other out-of-scope result.
// Local TSDB and remote read use the same physical matcher semantics.
func matchPrometheusSeries(selection extension.Selection, name string, attributes map[string]string) (bool, error) {
	object := selection
	object.Attributes = nil
	if !selectedSeries(object, name, attributes) {
		return false, errors.New("Prometheus returned a series outside the selected scope")
	}
	matched := true
	for key, value := range selection.Attributes {
		actual, present := attributes[key]
		if !present && value == "" {
			matched = false
			continue
		}
		if !present || actual != value {
			return false, errors.New("Prometheus returned a series outside the selected scope")
		}
	}
	return matched, nil
}

// A scan visits a series once per returned frame/chunk, then streams its points.
// This avoids repeating attribute copies or identity hashing for every point.
type seriesVisitor func(string, map[string]string) (func(int64, float64) error, error)

type seriesValues struct {
	series extension.Series
	values map[int64]float64
}
type seriesBuckets struct {
	query  extension.Query
	series map[string]*seriesValues
	count  int
}

func newSeriesBuckets(query extension.Query) (*seriesBuckets, error) {
	if err := validateSelection(query.Selection); err != nil {
		return nil, err
	}
	if err := ValidateRange(query.Start, query.End); err != nil {
		return nil, err
	}
	if query.Aggregation == "" {
		query.Aggregation = extension.Raw
	}
	if query.Aggregation != extension.Raw && query.Aggregation != extension.Max || query.Aggregation == extension.Raw && query.Step != 0 || query.Aggregation == extension.Max && (query.Step < time.Millisecond || query.Step%time.Millisecond != 0) {
		return nil, errors.New("invalid metrics aggregation or step")
	}
	return &seriesBuckets{query: query, series: make(map[string]*seriesValues)}, nil
}

func (b *seriesBuckets) visit(name string, attributes map[string]string) (func(int64, float64) error, error) {
	return b.visitor(name, attributes, false)
}

// visitBucket accepts a backend's already filtered, epoch-aligned MAX result.
func (b *seriesBuckets) visitBucket(name string, attributes map[string]string) (func(int64, float64) error, error) {
	return b.visitor(name, attributes, true)
}

func (b *seriesBuckets) visitor(name string, attributes map[string]string, bucketed bool) (func(int64, float64) error, error) {
	if !selectedSeries(b.query.Selection, name, attributes) || len(attributes) > 110 {
		return nil, errors.New("backend returned a series outside the selected scope")
	}
	raw, err := json.Marshal(struct {
		Name       string
		Attributes map[string]string
	}{name, attributes})
	if err != nil {
		return nil, err
	}
	key := string(raw)
	series := b.series[key]
	if series == nil {
		if len(b.series) >= 100000 {
			return nil, errors.New("metrics result exceeds series limit")
		}
		series = &seriesValues{series: extension.Series{Metric: name, Attributes: maps.Clone(attributes)}, values: make(map[int64]float64)}
		b.series[key] = series
	}
	return func(stamp int64, value float64) error {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil // Staleness markers are not observations.
		}
		lower := b.query.Start.UnixMilli()
		if bucketed {
			step := b.query.Step.Milliseconds()
			if b.query.Aggregation != extension.Max || step <= 0 || stamp%step != 0 {
				return ErrInvalidMetrics
			}
			lower = lower / step * step
			if stamp < lower || stamp > b.query.End.UnixMilli() {
				return ErrInvalidMetrics
			}
		}
		if stamp < lower || stamp > b.query.End.UnixMilli() {
			return nil
		}
		if b.query.Aggregation == extension.Max {
			step := b.query.Step.Milliseconds()
			stamp = stamp / step * step
		}
		previous, exists := series.values[stamp]
		if !exists {
			if b.count >= maxQueryPoints {
				return errors.New("metrics result exceeds point limit")
			}
			b.count++
			series.values[stamp] = value
		} else if b.query.Aggregation == extension.Max {
			series.values[stamp] = max(previous, value)
		} else if previous != value {
			return errors.New("backend returned conflicting raw observations at one timestamp")
		}
		return nil
	}, nil
}

func (b *seriesBuckets) result() []extension.Series {
	keys := make([]string, 0, len(b.series))
	for key := range b.series {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]extension.Series, 0, len(keys))
	for _, key := range keys {
		s := b.series[key]
		if len(s.values) == 0 {
			continue
		}
		for stamp, value := range s.values {
			s.series.Points = append(s.series.Points, extension.Point{Timestamp: time.UnixMilli(stamp).UTC(), Value: value})
		}
		sort.Slice(s.series.Points, func(i, j int) bool { return s.series.Points[i].Timestamp.Before(s.series.Points[j].Timestamp) })
		out = append(out, s.series)
	}
	return out
}
