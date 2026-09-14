package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/util"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

var ErrSeriesLimit = errors.New("telemetry: local storage series limit reached")

// Local contains only the embedded TSDB; no Prometheus server, scrape manager,
// rules, or alerting machinery. A transaction batches each Collector delivery.
type Local struct {
	db           *tsdb.DB
	retention    time.Duration
	writeMu      sync.Mutex
	closed       atomic.Bool
	now          func() time.Time
	failure      atomic.Pointer[storageFailure]
	errors       chan error
	stop         context.CancelFunc
	done         chan struct{}
	shutdownOnce sync.Once
	closeErr     error
}

type storageFailure struct{ err error }

func (l *Local) Errors() <-chan error { return l.errors }
func (l *Local) fail(err error) {
	if l.failure.CompareAndSwap(nil, &storageFailure{err: err}) {
		l.errors <- err
	}
}
func (l *Local) health() error {
	if failure := l.failure.Load(); failure != nil {
		return failure.err
	}
	if l.closed.Load() {
		return errors.New("telemetry TSDB is closed")
	}
	return nil
}

type seriesLimit struct {
	count   atomic.Int64
	enabled atomic.Bool
	max     int64
}

func (s *seriesLimit) PreCreation(labels.Labels) error {
	// Appends are serialized, and WAL replay finishes before enabling the cap.
	// GC can only decrease count concurrently. No reservations can leak when
	// TSDB discovers an already-created series during WAL replay.
	if s.enabled.Load() && s.count.Load() >= s.max {
		return ErrSeriesLimit
	}
	return nil
}
func (s *seriesLimit) PostCreation(labels.Labels) { s.count.Add(1) }
func (s *seriesLimit) PostDeletion(series map[chunks.HeadSeriesRef]labels.Labels) {
	s.count.Add(-int64(len(series)))
}

func OpenLocal(cfg config.TelemetryLocal, logger *slog.Logger) (*Local, error) {
	return openLocalWithClock(cfg, logger, time.Now)
}

func openLocalWithClock(cfg config.TelemetryLocal, logger *slog.Logger, now func() time.Time) (*Local, error) {
	retention, err := time.ParseDuration(cfg.Retention)
	if err != nil {
		return nil, err
	}
	maxBytes, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, err
	}
	limit := &seriesLimit{max: int64(cfg.MaxSeries)}
	local := &Local{retention: retention, now: now, errors: make(chan error, 1), done: make(chan struct{})}
	opts := tsdb.DefaultOptions()
	opts.RetentionDuration = retention.Milliseconds()
	opts.MaxBytes = int64(maxBytes)
	opts.MinBlockDuration = min((2 * time.Hour).Milliseconds(), max(int64(1000), retention.Milliseconds()/4))
	opts.MaxBlockDuration = opts.MinBlockDuration
	opts.WALSegmentSize = 16 << 20
	opts.WALReplayConcurrency = 1
	opts.SeriesLifecycleCallback = limit
	// Samples are in milliseconds. Modest reordering tolerates OTLP batching;
	// older/conflicting points return an explicit error, never alter history.
	opts.OutOfOrderTimeWindow = min((5 * time.Minute).Milliseconds(), retention.Milliseconds())
	// Prometheus normally bases block expiry on the newest observed sample.
	// A node whose sandboxes are all paused must still expire idle history.
	var dbReference atomic.Pointer[tsdb.DB]
	opts.BlocksToDelete = func(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
		deleted := make(map[ulid.ULID]struct{})
		if db := dbReference.Load(); db != nil {
			deleted = tsdb.DefaultBlocksToDelete(db)(blocks)
		}
		cutoff := local.now().Add(-retention).UnixMilli()
		for _, block := range blocks {
			if block.Meta().MaxTime <= cutoff || block.Meta().Compaction.Deletable {
				deleted[block.Meta().ULID] = struct{}{}
			}
		}
		return deleted
	}
	if logger == nil {
		logger = slog.Default()
	}
	db, err := tsdb.Open(cfg.Path, slog.New(&storageLogHandler{Handler: logger.Handler(), fail: local.fail}), prometheus.NewRegistry(), opts, nil)
	if err != nil {
		return nil, fmt.Errorf("open telemetry TSDB: %w", err)
	}
	if limit.count.Load() > limit.max {
		return nil, errors.Join(ErrSeriesLimit, db.Close())
	}
	if err := local.health(); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	limit.enabled.Store(true)
	local.db = db
	dbReference.Store(db)
	ctx, stop := context.WithCancel(context.Background())
	local.stop = stop
	go local.maintain(ctx)
	return local, nil
}

func (l *Local) Write(ctx context.Context, samples []extension.Sample) error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	if err := l.health(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(samples) > maxWriteSamples {
		return ErrInvalidMetrics
	}
	now := l.now()
	cutoff := now.Add(-l.retention).UnixMilli()
	appender := l.db.Appender(ctx)
	committed := false
	defer func() {
		if !committed {
			_ = appender.Rollback()
		}
	}()
	// Stable timestamp ordering avoids creating avoidable out-of-order writes
	// within one Collector delivery. Never reorder across different requests.
	ordered := append([]extension.Sample(nil), samples...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Timestamp.Before(ordered[j].Timestamp) })
	for _, sample := range ordered {
		if err := ctx.Err(); err != nil {
			return err
		}
		stamp := sample.Timestamp.UnixMilli()
		if stamp < cutoff {
			continue
		} // Retention also applies to the un-compacted head.
		if stamp > now.Add(time.Minute).UnixMilli() || stamp < 0 || math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
			return ErrInvalidMetrics
		}
		if sample.Metric == "" || len(sample.Metric) > 128 || len(sample.Labels) > 110 {
			return ErrInvalidMetrics
		}
		builder := labels.NewScratchBuilder(len(sample.Labels) + 1)
		builder.Add(labels.MetricName, sample.Metric)
		for key, value := range sample.Labels {
			if key == labels.MetricName || forbiddenAttribute(key) || len(key) > 160 || len(value) > 256 {
				return ErrInvalidMetrics
			}
			builder.Add(key, value)
		}
		builder.Sort()
		_, err := appender.Append(0, builder.Labels(), stamp, sample.Value)
		if err != nil {
			return fmt.Errorf("append telemetry sample: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := appender.Commit()
	committed = true // Commit owns its rollback/cleanup, also on error.
	return err
}

func (l *Local) Shutdown(context.Context) error {
	l.shutdownOnce.Do(func() {
		l.closed.Store(true)
		l.stop()
		<-l.done
		l.writeMu.Lock()
		defer l.writeMu.Unlock()
		l.closeErr = l.db.Close()
	})
	return l.closeErr
}

func (l *Local) maintain(ctx context.Context) {
	defer close(l.done)
	ticker := time.NewTicker(min(time.Minute, l.retention/2))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := l.expireHead(); err != nil {
			l.fail(fmt.Errorf("telemetry head retention: %w", err))
			return
		}
	}
}

func (l *Local) expireHead() error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	if l.closed.Load() {
		return nil
	}
	cutoff := l.now().Add(-l.retention).UnixMilli()
	if l.db.Head().MinTime() < cutoff {
		return l.db.Head().Truncate(cutoff)
	}
	return nil
}

func selectionMatchers(selection extension.Selection) []*labels.Matcher {
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, SandboxIDAttribute, selection.SandboxID)}
	if len(selection.Metrics) != 0 {
		names := make([]string, 0, len(selection.Metrics))
		for _, name := range selection.Metrics {
			names = append(names, regexp.QuoteMeta(name))
		}
		matchers = append(matchers, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, strings.Join(names, "|")))
	}
	for key, value := range selection.Attributes {
		if key != SandboxIDAttribute {
			matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, key, value))
		}
	}
	return matchers
}

func (l *Local) scan(ctx context.Context, selection extension.Selection, start, end time.Time, visit seriesVisitor) (err error) {
	if err := l.health(); err != nil {
		return err
	}
	if err := validateSelection(selection); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	startMS := max(start.UnixMilli(), l.now().Add(-l.retention).UnixMilli())
	if startMS > end.UnixMilli() {
		return nil
	}
	querier, err := l.db.Querier(startMS, end.UnixMilli())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, querier.Close()) }()
	set := querier.Select(ctx, false, &storage.SelectHints{Start: startMS, End: end.UnixMilli()}, selectionMatchers(selection)...)
	var iterator chunkenc.Iterator
	for set.Next() {
		series := set.At()
		attributes := make(map[string]string, series.Labels().Len())
		series.Labels().Range(func(label labels.Label) {
			if label.Name != labels.MetricName {
				attributes[label.Name] = label.Value
			}
		})
		name := series.Labels().Get(labels.MetricName)
		matched, err := matchPrometheusSeries(selection, name, attributes)
		if err != nil {
			return err
		}
		if !matched {
			continue
		}
		point, err := visit(name, attributes)
		if err != nil {
			return err
		}
		iterator = series.Iterator(iterator)
		for valueType := iterator.Next(); valueType != chunkenc.ValNone; valueType = iterator.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if valueType != chunkenc.ValFloat {
				return ErrInvalidMetrics
			}
			stamp, value := iterator.At()
			if stamp < startMS || stamp > end.UnixMilli() {
				continue
			}
			if err := point(stamp, value); err != nil {
				return err
			}
		}
		if err := iterator.Err(); err != nil {
			return err
		}
	}
	return set.Err()
}

func (l *Local) Bounds(ctx context.Context, selection extension.Selection) (start, end time.Time, found bool, err error) {
	err = l.scan(ctx, selection, time.Unix(0, 0), maxQueryTime, func(name string, attributes map[string]string) (func(int64, float64) error, error) {
		if !selectedSeries(selection, name, attributes) {
			return nil, errors.New("local series outside selected scope")
		}
		return func(stamp int64, sample float64) error {
			if math.IsNaN(sample) || math.IsInf(sample, 0) {
				return nil
			}
			value := time.UnixMilli(stamp).UTC()
			if !found || value.Before(start) {
				start = value
			}
			if !found || value.After(end) {
				end = value
			}
			found = true
			return nil
		}, nil
	})
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return
}

func (l *Local) Query(ctx context.Context, query extension.Query) ([]extension.Series, error) {
	buckets, err := newSeriesBuckets(query)
	if err != nil {
		return nil, err
	}
	err = l.scan(ctx, query.Selection, query.Start, query.End, buckets.visit)
	if err != nil {
		return nil, err
	}
	result := buckets.result()
	return result, ctx.Err()
}

var _ extension.QueryBackend = (*Local)(nil)
