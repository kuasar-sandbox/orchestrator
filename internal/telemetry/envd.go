package telemetry

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
)

type envdReceiver struct {
	view     *View
	cfg      envdReceiverConfig
	next     consumer.Metrics
	log      *slog.Logger
	cancel   context.CancelFunc
	done     chan struct{}
	schedule *scrapeSchedule
}

type envdReceiverConfig struct {
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	Timeout            time.Duration `mapstructure:"timeout"`
	Concurrency        int           `mapstructure:"concurrency"`
}

func (c *envdReceiverConfig) Validate() error {
	if c.CollectionInterval < time.Second || c.CollectionInterval > time.Hour || c.Timeout <= 0 || c.Timeout > c.CollectionInterval || c.Concurrency < 1 || c.Concurrency > 1024 {
		return errors.New("envd requires collection_interval in [1s, 1h], positive timeout <= interval and concurrency in [1, 1024]")
	}
	return nil
}

func envdFactory(view *View, logger *slog.Logger) receiver.Factory {
	return receiver.NewFactory(component.MustNewType("envd"), func() component.Config {
		return &envdReceiverConfig{CollectionInterval: 5 * time.Second, Timeout: time.Second, Concurrency: 64}
	}, receiver.WithMetrics(func(_ context.Context, _ receiver.Settings, raw component.Config, next consumer.Metrics) (receiver.Metrics, error) {
		cfg := raw.(*envdReceiverConfig)
		return &envdReceiver{view: view, cfg: *cfg, next: next, log: logger}, nil
	}, component.StabilityLevelStable))
}

func (r *envdReceiver) Start(ctx context.Context, _ component.Host) error {
	interval := r.cfg.CollectionInterval
	if interval <= 0 {
		return errors.New("invalid envd collection interval")
	}
	ctx, r.cancel = context.WithCancel(ctx)
	r.schedule = newScrapeSchedule(r.view, interval)
	r.done = make(chan struct{})
	go func() { defer close(r.done); r.run(ctx) }()
	return nil
}

func (r *envdReceiver) Shutdown(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *envdReceiver) run(ctx context.Context) {
	defer r.schedule.detach()
	interval := r.cfg.CollectionInterval
	transport := &http.Transport{
		MaxIdleConns: r.cfg.Concurrency, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1,
		IdleConnTimeout: 2 * interval, DisableCompression: true,
		DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			encoded, _, _ := strings.Cut(host, "-")
			path, err := hex.DecodeString(encoded)
			if err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, "unix", string(path))
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	jobs := make(chan *target, r.cfg.Concurrency)
	var workers sync.WaitGroup
	for range r.cfg.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case entry := <-jobs:
					err := r.scrape(ctx, client, entry)
					r.schedule.finished(entry)
					if err != nil && ctx.Err() == nil && entry.ctx.Err() == nil && r.log != nil {
						r.log.Debug("envd metric scrape failed", "sandbox_id", entry.route.SandboxID, "err", err)
					}
				}
			}
		}()
	}
	defer workers.Wait()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		now := time.Now()
		for _, entry := range r.schedule.takeDue(now, cap(jobs)-len(jobs)) {
			jobs <- entry
		}
		delay := r.schedule.nextDelay(now)
		if len(jobs) == cap(jobs) {
			delay = time.Hour
		} // A completion wakes us as soon as capacity is free.
		timer.Reset(delay)
		select {
		case <-ctx.Done():
			return
		case <-r.schedule.changed:
		case <-timer.C:
		}
	}
}

func (r *envdReceiver) scrape(ctx context.Context, client *http.Client, entry *target) error {
	timeout := r.cfg.Timeout
	pipelineCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stop := context.AfterFunc(entry.ctx, cancel)
	defer stop()
	if entry.ctx.Err() != nil {
		return ErrIdentity
	}
	// HTTP connection-pool identity includes the UDS, SID, and telemetry-local
	// generation. Neither another sandbox nor a replaced target can reuse it.
	host := hex.EncodeToString([]byte(entry.route.EnvdUDS)) + "-" + strconv.FormatUint(entry.generation, 16) + "-" + hex.EncodeToString([]byte(entry.route.SandboxID))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+host+"/metrics", nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-Access-Token", entry.route.EnvdAccessToken)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("envd status %d", response.StatusCode)
	}
	metrics, err := decodeEnvd(io.LimitReader(response.Body, 65537))
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := r.view.acceptMetrics(ctx, entry, "envd", metrics); err != nil {
		return err
	}
	// Entry invalidation can cancel an unfinished fetch, but not an accepted
	// sample's subsequent batch/queue delivery. The component context still
	// cancels shutdown, and the delivery retains a bounded timeout.
	pipelineCtx, stopPipeline := context.WithTimeout(pipelineCtx, timeout)
	defer stopPipeline()
	return r.next.ConsumeMetrics(pipelineCtx, metrics)
}

func decodeEnvd(reader io.Reader) (pmetric.Metrics, error) {
	var raw struct {
		Timestamp  *int64   `json:"ts"`
		CPUCount   *int32   `json:"cpu_count"`
		CPUUsedPct *float64 `json:"cpu_used_pct"`
		MemTotal   *int64   `json:"mem_total"`
		MemUsed    *int64   `json:"mem_used"`
		MemCache   *int64   `json:"mem_cache"`
		DiskTotal  *int64   `json:"disk_total"`
		DiskUsed   *int64   `json:"disk_used"`
	}
	data, err := io.ReadAll(io.LimitReader(reader, 65537))
	if err != nil || len(data) > 65536 {
		return pmetric.Metrics{}, errors.New("envd response too large or unreadable")
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return pmetric.Metrics{}, err
	}
	if raw.Timestamp == nil || raw.CPUCount == nil || raw.CPUUsedPct == nil || raw.MemTotal == nil || raw.MemUsed == nil || raw.MemCache == nil || raw.DiskTotal == nil || raw.DiskUsed == nil {
		return pmetric.Metrics{}, errors.New("envd response missing guest metric fields")
	}
	if *raw.Timestamp < 0 || *raw.Timestamp > maxQueryTime.Unix() || *raw.CPUCount <= 0 || *raw.CPUUsedPct < 0 || math.IsNaN(*raw.CPUUsedPct) || math.IsInf(*raw.CPUUsedPct, 0) {
		return pmetric.Metrics{}, ErrInvalidMetrics
	}
	values := [...]float64{float64(*raw.CPUCount), *raw.CPUUsedPct, float64(*raw.MemTotal), float64(*raw.MemUsed), float64(*raw.MemCache), float64(*raw.DiskTotal), float64(*raw.DiskUsed)}
	metrics := pmetric.NewMetrics()
	scope := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	scope.Scope().SetName("sandbox.envd")
	for i, definition := range resourceMetrics {
		if values[i] < 0 {
			return pmetric.Metrics{}, ErrInvalidMetrics
		}
		metric := scope.Metrics().AppendEmpty()
		metric.SetName(definition.name)
		metric.SetUnit(definition.unit)
		point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
		point.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(*raw.Timestamp, 0)))
		point.SetDoubleValue(values[i])
	}
	return metrics, nil
}
