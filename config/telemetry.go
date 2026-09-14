package config

import (
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kuasar-sandbox/orchestrator/internal/util"
	"go.opentelemetry.io/collector/confmap"
	"golang.org/x/net/http/httpguts"
)

// Telemetry is the independent node-ctl telemetry declarative configuration.
// It does not configure the conductor, data proxy, or sandbox lifecycle.
type Telemetry struct {
	Collector     map[string]any `yaml:"collector,omitempty" json:"collector,omitempty"`
	ConfigSocket  string         `yaml:"config_socket" json:"config_socket"`
	APISocket     string         `yaml:"api_socket" json:"api_socket"`
	ProxyNetNS    string         `yaml:"proxy_netns,omitempty" json:"proxy_netns,omitempty"`
	RouteCapacity int            `yaml:"route_capacity" json:"route_capacity"`
	Paths         TelemetryPaths `yaml:"paths" json:"paths"`
	Query         TelemetryQuery `yaml:"query" json:"query"`
	Local         TelemetryLocal `yaml:"local" json:"local"`
}

type TelemetryPaths struct {
	TelemetryExecutable string `yaml:"telemetry_executable,omitempty" json:"telemetry_executable,omitempty"`
}

// TelemetryLocal is explicit opt-in storage, independent of the chosen Reader.
// Collector writes use sandboxlocal; enabling this does not select a query backend.
type TelemetryLocal struct {
	Enabled   bool   `yaml:"enabled" json:"enabled"`
	Path      string `yaml:"path,omitempty" json:"path,omitempty"`
	Retention string `yaml:"retention" json:"retention"`
	MaxSize   string `yaml:"max_size" json:"max_size"`
	MaxSeries int    `yaml:"max_series" json:"max_series"`
}

// TelemetryQuery configures reads only. Lookback bounds remote Bounds/Query;
// it does not configure exporter retention or create remote schemas.
type TelemetryQuery struct {
	Backend    string              `yaml:"backend" json:"backend"`
	Handler    string              `yaml:"handler" json:"handler"`
	Lookback   string              `yaml:"lookback" json:"lookback"`
	E2B        TelemetryE2B        `yaml:"e2b" json:"e2b"`
	Prometheus TelemetryPrometheus `yaml:"prometheus,omitempty" json:"prometheus,omitempty"`
	ClickHouse TelemetryClickHouse `yaml:"clickhouse,omitempty" json:"clickhouse,omitempty"`
}

type TelemetryE2B struct {
	Source string `yaml:"source" json:"source"`
	// Metrics maps E2B response fields to the selected backend's metric names.
	Metrics map[string]string `yaml:"metrics" json:"metrics"`
}

type TelemetryPrometheus struct {
	Endpoint string            `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	Headers  map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	// Labels maps logical query attributes to physical remote-read labels.
	Labels map[string]string `yaml:"labels" json:"labels"`
}

type TelemetryClickHouse struct {
	Endpoint string                    `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	Headers  map[string]string         `yaml:"headers,omitempty" json:"headers,omitempty"`
	Database string                    `yaml:"database" json:"database"`
	Tables   TelemetryClickHouseTables `yaml:"tables" json:"tables"`
}

type TelemetryClickHouseTables struct {
	Gauge                string `yaml:"gauge" json:"gauge"`
	Sum                  string `yaml:"sum" json:"sum"`
	Summary              string `yaml:"summary" json:"summary"`
	Histogram            string `yaml:"histogram" json:"histogram"`
	ExponentialHistogram string `yaml:"exponential_histogram" json:"exponential_histogram"`
}

var defaultE2BMetrics = map[string]string{
	"cpuCount": "sandbox.cpu.count", "cpuUsedPct": "sandbox.cpu.used",
	"memTotal": "sandbox.memory.total", "memUsed": "sandbox.memory.used", "memCache": "sandbox.memory.cache",
	"diskTotal": "sandbox.disk.total", "diskUsed": "sandbox.disk.used",
}

func LoadTelemetry(path string) (*Telemetry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("telemetry config: %w", err)
	}
	defer file.Close()
	return DecodeTelemetry(file)
}

func DecodeTelemetry(reader io.Reader) (*Telemetry, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, maxConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxConfigBytes {
		return nil, fmt.Errorf("configuration exceeds %d bytes", maxConfigBytes)
	}
	var cfg Telemetry
	if err := decodeKnownYAML(raw, &cfg); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := validateTelemetry(&cfg, false); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Telemetry) applyDefaults() {
	if c.ConfigSocket == "" {
		c.ConfigSocket = "/run/sandbox/node-ctl.socket"
	}
	if c.APISocket == "" {
		c.APISocket = filepath.Join(filepath.Dir(c.ConfigSocket), "telemetry.sock")
	}
	if c.RouteCapacity == 0 {
		c.RouteCapacity = 65536
	}
	q := &c.Query
	if q.Backend == "" {
		q.Backend = "none"
	}
	if q.Handler == "" {
		q.Handler = "none"
		if q.Backend != "none" {
			q.Handler = "e2b"
		}
	}
	if q.Lookback == "" {
		q.Lookback = "168h"
	}
	if q.E2B.Source == "" {
		q.E2B.Source = "envd"
	}
	if q.E2B.Metrics == nil {
		q.E2B.Metrics = cloneMap(defaultE2BMetrics)
	}
	if q.Prometheus.Labels == nil {
		q.Prometheus.Labels = map[string]string{"sandbox.id": "sandbox_id", "sandbox.stable_id": "sandbox_stable_id", "sandbox.telemetry.source": "sandbox_telemetry_source"}
	}
	if q.ClickHouse.Database == "" {
		q.ClickHouse.Database = "default"
	}
	for key, field := range map[string]*string{"gauge": &q.ClickHouse.Tables.Gauge, "sum": &q.ClickHouse.Tables.Sum, "summary": &q.ClickHouse.Tables.Summary, "histogram": &q.ClickHouse.Tables.Histogram, "exponential_histogram": &q.ClickHouse.Tables.ExponentialHistogram} {
		if *field == "" {
			*field = "otel_metrics_" + key
		}
	}
	b := &c.Local
	if b.Enabled && b.Path == "" {
		b.Path = "/var/lib/sandbox/telemetry"
	}
	if b.Retention == "" {
		b.Retention = "168h"
	}
	if b.MaxSize == "" {
		b.MaxSize = "10GiB"
	}
	if b.MaxSeries == 0 {
		b.MaxSeries = 1000000
	}

}

// ValidateTelemetryFinal never reapplies defaults after a custom Configure hook.
func ValidateTelemetryFinal(c *Telemetry) error {
	return validateTelemetry(c, true)
}

func validateTelemetry(c *Telemetry, final bool) error {
	if c == nil {
		return fmt.Errorf("telemetry config is required")
	}
	for name, path := range map[string]string{"config_socket": c.ConfigSocket, "api_socket": c.APISocket} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 107 || strings.ContainsAny(path, "\x00\r\n") || path == "/" {
			return fmt.Errorf("telemetry config: %s must be a canonical absolute local socket path (max 107 bytes)", name)
		}
	}
	if c.APISocket == c.ConfigSocket {
		return fmt.Errorf("telemetry config: api_socket conflicts with config_socket")
	}
	if path := c.Paths.TelemetryExecutable; path != "" && !filepath.IsAbs(path) {
		return fmt.Errorf("paths.telemetry_executable must be absolute")
	}
	if strings.ContainsAny(c.ProxyNetNS, "\x00\r\n") || (c.ProxyNetNS != "" && !filepath.IsAbs(c.ProxyNetNS) && (strings.Contains(c.ProxyNetNS, "/") || c.ProxyNetNS == "." || c.ProxyNetNS == "..")) {
		return fmt.Errorf("telemetry proxy_netns must be a namespace name or absolute path")
	}
	if c.RouteCapacity < 1 || c.RouteCapacity > 1000000 {
		return fmt.Errorf("telemetry route_capacity must be in [1, 1000000]")
	}
	q := c.Query
	switch q.Backend {
	case "local", "none", "custom", "prometheus", "clickhouse":
	default:
		return fmt.Errorf("telemetry query.backend must be local, prometheus, clickhouse, none, or custom")
	}
	if q.Handler != "none" && q.Handler != "e2b" && q.Handler != "custom" {
		return fmt.Errorf("telemetry query.handler must be none, e2b, or custom")
	}
	if q.Backend == "none" && q.Handler != "none" {
		return fmt.Errorf("telemetry query handler requires a backend")
	}
	if q.Backend == "local" && !c.Local.Enabled {
		return fmt.Errorf("telemetry query.backend=local requires local.enabled")
	}
	lookback, err := time.ParseDuration(q.Lookback)
	if err != nil || lookback < time.Minute {
		return fmt.Errorf("telemetry query.lookback must be >= 1m")
	}
	if q.Handler == "e2b" {
		if q.E2B.Source == "" || len(q.E2B.Source) > 128 || !utf8.ValidString(q.E2B.Source) || strings.ContainsRune(q.E2B.Source, 0) {
			return fmt.Errorf("invalid E2B source")
		}
		if len(q.E2B.Metrics) != len(defaultE2BMetrics) {
			return fmt.Errorf("E2B mapping requires all seven response fields")
		}
		seen := map[string]bool{}
		for field := range defaultE2BMetrics {
			name := q.E2B.Metrics[field]
			if name == "" || len(name) > 128 || !utf8.ValidString(name) || strings.ContainsRune(name, 0) || seen[name] {
				return fmt.Errorf("invalid or duplicate E2B metric mapping")
			}
			seen[name] = true
		}
	}
	b := c.Local
	if b.Enabled && (!filepath.IsAbs(b.Path) || filepath.Clean(b.Path) != b.Path || b.Path == "/" || strings.ContainsRune(b.Path, 0)) {
		return fmt.Errorf("telemetry local.path must be a canonical absolute directory")
	}
	retention, err := time.ParseDuration(b.Retention)
	if err != nil || retention < time.Minute {
		return fmt.Errorf("telemetry local.retention must be >= 1m")
	}
	size, err := util.ParseSize(b.MaxSize)
	if err != nil || size < 64<<20 || size > math.MaxInt64 {
		return fmt.Errorf("telemetry local.max_size must be in [64MiB, MaxInt64]")
	}
	if b.MaxSeries < 1 || b.MaxSeries > 10000000 {
		return fmt.Errorf("telemetry local.max_series must be in [1, 10000000]")
	}
	switch q.Backend {
	case "prometheus":
		if err := validateTelemetryRemote(q.Prometheus.Endpoint, q.Prometheus.Headers, final); err != nil {
			return fmt.Errorf("telemetry query Prometheus: %w", err)
		}
		if len(q.Prometheus.Labels) > 110 {
			return fmt.Errorf("too many Prometheus label mappings")
		}
		seen := map[string]bool{}
		for key, value := range q.Prometheus.Labels {
			if key == "" || value == "" || key == "__name__" || value == "__name__" || len(key) > 160 || len(value) > 160 || !utf8.ValidString(key) || !utf8.ValidString(value) || strings.ContainsRune(key+value, 0) || seen[value] {
				return fmt.Errorf("invalid or duplicate Prometheus label mapping")
			}
			seen[value] = true
		}
		for key, value := range q.Prometheus.Labels {
			if key != value && q.Prometheus.Labels[value] != "" {
				return fmt.Errorf("overlapping Prometheus label mapping")
			}
		}
	case "clickhouse":
		if err := validateTelemetryRemote(q.ClickHouse.Endpoint, q.ClickHouse.Headers, final); err != nil {
			return fmt.Errorf("telemetry query ClickHouse: %w", err)
		}
		for _, name := range []string{q.ClickHouse.Database, q.ClickHouse.Tables.Gauge, q.ClickHouse.Tables.Sum, q.ClickHouse.Tables.Summary, q.ClickHouse.Tables.Histogram, q.ClickHouse.Tables.ExponentialHistogram} {
			if len(name) == 0 || len(name) > 63 {
				return fmt.Errorf("telemetry ClickHouse identifiers require 1..63 characters")
			}
			for i, char := range name {
				if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char == '_' || i > 0 && char >= '0' && char <= '9') {
					return fmt.Errorf("telemetry ClickHouse identifier is invalid")
				}
			}
		}
	}

	return nil
}

func validateTelemetryRemote(endpoint string, headers map[string]string, final bool) error {
	// A custom Configure hook may supply an omitted endpoint. An explicitly
	// malformed declaration is rejected even before dispatch, as in other Apps.
	if endpoint != "" || final {
		u, err := url.Parse(endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
			return fmt.Errorf("invalid HTTP endpoint")
		}
	}
	if len(headers) > 32 {
		return fmt.Errorf("too many headers")
	}
	for key, value := range headers {
		if len(key) > 128 || len(value) > 8192 || !httpguts.ValidHeaderFieldName(key) || !httpguts.ValidHeaderFieldValue(value) {
			return fmt.Errorf("invalid header")
		}
	}
	return nil
}

func (c *Telemetry) Clone() *Telemetry {
	if c == nil {
		return nil
	}
	out := *c
	if c.Collector != nil {
		out.Collector = confmap.NewFromStringMap(c.Collector).ToStringMap()
	}
	out.Query.Prometheus.Headers = cloneMap(c.Query.Prometheus.Headers)
	out.Query.Prometheus.Labels = cloneMap(c.Query.Prometheus.Labels)
	out.Query.ClickHouse.Headers = cloneMap(c.Query.ClickHouse.Headers)
	out.Query.E2B.Metrics = cloneMap(c.Query.E2B.Metrics)
	return &out
}
