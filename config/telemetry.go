package config

import (
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/util"
	"golang.org/x/net/http/httpguts"
)

// Telemetry is the independent node-ctl telemetry declarative configuration.
// It does not configure the conductor, data proxy, or sandbox lifecycle.
type Telemetry struct {
	ConfigSocket  string            `yaml:"config_socket" json:"config_socket"`
	APISocket     string            `yaml:"api_socket" json:"api_socket"`
	ProxyNetNS    string            `yaml:"proxy_netns,omitempty" json:"proxy_netns,omitempty"`
	RouteCapacity int               `yaml:"route_capacity" json:"route_capacity"`
	Paths         TelemetryPaths    `yaml:"paths" json:"paths"`
	Telemetry     TelemetryPipeline `yaml:"telemetry" json:"telemetry"`
}

type TelemetryPaths struct {
	TelemetryExecutable string `yaml:"telemetry_executable,omitempty" json:"telemetry_executable,omitempty"`
}

type TelemetryPipeline struct {
	Scrape    TelemetryScrape     `yaml:"scrape" json:"scrape"`
	OTLP      TelemetryOTLP       `yaml:"otlp" json:"otlp"`
	Storage   TelemetryStorage    `yaml:"storage" json:"storage"`
	Exporters []TelemetryExporter `yaml:"exporters,omitempty" json:"exporters,omitempty"`
}

type TelemetryScrape struct {
	Interval    string `yaml:"interval" json:"interval"`
	Timeout     string `yaml:"timeout" json:"timeout"`
	Concurrency int    `yaml:"concurrency" json:"concurrency"`
}

type TelemetryOTLP struct {
	Enabled         *bool  `yaml:"enabled" json:"enabled"`
	GRPCListen      string `yaml:"grpc_listen" json:"grpc_listen"`
	HTTPListen      string `yaml:"http_listen" json:"http_listen"`
	MaxConnections  int    `yaml:"max_connections" json:"max_connections"`
	MaxRequests     int    `yaml:"max_requests" json:"max_requests"`
	MaxRequestBytes int    `yaml:"max_request_bytes" json:"max_request_bytes"`
}

// TelemetryStorage selects the primary write AND read backend. Exporters below
// are only fan-out destinations; configuring one never enables metrics queries.
type TelemetryStorage struct {
	Type       string              `yaml:"type" json:"type"`
	Path       string              `yaml:"path,omitempty" json:"path,omitempty"`
	Retention  string              `yaml:"retention" json:"retention"`
	MaxSize    string              `yaml:"max_size" json:"max_size"`
	MaxSeries  int                 `yaml:"max_series" json:"max_series"`
	Prometheus TelemetryRemote     `yaml:"prometheus,omitempty" json:"prometheus,omitempty"`
	ClickHouse TelemetryClickHouse `yaml:"clickhouse,omitempty" json:"clickhouse,omitempty"`
}

type TelemetryRemote struct {
	Endpoint string            `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	Headers  map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
}

type TelemetryClickHouse struct {
	Endpoint string            `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	Headers  map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Database string            `yaml:"database,omitempty" json:"database,omitempty"`
	Table    string            `yaml:"table,omitempty" json:"table,omitempty"`
}

type TelemetryExporter struct {
	Name     string            `yaml:"name" json:"name"`
	Type     string            `yaml:"type" json:"type"`
	Endpoint string            `yaml:"endpoint" json:"endpoint"`
	Headers  map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
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
	s := &c.Telemetry.Scrape
	if s.Interval == "" {
		s.Interval = "5s"
	}
	if s.Timeout == "" {
		s.Timeout = "1s"
	}
	if s.Concurrency == 0 {
		s.Concurrency = 64
	}
	o := &c.Telemetry.OTLP
	if o.Enabled == nil {
		enabled := true
		o.Enabled = &enabled
	}
	if o.GRPCListen == "" {
		o.GRPCListen = ":4317"
	}
	if o.HTTPListen == "" {
		o.HTTPListen = ":4318"
	}
	if o.MaxConnections == 0 {
		o.MaxConnections = 256
	}
	if o.MaxRequests == 0 {
		o.MaxRequests = 32
	}
	if o.MaxRequestBytes == 0 {
		o.MaxRequestBytes = 4 << 20
	}
	b := &c.Telemetry.Storage
	if b.Type == "" {
		b.Type = "local"
	}
	if b.Path == "" && b.Type == "local" {
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
	if b.Type == "clickhouse" && b.ClickHouse.Database == "" {
		b.ClickHouse.Database = "default"
	}
	if b.Type == "clickhouse" && b.ClickHouse.Table == "" {
		b.ClickHouse.Table = "sandbox_metrics"
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
	s := c.Telemetry.Scrape
	interval, err := time.ParseDuration(s.Interval)
	if err != nil || interval < time.Second || interval > time.Hour {
		return fmt.Errorf("telemetry scrape.interval must be in [1s, 1h]")
	}
	timeout, err := time.ParseDuration(s.Timeout)
	if err != nil || timeout <= 0 || timeout > interval {
		return fmt.Errorf("telemetry scrape.timeout must be positive and <= interval")
	}
	if s.Concurrency < 1 || s.Concurrency > 1024 {
		return fmt.Errorf("telemetry scrape.concurrency must be in [1, 1024]")
	}
	o := c.Telemetry.OTLP
	if o.Enabled == nil {
		return fmt.Errorf("telemetry otlp.enabled must be resolved")
	}
	if o.MaxConnections < 1 || o.MaxConnections > 65536 || o.MaxRequests < 1 || o.MaxRequests > 4096 || o.MaxRequestBytes < 1024 || o.MaxRequestBytes > 64<<20 {
		return fmt.Errorf("telemetry OTLP limits are out of range")
	}
	if *o.Enabled {
		for _, address := range []string{o.GRPCListen, o.HTTPListen} {
			_, port, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("telemetry OTLP listen address: %w", err)
			}
			if n, err := strconv.Atoi(port); err != nil || n < 0 || n > 65535 {
				return fmt.Errorf("telemetry OTLP listen port must be in [0, 65535]")
			}
		}
		if o.GRPCListen == o.HTTPListen {
			return fmt.Errorf("telemetry OTLP listeners must be distinct")
		}
	}
	b := c.Telemetry.Storage
	switch b.Type {
	case "local", "none", "custom", "prometheus", "clickhouse":
	default:
		return fmt.Errorf("telemetry storage.type must be local, prometheus, clickhouse, none, or custom")
	}
	if b.Type == "local" && (!filepath.IsAbs(b.Path) || filepath.Clean(b.Path) != b.Path || b.Path == "/" || strings.ContainsRune(b.Path, 0)) {
		return fmt.Errorf("telemetry local storage.path must be a canonical absolute directory")
	}
	retention, err := time.ParseDuration(b.Retention)
	if err != nil || retention < time.Minute {
		return fmt.Errorf("telemetry storage.retention must be >= 1m")
	}
	size, err := util.ParseSize(b.MaxSize)
	if err != nil || size < 64<<20 || size > math.MaxInt64 {
		return fmt.Errorf("telemetry storage.max_size must be in [64MiB, MaxInt64]")
	}
	if b.MaxSeries < 7 || b.MaxSeries > 10000000 {
		return fmt.Errorf("telemetry storage.max_series must be in [7, 10000000]")
	}
	switch b.Type {
	case "prometheus":
		if err := validateTelemetryRemote(b.Prometheus.Endpoint, b.Prometheus.Headers, final); err != nil {
			return fmt.Errorf("telemetry primary Prometheus: %w", err)
		}
	case "clickhouse":
		if err := validateTelemetryRemote(b.ClickHouse.Endpoint, b.ClickHouse.Headers, final); err != nil {
			return fmt.Errorf("telemetry primary ClickHouse: %w", err)
		}
		for _, name := range []string{b.ClickHouse.Database, b.ClickHouse.Table} {
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
	seen := map[string]bool{}
	if len(c.Telemetry.Exporters) > 16 {
		return fmt.Errorf("telemetry supports at most 16 extra exporters")
	}
	for _, exporter := range c.Telemetry.Exporters {
		if exporter.Name == "" || len(exporter.Name) > 64 || strings.ContainsAny(exporter.Name, "/\x00\r\n") || seen[exporter.Name] {
			return fmt.Errorf("telemetry exporters require unique names (max 64 bytes)")
		}
		seen[exporter.Name] = true
		if exporter.Type != "otlphttp" {
			return fmt.Errorf("telemetry exporter %q: unsupported type %q", exporter.Name, exporter.Type)
		}
		if err := validateTelemetryRemote(exporter.Endpoint, exporter.Headers, final); err != nil {
			return fmt.Errorf("telemetry exporter %q: %w", exporter.Name, err)
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
	out.Telemetry.OTLP.Enabled = clonePtr(c.Telemetry.OTLP.Enabled)
	out.Telemetry.Storage.Prometheus.Headers = cloneMap(c.Telemetry.Storage.Prometheus.Headers)
	out.Telemetry.Storage.ClickHouse.Headers = cloneMap(c.Telemetry.Storage.ClickHouse.Headers)
	out.Telemetry.Exporters = cloneSlice(c.Telemetry.Exporters)
	for i := range out.Telemetry.Exporters {
		out.Telemetry.Exporters[i].Headers = cloneMap(c.Telemetry.Exporters[i].Headers)
	}
	return &out
}
