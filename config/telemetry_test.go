package config

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestTelemetryDefaultsAndStrictDecode(t *testing.T) {
	cfg, err := DecodeTelemetry(strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Query.Backend != "none" || cfg.Query.Handler != "none" || cfg.Local.Enabled || cfg.Collector != nil {
		t.Fatalf("defaults %+v", cfg)
	}
	for _, raw := range []string{"unknown: true", "telemetry:\n  scrape:\n    bogus: 1", "telemetry:\n  storage:\n    type: local\n    type: none", "{}\n---\n{}", "api_socket: relative", "paths:\n  telemetry_executable: relative", "proxy_netns: ../escape", "telemetry:\n  storage:\n    type: sqlite", "telemetry:\n  scrape:\n    timeout: 6s", "telemetry:\n  otlp:\n    grpc_listen: localhost:invalid"} {
		if _, err := DecodeTelemetry(strings.NewReader(raw)); err == nil {
			t.Fatalf("invalid config accepted: %s", raw)
		}
	}
}

func TestTelemetryProxyNetNSContract(t *testing.T) {
	for _, raw := range []string{`proxy_netns: sandbox-proxy`, `{"proxy_netns":"sandbox-proxy"}`} {
		cfg, err := DecodeTelemetry(strings.NewReader(raw))
		if err != nil || cfg.ProxyNetNS != "sandbox-proxy" {
			t.Fatalf("canonical netns: %v, %+v", err, cfg)
		}
		clone := cfg.Clone()
		if clone.ProxyNetNS != cfg.ProxyNetNS {
			t.Fatal("Clone lost proxy_netns")
		}
		for _, marshal := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
			encoded, err := marshal(clone)
			if err != nil || !strings.Contains(string(encoded), "proxy_netns") || strings.Contains(string(encoded), "sandbox_netns") {
				t.Fatalf("canonical output: %s, %v", encoded, err)
			}
			if _, err := DecodeTelemetry(strings.NewReader(string(encoded))); err != nil {
				t.Fatal(err)
			}
		}
		clone.ProxyNetNS = "../invalid"
		if err := ValidateTelemetryFinal(clone); err == nil || !strings.Contains(err.Error(), "proxy_netns") {
			t.Fatalf("invalid final namespace: %v", err)
		}
	}
	for _, raw := range []string{
		"sandbox_netns: sandbox-proxy", "sandbox_netns: ''", `{"sandbox_netns":"sandbox-proxy"}`,
		"proxy_netns: sandbox-proxy\nsandbox_netns: sandbox-proxy", "proxy_netns: ../invalid",
	} {
		if _, err := DecodeTelemetry(strings.NewReader(raw)); err == nil {
			t.Fatalf("invalid or legacy configuration accepted: %s", raw)
		}
	}
}

func TestTelemetryFinalValidationAndClone(t *testing.T) {
	cfg, err := DecodeTelemetry(strings.NewReader(`collector:
  exporters:
    otlp_http/extra:
      endpoint: https://collector.example.com
      headers: {Authorization: secret}
`))
	if err != nil {
		t.Fatal(err)
	}
	clone := cfg.Clone()
	header := func(c *Telemetry) map[string]any {
		return c.Collector["exporters"].(map[string]any)["otlp_http/extra"].(map[string]any)["headers"].(map[string]any)
	}
	header(clone)["Authorization"] = "other"
	if header(cfg)["Authorization"] != "secret" {
		t.Fatal("clone aliases Collector config")
	}

	for _, modify := range []func(*Telemetry){
		func(c *Telemetry) { c.Local.Retention = "0s" }, func(c *Telemetry) { c.Local.MaxSize = "1MiB" },
		func(c *Telemetry) { c.Local.MaxSeries = 0 }, func(c *Telemetry) { c.APISocket = c.ConfigSocket },
	} {
		candidate := cfg.Clone()
		modify(candidate)
		if err := ValidateTelemetryFinal(candidate); err == nil {
			t.Fatal("invalid final value accepted/defaulted")
		}
	}
}

func TestTelemetryExternalStorageValidation(t *testing.T) {
	for _, kind := range []string{"prometheus", "clickhouse"} {
		cfg, err := DecodeTelemetry(strings.NewReader("query:\n  backend: " + kind))
		if err != nil {
			t.Fatal("omitted endpoint must be configurable by a custom hook", err)
		}
		if err := ValidateTelemetryFinal(cfg); err == nil {
			t.Fatal("missing final endpoint accepted")
		}
		for _, endpoint := range []string{"file:///tmp/db", "https://user:password@example.com", "http://example.com?query=unsafe"} {
			if _, err := DecodeTelemetry(strings.NewReader("query:\n  backend: " + kind + "\n  " + kind + ":\n    endpoint: " + endpoint)); err == nil {
				t.Fatal("malformed explicit endpoint accepted")
			}
		}
	}
	if _, err := DecodeTelemetry(strings.NewReader("query:\n  backend: clickhouse\n  clickhouse:\n    endpoint: http://localhost:8123\n    tables:\n      gauge: 'metrics; DROP TABLE other'")); err == nil {
		t.Fatal("SQL identifier injection")
	}
}

func TestTelemetryIndependentReadWriteConfiguration(t *testing.T) {
	for name, raw := range map[string]string{
		"write-only":              "collector: {receivers: {}, exporters: {}, service: {pipelines: {}}}",
		"query-only":              "query: {backend: prometheus, prometheus: {endpoint: https://prometheus.example.com}}",
		"local-read":              "local: {enabled: true}\nquery: {backend: local}",
		"local-write-remote-read": "local: {enabled: true}\nquery: {backend: clickhouse, clickhouse: {endpoint: https://clickhouse.example.com}}",
		"custom-query":            "query: {backend: custom, handler: custom, e2b: {metrics: {}}}",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := DecodeTelemetry(strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateTelemetryFinal(cfg); err != nil {
				t.Fatal(err)
			}
			if !cfg.Local.Enabled && cfg.Local.Path != "" {
				t.Fatal("implicit local storage path")
			}
		})
	}
	for _, raw := range []string{
		"query: {backend: local}", "query: {backend: none, handler: e2b}",
		"query: {backend: custom, handler: e2b, e2b: {metrics: {cpuCount: one}}}",
		"query: {backend: prometheus, prometheus: {labels: {sandbox.id: same, sandbox.stable_id: same}}}",
		"query: {backend: prometheus, prometheus: {labels: {sandbox.id: sandbox_id, sandbox_id: alias}}}",
		"query: {backend: prometheus, prometheus: {labels: {sandbox.id: __name__}}}",
		"query: {backend: clickhouse, clickhouse: {tables: {sum: 'bad-table'}}}",
		"query: {backend: custom, handler: sql}",
		"telemetry: {storage: {type: none}}", "storage: {type: local}",
	} {
		if _, err := DecodeTelemetry(strings.NewReader(raw)); err == nil {
			t.Fatalf("invalid query contract accepted: %s", raw)
		}
	}
	cfg, err := DecodeTelemetry(strings.NewReader("query: {backend: prometheus, prometheus: {endpoint: https://prometheus.example.com}}"))
	if err != nil {
		t.Fatal(err)
	}
	clone := cfg.Clone()
	clone.Query.Prometheus.Labels["sandbox.id"] = "other_id"
	clone.Query.E2B.Metrics["cpuCount"] = "other_metric"
	if cfg.Query.Prometheus.Labels["sandbox.id"] != "sandbox_id" || cfg.Query.E2B.Metrics["cpuCount"] != "sandbox.cpu.count" {
		t.Fatal("query config clone aliases maps")
	}
	clone.Query.E2B.Metrics["cpuCount"] = clone.Query.E2B.Metrics["memTotal"]
	if err := ValidateTelemetryFinal(clone); err == nil {
		t.Fatal("duplicate E2B mapping after Configure")
	}
}
