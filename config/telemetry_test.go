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
	if cfg.Telemetry.Scrape.Interval != "5s" || cfg.Telemetry.Scrape.Timeout != "1s" || cfg.Telemetry.Storage.Type != "local" || !*cfg.Telemetry.OTLP.Enabled {
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
	cfg, err := DecodeTelemetry(strings.NewReader("telemetry:\n  exporters:\n    - name: extra\n      type: otlphttp\n      endpoint: https://collector.example.com\n      headers:\n        Authorization: secret\n"))
	if err != nil {
		t.Fatal(err)
	}
	clone := cfg.Clone()
	clone.Telemetry.Exporters[0].Headers["Authorization"] = "other"
	*clone.Telemetry.OTLP.Enabled = false
	if cfg.Telemetry.Exporters[0].Headers["Authorization"] != "secret" || !*cfg.Telemetry.OTLP.Enabled {
		t.Fatal("clone aliases config")
	}
	for _, modify := range []func(*Telemetry){
		func(c *Telemetry) { c.Telemetry.Scrape.Concurrency = 0 }, func(c *Telemetry) { c.Telemetry.OTLP.Enabled = nil },
		func(c *Telemetry) { c.Telemetry.Storage.Retention = "0s" }, func(c *Telemetry) { c.Telemetry.Storage.MaxSize = "1MiB" },
		func(c *Telemetry) { c.Telemetry.Storage.MaxSeries = 1 }, func(c *Telemetry) { c.APISocket = c.ConfigSocket },
		func(c *Telemetry) { c.Telemetry.OTLP.GRPCListen = c.Telemetry.OTLP.HTTPListen }, func(c *Telemetry) { c.Telemetry.Exporters[0].Headers["Authorization"] = "bad\r\nX: value" },
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
		cfg, err := DecodeTelemetry(strings.NewReader("telemetry:\n  storage:\n    type: " + kind))
		if err != nil {
			t.Fatal("omitted endpoint must be configurable by a custom hook", err)
		}
		if err := ValidateTelemetryFinal(cfg); err == nil {
			t.Fatal("missing final endpoint accepted")
		}
		for _, endpoint := range []string{"file:///tmp/db", "https://user:password@example.com", "http://example.com?query=unsafe"} {
			if _, err := DecodeTelemetry(strings.NewReader("telemetry:\n  storage:\n    type: " + kind + "\n    " + kind + ":\n      endpoint: " + endpoint)); err == nil {
				t.Fatal("malformed explicit endpoint accepted")
			}
		}
	}
	if _, err := DecodeTelemetry(strings.NewReader("telemetry:\n  storage:\n    type: clickhouse\n    clickhouse:\n      endpoint: http://localhost:8123\n      table: 'metrics; DROP TABLE other'")); err == nil {
		t.Fatal("SQL identifier injection")
	}
}
