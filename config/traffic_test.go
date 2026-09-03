package config_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
)

func TestProxyTrafficMaxInflightConfig(t *testing.T) {
	cfg, err := config.DecodeProxy(strings.NewReader(`
traffic:
  max_inflight:
    total: 128
    forward: 96
    "e2b:envd": 16
    "e2b:code-interpreter": 8
    exec: 8
`))
	if err != nil {
		t.Fatal(err)
	}
	want := config.MaxInflight{Total: 128, Forward: 96, E2BEnvd: 16, E2BCodeInterpreter: 8, Exec: 8}
	if got := cfg.Traffic.MaxInflight; got != want {
		t.Fatalf("max_inflight = %+v, want %+v", got, want)
	}
	clone := cfg.Clone()
	clone.Traffic.MaxInflight.Total = 1
	if cfg.Traffic.MaxInflight.Total != 128 {
		t.Fatal("Proxy.Clone aliased traffic config")
	}
}

func TestProxyTrafficDefaultIsUnlimited(t *testing.T) {
	cfg, err := config.DecodeProxy(strings.NewReader("{}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Traffic.MaxInflight.Unlimited() {
		t.Fatalf("default max_inflight = %+v", cfg.Traffic.MaxInflight)
	}
}

func TestProxyTrafficConfigIsStrict(t *testing.T) {
	tests := map[string]string{
		"traffic null":        "traffic: null\n",
		"max null":            "traffic: { max_inflight: null }\n",
		"leaf null":           "traffic: { max_inflight: { total: null } }\n",
		"unknown":             "traffic: { max_inflight: { future: 1 } }\n",
		"negative":            "traffic: { max_inflight: { total: -1 } }\n",
		"fraction":            "traffic: { max_inflight: { total: 1.5 } }\n",
		"string":              "traffic: { max_inflight: { total: '1' } }\n",
		"overflow":            "traffic: { max_inflight: { total: 4294967296 } }\n",
		"duplicate leaf":      "traffic: { max_inflight: { total: 1, total: 2 } }\n",
		"unknown traffic":     "traffic: { future: {} }\n",
		"duplicate max field": "traffic: { max_inflight: {}, max_inflight: {} }\n",
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := config.DecodeProxy(strings.NewReader(body)); err == nil {
				t.Fatalf("DecodeProxy accepted:\n%s", body)
			}
		})
	}
}

func TestProxyTrafficEffectiveJSONIsStrict(t *testing.T) {
	for name, raw := range map[string]string{
		"traffic null":   `null`,
		"max null":       `{"max_inflight":null}`,
		"leaf null":      `{"max_inflight":{"total":null}}`,
		"unknown":        `{"max_inflight":{"future":1}}`,
		"negative":       `{"max_inflight":{"total":-1}}`,
		"fraction":       `{"max_inflight":{"total":1.5}}`,
		"overflow":       `{"max_inflight":{"total":4294967296}}`,
		"duplicate leaf": `{"max_inflight":{"total":1,"total":2}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var traffic config.ProxyTrafficConfig
			if err := json.Unmarshal([]byte(raw), &traffic); err == nil {
				t.Fatalf("json.Unmarshal accepted %s", raw)
			}
		})
	}
}
