package config

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDiffCOWDiscardPublicConfigRoundtrip(t *testing.T) {
	for _, mode := range []string{"", "auto", "manual"} {
		t.Run("mode="+mode, func(t *testing.T) {
			leaf := ""
			if mode != "" {
				leaf = ", discard: " + mode
			}
			cfg, err := DecodeConductor(strings.NewReader("sandbox: {resources: {diff_cow: {cache_size: 64MiB" + leaf + "}}}"))
			if err != nil {
				t.Fatal(err)
			}
			for _, encode := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
				raw, err := encode(cfg)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := DecodeConductor(strings.NewReader(string(raw)))
				if err != nil {
					t.Fatal(err)
				}
				cow := decoded.Sandbox.Resources.DiffCOW
				if cow == nil || cow.CacheSize == nil || *cow.CacheSize != "64MiB" || cow.MaxDirtySize != nil ||
					(mode == "" && cow.Discard != nil) || (mode != "" && (cow.Discard == nil || *cow.Discard != mode)) {
					t.Fatalf("roundtrip changed policy presence: %+v", cow)
				}
			}
		})
	}
	// A nested custom decoder must preserve existing YAML merge behavior.
	cfg, err := DecodeConductor(strings.NewReader("sandbox: {resources: {diff_cow: {<<: {discard: manual}, cache_size: 64MiB}}}"))
	if err != nil || cfg.Sandbox.Resources.DiffCOW.Discard == nil || *cfg.Sandbox.Resources.DiffCOW.Discard != "manual" {
		t.Fatalf("merged discard: config=%+v, err=%v", cfg, err)
	}
}

func TestDiffCOWDiscardRejectsMalformedPublicConfig(t *testing.T) {
	for _, value := range []string{"''", "null", "true", "1", "[]", "{}", "off", "AUTO", "' manual '"} {
		t.Run(value, func(t *testing.T) {
			_, err := DecodeConductor(strings.NewReader("sandbox: {resources: {diff_cow: {discard: " + value + "}}}"))
			if err == nil || !strings.Contains(err.Error(), "diff_cow.discard") {
				t.Fatalf("discard %s: error=%v", value, err)
			}
		})
	}
	for _, fields := range []string{
		"discard: auto, discard: manual", "discards: manual", "<<: {discard: null}",
	} {
		if _, err := DecodeConductor(strings.NewReader("sandbox: {resources: {diff_cow: {" + fields + "}}}")); err == nil {
			t.Fatalf("accepted ambiguous or unknown fields: %s", fields)
		}
	}
	for _, raw := range []string{
		`{"diff_cow":{"discard":null}}`, `{"diff_cow":{"discard":false}}`,
		`{"diff_cow":{"discard":"auto","discard":"manual"}}`, `{"diff_cow":{"discards":"manual"}}`,
	} {
		var resources ResourcesConfig
		if err := json.Unmarshal([]byte(raw), &resources); err == nil {
			t.Fatalf("bootstrap JSON accepted %s", raw)
		}
	}
}
