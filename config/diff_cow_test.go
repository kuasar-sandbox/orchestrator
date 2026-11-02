package config_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"gopkg.in/yaml.v3"
)

func TestDiffCOWConductorRoundTrip(t *testing.T) {
	for _, tc := range []struct{ name, fields, cache, dirty string }{
		{"both", "{cache_size: 96MiB, max_dirty_size: 48MiB}", "96MiB", "48MiB"},
		{"cache", "{cache_size: 96MiB}", "96MiB", ""},
		{"dirty", "{max_dirty_size: 8MiB}", "", "8MiB"},
		{"omitted", "{}", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.DecodeConductor(strings.NewReader("sandbox:\n  resources:\n    diff_cow: " + tc.fields + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			want := config.ResourceDiffCOW{CacheSize: tc.cache, MaxDirtySize: tc.dirty}
			// DecodeConductor applies defaults through nodeResourcePolicy and
			// setNodeResourcePolicy; checking here catches losing either direction.
			if cfg.Sandbox.Resources.DiffCOW != want {
				t.Fatalf("decoded = %+v, want %+v", cfg.Sandbox.Resources.DiffCOW, want)
			}
			cfg.Sandbox.Resources = configresolve.MaterializedSandboxResources(cfg.Sandbox.Resources)
			if cfg.Clone().Sandbox.Resources.DiffCOW != want {
				t.Fatal("clone lost policy")
			}
			for _, format := range []string{"yaml", "json"} {
				var raw []byte
				if format == "yaml" {
					raw, err = yaml.Marshal(cfg)
				} else {
					raw, err = json.Marshal(cfg)
				}
				if err != nil {
					t.Fatal(err)
				}
				again, err := config.DecodeConductor(bytes.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				if again.Sandbox.Resources.DiffCOW != want {
					t.Fatalf("%s round trip lost policy: %+v", format, again.Sandbox.Resources.DiffCOW)
				}
			}
		})
	}
}

func TestDiffCOWConductorRejectsExplicitInvalidSizes(t *testing.T) {
	for _, field := range []string{"cache_size", "max_dirty_size"} {
		for _, value := range []string{`""`, "null", "0", "1KiB", "-4096", "18446744073709551616"} {
			_, err := config.DecodeConductor(strings.NewReader("sandbox:\n  resources:\n    diff_cow: {" + field + ": " + value + "}\n"))
			if err == nil || !strings.Contains(err.Error(), "diff_cow."+field) {
				t.Fatalf("%s=%s: %v", field, value, err)
			}
		}
		var cfg config.ResourcesConfig
		if err := json.Unmarshal([]byte(`{"diff_cow":{"`+field+`":""}}`), &cfg); err == nil {
			t.Fatalf("JSON accepted empty %s", field)
		}
	}
	for _, fields := range []string{`{future: 4KiB}`, `{cache_size: 32MiB, cache_size: 64MiB}`, `{cache_size: 8MiB, max_dirty_size: 16MiB}`} {
		if _, err := config.DecodeConductor(strings.NewReader("sandbox:\n  resources:\n    diff_cow: " + fields + "\n")); err == nil {
			t.Fatalf("accepted %s", fields)
		}
	}
}
