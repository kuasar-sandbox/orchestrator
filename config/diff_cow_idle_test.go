package config

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDiffCOWCacheIdleAfterPublicConfig(t *testing.T) {
	cfg, err := DecodeConductor(strings.NewReader("sandbox: {resources: {diff_cow: {cache_size: 64MiB, cache_idle_after: 25s}}}"))
	if err != nil {
		t.Fatal(err)
	}
	cow := cfg.Sandbox.Resources.DiffCOW
	if cow == nil || cow.CacheIdleAfter == nil || *cow.CacheIdleAfter != "25s" {
		t.Fatalf("policy lost: %+v", cow)
	}
	for _, encoder := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
		raw, err := encoder(cfg)
		if err != nil {
			t.Fatal(err)
		}
		after, err := DecodeConductor(strings.NewReader(string(raw)))
		if err != nil || after.Sandbox.Resources.DiffCOW.CacheIdleAfter == nil ||
			*after.Sandbox.Resources.DiffCOW.CacheIdleAfter != "25s" {
			t.Fatalf("roundtrip: %+v, %v", after.Sandbox.Resources.DiffCOW, err)
		}
	}
}

func TestDiffCOWCacheIdleAfterRejectsInvalidPublicConfig(t *testing.T) {
	for _, field := range []string{"cache_idle_after: null", "<<: {cache_idle_after: null}", "cache_idle_after: false", "cache_idle_after: 0s", "cache_idle_after: -1s"} {
		raw := "sandbox: {resources: {diff_cow: {" + field + "}}}"
		if _, err := DecodeConductor(strings.NewReader(raw)); err == nil || !strings.Contains(err.Error(), "cache_idle_after") {
			t.Errorf("invalid setting %s: %v", field, err)
		}
	}
}
