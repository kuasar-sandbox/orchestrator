package config

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"reflect"
	"strings"
	"testing"
)

func TestRunPoolConfigurationInputs(t *testing.T) {
	for _, tc := range []struct {
		name, input     string
		runner, builder []RunPoolConfig
	}{
		{"defaults", "{}", []RunPoolConfig{{defaultRunnerUnit, 0}}, []RunPoolConfig{{defaultBuilderUnit, 0}}},
		{"legacy", "units: {runner: old-run@.service, runner_pool_size: 3, builder: old-build@.service, builder_pool_size: 0}", []RunPoolConfig{{"old-run@.service", 3}}, []RunPoolConfig{{"old-build@.service", 0}}},
		{"new runner old builder", "units: {runner_pools: [{unit: r@.service, size: 0}], builder_pool_size: 4}", []RunPoolConfig{{"r@.service", 0}}, []RunPoolConfig{{defaultBuilderUnit, 4}}},
		{"old runner new builder", "units: {runner_pool_size: 2, builder_pools: [{unit: b@.service, size: 0}]}", []RunPoolConfig{{defaultRunnerUnit, 2}}, []RunPoolConfig{{"b@.service", 0}}},
		{"duplicates", "units: {runner_pools: [{unit: r@.service, size: 2}, {unit: r@.service, size: 0}, {unit: r@.service, size: 2}, {unit: other@.service, size: 1}], builder_pools: [{unit: b@.service, size: 0}, {unit: b@.service, size: 0}]}", []RunPoolConfig{{"r@.service", 2}, {"r@.service", 0}, {"r@.service", 2}, {"other@.service", 1}}, []RunPoolConfig{{"b@.service", 0}, {"b@.service", 0}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := DecodeConductor(strings.NewReader(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			check := func(c *Conductor) {
				t.Helper()
				if !reflect.DeepEqual(c.Units.RunnerPoolConfigs(), tc.runner) || !reflect.DeepEqual(c.Units.BuilderPoolConfigs(), tc.builder) {
					t.Fatalf("pools: %+v / %+v", c.Units.RunnerPoolConfigs(), c.Units.BuilderPoolConfigs())
				}
			}
			check(cfg)
			clone := cfg.Clone()
			clone.applyDefaults()
			if err := clone.validateDeclarative(); err != nil {
				t.Fatal(err)
			}
			check(clone)
			raw, err := yaml.Marshal(clone)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeConductor(strings.NewReader(string(raw)))
			if err != nil {
				t.Fatalf("YAML roundtrip: %v", err)
			}
			check(decoded)
			raw, err = json.Marshal(clone)
			if err != nil {
				t.Fatal(err)
			}
			var fromJSON Conductor
			if err := json.Unmarshal(raw, &fromJSON); err != nil {
				t.Fatal(err)
			}
			check(&fromJSON)
		})
	}
}

func TestRunPoolConfigurationRejectsInvalidAndMixed(t *testing.T) {
	for _, kind := range []string{"runner", "builder"} {
		for _, input := range []string{
			kind + "_pools: []", kind + "_pools: null", kind + "_pools: [null]",
			kind + "_pools: [{unit: r@.service, size: -1}]",
			kind + "_pools: [{unit: r.service, size: 0}]",
			kind + "_pools: [{unit: ../r@.service, size: 0}]",
			kind + "_pools: [{unit: 'r*@.service', size: 0}]",
			kind + "_pools: [{unit: '', size: 0}]", kind + "_pools: [{size: 0}]",
			kind + "_pools: [{unit: r@.service}]",
			kind + "_pools: [{unit: r@.service, size: null}]",
			kind + "_pools: [{unit: r@.service, size: 1.5}]",
			kind + "_pools: [{unit: r@.service, size: 0, name: extra}]",
			kind + "_pools: [{unit: r@.service, size: 0}], " + kind + ": r@.service",
			kind + "_pools: [{unit: r@.service, size: 0}], " + kind + "_pool_size: 0",
			kind + "_pools: [{unit: r@.service, size: 0}], " + kind + "_pool_size: 2",
			kind + "_pool_size: -1",
		} {
			t.Run(input, func(t *testing.T) {
				raw := "units: {" + input + "}"
				if _, err := DecodeConductor(strings.NewReader(raw)); err == nil {
					t.Fatalf("accepted %s", raw)
				}
				var m map[string]any
				if err := yaml.Unmarshal([]byte(raw), &m); err != nil {
					t.Fatal(err)
				}
				j, err := json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				var cfg Conductor
				if err := json.Unmarshal(j, &cfg); err == nil {
					t.Fatalf("accepted JSON %s", j)
				}
			})
		}
	}
}

func TestRunPoolDefaultsCloneAndMergedExplicitZero(t *testing.T) {
	cfg, err := DecodeConductor(strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Units.RunnerPools = []RunPoolConfig{{"custom@.service", 0}}
	cfg.Units.BuilderPools = []RunPoolConfig{{"build@.service", 1}}
	cfg.applyDefaults()
	if err := cfg.validateDeclarative(); err != nil {
		t.Fatalf("inserted defaults caused conflict: %v", err)
	}
	clone := cfg.Clone()
	clone.Units.RunnerPools[0].Unit = "clone@.service"
	clone.Units.BuilderPools[0].Size = 9
	if cfg.Units.RunnerPools[0].Unit != "custom@.service" || cfg.Units.BuilderPools[0].Size != 1 {
		t.Fatal("Clone aliases pool entries")
	}
	for _, raw := range []string{
		"units: {<<: {runner_pool_size: 0}, runner_pools: [{unit: r@.service, size: 0}]}",
		"units: {<<: {builder_pool_size: 0}, builder_pools: [{unit: b@.service, size: 0}]}",
	} {
		if _, err := DecodeConductor(strings.NewReader(raw)); err == nil {
			t.Fatalf("accepted merged explicit zero: %s", raw)
		}
	}
	cfg.Units.RunnerPools = []RunPoolConfig{}
	if cfg.Clone().Units.RunnerPools == nil {
		t.Fatal("Clone turned empty into absent")
	}
	if _, err := json.Marshal(cfg); err == nil {
		t.Fatal("output accepted invalid empty list")
	}
}

func TestRunPoolYAMLAnchorsAndMerges(t *testing.T) {
	for _, input := range []string{
		"paths:\n  run_root: &root /run/sandbox\nunits:\n  dir: *root\n  runner_pool_size: 0\n",
		"units: {runner_pools: [&pool {unit: r@.service, size: 0}, *pool]}",
		"units: {runner_pools: [{unit: &u r@.service, size: &n 0}, {unit: *u, size: *n}]}",
		"units: {runner_pools: [&pool {unit: r@.service, size: 0}, {<<: *pool, size: 2}]}",
		"units: {runner_pools: &p [{unit: r@.service, size: 0}], builder_pools: *p}",
		"units: {<<: {runner_pool_size: &n 0, builder_pool_size: *n}}",
	} {
		t.Run(input, func(t *testing.T) {
			cfg, err := DecodeConductor(strings.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(input, "paths:") && cfg.Units.Dir != cfg.Paths.RunRoot {
				t.Fatal("lost cross-block scalar alias")
			}
			if strings.Contains(input, "*pool") || strings.Contains(input, "unit: *u") {
				pools := cfg.Units.RunnerPoolConfigs()
				if len(pools) != 2 || pools[0].Unit != "r@.service" || pools[1].Unit != "r@.service" {
					t.Fatalf("lost duplicate entries: %+v", pools)
				}
				want := 0
				if strings.Contains(input, "size: 2") {
					want = 2
				}
				if pools[1].Size != want {
					t.Fatalf("merge precedence: %+v", pools)
				}
				clone := cfg.Clone()
				clone.Units.RunnerPools[0].Size = 9
				if cfg.Units.RunnerPools[0].Size != 0 || clone.Units.RunnerPools[1].Size != want {
					t.Fatal("aliased pool entries")
				}
			}
			raw, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			roundtrip, err := DecodeConductor(strings.NewReader(string(raw)))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg.Units.RunnerPoolConfigs(), roundtrip.Units.RunnerPoolConfigs()) {
				t.Fatal("roundtrip changed aliased pools")
			}
		})
	}
	for _, input := range []string{
		"sandbox: {timeout_sec: &fraction 1.5}\nunits: {runner_pools: [{unit: r@.service, size: *fraction}]}",
		"units: {runner_pools: [{unit: r@.service, size: &fraction 1.5}, {unit: r@.service, size: *fraction}]}",
		"units: {runner_pools: [&pool {unit: r@.service, size: 0}, {<<: *pool, pool_id: forbidden}]}",
		"units: {<<: {unknown: 1}, runner_pool_size: 0}",
		"units: {runner_pool_size: &n 0, runner_pools: [{unit: r@.service, size: *n}]}",
		"units: {<<: {runner_pool_size: &n 0}, runner_pools: [{unit: r@.service, size: *n}]}",
		"units: {install: &null null, runner_pools: *null}",
		"units: {runner_pools: [&pool {unit: r@.service, size: 0}, {<<: *pool, size: &n null}, {<<: *pool, size: *n}]}",
	} {
		if _, err := DecodeConductor(strings.NewReader(input)); err == nil {
			t.Fatalf("accepted invalid aliased/merged configuration: %s", input)
		}
	}
}
