package config

import (
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestConductorUsageDeploymentAndDocumentation(t *testing.T) {
	file, err := os.Open("../deploy/conductor.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	config, err := DecodeConductor(file)
	if err != nil || config.Sandbox.Usage.Enabled || config.Sandbox.Usage.SampleInterval != "1s" || config.Sandbox.Usage.FlushInterval != "5m" {
		t.Fatal("deployment policy", config, err)
	}
	for _, path := range []string{"../docs/node.md", "../docs/node_zh.md"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		section := regexp.MustCompile("(?s)### 3\\.3 [^\\n]+\\n(.*?)(?:\\n## |\\z)").FindSubmatch(body)
		if len(section) != 2 {
			t.Fatalf("%s: native usage policy section missing", path)
		}
		blocks := regexp.MustCompile("(?s)```yaml\\n(.*?)\\n```").FindAllSubmatch(section[1], -1)
		if len(blocks) != 1 {
			t.Fatalf("%s: expected one complete node policy", path)
		}
		config, err := DecodeConductor(strings.NewReader(string(blocks[0][1])))
		if err != nil || !config.Sandbox.Usage.Enabled || config.Sandbox.Usage.SampleInterval != "1s" || config.Sandbox.Usage.FlushInterval != "5m" {
			t.Fatal(path, config, err)
		}
	}
}

func TestConductorUsageStrictPolicyCloneAndFinalValidation(t *testing.T) {
	cfg, err := DecodeConductor(strings.NewReader("sandbox:\n  usage: {enabled: true, sample_interval: 2s, flush_interval: 8m}\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := UsageConfig{Enabled: true, SampleInterval: "2s", FlushInterval: "8m"}
	if cfg.Sandbox.Usage != want || cfg.Clone().Sandbox.Usage != want {
		t.Fatal("node policy lost", cfg.Sandbox.Usage)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var round Conductor
	if err := json.Unmarshal(raw, &round); err != nil || !reflect.DeepEqual(round.Sandbox.Usage, want) {
		t.Fatal("bootstrap round trip", string(raw), err)
	}
	cfg.API.Domain, cfg.Sandbox.Boot.Kernel, cfg.Sandbox.Boot.Runtime = "sandboxes.example.test", "/opt/sandbox/vmlinux", "/opt/sandbox/runtime.erofs"
	if err := ValidateConductorFinal(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Sandbox.Usage.FlushInterval = "1s"
	if err := ValidateConductorFinal(cfg); err == nil {
		t.Fatal("final hook policy bypassed native validation")
	}
	defaults, err := DecodeConductor(strings.NewReader("{}"))
	if err != nil || defaults.Sandbox.Usage.Enabled || defaults.Sandbox.Usage.SampleInterval != "1s" || defaults.Sandbox.Usage.FlushInterval != "5m" {
		t.Fatal(defaults, err)
	}
	for _, raw := range []string{
		"sandbox: {usage: null}", "sandbox: {usage: []}", "sandbox: {usage: {enabled: 'true'}}", "sandbox: {usage: {enabled: null}}", "sandbox: {usage: {enabeld: true}}", "sandbox: {usage: {sample_interval: ''}}", "sandbox: {usage: {sample_interval: -1s}}", "sandbox: {usage: {sample_interval: 2s, flush_interval: 1s}}", "sandbox: {usage: {enabled: true, enabled: false}}", "sandbox: {unknown_sibling: true}", "sandbox: {<<: {usage: null}}",
	} {
		if _, err := DecodeConductor(strings.NewReader(raw)); err == nil {
			t.Errorf("accepted YAML %s", raw)
		}
	}
	for _, raw := range []string{`null`, `[]`, `{"enabled":null}`, `{"enabled":"true"}`, `{"unknown":1}`, `{"enabled":true,"enabled":false}`, `{"sample_interval":1}`} {
		var usage UsageConfig
		if err := json.Unmarshal([]byte(raw), &usage); err == nil {
			t.Errorf("accepted JSON %s", raw)
		}
	}
}
