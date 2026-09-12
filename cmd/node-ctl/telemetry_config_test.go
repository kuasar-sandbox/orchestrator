package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
)

func TestTelemetryTemplateDeployAndFinalValidation(t *testing.T) {
	raw, err := renderTelemetryConfig(true, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "telemetry_executable:") {
		t.Fatal("missing static App configuration")
	}
	template, err := config.DecodeTelemetry(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	deployed, err := config.LoadTelemetry("../../deploy/telemetry.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(template, deployed) {
		t.Fatal("CLI and deploy telemetry defaults diverge")
	}
	if _, err := renderTelemetryConfig(false, "../../deploy/telemetry.example.yaml"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "telemetry.yaml")
	if err := os.WriteFile(path, []byte("telemetry:\n  storage:\n    type: prometheus\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := renderTelemetryConfig(false, path); err == nil {
		t.Fatal("built-in diagnostics skipped final validation")
	}
}
