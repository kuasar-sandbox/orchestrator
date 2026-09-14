package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/telemetryapp"
)

func bootstrap(t *testing.T) *componentexec.Bootstrap {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := componentexec.CurrentExecutableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(executable)
	if err != nil {
		t.Fatal(err)
	}
	original := info.Mode().Perm()
	if err := os.Chmod(executable, original&^0022); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(executable, original) })
	cfg, err := config.DecodeTelemetry(strings.NewReader("paths:\n  telemetry_executable: " + executable + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &componentexec.Bootstrap{Component: componentexec.ComponentTelemetry, Role: componentexec.RoleTelemetry, ComponentExecutable: executable, NodeCtlExecutable: "/bin/true", ComponentIdentity: identity, Config: raw}
}

func TestConfigureOnceFreezesConfigAndRuntime(t *testing.T) {
	boot := bootstrap(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	calls := 0
	var retainedConfig *Config
	var retainedRuntime *Runtime
	app := New(Hooks{Configure: func(_ context.Context, cfg *Config, runtime *Runtime) error {
		calls++
		retainedConfig = cfg
		retainedRuntime = runtime
		cfg.Collector = map[string]any{"exporters": map[string]any{"otlp_http/extra": map[string]any{"endpoint": "https://collector.example.com", "headers": map[string]any{"Authorization": "private"}}}}
		runtime.Logger = logger
		return nil
	}})
	app.receive = func() (*componentexec.Bootstrap, error) { return boot, nil }
	app.run = func(_ context.Context, cfg *config.Telemetry, runtime *telemetryapp.Runtime) error {
		retainedConfig.Collector["exporters"].(map[string]any)["otlp_http/extra"].(map[string]any)["headers"].(map[string]any)["Authorization"] = "mutated"
		retainedRuntime.Logger = nil
		if cfg.Collector["exporters"].(map[string]any)["otlp_http/extra"].(map[string]any)["headers"].(map[string]any)["Authorization"] != "private" || runtime.Logger != logger {
			t.Fatal("config/runtime not frozen")
		}
		return nil
	}
	if err := app.RunContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := app.RunContext(context.Background()); err == nil || calls != 1 {
		t.Fatal("not one-shot", err, calls)
	}
}

func TestAppRejectsInvalidBootstrapAndHooksBeforeCore(t *testing.T) {
	for _, phase := range []string{"missing", "wrong-executable", "duplicate", "unknown", "legacy-netns", "trailing", "hook", "path-mutation", "final-validation", "final-netns", "provider"} {
		t.Run(phase, func(t *testing.T) {
			boot := bootstrap(t)
			app := New(Hooks{Configure: func(_ context.Context, cfg *Config, runtime *Runtime) error {
				switch phase {
				case "hook":
					return errors.New("hook failed")
				case "path-mutation":
					cfg.Paths.TelemetryExecutable = "/another"
				case "final-validation":
					cfg.RouteCapacity = 0
				case "final-netns":
					cfg.ProxyNetNS = "../invalid"
				case "provider":
					cfg.Query.Backend = "prometheus"
					cfg.Query.Prometheus.Endpoint = "http://example.com"
					runtime.QueryHeaders = func(context.Context) (map[string]string, error) { return nil, errors.New("provider failed") }
				}
				return nil
			}})
			app.receive = func() (*componentexec.Bootstrap, error) {
				switch phase {
				case "missing":
					return nil, componentexec.ErrNoBootstrap
				case "wrong-executable":
					boot.ComponentIdentity.Inode++
				case "duplicate":
					boot.Config = []byte(`{"config_socket":"/a","config_socket":"/b"}`)
				case "unknown":
					boot.Config = []byte(`{"unknown":true}`)
				case "legacy-netns":
					boot.Config = []byte(`{"sandbox_netns":"sandbox-proxy"}`)
				case "trailing":
					boot.Config = append(boot.Config, []byte(` {}`)...)
				}
				return boot, nil
			}
			app.run = func(context.Context, *config.Telemetry, *telemetryapp.Runtime) error {
				t.Fatal("core started after failure")
				return nil
			}
			if err := app.RunContext(context.Background()); err == nil {
				t.Fatal("invalid startup accepted")
			}
		})
	}
}

func TestRuntimeIsNotSerializableAndZeroAppRejected(t *testing.T) {
	if _, err := json.Marshal(Runtime{}); err == nil {
		t.Fatal("runtime serialized")
	}
	if err := json.Unmarshal([]byte(`{}`), &Runtime{}); err == nil {
		t.Fatal("runtime deserialized")
	}
	var app App
	if err := app.RunContext(context.Background()); err == nil {
		t.Fatal("zero App accepted")
	}
	if err := New(Hooks{}).RunContext(nil); err == nil {
		t.Fatal("nil context accepted")
	}
}
