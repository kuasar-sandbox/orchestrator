package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/conductorapp"
)

func TestAppConfigureOnceFreezesConfigAndRuntime(t *testing.T) {
	bootstrap := testBootstrap(t)
	wantLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var configured atomic.Int32
	var retainedConfig *Config
	var retainedRuntime *Runtime
	app := New(Hooks{Configure: func(_ context.Context, cfg *Config, runtime *Runtime) error {
		configured.Add(1)
		retainedConfig = cfg
		retainedRuntime = runtime
		cfg.API.Domain = "custom.test"
		cfg.Sandbox.Boot.Kernel = "/custom/kernel"
		cfg.Sandbox.Boot.Runtime = "/custom/runtime"
		cfg.Cluster.Labels = map[string]string{"zone": "custom"}
		runtime.Logger = wantLogger
		runtime.EncryptionKeys = EncryptionKeyProviderFunc(func(context.Context) ([][]byte, error) {
			return [][]byte{make([]byte, 32)}, nil
		})
		return nil
	}})
	app.receive = func() (*componentexec.Bootstrap, error) { return bootstrap, nil }
	app.run = func(_ context.Context, cfg *publicconfig.Conductor, nodeCtl string, runtime *conductorapp.Runtime) error {
		retainedConfig.Cluster.Labels["zone"] = "mutated-after-hook"
		retainedRuntime.Logger = nil
		if cfg.API.Domain != "custom.test" || cfg.Cluster.Labels["zone"] != "custom" {
			t.Fatalf("effective config was not frozen: %+v", cfg)
		}
		if nodeCtl != bootstrap.NodeCtlExecutable {
			t.Fatalf("node-ctl path=%q", nodeCtl)
		}
		if runtime.Logger != wantLogger || runtime.SecretBox == nil {
			t.Fatalf("runtime was not frozen/resolved: %+v", runtime)
		}
		return nil
	}
	if err := app.RunContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := app.RunContext(context.Background()); err == nil || !strings.Contains(err.Error(), "only be run once") {
		t.Fatalf("second run error=%v", err)
	}
	if configured.Load() != 1 {
		t.Fatalf("Configure calls=%d", configured.Load())
	}
}

func TestAppWithNilHooksUsesBootstrappedConfiguration(t *testing.T) {
	bootstrap := testBootstrap(t)
	var cfg publicconfig.Conductor
	if err := json.Unmarshal(bootstrap.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	completeConfig(&cfg)
	cfg.EncryptionKey = strings.Repeat("a", 64)
	bootstrap.Config = mustJSON(t, &cfg)

	app := New(Hooks{})
	app.receive = func() (*componentexec.Bootstrap, error) { return bootstrap, nil }
	app.run = func(_ context.Context, got *publicconfig.Conductor, nodeCtl string, runtime *conductorapp.Runtime) error {
		if got.API.Domain != cfg.API.Domain || got.Sandbox.Boot.Kernel != cfg.Sandbox.Boot.Kernel {
			t.Fatalf("bootstrapped config changed: %+v", got)
		}
		if nodeCtl != bootstrap.NodeCtlExecutable || runtime.SecretBox == nil {
			t.Fatalf("unresolved built-in state: node-ctl=%q runtime=%+v", nodeCtl, runtime)
		}
		return nil
	}
	if err := app.RunContext(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAppHookAndProviderFailuresStartNoCore(t *testing.T) {
	for name, hook := range map[string]func(*atomic.Int32) Hooks{
		"hook": func(providerCalls *atomic.Int32) Hooks {
			return Hooks{Configure: func(_ context.Context, _ *Config, runtime *Runtime) error {
				runtime.EncryptionKeys = EncryptionKeyProviderFunc(func(context.Context) ([][]byte, error) {
					providerCalls.Add(1)
					return [][]byte{make([]byte, 32)}, nil
				})
				return errors.New("hook unavailable")
			}}
		},
		"provider": func(providerCalls *atomic.Int32) Hooks {
			return Hooks{Configure: func(_ context.Context, cfg *Config, runtime *Runtime) error {
				completeConfig(cfg)
				runtime.EncryptionKeys = EncryptionKeyProviderFunc(func(context.Context) ([][]byte, error) {
					providerCalls.Add(1)
					return nil, errors.New("kms unavailable")
				})
				return nil
			}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			var providerCalls atomic.Int32
			var coreCalls atomic.Int32
			app := New(hook(&providerCalls))
			app.receive = func() (*componentexec.Bootstrap, error) { return testBootstrap(t), nil }
			app.run = func(context.Context, *publicconfig.Conductor, string, *conductorapp.Runtime) error {
				coreCalls.Add(1)
				return nil
			}
			if err := app.RunContext(context.Background()); err == nil {
				t.Fatal("RunContext succeeded")
			}
			if coreCalls.Load() != 0 {
				t.Fatalf("core started %d times", coreCalls.Load())
			}
			if name == "hook" && providerCalls.Load() != 0 {
				t.Fatalf("provider called after hook failure")
			}
			if name == "provider" && providerCalls.Load() != 1 {
				t.Fatalf("provider calls=%d", providerCalls.Load())
			}
		})
	}
}

func TestRuntimeProviderIsAuthoritativeOverDeclarativeFallback(t *testing.T) {
	bootstrap := testBootstrap(t)
	var cfg publicconfig.Conductor
	if err := json.Unmarshal(bootstrap.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg.EncryptionKey = strings.Repeat("f", 64)
	bootstrap.Config = mustJSON(t, &cfg)
	app := New(Hooks{Configure: func(_ context.Context, cfg *Config, runtime *Runtime) error {
		completeConfig(cfg)
		runtime.EncryptionKeys = EncryptionKeyProviderFunc(func(context.Context) ([][]byte, error) {
			return nil, errors.New("authoritative provider failed")
		})
		return nil
	}})
	app.receive = func() (*componentexec.Bootstrap, error) { return bootstrap, nil }
	app.run = func(context.Context, *publicconfig.Conductor, string, *conductorapp.Runtime) error {
		t.Fatal("core started after provider failure")
		return nil
	}
	err := app.RunContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "authoritative provider failed") {
		t.Fatalf("error=%v", err)
	}
}

func TestAppRejectsExecutableMutationAndIncompleteFinalConfig(t *testing.T) {
	for name, configure := range map[string]func(context.Context, *Config, *Runtime) error{
		"executable": func(_ context.Context, cfg *Config, runtime *Runtime) error {
			completeConfig(cfg)
			cfg.Paths.ConductorExecutable = "/different/xconductor"
			return nil
		},
		"incomplete": func(context.Context, *Config, *Runtime) error { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			app := New(Hooks{Configure: configure})
			app.receive = func() (*componentexec.Bootstrap, error) { return testBootstrap(t), nil }
			app.run = func(context.Context, *publicconfig.Conductor, string, *conductorapp.Runtime) error {
				t.Fatal("core started")
				return nil
			}
			if err := app.RunContext(context.Background()); err == nil {
				t.Fatal("RunContext succeeded")
			}
		})
	}
}

func TestDirectCustomConductorIsRejected(t *testing.T) {
	componentexec.ClearEnvironment()
	app := New(Hooks{})
	err := app.RunContext(context.Background())
	if err == nil || err.Error() != "custom conductor must be invoked through node-ctl conductor" {
		t.Fatalf("error=%v", err)
	}
}

func TestRunContextPropagatesCancellationToCore(t *testing.T) {
	app := New(Hooks{Configure: func(_ context.Context, cfg *Config, runtime *Runtime) error {
		completeConfig(cfg)
		runtime.EncryptionKeys = EncryptionKeyProviderFunc(func(context.Context) ([][]byte, error) {
			return [][]byte{make([]byte, 32)}, nil
		})
		return nil
	}})
	app.receive = func() (*componentexec.Bootstrap, error) { return testBootstrap(t), nil }
	app.run = func(ctx context.Context, _ *publicconfig.Conductor, _ string, _ *conductorapp.Runtime) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := app.RunContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func testBootstrap(t *testing.T) *componentexec.Bootstrap {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(executable)
	if err != nil {
		t.Fatal(err)
	}
	originalMode := info.Mode().Perm()
	protectedMode := originalMode &^ 0o022
	if protectedMode != originalMode {
		if err := os.Chmod(executable, protectedMode); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(executable, originalMode) })
	}
	cfg, err := publicconfig.DecodeConductor(strings.NewReader("paths:\n  conductor_executable: " + executable + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return &componentexec.Bootstrap{
		Component: componentexec.ComponentConductor, Role: componentexec.RoleConductor,
		NodeCtlExecutable: "/bin/true", ComponentExecutable: executable,
		Config: mustJSON(t, cfg),
	}
}

func completeConfig(cfg *Config) {
	cfg.API.Domain = "custom.test"
	cfg.Sandbox.Boot.Kernel = "/kernel"
	cfg.Sandbox.Boot.Runtime = "/runtime"
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
