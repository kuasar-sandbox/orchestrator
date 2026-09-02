package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyapp"
)

type appTestMasterExtension struct{}

func (*appTestMasterExtension) Start(context.Context, MasterHost) error { return nil }

type appTestWorkerExtension struct{}

func (*appTestWorkerExtension) Start(context.Context, WorkerHost) error { return nil }

func TestFreezeRuntimeCarriesRoleExtensionsWithoutSerialization(t *testing.T) {
	master := &appTestMasterExtension{}
	worker := &appTestWorkerExtension{}
	bindings := freezeRuntime(&Runtime{MasterExtension: master, WorkerExtension: worker})
	if bindings.MasterExtension != master || bindings.WorkerExtension != worker {
		t.Fatalf("bindings master=%T worker=%T", bindings.MasterExtension, bindings.WorkerExtension)
	}
}

func TestMasterConfigureAndBindRuntimeExactlyOnce(t *testing.T) {
	bootstrap := testComponentBootstrap(t)
	var configureCalls atomic.Int32
	var bindCalls atomic.Int32
	var retained *Config
	extension := &appTestMasterExtension{}
	app := New(Hooks{
		Configure: func(_ context.Context, cfg *Config) error {
			configureCalls.Add(1)
			retained = cfg
			cfg.Paths.RunRoot = "/run/custom-proxy"
			cfg.DataListen = "127.0.0.1:8443"
			return nil
		},
		BindRuntime: func(_ context.Context, process Process, runtime *Runtime) error {
			bindCalls.Add(1)
			if process != (Process{Role: RoleMaster}) {
				t.Fatalf("process=%+v", process)
			}
			runtime.MasterExtension = extension
			return nil
		},
	})
	app.workerPresent = func() bool { return false }
	app.receiveComponent = func() (*componentexec.Bootstrap, error) { return bootstrap, nil }
	app.runMaster = func(_ context.Context, effective *proxyapp.EffectiveConfig, runtime *proxyapp.Runtime) error {
		retained.Paths.RunRoot = "/mutated-after-freeze"
		if got := effective.Config().Paths.RunRoot; got != "/run/custom-proxy" {
			t.Fatalf("frozen run_root=%q", got)
		}
		if runtime == nil || runtime.Logger == nil || runtime.MasterExtension != extension {
			t.Fatal("runtime was not resolved")
		}
		return nil
	}
	if err := app.RunContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if configureCalls.Load() != 1 || bindCalls.Load() != 1 {
		t.Fatalf("Configure=%d BindRuntime=%d", configureCalls.Load(), bindCalls.Load())
	}
	if err := app.RunContext(context.Background()); err == nil || !strings.Contains(err.Error(), "only be run once") {
		t.Fatalf("second RunContext error=%v", err)
	}
}

func TestMasterHookFailuresPrecedeCoreStartup(t *testing.T) {
	for name, hooks := range map[string]Hooks{
		"configure": {Configure: func(context.Context, *Config) error { return errors.New("configure failed") }},
		"bind": {
			Configure: func(_ context.Context, cfg *Config) error {
				cfg.Paths.RunRoot = "/run/custom"
				cfg.DataListen = "127.0.0.1:8443"
				return nil
			},
			BindRuntime: func(context.Context, Process, *Runtime) error { return errors.New("bind failed") },
		},
	} {
		t.Run(name, func(t *testing.T) {
			app := New(hooks)
			app.workerPresent = func() bool { return false }
			app.receiveComponent = func() (*componentexec.Bootstrap, error) { return testComponentBootstrap(t), nil }
			app.runMaster = func(context.Context, *proxyapp.EffectiveConfig, *proxyapp.Runtime) error {
				t.Fatal("core started after hook failure")
				return nil
			}
			if err := app.RunContext(context.Background()); err == nil {
				t.Fatal("RunContext succeeded")
			}
		})
	}
}

func TestMasterRejectsExecutableMutationAndIncompleteConfig(t *testing.T) {
	for name, configure := range map[string]func(context.Context, *Config) error{
		"executable": func(_ context.Context, cfg *Config) error {
			cfg.Paths.RunRoot = "/run/custom"
			cfg.Paths.ProxyExecutable = "/different/xproxy"
			return nil
		},
		"incomplete": func(context.Context, *Config) error { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			app := New(Hooks{Configure: configure})
			app.workerPresent = func() bool { return false }
			app.receiveComponent = func() (*componentexec.Bootstrap, error) { return testComponentBootstrap(t), nil }
			app.runMaster = func(context.Context, *proxyapp.EffectiveConfig, *proxyapp.Runtime) error {
				t.Fatal("core started")
				return nil
			}
			if err := app.RunContext(context.Background()); err == nil {
				t.Fatal("RunContext succeeded")
			}
		})
	}
}

func TestDirectCustomProxyIsRejected(t *testing.T) {
	componentexec.ClearEnvironment()
	proxyapp.ClearWorkerEnvironment()
	app := New(Hooks{})
	err := app.RunContext(context.Background())
	if err == nil || err.Error() != "custom proxy must be invoked through node-ctl proxy" {
		t.Fatalf("error=%v", err)
	}
}

func TestZeroAppFailsBeforeConsumingBootstrap(t *testing.T) {
	componentRead, componentWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer componentRead.Close()
	defer componentWrite.Close()
	workerRead, workerWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer workerRead.Close()
	defer workerWrite.Close()
	componentFD := strconv.Itoa(int(componentRead.Fd()))
	workerFD := strconv.Itoa(int(workerRead.Fd()))
	t.Setenv("KUASAR_INTERNAL_COMPONENT_BOOTSTRAP_FD", componentFD)
	t.Setenv("KUASAR_INTERNAL_PROXY_WORKER_BOOTSTRAP_FD", workerFD)

	var zero App
	want := "proxy App must be constructed with proxy.New"
	for name, run := range map[string]func() error{
		"Run":         zero.Run,
		"RunContext":  func() error { return zero.RunContext(context.Background()) },
		"nil Run":     (*App)(nil).Run,
		"nil context": func() error { return zero.RunContext(nil) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
		})
	}
	if got := os.Getenv("KUASAR_INTERNAL_COMPONENT_BOOTSTRAP_FD"); got != componentFD {
		t.Fatalf("zero App consumed component bootstrap environment: got %q, want %q", got, componentFD)
	}
	if got := os.Getenv("KUASAR_INTERNAL_PROXY_WORKER_BOOTSTRAP_FD"); got != workerFD {
		t.Fatalf("zero App consumed worker bootstrap environment: got %q, want %q", got, workerFD)
	}
	if _, err := componentRead.Stat(); err != nil {
		t.Fatalf("zero App consumed component bootstrap descriptor: %v", err)
	}
	if _, err := workerRead.Stat(); err != nil {
		t.Fatalf("zero App consumed worker bootstrap descriptor: %v", err)
	}

	if err := New(Hooks{}).RunContext(nil); err == nil {
		t.Fatal("nil context succeeded")
	}
}

func testComponentBootstrap(t *testing.T) *componentexec.Bootstrap {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := componentexec.CurrentExecutableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := publicconfig.DecodeProxy(strings.NewReader("paths:\n  proxy_executable: " + executable + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &componentexec.Bootstrap{
		Component: componentexec.ComponentProxy, Role: componentexec.RoleMaster,
		NodeCtlExecutable: "/bin/true", ComponentExecutable: executable, ComponentIdentity: identity,
		Config: raw,
	}
}
