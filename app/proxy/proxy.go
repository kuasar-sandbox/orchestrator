// Package proxy exposes the narrow startup API for statically customized
// external proxy masters and their internally re-executed workers.
package proxy

import (
	"bytes"
	"context"
	"crypto"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/signal"
	"sync/atomic"
	"syscall"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyapp"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

// Config is the public external proxy declarative configuration.
type Config = publicconfig.Proxy

// Role identifies the current Proxy App process.
type Role string

const (
	RoleMaster Role = "master"
	RoleWorker Role = "worker"
)

// Process identifies the process-local Runtime binding invocation. WorkerID
// and WorkerEpoch are zero values for the master.
type Process struct {
	Role        Role
	WorkerID    string
	WorkerEpoch uint64
}

// TLSMaterial contains DER-encoded leaf-first certificates, a matching signer,
// and optional client trust. The core owns TLS versions, ALPN, and client-auth
// policy.
type TLSMaterial struct {
	CertificateChain [][]byte
	PrivateKey       crypto.Signer
	ClientCAs        *x509.CertPool
}

// TLSMaterialProvider supplies startup TLS material. When non-nil it is
// authoritative; errors never fall back to configured certificate files.
type TLSMaterialProvider interface {
	TLSMaterial(context.Context) (TLSMaterial, error)
}

// TLSMaterialProviderFunc adapts a function to TLSMaterialProvider.
type TLSMaterialProviderFunc func(context.Context) (TLSMaterial, error)

func (f TLSMaterialProviderFunc) TLSMaterial(ctx context.Context) (TLSMaterial, error) {
	return f(ctx)
}

// Runtime holds non-serializable process-local startup bindings. A fresh value
// is bound in the master and every worker epoch.
type Runtime struct {
	Logger *slog.Logger
	TLS    TLSMaterialProvider
	// MasterExtension is the one trusted, statically linked extension used by
	// the master process. Worker processes ignore this field.
	MasterExtension proxyextension.MasterExtension
}

// MarshalJSON rejects accidental process-runtime serialization.
func (Runtime) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("proxy Runtime is process-local and cannot be serialized")
}

// UnmarshalJSON rejects attempts to reconstruct Runtime from a bootstrap.
func (*Runtime) UnmarshalJSON([]byte) error {
	return fmt.Errorf("proxy Runtime is process-local and cannot be deserialized")
}

// Hooks are the only startup customization points. Configure runs exactly once
// in the master; BindRuntime runs once in the master and once in every worker.
type Hooks struct {
	Configure   func(context.Context, *Config) error
	BindRuntime func(context.Context, Process, *Runtime) error
}

// App is a one-shot external proxy application. New has no side effects.
type App struct {
	hooks       Hooks
	initialized bool
	ran         atomic.Bool

	workerPresent    func() bool
	receiveWorker    func() (*proxyapp.WorkerBootstrap, error)
	receiveComponent func() (*componentexec.Bootstrap, error)
	runMaster        func(context.Context, *proxyapp.EffectiveConfig, *proxyapp.Runtime) error
	prepareWorker    func(*proxyapp.WorkerBootstrap) (*proxyapp.PreparedWorker, error)
	runWorker        func(context.Context, *proxyapp.PreparedWorker, *proxyapp.Runtime) error
}

// New constructs a one-shot App without I/O or hook invocation.
func New(hooks Hooks) *App {
	return &App{
		hooks:         hooks,
		initialized:   true,
		workerPresent: proxyapp.WorkerBootstrapPresent,
		receiveWorker: proxyapp.ReceiveWorkerBootstrap,
		receiveComponent: func() (*componentexec.Bootstrap, error) {
			return componentexec.Receive(componentexec.ComponentProxy, componentexec.RoleMaster)
		},
		runMaster:     proxyapp.RunMaster,
		prepareWorker: proxyapp.PrepareWorker,
		runWorker: func(ctx context.Context, worker *proxyapp.PreparedWorker, runtime *proxyapp.Runtime) error {
			return worker.Run(ctx, runtime)
		},
	}
}

// Run installs SIGINT/SIGTERM handling and runs the App once.
func (a *App) Run() error {
	if err := a.requireInitialized(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return a.RunContext(ctx)
}

// RunContext enters either the custom master bootstrap or the private worker
// bootstrap. Worker state is detected before any CLI/config-file handling.
func (a *App) RunContext(ctx context.Context) error {
	if err := a.requireInitialized(); err != nil {
		return err
	}
	if ctx == nil {
		return fmt.Errorf("proxy app context is nil")
	}
	if !a.ran.CompareAndSwap(false, true) {
		return fmt.Errorf("proxy app can only be run once")
	}
	if a.workerPresent() {
		componentexec.ClearEnvironment()
		return a.runWorkerProcess(ctx)
	}
	proxyapp.ClearWorkerEnvironment()
	return a.runMasterProcess(ctx)
}

func (a *App) requireInitialized() error {
	if a == nil || !a.initialized {
		return fmt.Errorf("proxy App must be constructed with proxy.New")
	}
	return nil
}

func (a *App) runMasterProcess(ctx context.Context) error {
	bootstrap, err := a.receiveComponent()
	if errors.Is(err, componentexec.ErrNoBootstrap) {
		return fmt.Errorf("custom proxy must be invoked through node-ctl proxy")
	}
	if err != nil {
		return err
	}
	if err := componentexec.VerifyCurrentExecutable(bootstrap.ComponentIdentity); err != nil {
		return err
	}
	var cfg publicconfig.Proxy
	if err := decodeConfig(bootstrap.Config, &cfg); err != nil {
		return fmt.Errorf("custom proxy bootstrap config: %w", err)
	}
	immutableExecutable := cfg.Paths.ProxyExecutable
	if immutableExecutable == "" || immutableExecutable != bootstrap.ComponentExecutable {
		return fmt.Errorf("custom proxy bootstrap executable does not match config")
	}
	if a.hooks.Configure != nil {
		if err := a.hooks.Configure(ctx, &cfg); err != nil {
			return fmt.Errorf("custom proxy configure: %w", err)
		}
	}
	if cfg.Paths.ProxyExecutable != immutableExecutable {
		return fmt.Errorf("custom proxy Configure must not change paths.proxy_executable")
	}
	effective, err := proxyapp.FreezeConfig(&cfg)
	if err != nil {
		return err
	}
	runtimeBindings := &Runtime{}
	process := Process{Role: RoleMaster}
	if a.hooks.BindRuntime != nil {
		if err := a.hooks.BindRuntime(ctx, process, runtimeBindings); err != nil {
			return fmt.Errorf("custom proxy bind runtime for master: %w", err)
		}
	}
	resolved, err := proxyapp.ResolveRuntime(ctx, effective.Config(), freezeRuntime(runtimeBindings))
	if err != nil {
		return err
	}
	return a.runMaster(ctx, effective, resolved)
}

func (a *App) runWorkerProcess(ctx context.Context) error {
	bootstrap, err := a.receiveWorker()
	if err != nil {
		return err
	}
	defer bootstrap.Close()
	if err := bootstrap.VerifyExecutable(); err != nil {
		return err
	}
	internalProcess := bootstrap.Process()
	process := Process{
		Role: Role(internalProcess.Role), WorkerID: internalProcess.WorkerID, WorkerEpoch: internalProcess.WorkerEpoch,
	}
	if process.Role != RoleWorker || process.WorkerID == "" || process.WorkerEpoch == 0 {
		return fmt.Errorf("proxy worker bootstrap: invalid process identity")
	}
	prepared, err := a.prepareWorker(bootstrap)
	if err != nil {
		return err
	}
	defer prepared.Close()
	runtimeBindings := &Runtime{}
	if a.hooks.BindRuntime != nil {
		if err := a.hooks.BindRuntime(ctx, process, runtimeBindings); err != nil {
			return fmt.Errorf("custom proxy bind runtime for worker: %w", err)
		}
	}
	effective := bootstrap.EffectiveConfig()
	resolved, err := proxyapp.ResolveRuntime(ctx, effective.Config(), freezeRuntime(runtimeBindings))
	if err != nil {
		return err
	}
	return a.runWorker(ctx, prepared, resolved)
}

func decodeConfig(raw []byte, out *publicconfig.Proxy) error {
	if err := strictjson.RejectDuplicateKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func freezeRuntime(runtime *Runtime) proxyapp.Bindings {
	bindings := proxyapp.Bindings{Logger: runtime.Logger, MasterExtension: runtime.MasterExtension}
	if runtime.TLS != nil {
		provider := runtime.TLS
		bindings.TLSMaterial = func(ctx context.Context) (proxyapp.TLSMaterial, error) {
			material, err := provider.TLSMaterial(ctx)
			if err != nil {
				return proxyapp.TLSMaterial{}, err
			}
			chain := make([][]byte, len(material.CertificateChain))
			for index, certificate := range material.CertificateChain {
				chain[index] = append([]byte(nil), certificate...)
			}
			return proxyapp.TLSMaterial{
				CertificateChain: chain, PrivateKey: material.PrivateKey, ClientCAs: clonePool(material.ClientCAs),
			}, nil
		}
	}
	return bindings
}

func clonePool(pool *x509.CertPool) *x509.CertPool {
	if pool == nil {
		return nil
	}
	return pool.Clone()
}
