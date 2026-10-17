// Package proxy exposes the narrow startup API for statically customized
// independent proxy masters and their internally re-executed workers.
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
	"maps"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/app/activation"
	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyapp"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

// Config is the public independent proxy declarative configuration.
type Config = publicconfig.Proxy

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
	// ListenerFiles supplies named TCP listeners to the master. Successful
	// BindRuntime transfers ownership of every delivered file to App, including
	// on later startup failure. A failing hook retains ownership. The name set
	// is frozen on return; callers retaining a handle must deliver a duplicate.
	// Workers receive listeners through the private bootstrap, not this field.
	ListenerFiles map[string]*os.File
	Logger        *slog.Logger
	TLS           TLSMaterialProvider
	// MasterExtension is the one trusted, statically linked extension used by
	// the master process. Worker processes ignore this field.
	MasterExtension proxyextension.MasterExtension
	// WorkerExtension is the one trusted, statically linked extension used by
	// this worker epoch. The master process ignores this field. BindRuntime must
	// construct a fresh instance for every worker invocation.
	WorkerExtension proxyextension.WorkerExtension
}

// MarshalJSON rejects accidental process-runtime serialization.
func (Runtime) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("proxy Runtime is process-local and cannot be serialized")
}

// UnmarshalJSON rejects attempts to reconstruct Runtime from a bootstrap.
func (*Runtime) UnmarshalJSON([]byte) error {
	return fmt.Errorf("proxy Runtime is process-local and cannot be deserialized")
}

// Hooks are the startup customization points. Each master incarnation calls
// Configure from the same accepted original input; workers use its frozen
// result. BindRuntime runs in each master and each worker epoch. Runtime objects
// and background tasks belong to that incarnation and must not outlive its
// optional extension Shutdown completion.
type Hooks struct {
	Configure   func(context.Context, *Config) error
	BindRuntime func(context.Context, Process, *Runtime) error
}

// App is a one-shot independent Proxy application. New has no side effects.
type App struct {
	hooks       Hooks
	initialized bool
	ran         atomic.Bool
	service     *proxyapp.Service

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
		service:       proxyapp.NewService(),
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
	return a.runContext(ctx, true)
}

// RunContext enters either the custom master bootstrap or the private worker
// bootstrap. Worker state is detected before any CLI/config-file handling.
func (a *App) RunContext(ctx context.Context) error {
	return a.runContext(ctx, false)
}

func (a *App) runContext(ctx context.Context, signals bool) (returnErr error) {
	if err := a.requireInitialized(); err != nil {
		return err
	}
	if ctx == nil {
		return fmt.Errorf("proxy app context is nil")
	}
	if !a.ran.CompareAndSwap(false, true) {
		return fmt.Errorf("proxy app can only be run once")
	}
	defer func() { a.service.FinishEntry(returnErr) }()
	if a.workerPresent() {
		componentexec.ClearEnvironment()
		return a.runWorkerProcess(ctx)
	}
	if proxyapp.ContinuationPresent() {
		continuation, err := proxyapp.ReceiveContinuation()
		if err != nil {
			return err
		}
		defer continuation.Close()
		componentexec.ClearEnvironment()
		return proxyapp.RunEntry(ctx, a.service, continuation.Input(), continuation, a.prepareService, a.runMaster)
	}
	proxyapp.ClearWorkerEnvironment()
	return a.runMasterProcess(ctx, signals)
}

func (a *App) requireInitialized() error {
	if a == nil || !a.initialized {
		return fmt.Errorf("proxy App must be constructed with proxy.New")
	}
	return nil
}

func (a *App) runMasterProcess(ctx context.Context, signals bool) error {
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
	return proxyapp.RunEntry(ctx, a.service, proxyapp.ServiceInput{Config: bootstrap.Config, Executable: bootstrap.ComponentExecutable, Signals: signals}, nil, a.prepareService, a.runMaster)
}

func (a *App) prepareService(ctx context.Context, input proxyapp.ServiceInput) (_ *proxyapp.Startup, returnErr error) {
	var cfg publicconfig.Proxy
	if err := decodeConfig(input.Config, &cfg); err != nil {
		return nil, fmt.Errorf("custom proxy bootstrap config: %w", err)
	}
	immutableExecutable := cfg.Paths.ProxyExecutable
	if !input.Native && (immutableExecutable == "" || immutableExecutable != input.Executable) {
		return nil, fmt.Errorf("custom proxy bootstrap executable does not match config")
	}
	if a.hooks.Configure != nil {
		if err := a.hooks.Configure(ctx, &cfg); err != nil {
			return nil, fmt.Errorf("custom proxy configure: %w", err)
		}
	}
	if cfg.Paths.ProxyExecutable != immutableExecutable {
		return nil, fmt.Errorf("custom proxy Configure must not change paths.proxy_executable")
	}
	effective, err := proxyapp.FreezeConfig(&cfg)
	if err != nil {
		return nil, err
	}
	runtimeBindings := &Runtime{}
	process := Process{Role: RoleMaster}
	if a.hooks.BindRuntime != nil {
		if err := a.hooks.BindRuntime(ctx, process, runtimeBindings); err != nil {
			return nil, fmt.Errorf("custom proxy bind runtime for master: %w", err)
		}
	}
	// Ownership begins at the successful BindRuntime return, before any
	// environment adaptation or runtime-material validation can fail.
	owned := maps.Clone(runtimeBindings.ListenerFiles)
	if owned == nil {
		owned = make(map[string]*os.File)
	}
	defer func() {
		if returnErr != nil {
			listenresource.Close(owned)
		}
	}()
	received, err := activation.Receive()
	if err != nil {
		return nil, err
	}
	defer listenresource.Close(received)
	for name, file := range received {
		if _, exists := owned[name]; exists {
			return nil, fmt.Errorf("proxy listener %q delivered by both activation and BindRuntime", name)
		}
		owned[name] = file
		delete(received, name) // The App's frozen collection now owns this file.
	}
	runtimeBindings.ListenerFiles = owned
	resolved, err := proxyapp.ResolveRuntime(ctx, effective.Config(), freezeRuntime(runtimeBindings))
	if err != nil {
		return nil, err
	}
	return &proxyapp.Startup{Effective: effective, Runtime: resolved, Release: func() { listenresource.Close(owned) }}, nil
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
	owned := maps.Clone(runtimeBindings.ListenerFiles)
	defer listenresource.Close(owned)
	if len(owned) != 0 {
		return fmt.Errorf("proxy worker BindRuntime must not deliver ListenerFiles")
	}
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
	bindings := proxyapp.Bindings{
		ListenerFiles: maps.Clone(runtime.ListenerFiles),
		Logger:        runtime.Logger, MasterExtension: runtime.MasterExtension,
		WorkerExtension: runtime.WorkerExtension,
	}
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
