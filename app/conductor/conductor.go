// Package conductor exposes the narrow startup API for statically customized
// conductors. Applications configure declarative state and bind process-local
// material, then reuse the upstream conductor core.
package conductor

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
	"time"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/conductorapp"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/filestore"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

// Config is the public conductor declarative configuration.
type Config = publicconfig.Conductor

// TLSPurpose identifies the core endpoint whose process-local TLS material is
// requested. The core, not the provider, owns TLS policy.
type TLSPurpose string

const (
	// TLSPurposeAPI requests the conductor's north/data-plane server material.
	TLSPurposeAPI TLSPurpose = "api"
	// TLSPurposeNodeLinkClient requests registry node-link client material.
	TLSPurposeNodeLinkClient TLSPurpose = "node-link-client"
)

// TLSMaterial contains DER-encoded leaf-first certificates, a matching signer,
// and optional trust pools. An empty value disables TLS for that purpose.
type TLSMaterial struct {
	CertificateChain [][]byte
	PrivateKey       crypto.Signer
	RootCAs          *x509.CertPool
	ClientCAs        *x509.CertPool
}

// TLSMaterialProvider supplies startup TLS material. When non-nil it is the
// authoritative source; errors never fall back to configured files.
type TLSMaterialProvider interface {
	TLSMaterial(context.Context, TLSPurpose) (TLSMaterial, error)
}

// TLSMaterialProviderFunc adapts a function to TLSMaterialProvider.
type TLSMaterialProviderFunc func(context.Context, TLSPurpose) (TLSMaterial, error)

func (f TLSMaterialProviderFunc) TLSMaterial(ctx context.Context, purpose TLSPurpose) (TLSMaterial, error) {
	return f(ctx, purpose)
}

// EncryptionKeyProvider returns an ordered AES-256 key set. Index zero encrypts
// new records; the remaining keys decrypt records written before rotation.
type EncryptionKeyProvider interface {
	EncryptionKeys(context.Context) ([][]byte, error)
}

// EncryptionKeyProviderFunc adapts a function to EncryptionKeyProvider.
type EncryptionKeyProviderFunc func(context.Context) ([][]byte, error)

func (f EncryptionKeyProviderFunc) EncryptionKeys(ctx context.Context) ([][]byte, error) {
	return f(ctx)
}

// ObjectStoreCredentials is provider-neutral S3-compatible credential
// material. Providers may refresh it by returning a new value on later calls.
type ObjectStoreCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expires         time.Time
	CanExpire       bool
}

// ObjectStoreCredentialsProvider supplies builder files-storage credentials.
// It is authoritative over static YAML and the ambient SDK chain.
type ObjectStoreCredentialsProvider interface {
	RetrieveObjectStoreCredentials(context.Context) (ObjectStoreCredentials, error)
}

// ObjectStoreCredentialsProviderFunc adapts a function to
// ObjectStoreCredentialsProvider.
type ObjectStoreCredentialsProviderFunc func(context.Context) (ObjectStoreCredentials, error)

func (f ObjectStoreCredentialsProviderFunc) RetrieveObjectStoreCredentials(ctx context.Context) (ObjectStoreCredentials, error) {
	return f(ctx)
}

// Runtime holds non-serializable, process-local startup bindings. None of these
// values enters the component bootstrap or declarative Config snapshot.
type Runtime struct {
	Logger                 *slog.Logger
	TLS                    TLSMaterialProvider
	EncryptionKeys         EncryptionKeyProvider
	ObjectStoreCredentials ObjectStoreCredentialsProvider
}

// MarshalJSON rejects accidental process-runtime serialization. Runtime
// providers and handles must never enter a component or worker snapshot.
func (*Runtime) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("conductor Runtime is process-local and cannot be serialized")
}

// UnmarshalJSON rejects attempts to reconstruct process-local bindings from a
// declarative/bootstrap document.
func (*Runtime) UnmarshalJSON([]byte) error {
	return fmt.Errorf("conductor Runtime is process-local and cannot be deserialized")
}

// Hooks contains the single startup customization point.
type Hooks struct {
	Configure func(context.Context, *Config, *Runtime) error
}

// App is a one-shot custom conductor application. New has no side effects.
type App struct {
	hooks Hooks
	ran   atomic.Bool

	receive func() (*componentexec.Bootstrap, error)
	run     func(context.Context, *publicconfig.Conductor, string, *conductorapp.Runtime) error
}

// New constructs a one-shot App without performing I/O or invoking hooks.
func New(hooks Hooks) *App {
	return &App{
		hooks: hooks,
		receive: func() (*componentexec.Bootstrap, error) {
			return componentexec.Receive(componentexec.ComponentConductor, componentexec.RoleConductor)
		},
		run: conductorapp.Run,
	}
}

// Run installs SIGINT/SIGTERM handling and runs the App once.
func (a *App) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return a.RunContext(ctx)
}

// RunContext consumes node-ctl bootstrap, invokes Configure exactly once,
// freezes the effective configuration/runtime bindings, and starts the core.
func (a *App) RunContext(ctx context.Context) error {
	if a == nil {
		return fmt.Errorf("conductor app is nil")
	}
	if ctx == nil {
		return fmt.Errorf("conductor app context is nil")
	}
	if !a.ran.CompareAndSwap(false, true) {
		return fmt.Errorf("conductor app can only be run once")
	}
	bootstrap, err := a.receive()
	if errors.Is(err, componentexec.ErrNoBootstrap) {
		return fmt.Errorf("custom conductor must be invoked through node-ctl conductor")
	}
	if err != nil {
		return err
	}
	if err := componentexec.VerifyCurrentExecutable(bootstrap.ComponentExecutable); err != nil {
		return err
	}
	var cfg publicconfig.Conductor
	if err := decodeConfig(bootstrap.Config, &cfg); err != nil {
		return fmt.Errorf("custom conductor bootstrap config: %w", err)
	}
	immutableExecutable := cfg.Paths.ConductorExecutable
	if immutableExecutable == "" || immutableExecutable != bootstrap.ComponentExecutable {
		return fmt.Errorf("custom conductor bootstrap executable does not match config")
	}
	if err := configresolve.ValidateComponentExecutable(immutableExecutable, bootstrap.NodeCtlExecutable); err != nil {
		return fmt.Errorf("paths.conductor_executable: %w", err)
	}

	runtimeBindings := &Runtime{}
	if a.hooks.Configure != nil {
		if err := a.hooks.Configure(ctx, &cfg, runtimeBindings); err != nil {
			return fmt.Errorf("custom conductor configure: %w", err)
		}
	}
	if cfg.Paths.ConductorExecutable != immutableExecutable {
		return fmt.Errorf("custom conductor Configure must not change paths.conductor_executable")
	}
	if err := publicconfig.ValidateConductorFinal(&cfg); err != nil {
		return err
	}
	frozenConfig := cfg.Clone()
	frozenBindings := freezeRuntime(runtimeBindings)
	resolved, err := conductorapp.ResolveRuntime(ctx, frozenConfig, frozenBindings)
	if err != nil {
		return err
	}
	return a.run(ctx, frozenConfig, bootstrap.NodeCtlExecutable, resolved)
}

func decodeConfig(raw []byte, out *publicconfig.Conductor) error {
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

func freezeRuntime(runtime *Runtime) conductorapp.Bindings {
	bindings := conductorapp.Bindings{Logger: runtime.Logger}
	if runtime.TLS != nil {
		provider := runtime.TLS
		bindings.TLSMaterial = func(ctx context.Context, purpose conductorapp.TLSPurpose) (conductorapp.TLSMaterial, error) {
			material, err := provider.TLSMaterial(ctx, TLSPurpose(purpose))
			if err != nil {
				return conductorapp.TLSMaterial{}, err
			}
			chain := make([][]byte, len(material.CertificateChain))
			for i, certificate := range material.CertificateChain {
				chain[i] = append([]byte(nil), certificate...)
			}
			return conductorapp.TLSMaterial{
				CertificateChain: chain, PrivateKey: material.PrivateKey,
				RootCAs: clonePool(material.RootCAs), ClientCAs: clonePool(material.ClientCAs),
			}, nil
		}
	}
	if runtime.EncryptionKeys != nil {
		provider := runtime.EncryptionKeys
		bindings.EncryptionKeys = func(ctx context.Context) ([][]byte, error) {
			keys, err := provider.EncryptionKeys(ctx)
			if err != nil {
				return nil, err
			}
			out := make([][]byte, len(keys))
			for i, key := range keys {
				out[i] = append([]byte(nil), key...)
			}
			return out, nil
		}
	}
	if runtime.ObjectStoreCredentials != nil {
		bindings.ObjectStoreCredentials = credentialAdapter{provider: runtime.ObjectStoreCredentials}
	}
	return bindings
}

type credentialAdapter struct {
	provider ObjectStoreCredentialsProvider
}

func (a credentialAdapter) Retrieve(ctx context.Context) (filestore.Credentials, error) {
	value, err := a.provider.RetrieveObjectStoreCredentials(ctx)
	if err != nil {
		return filestore.Credentials{}, err
	}
	return filestore.Credentials{
		AccessKeyID: value.AccessKeyID, SecretAccessKey: value.SecretAccessKey,
		SessionToken: value.SessionToken, Expires: value.Expires, CanExpire: value.CanExpire,
	}, nil
}

func clonePool(pool *x509.CertPool) *x509.CertPool {
	if pool == nil {
		return nil
	}
	return pool.Clone()
}
