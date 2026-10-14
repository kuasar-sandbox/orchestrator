// Source-binding regressions adapted from sandbox-ctl baseline 509cf540.
package sandboxsdk

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	storefs "github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOpenSandboxRunSourceLocalTarstream(t *testing.T) {
	dir := t.TempDir()
	portable := testRunFromPortable(t)
	runtimeBytes, err := config.MarshalPortableSandboxConfig(portable)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := sparse.NewSource(bytes.NewReader(bytes.Repeat([]byte{0x5a}, 4096)), 4096, nil)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := sandboxfile.BuildSource(payload, nil, runtimeBytes)
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := os.CreateTemp(dir, "sandbox-*.partial")
	if err != nil {
		t.Fatal(err)
	}
	scheme, digest, err := tarstream.WriteTo(context.Background(), tmp, "sandbox", logical)
	if err != nil {
		_ = tmp.Close()
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "alias.sandbox")
	if err := os.Rename(tmp.Name(), path); err != nil {
		t.Fatal(err)
	}
	storage, err := artifact.NewProcessStorage(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	source, err := openSandboxRunSource(context.Background(), path, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if source.RuntimeRef != "file://"+path+"@"+scheme+":"+digest {
		t.Fatalf("runtime ref = %q", source.RuntimeRef)
	}
	wantPortable := "file://" + digest + ".sandbox@" + scheme + ":" + digest
	if source.PortableRef != wantPortable {
		t.Fatalf("portable ref = %q, want %q", source.PortableRef, wantPortable)
	}
	if source.RelativeDir != dir {
		t.Fatalf("relative dir = %q, want %q", source.RelativeDir, dir)
	}
	if !bytes.Equal(source.Root.RuntimeConfig, runtimeBytes) {
		t.Fatal("runtime config changed")
	}
}

func TestOpenSandboxRunSourceEncryptedTarstream(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{0x31, 0x32, 0x33}
	cfg := runFromManifestConfig(key, "")
	cfg.Crypto.Local = manifestcrypto.LocalRequired
	storage, err := artifact.NewProcessStorage(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	logical := runFromLogical(t, runFromDirectPortable(t))
	_, path, err := snapshot.NewFileSink(t.TempDir(), "encrypted", storage.LocalCodec(), storage.LocalRequired(), nil).
		AbsorbSandbox(ctx, logical)
	if err != nil {
		t.Fatal(err)
	}
	source, err := openSandboxRunSource(ctx, path, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if source.Root.Portable.Boot.Root.Base != "self" || !strings.Contains(source.PortableRef, "@hmac:") {
		t.Fatalf("encrypted Sandbox source = ref:%q config:%+v", source.PortableRef, source.Root.Portable.Boot.Root)
	}
}

func TestOpenSandboxRunSourceManifestBundleDefaultAndExplicitRoot(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{0x41, 0x42, 0x43}
	cfg := runFromManifestConfig(key, "")
	storage, err := artifact.NewProcessStorage(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	dir := t.TempDir()
	sink, err := snapshot.NewBundleSink(ctx, dir, "bundle", cfg, storage.CustomerKeyFunc(), nil)
	if err != nil {
		t.Fatal(err)
	}
	rootRef, _, err := sink.AbsorbSandbox(ctx, runFromLogical(t, runFromDirectPortable(t)))
	if err == nil {
		err = sink.CommitSandbox(ctx, rootRef, "")
	}
	closeErr := sink.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("build Bundle err=%v close=%v", err, closeErr)
	}
	rootKey := strings.TrimPrefix(rootRef, "manifest://")
	alias := filepath.Join(dir, "bundle.sandbox")
	for _, input := range []string{
		alias,
		"file://" + filepath.Clean(mustEvalSymlinks(t, alias)) + "@manifest:" + rootKey,
	} {
		t.Run(filepath.Base(input), func(t *testing.T) {
			source, err := openSandboxRunSource(ctx, input, storage, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			if source.BundleReader == nil || source.BundleFetcher == nil || source.BundleSource == nil {
				t.Fatalf("Bundle source bindings are incomplete: %+v", source)
			}
			if source.BundleSource.Reader != source.BundleReader || source.BundleSource.Fetcher != source.BundleFetcher {
				t.Fatal("Bundle source provenance is not paired with its reader/fetcher")
			}
			if !strings.Contains(source.PortableRef, "@manifest:"+rootKey) {
				t.Fatalf("portable Bundle ref = %q", source.PortableRef)
			}
		})
	}
}

func TestOpenSandboxRunSourceManifestStore(t *testing.T) {
	ctx := context.Background()
	cfg, storage := runFromManifestStore(t)
	ingester, err := cfg.NewIngester(storage.CustomerKeyFunc(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, absorbErr := snapshot.NewIngestSink(ingester, nil).
		AbsorbSandbox(ctx, runFromLogical(t, runFromDirectPortable(t)))
	closeErr := ingester.Close()
	if absorbErr != nil || closeErr != nil {
		t.Fatalf("ingest Sandbox err=%v close=%v", absorbErr, closeErr)
	}
	source, err := openSandboxRunSource(ctx, ref, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if source.PortableRef != ref || source.RuntimeRef != ref || source.Root.Portable.Boot.Root.Base != "self" {
		t.Fatalf("Manifest Sandbox source = %+v", source)
	}
}

func TestOpenSandboxRunSourceManifestReadRecovery(t *testing.T) {
	for _, kind := range []string{"recover", "cancel", "permanent"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			timer := time.AfterFunc(5*time.Second, func() { cancel(context.DeadlineExceeded) })
			defer timer.Stop()
			stop := errors.New("run source operation stopped")
			var active atomic.Bool
			var calls atomic.Int32
			cfg, storage := runFromManifestStore(t, grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				if active.Load() && strings.HasSuffix(info.FullMethod, "/Get") {
					n := calls.Add(1)
					switch kind {
					case "recover":
						if n <= 2 {
							return status.Error([]codes.Code{codes.Unavailable, codes.DeadlineExceeded}[n-1], "injected source attempt")
						}
					case "cancel":
						cancel(stop)
						return status.Error(codes.Canceled, "operation canceled")
					case "permanent":
						return status.Error(codes.InvalidArgument, "invalid source request")
					}
				}
				return handler(srv, stream)
			}))
			ingester, err := cfg.NewIngester(storage.CustomerKeyFunc(), nil)
			if err != nil {
				t.Fatal(err)
			}
			ref, _, err := snapshot.NewIngestSink(ingester, nil).AbsorbSandbox(ctx, runFromLogical(t, runFromDirectPortable(t)))
			closeErr := ingester.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("seed Sandbox: %v / %v", err, closeErr)
			}
			active.Store(true)
			source, err := openSandboxRunSource(ctx, ref, storage, nil)
			if kind == "recover" {
				if err != nil {
					t.Fatal(err)
				}
				defer source.Close()
				if calls.Load() < 3 || source.PortableRef != ref || source.RuntimeRef != ref || source.Root.Portable.Boot.Root.Base != "self" {
					t.Fatalf("source did not recover intact: calls=%d source=%+v", calls.Load(), source)
				}
			} else if calls.Load() != 1 || source != nil || err == nil || kind == "cancel" && !errors.Is(err, stop) || kind == "permanent" && !readerr.IsPermanent(err) {
				t.Fatalf("source terminal changed: calls=%d source=%v err=%v", calls.Load(), source, err)
			}
		})
	}
}

func testRunFromPortable(t *testing.T) *config.PortableSandboxConfig {
	t.Helper()
	const a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	cfg := &config.PortableSandboxConfig{
		Version: config.PortableSandboxConfigVersion,
		Resources: config.PortableResourcesConfig{
			Capacity:    config.CapacityConfig{CPU: 1, Memory: "1GiB"},
			Allocatable: config.AllocatableConfig{CPU: 1, Memory: "1GiB"},
		},
		Boot: config.PortableBootConfig{
			Kernel: "file://vmlinux@digest:" + a, Runtime: "file://runtime.bundle@digest:" + b,
			Root: config.PortableRootConfig{Base: "file://base.erofs@digest:" + a, Overlay: &config.PortableOverlayConfig{Base: "self"}},
		},
		Launch: config.PortableLaunchConfig{Exec: "/bin/true", Workdir: "/", Restart: "never"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func runFromDirectPortable(t *testing.T) *config.PortableSandboxConfig {
	t.Helper()
	cfg := testRunFromPortable(t)
	cfg.Boot.Root = config.PortableRootConfig{Base: "self"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func runFromLogical(t *testing.T, portable *config.PortableSandboxConfig) sparse.Source {
	t.Helper()
	runtimeBytes, err := config.MarshalPortableSandboxConfig(portable)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := sparse.NewSource(bytes.NewReader(bytes.Repeat([]byte{0x6d}, 4096)), 4096, nil)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := sandboxfile.BuildSource(payload, nil, runtimeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return logical
}

func runFromManifestConfig(key [32]byte, endpoint string) *config.ManifestConfig {
	return &config.ManifestConfig{
		Manifest: manifest.ManifestSubConfig{Key: hex.EncodeToString(key[:]), WriteGeneration: "G1"},
		Store:    manifest.StoreConfig{Endpoint: endpoint, Pool: 2, Timeout: "5s"},
		Chunker:  chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}},
		Crypto:   manifestcrypto.Config{Chunk: "aes", Manifest: "aes", Local: manifestcrypto.LocalOff},
	}
}

func runFromManifestStore(t *testing.T, options ...grpc.ServerOption) (*config.ManifestConfig, *artifact.ProcessStorage) {
	t.Helper()
	backend, err := storefs.New(storefs.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	server, err := storeserver.New(storeserver.Options{
		Backend: backend, VerifyKey: true,
		Generations: func() []store.Generation { return []store.Generation{"G1"} },
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(options...)
	pb.RegisterStoreServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	cfg := runFromManifestConfig([32]byte{0x51, 0x52, 0x53}, listener.Addr().String())
	storage, err := artifact.NewProcessStorage(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	return cfg, storage
}

func mustEvalSymlinks(t *testing.T, path string) string {
	t.Helper()
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return realPath
}

func TestApplyDefaultArtifactBindings(t *testing.T) {
	dir := t.TempDir()
	host := &config.SandboxConfig{}
	portable := testRunFromPortable(t)
	applyDefaultArtifactBindings(host, portable, dir)
	if host.Boot.Kernel != "file://"+filepath.Join(dir, "vmlinux") {
		t.Fatalf("kernel = %q", host.Boot.Kernel)
	}
	if host.Boot.Runtime != "file://"+filepath.Join(dir, "runtime.bundle") {
		t.Fatalf("runtime = %q", host.Boot.Runtime)
	}
	host.Boot.Kernel = "file:///explicit/kernel"
	applyDefaultArtifactBindings(host, portable, dir)
	if !strings.Contains(host.Boot.Kernel, "explicit") {
		t.Fatal("explicit host binding was overwritten")
	}
}
