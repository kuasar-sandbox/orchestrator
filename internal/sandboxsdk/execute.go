package sandboxsdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	runtimesdk "github.com/kuasar-sandbox/sandboxer/pkg/runtime"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"golang.org/x/sys/unix"
)

// Execute blocks through VM cleanup and closure of all borrowed artifact
// readers. Start and Restore retain the public SDK identity/readiness policy;
// the adapter does not select a legacy CLI identity exception.
func Execute(ctx context.Context, in *Input, cgroup *os.File, readiness *Readiness) (int, error) {
	diagnostics, closeDiagnostics, err := diagnostics(in.LogTo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sandbox-ctl run: --log-to: %v\n", err)
		_ = readiness.Close()
		return 2, err
	}
	defer closeDiagnostics()
	return executeWithDiagnostics(ctx, in, cgroup, readiness, diagnostics)
}

func executeWithDiagnostics(ctx context.Context, in *Input, cgroup *os.File, readiness *Readiness, diagnostics io.Writer) (code int, retErr error) {
	// This defer runs after storage/source cleanup and before the diagnostic
	// writer closes, retaining terminal errors and partial-line draining.
	defer func() {
		if retErr != nil {
			fmt.Fprintln(diagnostics, retErr)
		}
	}()
	defer readiness.Close()
	opts, presence, storage, err := in.options(cgroup)
	if err != nil {
		var usage usageError
		if errors.As(err, &usage) {
			return 2, err
		}
		return 1, err
	}
	defer storage.Close()
	opts.NotifyReadiness = readiness.Notify
	switch in.Source.Kind {
	case types.ResumeSourceSnapshot:
		if err := opts.Cfg.ValidateRestoreHostConfigWithPresence(presence); err != nil {
			return 1, err
		}
		restore := runtimesdk.RestoreSpec{
			HostCfg: opts.Cfg, HostPresence: presence, ManifestCfg: opts.ManifestCfg, Fetcher: opts.Fetcher,
			CustomerKeyFn: opts.CustomerKeyFn, LocalCodec: opts.LocalCodec, LocalRequired: opts.LocalRequired,
			RefLocations: opts.RefLocations, SandboxID: opts.SandboxID, PathID: opts.PathID, CHBinary: opts.CHBinary,
			RuntimeRoot: opts.RuntimeRoot, BaseRoot: opts.BaseRoot, StatsInterval: opts.StatsInterval,
			StdioMode: opts.StdioMode, PingFatalThreshold: opts.PingFatalThreshold, Forwards: opts.Forwards, NotifyReadiness: opts.NotifyReadiness,
		}
		if err := in.snapshot(&restore); err != nil {
			return 2, err
		}
		owner, err := runtimesdk.Restore(ctx, restore)
		if err != nil {
			return 1, err
		}
		// Restore's RunLifecycle waits for the existing ACK/MUX readiness barrier.
		// WaitRun retains first-signal graceful shutdown and second-signal escalation.
		return cliRunResult(sandbox.WaitRun(ctx, owner))
	case types.ResumeSourceSandbox:
		raw, err := in.sourceRef()
		if err != nil {
			return 1, err
		}
		source, err := openSandboxRunSource(ctx, raw, storage, in.Locations)
		if err != nil {
			return 1, err
		}
		defer source.Close()
		applyDefaultArtifactBindings(opts.Cfg, source.Root.Portable, source.RelativeDir)
		opts.Cfg, opts.PortableConfig, err = config.ApplyFromRules(source.Root.Portable, opts.Cfg, presence, config.ApplyFromOptions{})
		if err != nil {
			return 1, err
		}
		opts.SourceBinding = &sandbox.RunSourceBinding{SandboxRef: source.PortableRef, RuntimeRef: source.RuntimeRef, RelativeDir: source.RelativeDir, BundleSource: source.BundleSource}
		opts.Fetcher = source.Fetcher
		opts.BundleReader = source.BundleReader
		opts.BundleFetcher = source.BundleFetcher
	}
	spec, err := startSpec(ctx, opts, storage)
	if err != nil {
		return 1, err
	}
	owner, err := runtimesdk.Start(ctx, spec)
	if err != nil {
		return 1, err
	}
	return cliRunResult(sandbox.WaitRun(ctx, owner))
}

// The CLI prints lifecycle errors and exits 1, even when the runtime also
// supplies a guest status. Only a successful wait propagates that status.
func cliRunResult(code int, err error) (int, error) {
	if err != nil {
		return 1, err
	}
	return code, nil
}

func (in *Input) options(cgroup *os.File) (sandbox.RunOptions, config.FieldPresence, *artifact.ProcessStorage, error) {
	var opts sandbox.RunOptions
	var presence config.FieldPresence
	var err error
	opts.StatsInterval = 30 * time.Second
	if raw := in.Env["SANDBOX_STATS_INTERVAL"]; raw != "" {
		opts.StatsInterval, err = time.ParseDuration(raw)
		if err != nil || opts.StatsInterval < 0 {
			return opts, presence, nil, usageError{errors.New("invalid SANDBOX_STATS_INTERVAL")}
		}
	}
	if raw := in.Env["SANDBOX_PING_FATAL_THRESHOLD"]; raw != "" {
		opts.PingFatalThreshold, err = strconv.Atoi(raw)
		if err != nil || opts.PingFatalThreshold < 0 {
			return opts, presence, nil, usageError{errors.New("invalid SANDBOX_PING_FATAL_THRESHOLD")}
		}
	}
	opts.CHBinary, err = in.locateCH()
	if err != nil {
		return opts, presence, nil, err
	}
	paths := strings.Split(in.Config, ":")
	for i, p := range paths {
		paths[i] = in.path(p)
	}
	if in.Source.Empty() {
		opts.Cfg, err = config.LoadMerged(paths)
	} else {
		opts.Cfg, presence, err = config.LoadMergedWithPresence(paths)
	}
	if err != nil {
		return opts, presence, nil, err
	}
	path, err := cgroupPath(cgroup)
	if err != nil {
		return opts, presence, nil, err
	}
	opts.Cfg.Resources.Control.CgroupPath = path
	opts.Cfg.Resources.Control.CgroupFD = int(cgroup.Fd())
	manifestPath := in.ManifestConfig
	if manifestPath == "" {
		manifestPath = in.Env["MANIFEST_CONFIG"]
	}
	opts.ManifestCfg, err = manifest.LoadConfig(in.path(manifestPath), "")
	if err != nil && !errors.Is(err, manifest.ErrConfigNotProvided) {
		return opts, presence, nil, err
	}
	key := in.Env["MANIFEST_KEY"]
	if key == "" && opts.ManifestCfg != nil {
		key = opts.ManifestCfg.Manifest.Key
	}
	storage, err := artifact.NewProcessStorageWithCustomerKey(opts.ManifestCfg, func() ([32]byte, error) {
		parsed, err := manifest.ParseHexKey(key)
		if err != nil {
			return [32]byte{}, errors.New("invalid manifest customer key")
		}
		return [32]byte(parsed), nil
	})
	if err != nil {
		return opts, presence, nil, err
	}

	opts.Fetcher = storage.Fetcher()
	opts.CustomerKeyFn = storage.CustomerKeyFunc()
	opts.LocalCodec = storage.LocalCodec()
	opts.LocalRequired = storage.LocalRequired()

	opts.SandboxID = in.SandboxID
	opts.PathID = in.PathID
	opts.RuntimeRoot = in.RunRoot
	opts.BaseRoot = in.BaseRoot
	opts.RefLocations = in.Locations
	opts.StdioMode = in.Stdio
	opts.Forwards = in.Forwards

	return opts, presence, storage, nil
}

func (in *Input) snapshot(spec *runtimesdk.RestoreSpec) error {
	raw := in.Source.Ref
	if !strings.HasPrefix(raw, "file://") && !strings.HasPrefix(raw, "manifest://") {
		spec.SnapshotPath = in.path(raw)
		return nil
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return err
	}
	switch ref.Scheme {
	case manifest.RefSchemeManifest:
		if spec.ManifestCfg == nil {
			return errors.New("manifest Snapshot requires manifest config")
		}
		spec.SnapshotRef = ref.String()
		spec.SnapshotManifestKey = ref.Path
	case manifest.RefSchemeFile:
		spec.SnapshotPath, err = in.Locations.ResolveFile(ref, "")
		if err != nil {
			return err
		}
		spec.SnapshotPath = in.path(spec.SnapshotPath)
		if ref.Location != "" || ref.Digest != "" {
			spec.SnapshotRef = ref.String()
		}
	default:
		return errors.New("unsupported Snapshot source")
	}
	return nil
}

// The caller lends the already opened vmm descriptor; never infer a target from
// LaunchSpec or close/reopen it by pathname. resctl duplicates it for CH.
func cgroupPath(f *os.File) (string, error) {
	if f == nil || f.Fd() < 3 {
		return "", errors.New("missing vmm cgroup descriptor")
	}
	fd := int(f.Fd())
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return "", err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return "", errors.New("vmm cgroup descriptor is not a directory")
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return "", err
	}
	if fs.Type != unix.CGROUP2_SUPER_MAGIC {
		return "", errors.New("vmm descriptor is not cgroup v2")
	}
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return "", err
	}
	if flags&unix.FD_CLOEXEC == 0 {
		return "", errors.New("vmm descriptor must be close-on-exec")
	}
	path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(path) || strings.HasSuffix(path, " (deleted)") {
		return "", errors.New("invalid vmm descriptor target")
	}
	return filepath.Clean(path), nil
}
func (in *Input) locateCH() (string, error) {
	if p := in.Env["SANDBOX_CH_PATH"]; p != "" {
		if strings.ContainsRune(p, os.PathSeparator) {
			return in.path(p), nil
		}
		return in.lookPath(p)
	}
	// Resolve the old sandbox-ctl executable's directory, including symlinks,
	// because node-ctl can be installed in a different directory.
	exe := in.Exec
	if real, err := filepath.EvalSymlinks(exe); err == nil {
		exe = real
	}
	adjacent := filepath.Join(filepath.Dir(exe), "cloud-hypervisor")
	if _, err := os.Stat(adjacent); err == nil {
		return adjacent, nil
	}
	return in.lookPath("cloud-hypervisor")
}

func (in *Input) lookPath(name string) (string, error) {
	for _, dir := range filepath.SplitList(in.Env["PATH"]) {
		candidate := in.path(filepath.Join(dir, name))
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() && st.Mode()&0111 != 0 {
			// exec.LookPath rejects implicit relative PATH lookup (ErrDot). Keep
			// that rejection rather than silently resolving against the SDK cwd.
			if !filepath.IsAbs(dir) {
				return "", fmt.Errorf("%s resolves through relative PATH entry", name)
			}
			return candidate, nil
		}
	}
	return "", fmt.Errorf("%s not found in task PATH", name)
}

// Only unlocated file refs need workdir binding; location names and canonical
// identities remain intact for the library's source validation.
func (in *Input) sourceRef() (string, error) {
	raw := in.Source.Ref
	if !strings.HasPrefix(raw, "file://") && !strings.HasPrefix(raw, "manifest://") {
		return in.path(raw), nil
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return "", err
	}
	if ref.Scheme == manifest.RefSchemeFile && ref.Location == "" {
		ref.Path = in.path(ref.Path)
	}
	return ref.String(), nil
}

// usageError preserves CLI exit 2 for invalid environment defaults.
type usageError struct{ error }
