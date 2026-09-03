// Package builder is the build pipeline orchestrator behind run-builder inside a
// sandbox-builder@<run-id>.service instance. It receives the BuildSpec over the
// config-socket and drives up to three phases, each a microVM it spawns as a
// DIRECT child (sandbox-ctl run, in this unit's cgroup), reusing ONE
// pre-attached network slot sequentially:
//
//	A import   — an EMPTY single-disk sandbox on the single guest runtime
//	             (flatten-ctl/mkfs.erofs ride /opt/sandbox-runtime, bind-
//	             mounted into any rootfs); flatten-ctl pulls the image WITH
//	             TENANT creds (exec env, never host-side) over the tenant
//	             network, flattens, and streams the tarstream image artifact
//	             back over exec stdio.
//	B steps    — an e2b-shaped sandbox whose root is the (local or template)
//	             base image, with envd as the app; RUN steps execute THROUGH
//	             ENVD (the e2b exec channel, envdExec) with the accumulated
//	             ENV/WORKDIR/USER context seeded from the base image config
//	             (ARG substitutes only); then flatten-ctl exports the rootfs
//	             (its tmpdir/output home is a self-bind mountpoint, excluded
//	             by --skip-mounts) and streams the new image artifact back.
//	C memory  — only for target={sandbox,memory:true}: a production-runtime
//	            sandbox cold-booted from the final image. For a Sandbox source,
//	            run --from E --replace-boot preserves portable non-boot defaults
//	            while installing that image as the complete boot. startCmd runs
//	            through envd; readyCmd polls every 2s, or its absence causes a
//	            cancelable fixed 20-second wait, before the memory snapshot.
//
// Two guest channels, deliberately distinct: e2b-SEMANTIC commands
// (steps/startCmd/readyCmd) go through envd exactly as e2b's own template
// build does; PLATFORM plumbing (flatten-ctl pulls/exports, config
// injection, artifact streaming, probes) goes through sandbox-ctl exec,
// which works on any rootfs and carries raw stdio.
//
// A Sandbox output fixes the final image's manifest identity before offline E
// assembly or Phase C, so neither output retains the source E/S graph. The
// target then selects exactly one returned image, Sandbox, or Snapshot ref for
// the orchestrator's fail-closed result validation.
package builder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// PhaseReporter records the currently active ordinary sandbox. A starting
// report is made before spawning the phase, so losing the controller-side
// execution claim fails closed before any phase resource side effect.
type PhaseReporter func(phase, sandboxID, state string) error

// Run drives the build pipeline for spec and returns its Result. parent carries
// task cancellation (including SIGTERM) into every phase subprocess.
func Run(parent context.Context, spec *configsock.BuildSpec, vmmCgroup *os.File, report PhaseReporter, log *slog.Logger) Result {
	p := &buildPipeline{parent: parent, spec: spec, vmmCgroup: vmmCgroup, report: report, log: log, now: time.Now}
	return p.run()
}

// Result mirrors orch.buildResult.
type Result struct {
	Target      types.BuildTarget `json:"target"`
	ImageRef    string            `json:"image_ref,omitempty"`
	SandboxRef  string            `json:"sandbox_ref,omitempty"`
	SnapshotRef string            `json:"snapshot_ref,omitempty"`
	StartCmd    string            `json:"start_cmd,omitempty"`
	ReadyCmd    string            `json:"ready_cmd,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type buildPipeline struct {
	parent    context.Context
	spec      *configsock.BuildSpec
	vmmCgroup *os.File
	report    PhaseReporter
	log       *slog.Logger
	now       func() time.Time // publication-date bucketing clock; overridable in tests
	out       *buildJournal    // curated build progress → journald SYSLOG_IDENTIFIER=build (SDK-visible)
	profile   types.Profile

	ctx    context.Context
	cancel context.CancelFunc

	imagePath    string // BuildBaseDir/checkpoint/image.img once a local image exists
	baseImageRef string // portable ref for an already-published base image
	baseRef      string // phase B/C boot.root.base ("file://..." | "manifest://...")
	startCmd     string // effective (request else template-inherited)
	readyCmd     string
	target       types.BuildTarget
}

func (p *buildPipeline) phaseRunDir(pathID string) string {
	return filepath.Join(p.spec.RunDir, pathID)
}

func (p *buildPipeline) phaseBaseDir(pathID string) string {
	return filepath.Join(p.spec.BaseDir, pathID)
}

func (p *buildPipeline) checkpointDir() string {
	return filepath.Join(p.spec.BaseDir, "checkpoint")
}

func (p *buildPipeline) imageFile() string {
	return filepath.Join(p.checkpointDir(), "image.img")
}

func (p *buildPipeline) nextImageFile() string {
	return filepath.Join(p.checkpointDir(), "image.next.img")
}

const guestFlatten = "/opt/sandbox-runtime/bin/flatten-ctl"

// The conductor's absolute execution deadline includes durable reporting (and
// excludes its separately bounded cleanup). Reserve a tail inside that same
// deadline so run-builder can report a work timeout before the RPC is canceled.
const buildResultReportGrace = 5 * time.Second

// Guest-side paths for the flatten-ctl TLS config (projected via sandbox YAML
// files into the Phase A import sandbox when a per-build registry TLS policy is
// configured). /run is a tmpfs mounted by sandbox-init before applyFiles, so
// these never land on the build root disk; phase B's --skip-mounts excludes
// /run too. flatten-ctl reads the YAML via --config; tls.ca_cert points at the
// projected CA bundle. Files are read-only (0444).
const (
	guestFlattenCfg = "/run/kuasar-build/flatten/config.yaml"
	guestCACert     = "/run/kuasar-build/flatten/registry-ca.pem"
)

// journald SYSLOG_IDENTIFIER tags (shared contract with the orchestrator's log
// query, defined in configsock): buildTag = curated build progress (SDK-visible),
// consoleTag = guest kernel dmesg (host-only).
const (
	buildTag   = configsock.BuildLogTag
	consoleTag = configsock.ConsoleTag
)

func (p *buildPipeline) run() (res Result) {
	s := p.spec
	p.ctx, p.cancel = buildPipelineContext(p.parent, s.Timeouts)
	defer p.cancel()
	p.out = newBuildJournal(s.RunID, s.BuildID)
	defer p.out.Close()
	fail := func(err error) Result {
		p.log.Error("build", "bid", s.BuildID, "err", err)
		p.progress("build failed: %v", err) // surface the failure in the build log too
		return Result{Error: err.Error()}
	}
	profile, err := validateBuildProfile(s)
	if err != nil {
		return fail(err)
	}
	p.profile = profile

	p.startCmd, p.readyCmd = s.StartCmd, s.ReadyCmd
	if err := p.resolveBase(); err != nil {
		return fail(err)
	}
	if err := p.resolveTarget(); err != nil {
		return fail(err)
	}

	if s.FromImage != "" {
		if err := p.runPhase("a", p.phaseImport); err != nil {
			return fail(fmt.Errorf("import: %w", err))
		}
	}
	if len(s.Steps) > 0 || s.SourceSandboxConfig != nil {
		if err := p.runPhase("b", p.phaseSteps); err != nil {
			return fail(fmt.Errorf("steps: %w", err))
		}
	}
	var bundle string
	if p.target.Kind == types.BuildTargetSandbox && p.target.Memory {
		if err := p.prepareFinalImageRef(); err != nil {
			return fail(fmt.Errorf("final image: %w", err))
		}
		var b string
		err := p.runPhase("c", func() error {
			var phaseErr error
			b, phaseErr = p.phaseTemplate()
			return phaseErr
		})
		if err != nil {
			return fail(fmt.Errorf("template: %w", err))
		}
		bundle = b
	}

	// Finale is selected only by the resolved target; returned fields never
	// reverse-infer or alter it.
	switch {
	case p.target.Kind == types.BuildTargetSandbox && p.target.Memory:
		if bundle == "" {
			return fail(fmt.Errorf("memory Sandbox target produced no snapshot"))
		}
		key, err := p.publishSnapshot(bundle)
		if err != nil {
			return fail(fmt.Errorf("publish snapshot: %w", err))
		}
		res.SnapshotRef = key
	case p.target.Kind == types.BuildTargetSandbox:
		ref, err := p.publishOfflineSandbox()
		if err != nil {
			return fail(fmt.Errorf("publish Sandbox: %w", err))
		}
		res.SandboxRef = ref
	case p.target.Kind == types.BuildTargetImage:
		ref, err := p.uploadImage()
		if err != nil {
			return fail(fmt.Errorf("upload image: %w", err))
		}
		res.ImageRef = ref
	default:
		return fail(fmt.Errorf("unsupported resolved build target %+v", p.target))
	}
	res.Target = p.target
	res.StartCmd, res.ReadyCmd = p.startCmd, p.readyCmd
	return res
}

func buildPipelineContext(parent context.Context, timeouts configsock.BuildTimeouts) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeouts.AbsoluteDeadlineUnixNano > 0 {
		deadline := PreResultDeadline(
			time.Unix(0, timeouts.AbsoluteDeadlineUnixNano), time.Now(),
		)
		return context.WithDeadline(parent, deadline)
	}
	return context.WithTimeout(parent, time.Duration(timeouts.TotalSec)*time.Second)
}

// PreResultDeadline reserves a bounded tail inside absolute for the task's
// durable result report. Callers use the returned deadline for snapshot/host
// preparation, phase reporting, and pipeline execution; only the final result
// POST may consume the remaining tail.
func PreResultDeadline(absolute, now time.Time) time.Time {
	remaining := absolute.Sub(now)
	if remaining <= 0 {
		return absolute
	}
	grace := buildResultReportGrace
	if half := remaining / 2; grace > half {
		grace = half
	}
	return absolute.Add(-grace)
}

func (p *buildPipeline) runPhase(phase string, run func() error) error {
	sid := phaseSandboxID(phase, p.spec.BuildID)
	if p.report != nil {
		if err := p.report(phase, sid, "starting"); err != nil {
			return fmt.Errorf("report phase %s starting: %w", phase, err)
		}
	}
	err := run()
	if cleanupErr := p.requirePhaseVMMCgroupEmpty(); cleanupErr != nil {
		err = errors.Join(err, cleanupErr)
	}
	if err != nil {
		if p.report != nil {
			if reportErr := p.report(phase, sid, "failed"); reportErr != nil {
				p.log.Error("report failed phase", "phase", phase, "sid", sid, "err", reportErr)
			}
		}
		return err
	}
	if p.report != nil {
		if err := p.report(phase, sid, "finished"); err != nil {
			return fmt.Errorf("report phase %s finished: %w", phase, err)
		}
	}
	return nil
}

func validateBuildProfile(s *configsock.BuildSpec) (types.Profile, error) {
	profile, err := types.ParseProfile(s.Profile)
	if err != nil {
		return "", fmt.Errorf("build profile: %w", err)
	}
	if profile == types.ProfileBare && (s.StartCmd != "" || s.ReadyCmd != "") {
		return "", fmt.Errorf("bare profile does not support start or ready commands")
	}
	return profile, nil
}

// resolveBase fixes the phase B/C base ref and inherits start/ready from a
// task-local source E's portable metadata (e2b.start_cmd / e2b.ready_cmd).
func (p *buildPipeline) resolveBase() error {
	s := p.spec
	switch {
	case s.FromTemplateRef == "":
		return nil // base = the imported local image (set by phaseImport)
	case s.FromTemplateKind == "img":
		p.baseRef = s.FromTemplateRef
		p.baseImageRef = s.FromTemplateRef
		return nil
	case s.FromTemplateKind == "sbx", s.FromTemplateKind == "snp":
		if s.SourceSandboxConfig == nil || s.SourceSandboxRef == "" {
			return fmt.Errorf("base template %s: task-local Sandbox E preparation is missing", s.FromTemplateRef)
		}
		if p.profile == types.ProfileE2B && p.startCmd == "" {
			p.startCmd = s.SourceSandboxConfig.Metadata[sandboxcfg.E2BStartCommandMetadata]
		}
		if p.profile == types.ProfileE2B && p.readyCmd == "" {
			p.readyCmd = s.SourceSandboxConfig.Metadata[sandboxcfg.E2BReadyCommandMetadata]
		}
		return nil
	default:
		return fmt.Errorf("base template %s has unsupported kind %q", s.FromTemplateRef, s.FromTemplateKind)
	}
}

func (p *buildPipeline) resolveTarget() error {
	// An explicit image deliberately ignores inherited source commands. Trigger
	// commands already conflict at the API boundary, but enforce it again here.
	if p.spec.RequestedTarget != nil && p.spec.RequestedTarget.Kind == types.BuildTargetImage {
		if p.spec.StartCmd != "" || p.spec.ReadyCmd != "" {
			return fmt.Errorf("explicit image target conflicts with trigger start/ready commands")
		}
		p.startCmd, p.readyCmd = "", ""
	}
	p.target = types.ResolveBuildTarget(p.spec.RequestedTarget, p.startCmd, p.readyCmd)
	if err := p.target.Validate(); err != nil {
		return err
	}
	switch {
	case p.target.Kind == types.BuildTargetImage && (p.spec.HasSandboxConfig || p.spec.HasInstanceConfig):
		return fmt.Errorf("image target cannot represent registered Sandbox configuration")
	case p.target.Kind == types.BuildTargetSandbox && !p.target.Memory && p.spec.HasInstanceConfig:
		return fmt.Errorf("sandbox target with memory=false cannot apply instance-only configuration")
	}
	return nil
}
