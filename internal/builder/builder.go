// Package builder is the build pipeline orchestrator behind run-builder (the
// ExecStart of sandbox-builder@<bid>.service). It fetches the BuildSpec over
// the config-socket and drives up to three phases, each a microVM it spawns as
// a DIRECT child (sandbox-ctl run, in this unit's cgroup), reusing ONE
// pre-attached network slot sequentially:
//
//	A import   — an EMPTY single-disk sandbox on the builder runtime flavor
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
//	C template — a PRODUCTION-runtime sandbox cold-booted from the final
//	             image (the runtime ref freezes into the snapshot — template
//	             children must not inherit the builder toolchain); startCmd
//	             launches THROUGH ENVD and stays an envd-MANAGED process in
//	             the snapshot (the stream is held until ready, then dropped
//	             — envd never kills on stream loss), readyCmd polls every 2s
//	             to success, then sandbox-ctl snapshot writes the bundle.
//
// Two guest channels, deliberately distinct: e2b-SEMANTIC commands
// (steps/startCmd/readyCmd) go through envd exactly as e2b's own template
// build does; PLATFORM plumbing (flatten-ctl pulls/exports, config
// injection, artifact streaming, probes) goes through sandbox-ctl exec,
// which works on any rootfs and carries raw stdio.
//
// The finale uploads what was produced — platform credentials appear ONLY
// here: an image-only build runs `manifest-ctl store image.img`; a snapshot
// build runs ONE `sandbox-ctl upload-snapshot` (it auto-uploads every local
// artifact the snapshot.cfg references, the base image included, and
// rewrites the refs to manifest://). The result JSON goes to stdout, which
// the unit captures to <bid>.result for the orchestrator.
package builder

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// Run drives the build pipeline for spec and returns its Result. The result
// JSON is serialized by the caller to <bid>.result.
func Run(spec *configsock.BuildSpec, log *slog.Logger) Result {
	p := &buildPipeline{spec: spec, log: log}
	return p.run()
}

// Result mirrors orch.buildResult.
type Result struct {
	ImageKey    string `json:"image_key,omitempty"`
	SnapshotKey string `json:"snapshot_key,omitempty"`
	StartCmd    string `json:"start_cmd,omitempty"`
	ReadyCmd    string `json:"ready_cmd,omitempty"`
	Error       string `json:"error,omitempty"`
}

type buildPipeline struct {
	spec *configsock.BuildSpec
	log  *slog.Logger
	out  *buildJournal // curated build progress → journald SYSLOG_IDENTIFIER=build (SDK-visible)

	ctx    context.Context
	cancel context.CancelFunc

	imagePath   string // workdir/image.img once a local image exists
	baseRef     string // phase B/C boot.root.base ("file://..." | "manifest://...")
	overlayBase string // phase B/C boot.root.overlay.base: fromTemplate's accumulated diff, stacked read-only under the fresh overlay ("" = none)
	startCmd    string // effective (request else template-inherited)
	readyCmd    string
}

const guestFlatten = "/opt/sandbox-runtime/bin/flatten-ctl"

// journald SYSLOG_IDENTIFIER tags (shared contract with the orchestrator's log
// query, defined in configsock): buildTag = curated build progress (SDK-visible),
// consoleTag = guest kernel dmesg (host-only).
const (
	buildTag   = configsock.BuildLogTag
	consoleTag = configsock.ConsoleTag
)

func (p *buildPipeline) run() (res Result) {
	s := p.spec
	p.ctx, p.cancel = context.WithTimeout(context.Background(),
		time.Duration(s.Timeouts.TotalSec)*time.Second)
	defer p.cancel()
	p.out = newBuildJournal()
	defer p.out.Close()
	fail := func(err error) Result {
		p.log.Error("build", "bid", s.BuildID, "err", err)
		p.progress("build failed: %v", err) // surface the failure in the build log too
		return Result{Error: err.Error()}
	}

	p.startCmd, p.readyCmd = s.StartCmd, s.ReadyCmd
	if err := p.resolveBase(); err != nil {
		return fail(err)
	}

	if s.FromImage != "" {
		if err := p.phaseImport(); err != nil {
			return fail(fmt.Errorf("import: %w", err))
		}
	}
	if len(s.Steps) > 0 {
		if err := p.phaseSteps(); err != nil {
			return fail(fmt.Errorf("steps: %w", err))
		}
	}
	var bundle string
	if p.startCmd != "" {
		b, err := p.phaseTemplate()
		if err != nil {
			return fail(fmt.Errorf("template: %w", err))
		}
		bundle = b
	}

	// Finale: upload what was produced (the only place platform creds act).
	switch {
	case bundle != "":
		key, err := p.uploadSnapshot(bundle)
		if err != nil {
			return fail(fmt.Errorf("upload snapshot: %w", err))
		}
		res.SnapshotKey = key
	case p.imagePath != "":
		key, err := p.uploadImage()
		if err != nil {
			return fail(fmt.Errorf("upload image: %w", err))
		}
		res.ImageKey = key
	default:
		return fail(fmt.Errorf("nothing produced (no image, no snapshot)"))
	}
	res.StartCmd, res.ReadyCmd = p.startCmd, p.readyCmd
	return res
}

// resolveBase fixes the phase B/C base ref and inherits start/ready from a
// base template's snapshot.cfg metadata (e2b.start_cmd / e2b.ready_cmd).
func (p *buildPipeline) resolveBase() error {
	s := p.spec
	switch {
	case s.FromTemplate == "":
		return nil // base = the imported local image (set by phaseImport)
	case s.FromTemplateKind == "img":
		p.baseRef = "manifest://" + s.FromTemplate
		return nil
	default: // snp: the snapshot.cfg names the base image + overlay diff + start/ready
		out, err := p.hostCmdEnv(s.Env, p.spec.Paths.SandboxCtl,
			"info", "--json", "--manifest-config", s.Paths.ManifestConfig,
			"manifest://"+s.FromTemplate)
		if err != nil {
			return fmt.Errorf("read base template cfg: %w", err)
		}
		baseRef, overlayBase, meta, err := parseTemplateDisk(out)
		if err != nil {
			return fmt.Errorf("base template %s: %w", s.FromTemplate, err)
		}
		p.baseRef = baseRef
		p.overlayBase = overlayBase
		if p.startCmd == "" {
			p.startCmd = meta["e2b.start_cmd"]
		}
		if p.readyCmd == "" {
			p.readyCmd = meta["e2b.ready_cmd"]
		}
		return nil
	}
}

// parseTemplateDisk extracts a base template's disk layout from its
// `sandbox-ctl info --json` (the snapshot.cfg, §3.4): the erofs base image
// (boot.root.base_ref) AND the accumulated overlay (boot.root.overlay) — the
// read-only lower a fromTemplate cold-start MUST stack under its fresh writable
// overlay, or the template's filesystem is lost. The overlay's captured top
// (overlay.base) and its lower chain (overlay.base_from_refs) are folded into
// one multi-key manifest ref (top-first, the order restore layers them) and
// returned as overlayBase ("" when the template has no overlay). info --json
// re-emits restore.SnapshotCfg by Go field name, hence the BaseRef / Overlay /
// Base / BaseFromRefs JSON keys.
func parseTemplateDisk(infoJSON []byte) (baseRef, overlayBase string, meta map[string]string, err error) {
	var cfg struct {
		Metadata map[string]string `json:"Metadata"`
		Boot     struct {
			Root struct {
				BaseRef string `json:"BaseRef"`
				Overlay *struct {
					Base         string   `json:"Base"`
					BaseFromRefs []string `json:"BaseFromRefs"`
				} `json:"Overlay"`
			} `json:"Root"`
		} `json:"Boot"`
	}
	if err := json.Unmarshal(infoJSON, &cfg); err != nil {
		return "", "", nil, fmt.Errorf("parse template cfg: %w", err)
	}
	if cfg.Boot.Root.BaseRef == "" {
		return "", "", nil, fmt.Errorf("no base image ref (boot.root.base_ref)")
	}
	if ov := cfg.Boot.Root.Overlay; ov != nil {
		overlayBase, err = foldOverlayChain(ov.Base, ov.BaseFromRefs)
		if err != nil {
			return "", "", nil, err
		}
	}
	return cfg.Boot.Root.BaseRef, overlayBase, cfg.Metadata, nil
}

// foldOverlayChain combines a snapshot.cfg overlay's captured top (overlay.base)
// and its lower chain (overlay.base_from_refs) into one multi-key manifest ref
// for a cold-start boot.root.overlay.base. The runtime layers manifest://k1:k2
// top→bottom in list order (fetch.NewLayered), exactly the order snapshot.cfg
// records ([overlay.base] ++ base_from_refs) and that restore's reconstructDisk
// rebuilds — so this is a plain key concatenation, no reordering. Every layer
// must be a manifest:// ref (an uploaded template's all are); a layer that is
// itself multi-key is flattened in place.
func foldOverlayChain(top string, chain []string) (string, error) {
	var keys []string
	for _, ref := range append([]string{top}, chain...) {
		if ref == "" {
			continue
		}
		hexes, ok := strings.CutPrefix(ref, "manifest://")
		if !ok {
			return "", fmt.Errorf("overlay layer %q is not a manifest:// ref (a fromTemplate base must be uploaded)", ref)
		}
		keys = append(keys, strings.Split(hexes, ":")...)
	}
	if len(keys) == 0 {
		return "", nil
	}
	return "manifest://" + strings.Join(keys, ":"), nil
}
