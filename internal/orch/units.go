package orch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
)

// InstallUnits generates and installs the systemd template units (+ slices)
// node-ctl drives, then daemon-reloads if anything changed. When
// units.install=false, the operator manages unit files out of band and this
// method performs no file or systemd operations.
//
//   - <runner>  (sandbox-runner@.service): one run-id unit that waits for a sandbox
//     assignment, then exec-replaces into sandbox-ctl run with config pulled over
//     the config-socket.
//   - <builder> (sandbox-builder@.service): one run-id unit that waits for a build
//     assignment, authenticates an exact-run bootstrap, prepares an artifact root
//     task-locally when required, then drives the target-selected pipeline and posts
//     the result back to the socket.
//
// Both run in their own cgroup (KillMode=control-group / a dedicated slice) so the
// reaper and the builder resource pool can account and reclaim them.
func (o *Orchestrator) InstallUnits(ctx context.Context) error {
	if o.cfg.Units.Install != nil && !*o.cfg.Units.Install {
		return nil
	}
	files := map[string]string{
		"sandbox-runner.slice":  sliceFile("kuasar sandbox runners"),
		"sandbox-builder.slice": sliceFile("kuasar image builders"),
	}
	for _, kind := range []struct {
		pools   []config.RunPoolConfig
		content string
	}{
		{o.cfg.Units.RunnerPoolConfigs(), o.runnerUnitFile()},
		{o.cfg.Units.BuilderPoolConfigs(), o.builderUnitFile()},
	} {
		for _, pool := range kind.pools {
			if old, exists := files[pool.Unit]; exists && old != kind.content {
				return fmt.Errorf("orch: conflicting generated content for unit %s", pool.Unit)
			}
			files[pool.Unit] = kind.content
		}
	}
	changed := false
	for name, content := range files {
		path := filepath.Join(o.cfg.Units.Dir, name)
		if old, err := os.ReadFile(path); err == nil && string(old) == content {
			continue
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return fmt.Errorf("orch: install unit %s: %w", name, err)
		}
		changed = true
		o.log.Info("installed unit", "path", path)
	}
	if changed {
		if err := o.lc.Reload(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (o *Orchestrator) runnerUnitFile() string {
	return fmt.Sprintf(`[Unit]
Description=kuasar sandbox runner %%i
CollectMode=inactive-or-failed
# %%i is a run-id, not a sandbox id. node-ctl run-sandbox waits on the config
# socket until this run-id is assigned a sandbox id, then fetches the sandbox's
# LaunchSpec and exec-replaces into sandbox-ctl.

[Service]
Type=exec
WorkingDirectory=%s
ExecStart=%s run-sandbox --pidfile=%s --config-socket=%s --run-id=%%i
ExecStopPost=/bin/rm -f %s
# One process per sandbox, stateful: a crash means the sandbox is gone, not retryable.
Restart=no
KillMode=control-group
TimeoutStopSec=20
Slice=sandbox-runner.slice
# node-ctl moves itself into ctl/ before enabling cgroup-v2 domain controllers;
# after exec, sandbox-ctl stays there while node-ctl creates vmm/ for CH.
Delegate=yes
# KillMode=control-group recursively covers both delegated subgroups.
`, o.cfg.Paths.RunRoot, o.executables.OrchestratorCtl(), nodepath.RunnerPID(o.cfg.Paths.RunRoot, "%i"), o.cfg.Paths.ConfigSocket, nodepath.RunnerPID(o.cfg.Paths.RunRoot, "%i"))
}

func (o *Orchestrator) builderUnitFile() string {
	return fmt.Sprintf(`[Unit]
Description=kuasar image build runner %%i
CollectMode=inactive-or-failed
# %%i is a run-id, not a build id. node-ctl run-builder waits on the config
# socket until this run-id is assigned a build id, then fetches the BuildSpec,
# runs one build, posts the result back over the socket, and exits.

[Service]
Type=exec
WorkingDirectory=%s
StandardError=journal
# Builds are few and we want the FULL detail in the journal (every phase
# sandbox console line + flatten/RUN progress); disable journald rate limiting
# for this unit so no line is dropped. (Runner units keep the default limit —
# thousands of sandboxes must not flood the journal.)
LogRateLimitIntervalSec=0
# run-builder pulls its assignment and BuildSpec (secrets in env, never on disk)
# over the config-socket, drives the target-selected build pipeline itself, and posts the
# result back to the socket. Its phase sandboxes (sandbox-ctl run +
# cloud-hypervisor) are direct children, so the whole build accounts to this
# unit's cgroup under sandbox-builder.slice.
ExecStart=%s run-builder --pidfile=%s --config-socket=%s --run-id=%%i
ExecStopPost=/bin/rm -f %s
KillMode=control-group
Slice=sandbox-builder.slice
# run-builder moves itself to ctl/ and hands the sibling vmm/ cgroup to each
# strictly serial phase sandbox by inherited descriptor.
Delegate=yes
`, o.cfg.Paths.RunRoot, o.executables.OrchestratorCtl(), nodepath.RunnerPID(o.cfg.Paths.RunRoot, "%i"), o.cfg.Paths.ConfigSocket, nodepath.RunnerPID(o.cfg.Paths.RunRoot, "%i"))
}

func sliceFile(desc string) string {
	return fmt.Sprintf("[Unit]\nDescription=%s\nBefore=slices.target\n\n[Slice]\n", desc)
}

// --- unit-name helpers (honor the configurable template names) ---

// instanceUnit turns a template unit ("name@.service") into an instance unit
// ("name@<id>.service").
func instanceUnit(tmpl, id string) string {
	return strings.TrimSuffix(tmpl, ".service") + id + ".service"
}

// The index contains actual names, never names guessed from a default template.
func (o *Orchestrator) runnerUnit(runID string) string  { return o.runs.unit(runID) }
func (o *Orchestrator) builderUnit(runID string) string { return o.runs.unit(runID) }

func (o *Orchestrator) poolConfigs(kind string) []config.RunPoolConfig {
	if kind == runKindBuild {
		return o.cfg.Units.BuilderPoolConfigs()
	}
	return o.cfg.Units.RunnerPoolConfigs()
}

func (o *Orchestrator) unitToRunID(name string) string {
	return o.runIDFromUnit(runKindSandbox, name)
}
func (o *Orchestrator) builderUnitToRunID(name string) string {
	return o.runIDFromUnit(runKindBuild, name)
}
func (o *Orchestrator) runIDFromUnit(kind, name string) string {
	for _, pool := range o.poolConfigs(kind) {
		if id := unitToRunIDFromTemplate(pool.Unit, name); id != "" {
			return id
		}
	}
	return ""
}

// listRunUnits deduplicates templates and instances, not pools. Publish recovered
// associations only during recovery or exact-run lookup. No historical pool slot
// can be inferred from a shared template.
func (o *Orchestrator) listRunUnits(ctx context.Context, kind string) ([]launcher.Unit, error) {
	templates := make(map[string]bool)
	instances := make(map[string]bool)
	owners := make(map[string]string)
	var out []launcher.Unit
	for _, pool := range o.poolConfigs(kind) {
		if templates[pool.Unit] {
			continue
		}
		templates[pool.Unit] = true
		units, err := o.lc.List(ctx, strings.TrimSuffix(pool.Unit, ".service")+"*.service")
		if err != nil {
			return nil, err
		}
		for _, unit := range units {
			id := unitToRunIDFromTemplate(pool.Unit, unit.Name)
			if id == "" || instances[unit.Name] {
				continue
			}
			if previous, exists := owners[id]; exists && previous != unit.Name {
				return nil, fmt.Errorf("orch: run %s has conflicting units %s and %s", id, previous, unit.Name)
			}
			instances[unit.Name], owners[id] = true, unit.Name
			out = append(out, unit)
		}
	}
	return out, nil
}

// Missing process-local state must be resolved by authoritative enumeration,
// including cleanup paths invoked before the main startup scan. Only a successful
// complete scan can prove that systemd has already collected an instance.
func (o *Orchestrator) resolveRunUnit(ctx context.Context, kind, runID string) (string, error) {
	if runID == "" {
		return "", nil
	}
	if unit := o.runs.unit(runID); unit != "" {
		return unit, nil
	}
	units, err := o.listRunUnits(ctx, kind)
	if err != nil {
		return "", fmt.Errorf("orch: locate %s run %s: %w", kind, runID, err)
	}
	for _, unit := range units {
		if o.runIDFromUnit(kind, unit.Name) == runID {
			o.runs.restore(runID, unit.Name)
			return unit.Name, nil
		}
	}
	return "", nil
}

func unitToRunIDFromTemplate(template, name string) string {
	prefix := strings.TrimSuffix(template, ".service")
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".service") {
		return ""
	}
	return strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".service")
}
