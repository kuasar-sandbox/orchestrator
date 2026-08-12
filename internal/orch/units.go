package orch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// InstallUnits generates and installs the systemd template units (+ slices)
// node-ctl drives, then daemon-reloads if anything changed. It is a
// no-op when install_units=false (operator manages units out of band).
//
//   - <runner>  (sandbox-runner@.service): one run-id unit that waits for a sandbox
//     assignment, then exec-replaces into sandbox-ctl run with config pulled over
//     the config-socket.
//   - <builder> (sandbox-builder@.service): one run-id unit that waits for a build
//     assignment, pulls the BuildSpec (MANIFEST_KEY + tenant registry creds in env)
//     over the config-socket, drives the three-phase pipeline itself, and posts the
//     result back to the socket.
//
// Both run in their own cgroup (KillMode=control-group / a dedicated slice) so the
// reaper and the builder resource pool can account and reclaim them.
func (o *Orchestrator) InstallUnits(ctx context.Context) error {
	if o.cfg.Units.Install != nil && !*o.cfg.Units.Install {
		return nil
	}
	files := map[string]string{
		o.cfg.Units.Runner:      o.runnerUnitFile(),
		o.cfg.Units.Builder:     o.builderUnitFile(),
		"sandbox-runner.slice":  sliceFile("kuasar sandbox runners", ""),
		"sandbox-builder.slice": sliceFile("kuasar image builders", o.builderSliceCaps()),
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
		return o.lc.Reload(ctx)
	}
	return nil
}

func (o *Orchestrator) runnerUnitFile() string {
	return fmt.Sprintf(`[Unit]
Description=kuasar sandbox runner %%i
# %%i is a run-id, not a sandbox id. node-ctl run-sandbox waits on the config
# socket until this run-id is assigned a sandbox id, then fetches the sandbox's
# LaunchSpec and exec-replaces into sandbox-ctl.

[Service]
Type=exec
WorkingDirectory=%s
ExecStart=%s run-sandbox --pidfile=%s/runs/%%i.pid --config-socket=%s --run-id=%%i
ExecStopPost=/bin/rm -f %s/runs/%%i.pid
# One process per sandbox, stateful: a crash means the sandbox is gone, not retryable.
Restart=no
KillMode=control-group
TimeoutStopSec=20
Slice=sandbox-runner.slice
# node-ctl moves itself into ctl/ before enabling cgroup-v2 domain controllers;
# after exec, sandbox-ctl stays there while node-ctl creates vmm/ for CH.
Delegate=yes
# KillMode=control-group recursively covers both delegated subgroups.
`, o.cfg.Paths.RunRoot, o.cfg.OrchestratorCtl(), o.cfg.Paths.RunRoot, o.cfg.Paths.ConfigSocket, o.cfg.Paths.RunRoot)
}

func (o *Orchestrator) builderUnitFile() string {
	return fmt.Sprintf(`[Unit]
Description=kuasar image build runner %%i
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
# over the config-socket, drives the three-phase pipeline itself, and posts the
# result back to the socket. Its phase sandboxes (sandbox-ctl run +
# cloud-hypervisor) are direct children, so the whole build accounts to this
# unit's cgroup under sandbox-builder.slice.
ExecStart=%s run-builder --pidfile=%s/runs/%%i.pid --config-socket=%s --run-id=%%i
ExecStopPost=/bin/rm -f %s/runs/%%i.pid
KillMode=control-group
Slice=sandbox-builder.slice
`, o.cfg.Paths.RunRoot, o.cfg.OrchestratorCtl(), o.cfg.Paths.RunRoot, o.cfg.Paths.ConfigSocket, o.cfg.Paths.RunRoot)
}

func sliceFile(desc, caps string) string {
	return fmt.Sprintf("[Unit]\nDescription=%s\nBefore=slices.target\n\n[Slice]\n%s", desc, caps)
}

// builderSliceCaps renders the cgroup ceiling for the builder pool from config.
func (o *Orchestrator) builderSliceCaps() string {
	var b strings.Builder
	if o.cfg.Builder.CPUQuota != "" {
		fmt.Fprintf(&b, "CPUQuota=%s\n", o.cfg.Builder.CPUQuota)
	}
	if o.cfg.Builder.MemoryMax != "" {
		fmt.Fprintf(&b, "MemoryMax=%s\n", o.cfg.Builder.MemoryMax)
	}
	return b.String()
}

// --- unit-name helpers (honor the configurable template names) ---

// instanceUnit turns a template unit ("name@.service") into an instance unit
// ("name@<id>.service").
func instanceUnit(tmpl, id string) string {
	return strings.TrimSuffix(tmpl, ".service") + id + ".service"
}

func (o *Orchestrator) runnerUnit(runID string) string {
	return instanceUnit(o.cfg.Units.Runner, runID)
}
func (o *Orchestrator) builderUnit(runID string) string {
	return instanceUnit(o.cfg.Units.Builder, runID)
}

// runnerPattern is the ListUnitsByPatterns glob for live runner instances.
func (o *Orchestrator) runnerPattern() string {
	return strings.TrimSuffix(o.cfg.Units.Runner, ".service") + "*.service"
}

// unitToRunID extracts the run-id from a template instance unit name.
func (o *Orchestrator) unitToRunID(name string) string {
	name = strings.TrimPrefix(name, strings.TrimSuffix(o.cfg.Units.Runner, ".service"))
	return strings.TrimSuffix(name, ".service")
}
