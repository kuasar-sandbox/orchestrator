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
//   - <runner>  (sandbox-runner@.service): one microVM sandbox; also runs snapshot
//     builds. ExecStart=node-ctl run-sandbox (exec-replaces into sandbox-ctl
//     run), config pulled over the config-socket.
//   - <builder> (sandbox-builder@.service): one template build. ExecStart=node-ctl
//     run-builder, which pulls the BuildSpec (MANIFEST_KEY + tenant registry creds in
//     env) over the config-socket and drives the three-phase pipeline itself
//     (import/steps/template sandboxes as direct children; result JSON on stdout).
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
Description=kuasar sandbox %%i
# node-ctl prepares %s/%%i and %s/%%i + writes %s/%%i/%%i.yaml before start.

[Service]
Type=exec
WorkingDirectory=%s/%%i
# run-sandbox locks+writes the pidfile, pulls the launch spec (exec=sandbox-ctl, the
# secret MANIFEST_KEY in env) over the config-socket, then exec-replaces into
# sandbox-ctl so it inherits this PID + the unit cgroup (sandbox-ctl --cgroup-adopt).
ExecStart=%s run-sandbox --pidfile=%s/%%i/%%i.pid --config-socket=%s --sandbox-id=%%i
# One process per sandbox, stateful: a crash means the sandbox is gone, not retryable.
Restart=no
KillMode=control-group
TimeoutStopSec=20
Slice=sandbox-runner.slice
# Delegate the cgroup controllers to the unit so sandbox-ctl --cgroup-adopt can
# write the sandbox's resource limits (cpu.max/memory.max) into this cgroup.
Delegate=yes
# --cgroup-adopt (in the launch spec) makes THIS unit's cgroup the sandbox resource
# cgroup: sandbox-ctl + cloud-hypervisor share it, sentinel manages it in place, and
# KillMode=control-group SIGKILLs the whole group on StopUnit.
`, o.cfg.Paths.RunRoot, o.cfg.Paths.BaseRoot, o.cfg.Paths.RunRoot, o.cfg.Paths.RunRoot,
		o.cfg.OrchestratorCtl(), o.cfg.Paths.RunRoot, o.cfg.Paths.ConfigSocket)
}

func (o *Orchestrator) builderUnitFile() string {
	return fmt.Sprintf(`[Unit]
Description=kuasar image build %%i

[Service]
Type=oneshot
WorkingDirectory=%s/%%i
# run-builder's stdout (the result JSON) is captured here for the orchestrator.
# StandardError=journal keeps pipeline progress/errors OUT of the result file
# (StandardError defaults to inherit, which would mirror stdout into the file).
StandardOutput=file:%s/%%i/%%i.result
StandardError=journal
# Builds are few and we want the FULL detail in the journal (every phase
# sandbox console line + flatten/RUN progress); disable journald rate limiting
# for this unit so no line is dropped. (Runner units keep the default limit —
# thousands of sandboxes must not flood the journal.)
LogRateLimitIntervalSec=0
# run-builder pulls the BuildSpec (secrets in env, never on disk) over the
# config-socket and drives the three-phase pipeline itself — its phase
# sandboxes (sandbox-ctl run + cloud-hypervisor) are direct children, so the
# whole build accounts to this unit's cgroup under sandbox-builder.slice.
ExecStart=%s run-builder --pidfile=%s/%%i/%%i.pid --config-socket=%s --build-id=%%i
TimeoutStartSec=%d
KillMode=control-group
Slice=sandbox-builder.slice
`, o.cfg.Paths.RunRoot, o.cfg.Paths.RunRoot, o.cfg.OrchestratorCtl(), o.cfg.Paths.RunRoot, o.cfg.Paths.ConfigSocket, o.cfg.Builder.TotalTimeoutSec+60)
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

func (o *Orchestrator) runnerUnit(sid string) string  { return instanceUnit(o.cfg.Units.Runner, sid) }
func (o *Orchestrator) builderUnit(bid string) string { return instanceUnit(o.cfg.Units.Builder, bid) }

// runnerPattern is the ListUnitsByPatterns glob for live runner instances.
func (o *Orchestrator) runnerPattern() string {
	return strings.TrimSuffix(o.cfg.Units.Runner, ".service") + "*.service"
}

// unitToSID extracts the sandbox id from a runner instance unit name.
func (o *Orchestrator) unitToSID(name string) string {
	name = strings.TrimPrefix(name, strings.TrimSuffix(o.cfg.Units.Runner, ".service"))
	return strings.TrimSuffix(name, ".service")
}
