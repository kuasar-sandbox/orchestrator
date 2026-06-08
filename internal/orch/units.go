package orch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// InstallUnits generates and installs the systemd template units (+ slices)
// orchestrator-ctl drives, then daemon-reloads if anything changed. It is a
// no-op when install_units=false (operator manages units out of band).
//
//   - <runner>  (sandbox-runner@.service): one microVM sandbox; also runs snapshot
//     builds. ExecStart=sandbox-ctl run, config pulled over the config-socket.
//   - <builder> (sandbox-builder@.service): one image build. ExecStart=orchestrator-ctl
//     run-task, which pulls the build LaunchSpec (exec=flatten-ctl, MANIFEST_KEY in
//     env) over the config-socket and exec-replaces into flatten-ctl.
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
# orchestrator-ctl prepares %s/%%i and %s/%%i + writes %s/%%i/%%i.yaml before start.

[Service]
Type=exec
WorkingDirectory=%s/%%i
# run-task locks+writes the pidfile, pulls the launch spec (exec=sandbox-ctl, the
# secret MANIFEST_KEY in env) over the config-socket, then exec-replaces into
# sandbox-ctl so it inherits this PID + the unit cgroup (sandbox-ctl --cgroup-adopt).
ExecStart=%s run-task --pidfile=%s/%%i/%%i.pid --config-socket=%s --config-id=sandbox:%%i
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
# flatten-ctl's stdout (the 64-hex manifest key) is captured here for the orchestrator.
# StandardError=journal keeps flatten-ctl's progress/errors OUT of the result file
# (StandardError defaults to inherit, which would mirror stdout into the file).
StandardOutput=file:%s/%%i/%%i.result
StandardError=journal
# run-task pulls the build launch spec (exec=flatten-ctl, MANIFEST_KEY in env) over
# the config-socket and exec-replaces into flatten-ctl (no on-disk secret). Runs in
# this unit's cgroup under sandbox-builder.slice (pool accounting).
ExecStart=%s run-task --pidfile=%s/%%i/%%i.pid --config-socket=%s --config-id=build:%%i
TimeoutStartSec=1800
KillMode=control-group
Slice=sandbox-builder.slice
`, o.cfg.Paths.RunRoot, o.cfg.Paths.RunRoot, o.cfg.OrchestratorCtl(), o.cfg.Paths.RunRoot, o.cfg.Paths.ConfigSocket)
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
