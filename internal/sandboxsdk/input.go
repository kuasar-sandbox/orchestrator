// Package sandboxsdk adapts only the LaunchSpec emitted by sandboxFinalLaunchSpec.
// It is not a sandbox-ctl command-line frontend.
package sandboxsdk

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/stdio"
)

type Input struct {
	Exec, Workdir                                                       string
	SandboxID, PathID, Config, ManifestConfig, RunRoot, BaseRoot, LogTo string
	Stdio                                                               stdio.Mode
	Forwards                                                            []sandbox.ForwardSpec
	Locations                                                           config.RefLocations
	Source                                                              types.ResumeSource
	Env                                                                 map[string]string
}

// Parse keeps task-owned sources and the node-owned cgroup outside LaunchSpec.
func Parse(spec configsock.LaunchSpec, source types.ResumeSource, locations map[string]string, env map[string]string, sandboxID string) (*Input, error) {
	if filepath.Base(spec.Exec) != "sandbox-ctl" || !filepath.IsAbs(spec.Exec) {
		return nil, fmt.Errorf("unexpected sandbox executable")
	}
	if len(spec.Args) == 0 || spec.Args[0] != "run" {
		return nil, fmt.Errorf("sandbox launch must invoke run")
	}
	in := &Input{Exec: spec.Exec, Workdir: spec.Workdir, Source: source, Locations: config.RefLocations{}, Env: map[string]string{}}
	for k, v := range env {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) {
			return nil, fmt.Errorf("invalid sandbox environment key")
		}
		in.Env[k] = v
	}
	if _, ok := in.Env["MANIFEST_KEY"]; !ok {
		return nil, fmt.Errorf("missing authoritative manifest key")
	}
	values := map[string]string{}
	allowed := map[string]bool{"--sandbox-id": true, "--path-id": true, "--config": true, "--manifest-config": true, "--run-root": true, "--base-root": true, "--log-to": true, "--stdout-to": true, "--stderr-to": true, "--console": true}
	for i := 1; i < len(spec.Args); i += 2 {
		flag := spec.Args[i]
		if i+1 >= len(spec.Args) {
			return nil, fmt.Errorf("missing value for sandbox launch argument %q", flag)
		}
		value := spec.Args[i+1]
		if strings.ContainsRune(value, 0) {
			return nil, fmt.Errorf("invalid sandbox launch value")
		}
		switch flag {
		case "--connect":
			f, err := sandbox.ParseForwardSpec(value)
			if err != nil {
				return nil, err
			}
			if f.ListenFD != 0 {
				return nil, fmt.Errorf("sandbox launch must not borrow a forwarding descriptor")
			}
			in.Forwards = append(in.Forwards, f)
		case "--ref-location":
			if err := in.Locations.Set(value); err != nil {
				return nil, err
			}
		default:
			if !allowed[flag] {
				return nil, fmt.Errorf("unexpected sandbox launch argument %q", flag)
			}
			if _, ok := values[flag]; ok {
				return nil, fmt.Errorf("duplicate sandbox launch argument %q", flag)
			}
			values[flag] = value
		}
	}
	for flag := range allowed {
		if _, ok := values[flag]; !ok {
			return nil, fmt.Errorf("missing sandbox launch argument %q", flag)
		}
	}
	in.SandboxID = values["--sandbox-id"]
	in.PathID = values["--path-id"]
	if in.SandboxID != sandboxID || in.PathID != sandboxID {
		return nil, fmt.Errorf("sandbox launch identity mismatch")
	}
	if _, err := sandbox.ResolvePathID(in.SandboxID, in.PathID); err != nil {
		return nil, err
	}
	in.Config = values["--config"]
	in.ManifestConfig = values["--manifest-config"]
	in.RunRoot = values["--run-root"]
	in.BaseRoot = values["--base-root"]
	in.LogTo = values["--log-to"]
	for _, p := range []string{in.Workdir, in.RunRoot, in.BaseRoot} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return nil, fmt.Errorf("sandbox launch requires clean absolute directories")
		}
	}
	st, err := os.Stat(in.Workdir)
	if err != nil {
		return nil, fmt.Errorf("sandbox workdir: %w", err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("sandbox workdir is not a directory")
	}
	if in.Config == "" {
		return nil, fmt.Errorf("sandbox config is required")
	}
	for name, uri := range locations {
		if err := in.Locations.Set(name + "=" + uri); err != nil {
			return nil, err
		}
	}
	if !source.Empty() {
		if !source.Valid() {
			return nil, fmt.Errorf("invalid prepared sandbox source")
		}
	}
	in.Stdio, err = stdio.FromFlags(nil, nil, nil, "", values["--stdout-to"], values["--stderr-to"], nil, values["--console"])
	if err != nil {
		return nil, err
	}
	// Resolve host paths against the task workdir without changing process cwd.
	in.Stdio.Stdout.Path = in.path(in.Stdio.Stdout.Path)
	in.Stdio.Stderr.Path = in.path(in.Stdio.Stderr.Path)
	in.Stdio.Console.Path = in.path(in.Stdio.Console.Path)
	for i := range in.Forwards {
		if !strings.HasPrefix(in.Forwards[i].UDSPath, "@") {
			in.Forwards[i].UDSPath = in.path(in.Forwards[i].UDSPath)
		}
	}
	return in, nil
}

func (in *Input) path(p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(in.Workdir, p)
}
