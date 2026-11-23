package sandboxcfg

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestNewE2BEnvironmentPreservesImageDefaultsAndExplicitPATH(t *testing.T) {
	for _, mode := range []string{"off", "auto", "runtime", "provided"} {
		for _, explicit := range []string{"absent", "", "/tenant/bin:/custom/bin", "/image/special/bin:/usr/bin", systemdToolsPath + ":/image/special/bin:/usr/bin"} {
			t.Run(mode+"/"+explicit, func(t *testing.T) {
				image := map[string]string{"PATH": "/image/special/bin:/usr/bin", "IMAGE": "yes", "EMPTY": "", "WIN": "image"}
				request := map[string]string{"WIN": "request", "INSTANCE": "yes"}
				if explicit != "absent" {
					request["PATH"] = explicit
				}
				launch := newE2BLaunch(mode, request, false)
				if err := ProjectNewE2BEnvironment(&launch, image); err != nil {
					t.Fatal(err)
				}
				wantPath := explicit
				if explicit == "absent" {
					wantPath = envdDefaultPath(mode, image["PATH"])
				}
				plugin := launch.Plugin[0].Env
				if plugin["PATH"] != wantPath || plugin["IMAGE"] != "yes" || plugin["WIN"] != "request" || plugin["INSTANCE"] != "yes" {
					t.Fatalf("incomplete product env: %#v", plugin)
				}
				if _, exists := plugin["EMPTY"]; !exists || launch.PersistentSystemdPathDefault() != (explicit == "absent") || launch.Plugin[0].PersistentSystemdPathDefault() != (explicit == "absent") {
					t.Fatal("lost empty value or default PATH origin")
				}
				before := mergeStr(nil, plugin)
				if err := ProjectNewE2BEnvironment(&launch, image); err != nil || !reflect.DeepEqual(before, launch.Plugin[0].Env) {
					t.Fatal("projection replay changed environment", err)
				}
			})
		}
	}
}

func TestSavedEnvdPATHOnlyChangesWithKnownProvenance(t *testing.T) {
	const base = "/image/custom/bin:/usr/bin"
	for _, original := range []string{base, systemdToolsPath + ":" + base, "/plugin/private/bin"} {
		for _, provenance := range []bool{false, true} {
			for _, mode := range []string{"", "off", "auto", "runtime", "provided"} {
				for _, request := range []string{"inherit", "same-value", "empty"} {
					t.Run(fmt.Sprintf("%s/%t/%s/%s", original, provenance, mode, request), func(t *testing.T) {
						source := rendererPortableConfig()
						source.Launch.Exec, source.Launch.Args = "", nil
						source.Launch.Placeholder = true
						source.Launch.Systemd = "runtime"
						source.Launch.Env["PATH"] = base
						// A top-level marker alone never licenses rewriting a plugin.
						source.Launch.SystemdPathDefault = true
						source.Launch.Plugin = []rtconfig.PortablePluginConfig{{Exec: envdExec, Args: []string{"-port", "49983"}, Env: map[string]string{"PATH": original, "SOURCE": "yes"}, SystemdPathDefault: provenance}}
						p := artifactRendererParams(types.LaunchCold)
						p.Sandbox.ResumeSource = types.ResumeSource{}
						p.Template.Kind, p.Template.Profile = types.KindSbx, types.ProfileE2B
						if mode != "" {
							p.Spec = SandboxSpec{Launch: &rtconfig.LaunchConfig{Systemd: mode}, LaunchKeys: map[string]bool{"systemd": true}}
						}
						p.EnvVars = map[string]string{"INSTANCE": "yes"}
						if request == "same-value" {
							p.EnvVars["PATH"] = original
						} else if request == "empty" {
							p.EnvVars["PATH"] = ""
						}
						raw, err := p.BuildYAML()
						if err != nil {
							t.Fatal(err)
						}
						projected, err := ProjectSourceLaunch(raw, types.ProfileE2B, source)
						if err != nil {
							t.Fatal(err)
						}
						host, presence, err := rtconfig.LoadConfigBytesWithPresence(projected.Config)
						if err != nil {
							t.Fatal(err)
						}
						if err := rtconfig.ApplyConfigProvenance(projected.Config, host, projected.Provenance); err != nil {
							t.Fatal(err)
						}
						runtime, c0, err := rtconfig.ApplyFromRules(source, host, presence, rtconfig.ApplyFromOptions{})
						if err != nil {
							t.Fatal(err)
						}
						want := original
						wantDefault := provenance && request == "inherit"
						if request == "empty" {
							want = ""
						} else if wantDefault && mode != "" {
							want = envdDefaultPath(mode, original)
						}
						plugin := runtime.Launch.Plugin[0]
						if plugin.Env["PATH"] != want || plugin.Env["SOURCE"] != "yes" || plugin.Env["INSTANCE"] != "yes" || plugin.PersistentSystemdPathDefault() != wantDefault || c0.Launch.Plugin[0].SystemdPathDefault != wantDefault {
							t.Fatalf("saved envd changed incorrectly: %#v, source=%t, portable=%t; want PATH %q default=%t", plugin.Env, plugin.PersistentSystemdPathDefault(), c0.Launch.Plugin[0].SystemdPathDefault, want, wantDefault)
						}
						if source.Launch.Plugin[0].Env["PATH"] != original || source.Launch.Plugin[0].SystemdPathDefault != provenance {
							t.Fatal("mutated source")
						}
					})
				}
			}
		}
	}
}

func TestImageProjectionRejectsDifferentRootIdentity(t *testing.T) {
	p := baseParams(types.ProfileE2B)
	raw, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ProjectImageLaunch(raw, "manifest://different", map[string]string{"IMAGE": "wrong"}); err == nil {
		t.Fatal("accepted metadata from a different root image")
	}
}

func TestSavedEnvdWithoutEnvironmentCanChangeMode(t *testing.T) {
	source := rendererPortableConfig()
	source.Launch.Exec, source.Launch.Args, source.Launch.Env = "", nil, nil
	source.Launch.Placeholder = true
	source.Launch.Plugin = []rtconfig.PortablePluginConfig{{Exec: envdExec, Args: []string{"-port", "49983"}}}
	p := artifactRendererParams(types.LaunchCold)
	p.Sandbox.ResumeSource = types.ResumeSource{}
	p.Template.Kind, p.Template.Profile = types.KindSbx, types.ProfileE2B
	p.EnvVars = nil
	p.Spec = SandboxSpec{Launch: &rtconfig.LaunchConfig{Systemd: "runtime"}, LaunchKeys: map[string]bool{"systemd": true}}
	raw, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	projection, err := ProjectSourceLaunch(raw, types.ProfileE2B, source)
	if err != nil {
		t.Fatal(err)
	}
	host, err := rtconfig.LoadConfigBytes(projection.Config)
	if err != nil {
		t.Fatal(err)
	}
	if err := rtconfig.ApplyConfigProvenance(projection.Config, host, projection.Provenance); err != nil {
		t.Fatal(err)
	}
	plugin := host.Launch.Plugin[0]
	if plugin.Env["PATH"] != systemdToolsPath+":"+defaultExecPath || !plugin.PersistentSystemdPathDefault() {
		t.Fatalf("implicit plugin default was lost: %+v", plugin)
	}
	if source.Launch.Plugin[0].Env != nil {
		t.Fatal("mutated source's absent environment")
	}
}
