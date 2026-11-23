package sandboxcfg

import (
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestNewE2BEnvironmentPreservesImageDefaultsAndExplicitPATH(t *testing.T) {
	for _, mode := range []string{"off", "auto", "runtime", "provided"} {
		for _, explicit := range []string{"absent", "", "/tenant/bin:/custom/bin"} {
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
				if _, exists := plugin["EMPTY"]; !exists || launch.PersistentSystemdPathDefault() != (explicit == "absent") {
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
			for _, mode := range []string{"off", "runtime", "provided"} {
				source := rendererPortableConfig()
				source.Launch.Exec, source.Launch.Args = "", nil
				source.Launch.Placeholder = true
				source.Launch.Env["PATH"] = base
				source.Launch.SystemdPathDefault = provenance
				source.Launch.Plugin = []rtconfig.PluginConfig{{Exec: envdExec, Args: []string{"-port", "49983"}, Env: map[string]string{"PATH": original, "SOURCE": "yes"}}}
				p := artifactRendererParams(types.LaunchCold)
				p.Sandbox.ResumeSource = types.ResumeSource{}
				p.Template.Kind, p.Template.Profile = types.KindSbx, types.ProfileE2B
				p.Spec = SandboxSpec{Launch: &rtconfig.LaunchConfig{Systemd: mode}, LaunchKeys: map[string]bool{"systemd": true}}
				p.EnvVars = map[string]string{"INSTANCE": "yes"}
				raw, err := p.BuildYAML()
				if err != nil {
					t.Fatal(err)
				}
				raw, err = ProjectSourceLaunch(raw, types.ProfileE2B, source)
				if err != nil {
					t.Fatal(err)
				}
				host, _, err := rtconfig.LoadConfigBytesWithPresence(raw)
				if err != nil {
					t.Fatal(err)
				}
				want := original
				if provenance && (original == base || original == systemdToolsPath+":"+base) {
					want = envdDefaultPath(mode, base)
				}
				plugin := host.Launch.Plugin[0]
				if plugin.Env["PATH"] != want || plugin.Env["SOURCE"] != "yes" || plugin.Env["INSTANCE"] != "yes" {
					t.Fatalf("saved envd changed incorrectly: %#v want PATH %q", plugin.Env, want)
				}
				if source.Launch.Plugin[0].Env["PATH"] != original {
					t.Fatal("mutated source")
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
