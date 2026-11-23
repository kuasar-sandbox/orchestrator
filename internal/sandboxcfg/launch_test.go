package sandboxcfg

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

func TestSystemdLaunchRequestPermissionsSurviveJSON(t *testing.T) {
	for _, raw := range []string{
		`{"systemd":"auto","exec":""}`, `{"systemd":"auto","args":[]}`,
		`{"systemd":"auto","plugin":[]}`, `{"systemd":"auto","cgroup_control":false}`,
		`{"systemd":"auto","unknown":null}`, `{"placeholder":false}`,
	} {
		t.Run(raw, func(t *testing.T) {
			spec, err := ParseSpec(map[string]string{NsLaunch: raw})
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(spec)
			if err != nil {
				t.Fatal(err)
			}
			var roundTrip SandboxSpec
			if err := json.Unmarshal(encoded, &roundTrip); err != nil {
				t.Fatal(err)
			}
			for _, candidate := range []SandboxSpec{spec, roundTrip} {
				if err := ValidateLaunchForProfile(types.ProfileE2B, candidate); err == nil {
					t.Fatalf("e2b accepted product-owned keys: %s", encoded)
				}
			}
		})
	}
	for _, mode := range []string{"off", "auto", "runtime", "provided"} {
		spec, err := ParseSpec(map[string]string{NsLaunch: `{"systemd":"` + mode + `"}`})
		if err != nil {
			t.Fatal(err)
		}
		for _, profile := range []types.Profile{types.ProfileBare, types.ProfileE2B} {
			if err := ValidateLaunchForProfile(profile, spec); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, value := range []string{`null`, `true`, `{}`, `[]`, `""`, `"disabled"`, `"AUTO"`, `1`} {
		if _, err := ParseSpec(map[string]string{NsLaunch: `{"systemd":` + value + `}`}); err == nil {
			t.Errorf("accepted invalid mode %s", value)
		}
	}
	for _, raw := range []string{`null`, `[]`, `"auto"`, `{"systemd":"auto","systemd":"off"}`} {
		if _, err := ParseSpec(map[string]string{NsLaunch: raw}); err == nil {
			t.Errorf("accepted invalid launch object %s", raw)
		}
	}
	missing, err := ParseSpec(nil)
	if err != nil || missing.HasSystemdOverride() || missing.Launch != nil {
		t.Fatalf("missing mode: %+v, %v", missing, err)
	}
}

func TestNewE2BLaunchUsesPlaceholderAndOrdinaryEnvdPlugin(t *testing.T) {
	for _, mode := range []string{"", "off", "auto", "runtime", "provided"} {
		for _, mmds := range []bool{false, true} {
			p := baseParams(types.ProfileE2B)
			p.MMDSEnabled, p.EnvVars = mmds, map[string]string{"PRODUCT": "value"}
			if mode != "" {
				p.Spec.Launch = &rtconfig.LaunchConfig{Systemd: mode}
			}
			cfg, err := p.buildImageColdConfig()
			if err != nil {
				t.Fatal(err)
			}
			launch := cfg.Launch
			if !launch.Placeholder || launch.Exec != "" || !launch.CgroupControl || launch.PIDNamespace != "shared" || len(launch.Plugin) != 1 {
				t.Fatalf("new e2b topology: %+v", launch)
			}
			plugin := launch.Plugin[0]
			if plugin.Exec != envdExec || plugin.User != "0:0" || plugin.Restart != "always" || plugin.Env["PRODUCT"] != "value" {
				t.Fatalf("envd supervision: %+v", plugin)
			}
			args := strings.Join(plugin.Args, " ")
			if !strings.HasSuffix(args, "-cgroup-root "+envdCgroupRoot(mode)) || strings.Contains(args, "-isnotfc") == mmds {
				t.Fatalf("mode %q, MMDS %t, args %q", mode, mmds, args)
			}
			if launch.Systemd == "" {
				t.Fatal("complete definition omitted mode")
			}
		}
	}
}

func TestPreparedSourceProjectionPreservesTopologyAndPresence(t *testing.T) {
	for _, layout := range []string{"primary", "plugin", "unknown"} {
		for _, mode := range []string{"", "off", "auto", "runtime", "provided"} {
			t.Run(layout+"/"+mode, func(t *testing.T) {
				source := rendererPortableConfig()
				source.Launch.Systemd = "runtime"
				source.Launch.CgroupControl = false // raw legacy choice must survive
				source.Launch.PIDNamespace = "shared"
				source.Launch.User = "0:0"
				source.Launch.Args = []string{"-port", "49983", "-cgroup-root", systemdCgroupRoot}
				source.Launch.Exec = envdExec
				if layout == "plugin" {
					source.Launch.Exec, source.Launch.Args, source.Launch.Placeholder = "", nil, true
					source.Launch.Plugin = []rtconfig.PortablePluginConfig{{Exec: envdExec, Args: []string{"-port", "49983", "-cgroup-root=" + systemdCgroupRoot}, User: "0:0", Restart: "always"}, {Exec: "/other", Args: []string{"unchanged"}}}
				} else if layout == "unknown" {
					source.Launch.Exec = "/custom/product"
				}
				before, _ := json.Marshal(source)
				p := artifactRendererParams(types.LaunchCold)
				p.Sandbox.ResumeSource = types.ResumeSource{}
				p.Template.Kind, p.Template.Profile = types.KindSbx, types.ProfileE2B
				p.EnvVars = map[string]string{"INSTANCE": "yes"}
				if mode != "" {
					var err error
					p.Spec, err = ParseSpec(map[string]string{NsLaunch: `{"systemd":"` + mode + `"}`})
					if err != nil {
						t.Fatal(err)
					}
				}
				raw, err := p.BuildYAML()
				if err != nil {
					t.Fatal(err)
				}
				if mode == "" && strings.Contains(string(raw), "systemd:") {
					t.Fatalf("overlay eagerly defaulted mode:\n%s", raw)
				}
				projected, err := ProjectSourceLaunch(raw, types.ProfileE2B, source)
				if layout == "unknown" && mode != "" {
					if err == nil || !strings.Contains(err.Error(), "new cold definition") {
						t.Fatalf("unknown layout: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				host, presence, err := rtconfig.LoadConfigBytesWithPresence(projected.Config)
				if err != nil {
					t.Fatal(err)
				}
				if !presence.Has("launch.systemd") || presence.Has("boot.root.base") {
					t.Fatalf("projection lost host-only ownership:\n%s", projected.Config)
				}
				launch := host.Launch
				if launch.Exec != source.Launch.Exec || launch.Placeholder != source.Launch.Placeholder || launch.CgroupControl || launch.PIDNamespace != "shared" || launch.Env["ARTIFACT"] != "yes" || launch.Env["INSTANCE"] != "yes" {
					t.Fatalf("source was regenerated: %+v", launch)
				}
				wantMode := mode
				if wantMode == "" {
					wantMode = "runtime"
				}
				if launch.Systemd != wantMode {
					t.Fatalf("effective mode %q, want %q", launch.Systemd, wantMode)
				}
				if mode == "" {
					gotArgs, _ := yaml.Marshal(launch.Args)
					wantArgs, _ := yaml.Marshal(source.Launch.Args)
					gotPlugins, _ := yaml.Marshal(launch.Plugin)
					expectedPlugins := append([]rtconfig.PortablePluginConfig(nil), source.Launch.Plugin...)
					if layout == "plugin" {
						expectedPlugins[0].Env = mergeStr(expectedPlugins[0].Env, p.EnvVars)
					}
					wantPlugins, _ := yaml.Marshal(expectedPlugins)
					if string(gotArgs) != string(wantArgs) || string(gotPlugins) != string(wantPlugins) {
						t.Fatal("no-override create rewrote layout")
					}
				} else if layout == "plugin" {
					if !strings.HasSuffix(launch.Plugin[0].Args[2], "="+envdCgroupRoot(mode)) || launch.Plugin[1].Exec != source.Launch.Plugin[1].Exec || !reflect.DeepEqual(launch.Plugin[1].Args, source.Launch.Plugin[1].Args) {
						t.Fatalf("plugin rewrite: %+v", launch.Plugin)
					}
				} else if launch.Args[3] != envdCgroupRoot(mode) {
					t.Fatalf("primary rewrite: %v", launch.Args)
				}
				after, _ := json.Marshal(source)
				if string(before) != string(after) {
					t.Fatal("projection mutated prepared source")
				}
			})
		}
	}
}

func TestBuildSourceModePrecedesNewE2BProjection(t *testing.T) {
	for _, mode := range []string{"", "off", "provided"} {
		base := baseParams(types.ProfileE2B)
		source := rendererPortableConfig()
		source.Launch.Systemd = "runtime"
		p := BuildColdParams{Profile: types.ProfileE2B, ImageRef: base.Template.Ref, Kernel: base.Kernel,
			Runtime: base.Runtime, Source: source, Resources: base.Resources, EnvVars: map[string]string{"INSTANCE": "yes"}}
		if mode != "" {
			p.Spec.Launch = &rtconfig.LaunchConfig{Systemd: mode}
		}
		cfg, err := p.BuildColdConfig()
		if err != nil {
			t.Fatal(err)
		}
		want := mode
		if want == "" {
			want = "runtime"
		}
		if cfg.Launch.Systemd != want || !cfg.Launch.Placeholder || len(cfg.Launch.Plugin) != 1 || cfg.Launch.Env["ARTIFACT"] != "yes" || cfg.Launch.Plugin[0].Env["INSTANCE"] != "yes" {
			t.Fatalf("build source mode %q: %+v", mode, cfg.Launch)
		}
		args := cfg.Launch.Plugin[0].Args
		if args[len(args)-1] != envdCgroupRoot(want) {
			t.Fatalf("projection preceded merge: %v", args)
		}
		raw, err := MarshalBuildColdConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var output rtconfig.SandboxConfig
		if err := yaml.Unmarshal(raw, &output); err != nil {
			t.Fatal(err)
		}
		wantLaunch, _ := yaml.Marshal(completeLaunchMap(cfg.Launch))
		gotLaunch, _ := yaml.Marshal(completeLaunchMap(output.Launch))
		if string(gotLaunch) != string(wantLaunch) {
			t.Fatalf("E and C0 projection differ:\n%s", raw)
		}
	}
}

func TestSnapshotSystemdOverrideAndPausedSourceAuthority(t *testing.T) {
	for _, mode := range []string{"off", "auto", "runtime", "provided"} {
		p := artifactRendererParams(types.LaunchMemory)
		p.Spec.Launch = &rtconfig.LaunchConfig{Systemd: mode}
		if _, err := p.BuildYAML(); err != nil {
			t.Fatalf("durable source reapplied historical create metadata: %v", err)
		}
		p.Sandbox.ResumeSource = types.ResumeSource{}
		if _, err := p.BuildYAML(); err == nil {
			t.Fatalf("fresh S accepted %s", mode)
		}
	}
}

func TestSourceDefaultPATHKeepsProvenanceAcrossHostProjection(t *testing.T) {
	for _, defaultPath := range []bool{false, true} {
		for _, explicit := range []string{"inherit", "", "/request/bin"} {
			t.Run(explicit+"/"+map[bool]string{true: "default", false: "legacy-explicit"}[defaultPath], func(t *testing.T) {
				source := rendererPortableConfig()
				source.Launch.Exec, source.Launch.Systemd = envdExec, "runtime"
				source.Launch.Env["PATH"] = "/image/bin:/usr/bin"
				source.Launch.SystemdPathDefault = defaultPath
				p := artifactRendererParams(types.LaunchCold)
				p.Sandbox.ResumeSource = types.ResumeSource{}
				p.Template.Kind, p.Template.Profile = types.KindSbx, types.ProfileE2B
				p.Spec.Launch = &rtconfig.LaunchConfig{Systemd: "off"}
				p.EnvVars = map[string]string{"INSTANCE": "yes"}
				if explicit != "inherit" {
					p.EnvVars["PATH"] = explicit
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
				wantDefault := defaultPath && explicit == "inherit"
				if !presence.Has("launch.env.PATH") || host.Launch.PersistentSystemdPathDefault() != wantDefault {
					t.Fatalf("host overlay lost concrete PATH or trusted origin:\n%s", projected.Config)
				}
				runtime, c0, err := rtconfig.ApplyFromRules(source, host, presence, rtconfig.ApplyFromOptions{})
				if err != nil {
					t.Fatal(err)
				}
				wantPath := "/image/bin:/usr/bin"
				if explicit != "inherit" {
					wantPath = explicit
				}
				if runtime.Launch.Env["PATH"] != wantPath || runtime.Launch.PersistentSystemdPathDefault() != wantDefault || c0.Launch.SystemdPathDefault != wantDefault {
					t.Fatalf("PATH=%q default=%t portable=%t, want %q/%t", runtime.Launch.Env["PATH"], runtime.Launch.PersistentSystemdPathDefault(), c0.Launch.SystemdPathDefault, wantPath, wantDefault)
				}
			})
		}
	}
}

func TestBuildDefaultPATHSurvivesTypedAssemblyAndC0(t *testing.T) {
	source := rendererPortableConfig()
	source.Launch.Systemd = "runtime"
	source.Launch.Env["PATH"] = "/image/bin:/usr/bin"
	source.Launch.SystemdPathDefault = true
	base := baseParams(types.ProfileE2B)
	base.Resources.Overhead = nil
	for _, explicit := range []bool{false, true} {
		p := BuildColdParams{Profile: types.ProfileE2B, ImageRef: base.Template.Ref,
			Kernel: base.Kernel, Runtime: base.Runtime, Resources: base.Resources, Source: source,
			OverlayDiffTpl: "file:///overlay.ext4", TapFD: TapFD{Exec: []string{"/bin/true"}},
			PortMAC: base.Sandbox.PortMAC, InnerIP: base.Sandbox.InnerIP}
		if explicit {
			p.EnvVars = map[string]string{"PATH": ""}
		}
		cfg, err := p.BuildColdConfig()
		if err != nil {
			t.Fatal(err)
		}
		want := "/image/bin:/usr/bin"
		if explicit {
			want = ""
		}
		if cfg.Launch.Env["PATH"] != want || cfg.Launch.PersistentSystemdPathDefault() == explicit {
			t.Fatal("typed top-level E config lost source PATH provenance")
		}
		pluginPath := want
		if !explicit {
			pluginPath = systemdToolsPath + ":" + want
		}
		if cfg.Launch.Plugin[0].Env["PATH"] != pluginPath {
			t.Fatal("envd plugin lost its product PATH projection")
		}
		raw, err := MarshalBuildColdConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		host, presence, err := rtconfig.LoadConfigBytesWithPresence(raw)
		if err != nil {
			t.Fatal(err)
		}
		if presence.Has("launch.env.PATH") != explicit {
			t.Fatal("C0 overlay lost PATH provenance")
		}
		runtime, _, err := rtconfig.ApplyFromRules(source, host, presence, rtconfig.ApplyFromOptions{ReplaceBoot: true})
		if err != nil {
			t.Fatal(err)
		}
		if runtime.Launch.Env["PATH"] != want || runtime.Launch.PersistentSystemdPathDefault() == explicit {
			t.Fatal("C0 launch lost PATH provenance")
		}
	}
}

func TestEnvdModeOverrideRejectsAmbiguousArgumentBoundaries(t *testing.T) {
	for _, args := range [][]string{
		{"--", "-cgroup-root", "/saved"},
		{"positional"},
		{"-unknown", "-cgroup-root"},
		{"-port", "-cgroup-root", "/saved"},
		{"-cgroup-root", "/one", "--cgroup-root=/two"},
		{"-cgroup-root"},
		{"-isnotfc", "false"},
	} {
		launch := rtconfig.LaunchConfig{Systemd: "runtime", Exec: envdExec, Args: args}
		if err := projectExistingEnvdRoot(&launch); err == nil || !strings.Contains(err.Error(), "new cold definition") {
			t.Fatalf("accepted %v: %v", args, err)
		}
		if !reflect.DeepEqual(launch.Args, args) {
			t.Fatal("failed projection mutated saved args")
		}
	}
	launch := rtconfig.LaunchConfig{Systemd: "provided", Exec: envdExec, Args: []string{"--isnotfc=false", "--port=49983", "--cgroup-root=/saved"}}
	if err := projectExistingEnvdRoot(&launch); err != nil {
		t.Fatal(err)
	}
	if launch.Args[2] != "--cgroup-root="+systemdCgroupRoot {
		t.Fatalf("known flags not projected: %v", launch.Args)
	}
}
