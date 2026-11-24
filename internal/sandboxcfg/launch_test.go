package sandboxcfg

import (
	"encoding/json"
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

// The existing envd primary remains the workload in every systemd mode.
func TestNewE2BLaunchKeepsEnvdPrimary(t *testing.T) {
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
			if launch.Placeholder || launch.Exec != envdExec || !launch.CgroupControl ||
				launch.PIDNamespace != "shared" || len(launch.Plugin) != 0 || launch.User != "0:0" ||
				launch.Restart != "always" || launch.Env["PRODUCT"] != "value" {
				t.Fatalf("new E2B topology changed: %+v", launch)
			}
			args := strings.Join(launch.Args, " ")
			want := mode
			if want == "" {
				want = "off"
			}
			if launch.Systemd != want || strings.Contains(args, "-isnotfc") == mmds {
				t.Fatalf("mode %q mmds=%t args=%s", want, mmds, args)
			}
			if want == "off" && strings.Contains(args, "-cgroup-root") {
				t.Fatalf("direct envd gained unnecessary cgroup flag: %v", launch.Args)
			}
			if want != "off" && !strings.HasSuffix(args, "-cgroup-root "+systemdCgroupRoot) {
				t.Fatalf("systemd envd lost delegated root: %v", launch.Args)
			}
		}
	}
}

func TestPreparedSourceProjectOnlyRewritesKnownEnvdPrimary(t *testing.T) {
	for _, mode := range []string{"", "off", "auto", "runtime", "provided"} {
		source := rendererPortableConfig()
		source.Launch.Systemd = "runtime"
		source.Launch.Exec = envdExec
		source.Launch.Args = []string{"-port", "49983", "-cgroup-root", systemdCgroupRoot}
		source.Launch.PIDNamespace = "shared"
		source.Launch.CgroupControl = false // raw saved setting must survive
		source.Launch.Env = map[string]string{"ARTIFACT": "yes", "PATH": "/chosen/bin:/bin"}
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
		projected, err := ProjectSourceLaunch(raw, types.ProfileE2B, source)
		if err != nil {
			t.Fatal(err)
		}
		host, presence, err := rtconfig.LoadConfigBytesWithPresence(projected.Config)
		if err != nil {
			t.Fatal(err)
		}
		want := mode
		if want == "" {
			want = "runtime"
		}
		if !presence.Has("launch.systemd") || host.Launch.Systemd != want ||
			host.Launch.Exec != envdExec || host.Launch.Placeholder || len(host.Launch.Plugin) != 0 ||
			host.Launch.CgroupControl || host.Launch.Env["ARTIFACT"] != "yes" ||
			host.Launch.Env["INSTANCE"] != "yes" || host.Launch.Env["PATH"] != "/chosen/bin:/bin" {
			t.Fatalf("source projection: %+v", host.Launch)
		}
		args := strings.Join(host.Launch.Args, " ")
		if !strings.HasSuffix(args, "-cgroup-root "+envdCgroupRoot(want)) {
			t.Fatalf("wrong envd root %v", host.Launch.Args)
		}
		after, _ := json.Marshal(source)
		if string(after) != string(before) {
			t.Fatal("mutated authenticated source E")
		}
	}
}

func TestEnvdModeOverrideRejectsUnrecognizedLayout(t *testing.T) {
	for _, args := range [][]string{
		{"--", "-cgroup-root", "/saved"}, {"positional"}, {"-unknown"},
		{"-port", "-cgroup-root", "/saved"},
		{"-cgroup-root", "/one", "--cgroup-root=/two"},
		{"-cgroup-root"}, {"-isnotfc", "false"},
	} {
		launch := rtconfig.LaunchConfig{Systemd: "runtime", Exec: envdExec, Args: args}
		before := strings.Join(args, "\x00")
		if err := projectExistingEnvdRoot(&launch); err == nil {
			t.Errorf("accepted unknown args %v", args)
		}
		if strings.Join(launch.Args, "\x00") != before {
			t.Fatal("failure modified original envd args")
		}
	}
	for _, layout := range []rtconfig.LaunchConfig{
		{Systemd: "runtime", Exec: "/custom"},
		{Systemd: "runtime", Placeholder: true},
		{Systemd: "runtime", Exec: envdExec, Plugin: []rtconfig.PluginConfig{{Exec: "/other"}}},
	} {
		if err := projectExistingEnvdRoot(&layout); err == nil {
			t.Errorf("accepted layout %+v", layout)
		}
	}
	launch := rtconfig.LaunchConfig{Systemd: "provided", Exec: envdExec, Args: []string{"--isnotfc=false", "--port=49983", "--cgroup-root=/old"}}
	if err := projectExistingEnvdRoot(&launch); err != nil {
		t.Fatal(err)
	}
	if launch.Args[2] != "--cgroup-root="+systemdCgroupRoot {
		t.Fatalf("envd flags %v", launch.Args)
	}
}

func TestBuildSourceModePrecedesE2BLaunch(t *testing.T) {
	base := baseParams(types.ProfileE2B)
	for _, mode := range []string{"", "off", "provided"} {
		source := rendererPortableConfig()
		source.Launch.Systemd = "runtime"
		source.Launch.Exec = envdExec
		source.Launch.Env = map[string]string{"ARTIFACT": "yes", "PATH": "/image/bin:/usr/bin"}
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
		if cfg.Launch.Exec != envdExec || cfg.Launch.Placeholder || len(cfg.Launch.Plugin) != 0 ||
			cfg.Launch.Systemd != want || cfg.Launch.Env["PATH"] != "/image/bin:/usr/bin" ||
			cfg.Launch.Env["INSTANCE"] != "yes" {
			t.Fatalf("source Build topology/Env changed: %+v", cfg.Launch)
		}
		args := strings.Join(cfg.Launch.Args, " ")
		if want != "off" && !strings.HasSuffix(args, "-cgroup-root "+systemdCgroupRoot) {
			t.Fatalf("Build delegated envd root missing: %v", cfg.Launch.Args)
		}
		raw, err := MarshalBuildColdConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var decoded rtconfig.SandboxConfig
		if err := yaml.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Launch.Systemd != want || decoded.Launch.Exec != envdExec {
			t.Fatal("C0 differs from E")
		}
	}
}

func TestSnapshotSystemdOverrideAndPausedSourceAuthority(t *testing.T) {
	for _, mode := range []string{"off", "auto", "runtime", "provided"} {
		p := artifactRendererParams(types.LaunchMemory)
		p.Spec.Launch = &rtconfig.LaunchConfig{Systemd: mode}
		if _, err := p.BuildYAML(); err != nil {
			t.Fatalf("durable source ignored %s: %v", mode, err)
		}
		p.Sandbox.ResumeSource = types.ResumeSource{}
		if _, err := p.BuildYAML(); err == nil {
			t.Fatalf("fresh S accepted %s", mode)
		}
	}
}
