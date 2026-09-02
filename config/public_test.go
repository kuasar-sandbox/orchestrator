package config_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
	"gopkg.in/yaml.v3"
)

func TestDecodeConductorStrictAndCustomBootstrap(t *testing.T) {
	if _, err := config.DecodeConductor(strings.NewReader("future: true\n")); err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("unknown field error = %v", err)
	}
	for _, body := range []string{
		`{"proxy":{"mode":"internal"}}`,
		`{"proxy":{"data_listen":"127.0.0.1:8443"}}`,
		`{"proxy":{"proxy_netns":"sw0_mgmt"}}`,
	} {
		if _, err := config.DecodeConductor(strings.NewReader(body)); err == nil {
			t.Fatalf("JSON conductor accepted removed field: %s", body)
		}
	}
	if _, err := config.DecodeConductor(strings.NewReader("paths:\n  conductor_executable: relative\n")); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative executable error = %v", err)
	}

	cfg, err := config.DecodeConductor(strings.NewReader("paths:\n  conductor_executable: /opt/kuasar/xconductor\n"))
	if err != nil {
		t.Fatalf("custom bootstrap should defer final required fields to Configure: %v", err)
	}
	if cfg.API.Listen != ":443" || cfg.Paths.RunRoot != "/run/sandbox" {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	declarative, err := config.DecodeConductor(strings.NewReader("{}\n"))
	if err != nil {
		t.Fatalf("incomplete declarative config did not decode: %v", err)
	}
	if err := config.ValidateConductorFinal(declarative); err == nil || !strings.Contains(err.Error(), "api.domain") {
		t.Fatalf("final conductor error = %v", err)
	}
	if _, err := config.DecodeConductor(strings.NewReader("paths:\n  conductor_executable: /opt/x\n---\n{}\n")); err == nil || !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Fatalf("multiple document error = %v", err)
	}
	for name, body := range map[string]string{
		"removed":  "proxy:\n  mode: internal\n",
		"duration": "units:\n  pool_wait_timeout: later\n",
		"range":    "sandbox:\n  resources:\n    capacity: { cpu: -1 }\n",
	} {
		t.Run("custom rejects explicit "+name, func(t *testing.T) {
			_, err := config.DecodeConductor(strings.NewReader("paths:\n  conductor_executable: /opt/x\n" + body))
			if err == nil {
				t.Fatalf("explicitly invalid custom bootstrap was accepted:\n%s", body)
			}
		})
	}
}

func TestDecodeProxyStrictAndCustomBootstrap(t *testing.T) {
	if _, err := config.DecodeProxy(strings.NewReader("future: true\n")); err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := config.DecodeProxy(strings.NewReader(`{"proxy_socket":"/tmp/proxy.sock"}`)); err == nil || !strings.Contains(err.Error(), "proxy_socket") {
		t.Fatalf("JSON proxy removed-field error = %v", err)
	}
	if _, err := config.DecodeProxy(strings.NewReader("paths:\n  proxy_executable: relative\n")); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative executable error = %v", err)
	}
	cfg, err := config.DecodeProxy(strings.NewReader("paths:\n  proxy_executable: /opt/kuasar/xproxy\n"))
	if err != nil {
		t.Fatalf("custom proxy bootstrap should defer run_root to Configure: %v", err)
	}
	if cfg.Workers != 1 || cfg.ConfigSocket != "/run/sandbox/node-ctl.socket" {
		t.Fatalf("proxy defaults not applied: %+v", cfg)
	}
	declarative, err := config.DecodeProxy(strings.NewReader("{}\n"))
	if err != nil {
		t.Fatalf("incomplete proxy declarative config did not decode: %v", err)
	}
	if err := config.ValidateProxyFinal(declarative); err == nil || !strings.Contains(err.Error(), "paths.run_root") {
		t.Fatalf("final proxy error = %v", err)
	}
	for name, body := range map[string]string{
		"enum":     "auth: future\n",
		"duration": "park_timeout: later\n",
		"range":    "workers: -1\n",
	} {
		t.Run("custom rejects explicit "+name, func(t *testing.T) {
			_, err := config.DecodeProxy(strings.NewReader("paths:\n  proxy_executable: /opt/x\n" + body))
			if err == nil {
				t.Fatalf("explicitly invalid custom bootstrap was accepted:\n%s", body)
			}
		})
	}
}

func TestDecodeConductorIsIndependentOfEncryptionEnvironment(t *testing.T) {
	const input = "api:\n  domain: custom.test\nsandbox:\n  boot:\n    kernel: /kernel\n    runtime: /runtime\n"
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	withoutEnvironment, err := config.DecodeConductor(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", strings.Repeat("a", 64))
	withEnvironment, err := config.DecodeConductor(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(withoutEnvironment, withEnvironment) {
		t.Fatalf("DecodeConductor depended on ambient encryption key:\nwithout=%+v\nwith=%+v", withoutEnvironment, withEnvironment)
	}
}

func TestFinalValidationRunsAfterCustomConfigurationWithoutDefaults(t *testing.T) {
	conductor, err := config.DecodeConductor(strings.NewReader("paths:\n  conductor_executable: /opt/kuasar/xconductor\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := config.ValidateConductorFinal(conductor); err == nil || !strings.Contains(err.Error(), "api.domain") {
		t.Fatalf("incomplete final conductor error = %v", err)
	}
	conductor.API.Domain = "custom.test"
	conductor.Sandbox.Boot.Kernel = "/kernel"
	conductor.Sandbox.Boot.Runtime = "/runtime"
	if err := config.ValidateConductorFinal(conductor); err != nil {
		t.Fatalf("configured conductor: %v", err)
	}
	// Encryption material is resolved separately so a Runtime provider can be
	// authoritative; final declarative validation must not require YAML/env key.
	if conductor.EncryptionKey != "" {
		t.Fatalf("test unexpectedly has YAML encryption material: %q", conductor.EncryptionKey)
	}
	proxy, err := config.DecodeProxy(strings.NewReader("paths:\n  proxy_executable: /opt/kuasar/xproxy\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := config.ValidateProxyFinal(proxy); err == nil || !strings.Contains(err.Error(), "paths.run_root") {
		t.Fatalf("incomplete final proxy error = %v", err)
	}
	proxy.Paths.RunRoot = "/run/custom"
	if err := config.ValidateProxyFinal(proxy); err == nil || !strings.Contains(err.Error(), "data_listen") {
		t.Fatalf("incomplete final proxy data listener error = %v", err)
	}
	proxy.DataListen = "127.0.0.1:8443"
	if err := config.ValidateProxyFinal(proxy); err != nil {
		t.Fatalf("configured proxy: %v", err)
	}
}

func TestConductorCloneIsDeep(t *testing.T) {
	one := 1.0
	ratio := 0.8
	install := true
	merge := false
	max := int64(3)
	cpu := config.CPUCores("2")
	memory := "4GiB"
	allocatableMemory := "512MiB"
	cfg := &config.Conductor{
		Units:   config.UnitsConfig{Install: &install},
		Cluster: config.ClusterConfig{Labels: map[string]string{"zone": "z1"}},
		Sandbox: config.SandboxConfig{
			Network: config.NetworkConfig{DNS: []string{"1.1.1.1"}},
			Resources: config.ResourcesConfig{
				Allocatable:   config.ResourceAllocatable{CPU: &one, Memory: &allocatableMemory},
				Startup:       &config.ResourceStartup{Memory: "1GiB"},
				WatermarkHigh: &config.ResourceWatermarkHigh{Ratio: &ratio},
			},
		},
		Builder: config.BuilderConfig{
			Admission: config.BuilderAdmissionConfig{Execution: &config.BuildAdmissionLimitConfig{
				MaxBuilds: &max,
				Resources: config.BuildAdmissionResourcesConfig{CPU: &cpu, Memory: &memory},
			}},
			FilesStorage: &config.FilesStorageConfig{Bucket: "bucket"},
		},
		ResourceListen: &config.ResourceListenConfig{CgroupScanPaths: []string{"/a"}},
		Checkpoint:     config.CheckpointConfig{MergeRef: &merge},
		MMDS: config.MMDSConfig{
			Routes:   config.MMDSRoutesConfig{ReservedPathPrefixes: []string{"/internal/"}},
			Services: map[string]config.MMDSServiceRegistryEntry{"svc": {Endpoint: "unix:///run/svc.sock"}},
		},
	}

	clone := cfg.Clone()
	*clone.Units.Install = false
	clone.Cluster.Labels["zone"] = "z2"
	clone.Sandbox.Network.DNS[0] = "8.8.8.8"
	*clone.Sandbox.Resources.Allocatable.CPU = 2
	*clone.Sandbox.Resources.Allocatable.Memory = "1GiB"
	clone.Sandbox.Resources.Startup.Memory = "2GiB"
	*clone.Sandbox.Resources.WatermarkHigh.Ratio = 0.9
	*clone.Builder.Admission.Execution.MaxBuilds = 9
	*clone.Builder.Admission.Execution.Resources.CPU = "4"
	*clone.Builder.Admission.Execution.Resources.Memory = "8GiB"
	clone.Builder.FilesStorage.Bucket = "other"
	clone.ResourceListen.CgroupScanPaths[0] = "/b"
	*clone.Checkpoint.MergeRef = true
	clone.MMDS.Routes.ReservedPathPrefixes[0] = "/changed/"
	clone.MMDS.Services["svc"] = config.MMDSServiceRegistryEntry{Endpoint: "unix:///run/other.sock"}

	if !*cfg.Units.Install || cfg.Cluster.Labels["zone"] != "z1" || cfg.Sandbox.Network.DNS[0] != "1.1.1.1" ||
		*cfg.Sandbox.Resources.Allocatable.CPU != 1 || *cfg.Sandbox.Resources.Allocatable.Memory != "512MiB" ||
		cfg.Sandbox.Resources.Startup.Memory != "1GiB" ||
		*cfg.Sandbox.Resources.WatermarkHigh.Ratio != 0.8 || *cfg.Builder.Admission.Execution.MaxBuilds != 3 ||
		*cfg.Builder.Admission.Execution.Resources.CPU != "2" || *cfg.Builder.Admission.Execution.Resources.Memory != "4GiB" ||
		cfg.Builder.FilesStorage.Bucket != "bucket" || cfg.ResourceListen.CgroupScanPaths[0] != "/a" ||
		*cfg.Checkpoint.MergeRef || cfg.MMDS.Routes.ReservedPathPrefixes[0] != "/internal/" ||
		cfg.MMDS.Services["svc"].Endpoint != "unix:///run/svc.sock" {
		t.Fatalf("clone shares mutable state with source: source=%+v clone=%+v", cfg, clone)
	}
}

func TestConfigJSONRoundTripAndNoInternalTypes(t *testing.T) {
	cfg, err := config.DecodeConductor(strings.NewReader("paths:\n  conductor_executable: /opt/kuasar/xconductor\ncluster:\n  labels: {zone: z1}\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ResourceListen = &config.ResourceListenConfig{Enabled: true}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var round config.Conductor
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatal(err)
	}
	if round.Paths.ConductorExecutable != cfg.Paths.ConductorExecutable || round.Cluster.Labels["zone"] != "z1" {
		t.Fatalf("JSON round trip = %+v", round)
	}
	if cfg.Sandbox.Resources.Allocatable.Memory != nil || round.Sandbox.Resources.Allocatable.Memory != nil {
		t.Fatalf("JSON round trip lost inherited resource presence: before=%+v after=%+v", cfg.Sandbox.Resources, round.Sandbox.Resources)
	}
	if strings.Contains(string(b), "audit_path") {
		t.Fatalf("public config JSON contains removed resource_listen.audit_path: %s", b)
	}

	seen := map[reflect.Type]bool{}
	assertPublicType(t, reflect.TypeOf(config.Conductor{}), seen)
	assertPublicType(t, reflect.TypeOf(config.Proxy{}), seen)
}

func TestConfigJSONPreservesExplicitResourcePresenceAndNumericCPU(t *testing.T) {
	cfg, err := config.DecodeConductor(strings.NewReader(`
paths:
  conductor_executable: /opt/kuasar/xconductor
sandbox:
  resources:
    allocatable: { memory: 256MiB }
builder:
  admission:
    execution:
      resources: { cpu: 2.0001 }
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Sandbox.Resources.Allocatable.Memory == nil {
		t.Fatal("explicit allocatable.memory was marked inherited")
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"cpu":2.0001`) || strings.Contains(string(b), `"cpu":"2.0001"`) {
		t.Fatalf("CPU was not encoded as a JSON number: %s", b)
	}
	var round config.Conductor
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatal(err)
	}
	if round.Sandbox.Resources.Allocatable.Memory == nil || *round.Sandbox.Resources.Allocatable.Memory != "256MiB" {
		t.Fatalf("explicit resource presence changed: %+v", round.Sandbox.Resources)
	}
	if got := *round.Builder.Admission.Execution.Resources.CPU; got != config.CPUCores("2.0001") {
		t.Fatalf("CPU = %q, want 2.0001", got)
	}
}

func TestAllocatableMemoryPresenceSurvivesYAMLRoundTrip(t *testing.T) {
	for name, input := range map[string]struct {
		yaml     string
		explicit bool
	}{
		"inherited": {
			yaml: "paths:\n  conductor_executable: /opt/kuasar/xconductor\n",
		},
		"null inherits": {
			yaml: "paths:\n  conductor_executable: /opt/kuasar/xconductor\nsandbox:\n  resources:\n    allocatable:\n      memory: null\n",
		},
		"explicit default": {
			yaml:     "paths:\n  conductor_executable: /opt/kuasar/xconductor\nsandbox:\n  resources:\n    allocatable:\n      memory: 256MiB\n",
			explicit: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := config.DecodeConductor(strings.NewReader(input.yaml))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			round, err := config.DecodeConductor(strings.NewReader(string(raw)))
			if err != nil {
				t.Fatal(err)
			}
			if input.explicit {
				if round.Sandbox.Resources.Allocatable.Memory == nil || *round.Sandbox.Resources.Allocatable.Memory != "256MiB" {
					t.Fatalf("explicit memory lost across YAML round trip: %s", raw)
				}
				return
			}
			if round.Sandbox.Resources.Allocatable.Memory != nil || strings.Contains(string(raw), "memory: 256MiB") {
				t.Fatalf("inherited memory became explicit across YAML round trip: %s", raw)
			}
		})
	}
}

func TestResourceAllocatableMemoryConvenienceMethods(t *testing.T) {
	var allocatable config.ResourceAllocatable
	allocatable.SetMemory("512MiB")
	if allocatable.Memory == nil || *allocatable.Memory != "512MiB" {
		t.Fatalf("SetMemory result = %+v", allocatable)
	}
	allocatable.InheritMemory()
	if allocatable.Memory != nil {
		t.Fatalf("InheritMemory result = %+v", allocatable)
	}
}

func TestConfigJSONRejectsUnknownNestedResourceField(t *testing.T) {
	for name, raw := range map[string]string{
		"resource":    `{"sandbox":{"resources":{"future":true}}}`,
		"allocatable": `{"sandbox":{"resources":{"allocatable":{"future":true}}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var cfg config.Conductor
			if err := json.Unmarshal([]byte(raw), &cfg); err == nil || !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("unknown nested field error = %v", err)
			}
		})
	}
}

func TestDecodeConfigSizeBound(t *testing.T) {
	oversized := strings.NewReader("#" + strings.Repeat("x", 4<<20) + "\n")
	if _, err := config.DecodeConductor(oversized); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("conductor oversized error = %v", err)
	}
	oversized = strings.NewReader("#" + strings.Repeat("x", 4<<20) + "\n")
	if _, err := config.DecodeProxy(oversized); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("proxy oversized error = %v", err)
	}
}

func assertPublicType(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Map {
		assertPublicType(t, typ.Key(), seen)
		assertPublicType(t, typ.Elem(), seen)
		return
	}
	if seen[typ] {
		return
	}
	seen[typ] = true
	if strings.Contains(typ.PkgPath(), "/internal/") {
		t.Fatalf("public schema exposes internal type %s", typ)
	}
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		for arg := 0; arg < method.Type.NumIn(); arg++ {
			assertPublicType(t, method.Type.In(arg), seen)
		}
		for result := 0; result < method.Type.NumOut(); result++ {
			assertPublicType(t, method.Type.Out(result), seen)
		}
	}
	if typ.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.IsExported() {
			assertPublicType(t, field.Type, seen)
		}
	}
}
