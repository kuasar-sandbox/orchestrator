package builder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestDecodeImportRefererLookupRequiresDigestSubject(t *testing.T) {
	digest := strings.Repeat("a", 64)
	valid := fmt.Sprintf(`{"supported":true,"subject":"registry.example/repo@sha256:%s"}`, digest)
	got, err := decodeImportRefererLookup([]byte(valid))
	if err != nil || got.Subject != "registry.example/repo@sha256:"+digest {
		t.Fatalf("valid lookup=%+v err=%v", got, err)
	}
	if ref := importSourceReference("registry.example/repo:latest", got); ref != got.Subject {
		t.Fatalf("supported import ref = %q, want %q", ref, got.Subject)
	}
	for _, raw := range []string{
		`{"supported":true,"subject":"registry.example/repo:latest"}`,
		`{"supported":true,"subject":""}`,
	} {
		if _, err := decodeImportRefererLookup([]byte(raw)); err == nil {
			t.Fatalf("accepted mutable/empty supported subject: %s", raw)
		}
	}
	if got, err := decodeImportRefererLookup([]byte(`{"supported":false,"subject":""}`)); err != nil || got.Supported {
		t.Fatalf("unsupported lookup=%+v err=%v", got, err)
	} else if ref := importSourceReference("registry.example/repo:latest", got); ref != "registry.example/repo:latest" {
		t.Fatalf("unsupported import ref = %q", ref)
	}
}

func TestValidateBuildProfile(t *testing.T) {
	tests := []struct {
		name    string
		spec    configsock.BuildSpec
		want    string
		wantErr bool
	}{
		{name: "e2b", spec: configsock.BuildSpec{Profile: "e2b"}, want: "e2b"},
		{name: "e2b start", spec: configsock.BuildSpec{Profile: "e2b", StartCmd: "serve"}, want: "e2b"},
		{name: "bare", spec: configsock.BuildSpec{Profile: "bare"}, want: "bare"},
		{name: "bare start", spec: configsock.BuildSpec{Profile: "bare", StartCmd: "serve"}, wantErr: true},
		{name: "bare ready", spec: configsock.BuildSpec{Profile: "bare", ReadyCmd: "check"}, wantErr: true},
		{name: "missing", spec: configsock.BuildSpec{}, wantErr: true},
		{name: "unknown", spec: configsock.BuildSpec{Profile: "other"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateBuildProfile(&tt.spec)
			if (err != nil) != tt.wantErr || string(got) != tt.want {
				t.Fatalf("validateBuildProfile() = %q, %v; want %q, error=%t", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestRunPhaseReportsBeforeWorkAndAfterTeardown(t *testing.T) {
	var events []string
	p := &buildPipeline{
		spec: &configsock.BuildSpec{BuildID: "build-observable"},
		report: func(phase, sid, state string) error {
			events = append(events, phase+":"+sid+":"+state)
			return nil
		},
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := p.runPhase("a", func() error {
		events = append(events, "work")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wantSID := phaseSandboxID("a", p.spec.BuildID)
	want := []string{"a:" + wantSID + ":starting", "work", "a:" + wantSID + ":finished"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("phase events = %#v, want %#v", events, want)
	}
}

func TestRunPhaseFailsClosedWhenStartingReportFails(t *testing.T) {
	wantErr := errors.New("controller unavailable")
	workRan := false
	p := &buildPipeline{
		spec:   &configsock.BuildSpec{BuildID: "build-fail-closed"},
		report: func(_, _, _ string) error { return wantErr },
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err := p.runPhase("a", func() error { workRan = true; return nil })
	if !errors.Is(err, wantErr) || workRan {
		t.Fatalf("runPhase = %v, workRan=%v", err, workRan)
	}
}

func TestRunPhaseFailsClosedUntilVMMCgroupIsEmpty(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "cgroup.events")
	if err := os.WriteFile(eventsPath, []byte("populated 1\nfrozen 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	vmmCgroup, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer vmmCgroup.Close()

	var states []string
	p := &buildPipeline{
		spec:      &configsock.BuildSpec{BuildID: "build-cgroup-fence"},
		vmmCgroup: vmmCgroup,
		report: func(_, _, state string) error {
			states = append(states, state)
			return nil
		},
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := p.runPhase("a", func() error { return nil }); err == nil || !strings.Contains(err.Error(), "still populated") {
		t.Fatalf("runPhase with live VMM processes = %v", err)
	}
	if want := []string{"starting", "failed"}; !reflect.DeepEqual(states, want) {
		t.Fatalf("states with live VMM processes = %v, want %v", states, want)
	}

	if err := os.WriteFile(eventsPath, []byte("populated 0\nfrozen 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	states = nil
	if err := p.runPhase("b", func() error { return nil }); err != nil {
		t.Fatalf("runPhase after VMM cleanup: %v", err)
	}
	if want := []string{"starting", "finished"}; !reflect.DeepEqual(states, want) {
		t.Fatalf("states after VMM cleanup = %v, want %v", states, want)
	}
}

func TestUseLocalImageQualifiesTarstreamIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "image.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("builder image payload")
	scheme, digest, writeErr := tarstream.WriteTo(
		context.Background(), f, "image", sparse.Dense(bytes.NewReader(body), uint64(len(body))),
	)
	closeErr := f.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if scheme != tarstream.DigestSchemeSHA256 {
		t.Fatalf("fixture scheme = %q", scheme)
	}

	p := &buildPipeline{
		imagePath:           "old.img",
		baseImageRef:        "manifest://old",
		baseRef:             "manifest://old",
		overlayBase:         "manifest://overlay",
		overlayBaseFromRefs: []string{"manifest://parent"},
	}
	if err := p.useLocalImage(path); err != nil {
		t.Fatal(err)
	}
	want := manifest.Ref{
		Scheme:       manifest.RefSchemeFile,
		Path:         path,
		DigestScheme: scheme,
		Digest:       digest,
	}.String()
	if p.baseRef != want {
		t.Fatalf("baseRef = %q, want %q", p.baseRef, want)
	}
	if p.imagePath != path || p.baseImageRef != "" || p.overlayBase != "" || p.overlayBaseFromRefs != nil {
		t.Fatalf("pipeline image state = %+v", p)
	}
	ref, err := manifest.ParseRef(p.baseRef)
	if err != nil {
		t.Fatal(err)
	}
	if ref.DigestScheme != tarstream.DigestSchemeSHA256 || ref.Digest != digest {
		t.Fatalf("qualified ref = %+v", ref)
	}
}

func TestUseLocalImageRejectsMalformedArtifactWithoutChangingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "image.img")
	if err := os.WriteFile(path, []byte("not a tarstream"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := &buildPipeline{imagePath: "old.img", baseImageRef: "manifest://old", baseRef: "manifest://old"}
	if err := p.useLocalImage(path); err == nil {
		t.Fatal("useLocalImage accepted a malformed artifact")
	}
	if p.imagePath != "old.img" || p.baseImageRef != "manifest://old" || p.baseRef != "manifest://old" {
		t.Fatalf("failed validation changed pipeline state: %+v", p)
	}
}

func TestResolveBaseConsumesTaskLocalSnapshotPreparation(t *testing.T) {
	p := &buildPipeline{
		profile: types.ProfileE2B,
		spec: &configsock.BuildSpec{
			FromTemplateRef: "manifest://root", FromTemplateKind: "snp",
			SnapshotPreparation: &configsock.BuildSnapshotPreparation{
				BaseRef: "manifest://base", OverlayBase: "manifest://top",
				OverlayBaseFromRefs: []string{"manifest://lower-1", "manifest://lower-2"},
				StartCmd:            "node server.js", ReadyCmd: "curl -sf localhost:3000",
			},
		},
	}
	if err := p.resolveBase(); err != nil {
		t.Fatal(err)
	}
	if p.baseRef != "manifest://base" || p.overlayBase != "manifest://top" ||
		strings.Join(p.overlayBaseFromRefs, ",") != "manifest://lower-1,manifest://lower-2" {
		t.Fatalf("resolved disk = base %q overlay %q chain %v", p.baseRef, p.overlayBase, p.overlayBaseFromRefs)
	}
	if p.startCmd != "node server.js" || p.readyCmd != "curl -sf localhost:3000" {
		t.Fatalf("inherited commands = %q / %q", p.startCmd, p.readyCmd)
	}
	p.spec.SnapshotPreparation = nil
	if err := p.resolveBase(); err == nil {
		t.Fatal("missing task-local snapshot preparation was accepted")
	}
}

func TestAuthoritativeProcessEnvReplacesManifestKeyOnce(t *testing.T) {
	t.Setenv("MANIFEST_KEY", "inherited-wrong-key")
	env := authoritativeProcessEnv(map[string]string{
		"MANIFEST_KEY": "task-key",
		"BUILD_ONLY":   "yes",
	})
	manifestEntries := 0
	for _, entry := range env {
		if strings.HasPrefix(entry, "MANIFEST_KEY=") {
			manifestEntries++
			if entry != "MANIFEST_KEY=task-key" {
				t.Fatalf("manifest entry = %q", entry)
			}
		}
	}
	if manifestEntries != 1 {
		t.Fatalf("authoritative MANIFEST_KEY entries = %d", manifestEntries)
	}
}

func TestBuildPipelineContextPropagatesTaskCancelAndReservesReportGrace(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	canceled, cancel := buildPipelineContext(parent, configsock.BuildTimeouts{TotalSec: 60})
	defer cancel()
	if !errors.Is(canceled.Err(), context.Canceled) {
		t.Fatalf("pipeline context ignored task cancellation: %v", canceled.Err())
	}

	absolute := time.Now().Add(time.Minute).Round(0)
	withDeadline, cancelDeadline := buildPipelineContext(context.Background(), configsock.BuildTimeouts{
		AbsoluteDeadlineUnixNano: absolute.UnixNano(),
	})
	defer cancelDeadline()
	got, ok := withDeadline.Deadline()
	want := absolute.Add(-buildResultReportGrace)
	if !ok || !got.Equal(want) {
		t.Fatalf("pipeline deadline = %v, %t; want %v", got, ok, want)
	}
}

func TestUploadImageReusesBaseImageRef(t *testing.T) {
	ref := "manifest://" + strings.Repeat("a", 64)
	p := &buildPipeline{baseImageRef: ref}
	got, err := p.uploadImage()
	if err != nil {
		t.Fatalf("uploadImage: %v", err)
	}
	if got != ref {
		t.Fatalf("uploadImage = %q, want %q", got, ref)
	}
}

func TestRootDocKeepsExplicitOverlayChain(t *testing.T) {
	p := &buildPipeline{
		baseRef:             "manifest://base",
		overlayBase:         "manifest://top",
		overlayBaseFromRefs: []string{"manifest://parent-1", "manifest://parent-2"},
	}
	root := p.rootDoc("file:///tmp/diff")
	overlay := root["overlay"].(map[string]any)
	chain := overlay["base_from_refs"].([]string)
	if overlay["base"] != p.overlayBase || strings.Join(chain, ",") != strings.Join(p.overlayBaseFromRefs, ",") {
		t.Fatalf("overlay = %#v", overlay)
	}
}

func TestBuildJournalIdentityFields(t *testing.T) {
	b := newBuildJournal("br-test", "build-test")
	if got := b.fields["SYSLOG_IDENTIFIER"]; got != buildTag {
		t.Fatalf("SYSLOG_IDENTIFIER = %q, want %q", got, buildTag)
	}
	if got := b.fields["KUASAR_RUN_ID"]; got != "br-test" {
		t.Fatalf("KUASAR_RUN_ID = %q", got)
	}
	if got := b.fields["KUASAR_BUILD_ID"]; got != "build-test" {
		t.Fatalf("KUASAR_BUILD_ID = %q", got)
	}
}

func TestNetworkDocTapFDSocket(t *testing.T) {
	p := &buildPipeline{spec: &configsock.BuildSpec{
		Net: configsock.BuildNet{
			TapFD: configsock.TapFDConfig{
				Socket:  "/run/kuasar/connector/sw0/tapfd.sock",
				Request: "VSWITCH=sw0 PORT=9",
			},
			Hostname: "build-test",
		},
	}}
	doc := p.networkDoc()
	tapfdDoc, ok := doc["tapfd"].(map[string]any)
	if !ok {
		t.Fatalf("tapfd doc = %#v", doc["tapfd"])
	}
	if tapfdDoc["socket"] != "/run/kuasar/connector/sw0/tapfd.sock" || tapfdDoc["request"] != "VSWITCH=sw0 PORT=9" {
		t.Fatalf("tapfd doc = %#v", tapfdDoc)
	}
	if _, ok := tapfdDoc["exec"]; ok {
		t.Fatalf("tapfd doc unexpectedly has exec: %#v", tapfdDoc)
	}
}

func TestTemplateYAMLPersistsTemplateNetwork(t *testing.T) {
	network := sandboxcfg.NetworkSpec{
		Hostname:         "sandbox",
		DNS:              []string{"169.254.169.253"},
		InnerIP:          "10.0.0.5/24",
		Nexthop:          "10.0.0.1",
		TransitGatewayIP: "192.0.2.1",
		TransitGeneveVNI: 42,
		TransitMAC:       "aa:bb:cc:dd:ee:ff",
	}
	p := &buildPipeline{
		spec: &configsock.BuildSpec{
			TemplateNetwork: network,
			Net: configsock.BuildNet{
				Hostname: "build-deadbeef",
			},
		},
		startCmd: "node server.js",
		readyCmd: "curl -sf localhost:3000",
	}
	doc, err := p.templateYAML()
	if err != nil {
		t.Fatal(err)
	}
	meta, ok := doc["metadata"].(map[string]string)
	if !ok {
		t.Fatalf("metadata = %#v", doc["metadata"])
	}
	if strings.Contains(meta[sandboxcfg.NsNetwork], "build-deadbeef") {
		t.Fatalf("template metadata leaked temporary hostname: %s", meta[sandboxcfg.NsNetwork])
	}
	var got sandboxcfg.NetworkSpec
	if err := json.Unmarshal([]byte(meta[sandboxcfg.NsNetwork]), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, network) {
		t.Fatalf("persisted network = %+v, want %+v", got, network)
	}
}

func TestEnvdYAMLDelegatesCgroupControl(t *testing.T) {
	p := &buildPipeline{spec: &configsock.BuildSpec{}}

	stepsLaunch := p.stepsYAML()["launch"].(map[string]any)
	if got := stepsLaunch["cgroup_control"]; got != true {
		t.Fatalf("steps launch.cgroup_control = %#v, want true", got)
	}
	if got := stepsLaunch["args"]; !reflect.DeepEqual(got, []string{"-isnotfc", "-port", "49983"}) {
		t.Fatalf("steps envd args = %#v", got)
	}

	template, err := p.templateYAML()
	if err != nil {
		t.Fatal(err)
	}
	templateLaunch := template["launch"].(map[string]any)
	if got := templateLaunch["cgroup_control"]; got != true {
		t.Fatalf("template launch.cgroup_control = %#v, want true", got)
	}
	if got := templateLaunch["args"]; !reflect.DeepEqual(got, []string{"-isnotfc", "-port", "49983"}) {
		t.Fatalf("template envd args = %#v", got)
	}

	p.spec.MMDSEnabled = true
	template, err = p.templateYAML()
	if err != nil {
		t.Fatal(err)
	}
	templateLaunch = template["launch"].(map[string]any)
	if templateLaunch["cgroup_control"] != true ||
		!reflect.DeepEqual(templateLaunch["args"], []string{"-port", "49983"}) {
		t.Fatalf("MMDS template launch = %#v", templateLaunch)
	}
}

func TestEveryPhaseYAMLUsesTheCompleteResolvedSandboxResources(t *testing.T) {
	deflate := true
	resources := rtconfig.ResourcesConfig{
		Capacity: rtconfig.CapacityConfig{CPU: 4, Memory: "8GiB"},
		Allocatable: rtconfig.AllocatableConfig{
			CPU: 1.5, Memory: "256MiB", DeflateOnOOM: &deflate,
		},
		Startup:  &rtconfig.StartupConfig{Memory: "8GiB"},
		Overhead: &rtconfig.OverheadConfig{Memory: "32MiB"},
		Control:  rtconfig.ControlConfig{Controller: "/run/sandbox-resource.sock"},
	}
	p := &buildPipeline{spec: &configsock.BuildSpec{Resources: resources}}
	phaseA, err := p.importYAML()
	if err != nil {
		t.Fatal(err)
	}
	phaseB := p.stepsYAML()
	phaseC, err := p.templateYAML()
	if err != nil {
		t.Fatal(err)
	}
	for phase, doc := range map[string]map[string]any{"a": phaseA, "b": phaseB, "c": phaseC} {
		got, ok := doc["resources"].(rtconfig.ResourcesConfig)
		if !ok || !reflect.DeepEqual(got, resources) {
			t.Fatalf("phase %s resources = %#v, want %#v", phase, doc["resources"], resources)
		}
	}
}

func TestPhaseSandboxIDsUseCompleteOpaqueBuildIdentity(t *testing.T) {
	first := phaseSandboxID("a", "same-prefix-build-one")
	second := phaseSandboxID("a", "same-prefix-build-two")
	if first == second {
		t.Fatalf("distinct Build IDs produced the same phase Sandbox ID %q", first)
	}
	if first == phaseSandboxID("b", "same-prefix-build-one") {
		t.Fatalf("distinct phases produced the same Sandbox ID %q", first)
	}
	for _, sid := range []string{first, second} {
		if !types.ValidLocalSandboxID(sid) {
			t.Fatalf("phase Sandbox ID %q is not node-local safe", sid)
		}
	}
	if got, want := phaseSandboxID("a", "same-prefix-build-one"), first; got != want {
		t.Fatalf("phase Sandbox ID is not deterministic: got %q want %q", got, want)
	}

	if len(first) != len("bp-a-")+20 {
		t.Fatalf("phase Sandbox ID %q has unexpected fixed length", first)
	}
}

// testCACertPEM is a self-signed X.509 certificate (CN=test-ca, RSA 2048,
// 1-day validity) used to exercise CA-bundle projection and validation. It is
// parseable by x509.AppendCertsFromPEM but trusts nothing in production.
const testCACertPEM = `-----BEGIN CERTIFICATE-----
MIIDBTCCAe2gAwIBAgIUbWpRZGX/cczZpPOfcQzODmJMdNwwDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNjA4MDQxMTU5MjNaFw0yNjA4MDUx
MTU5MjNaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQC2xO647J/yYuOueFHc2PXnAKvHkPNb4HbWH5FLPfe2nmvUd3bY
lbELV6teLY6yN+NmtvSJ63j2OF+RNIpC3ZFhsWrBDxEuP0juMqKnZ9WVnB/9KsLX
OjfohHY4k1qoQYFxT9yU8eJxmY82Wjd/yf5tV8xHC1zL4UAd0y7rOTyAvozBOXZv
uLr9K3eQgQfylpmP1tYwVoQVvUUf1D5fk5yD9vcUtok2e+7ktBN1f667URznf8hP
hLaQ5C2bZCgOOh78huTrcFqU9LORyk8AS/QYg8esglmrgy3y5iz0M4IN30csTqZN
SvKk9u/2eW26htiq5IOvKu0Xj5xW1wbOmvFlAgMBAAGjUzBRMB0GA1UdDgQWBBQQ
cFRflziG3f5nJyv8FwuE1d0LcTAfBgNVHSMEGDAWgBQQcFRflziG3f5nJyv8FwuE
1d0LcTAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAoHtMT9F8m
HT+8tQke93pIbS9pq41PiOeBuDdF/yrgB5IKlVticAhzUCHHAG8UpEuqU2OjbF30
tIgwoUe1A+vNeanPjiOq3+rWADvMbgcleVWRUfxYlyxAdZ2yq+PfiqTI96UQIw3n
STeBSM7HZ6i/DqbAV+GvFaGmEC0OsOxOQAxPPuK8hFLU2eJ3HIdluW7stLcXtMe7
MuijqSVF8COlC+zKndt52yoJpU70bHZzLnEHYU7NvBeHgfUHqGBfvmyg3auGlSfE
peTd6+1IyyBTa6XbTg9wcMRPZE0uB+xsns0ArNR+jALUzNgoe7tBChAaFNyQPD1u
8NdJbsFlXbvO
-----END CERTIFICATE-----
`

// flattenFile finds the projected file entry at guestPath, or fails.
func flattenFile(t *testing.T, files []map[string]any, guestPath string) map[string]any {
	t.Helper()
	for _, f := range files {
		if f["path"] == guestPath {
			return f
		}
	}
	t.Fatalf("projected file %q not found in %v", guestPath, files)
	return nil
}

func TestFlattenConfigFilesNilWithoutTLSConfig(t *testing.T) {
	p := &buildPipeline{spec: &configsock.BuildSpec{}}
	files, err := p.flattenConfigFiles()
	if err != nil {
		t.Fatalf("flattenConfigFiles: %v", err)
	}
	if files != nil {
		t.Fatalf("flattenConfigFiles() = %v, want nil", files)
	}
}

func TestFlattenConfigFilesProjectsCABundle(t *testing.T) {
	p := &buildPipeline{spec: &configsock.BuildSpec{
		RegistryTLS: &configsock.BuildRegistryTLS{CABundlePEM: testCACertPEM},
	}}
	files, err := p.flattenConfigFiles()
	if err != nil {
		t.Fatalf("flattenConfigFiles: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("got %d files, want 2 (CA + yaml)", len(files))
	}
	ca := flattenFile(t, files, guestCACert)
	// Inline PEM content, read-only, 0444 — never on the build root disk.
	if ca["mode"] != "0444" || ca["read_only"] != true {
		t.Fatalf("CA file not read-only: %#v", ca)
	}
	if ca["content"] != testCACertPEM {
		t.Fatalf("CA content mismatch: got %q", ca["content"])
	}
	cfg := flattenFile(t, files, guestFlattenCfg)
	if cfg["mode"] != "0444" || cfg["read_only"] != true {
		t.Fatalf("flatten yaml not read-only: %#v", cfg)
	}
	// tls.ca_cert must point at the guest path (no host path crosses the wire).
	if !strings.Contains(cfg["content"].(string), "ca_cert: "+guestCACert) {
		t.Fatalf("flatten yaml missing guest ca_cert:\n%s", cfg["content"])
	}
	if strings.Contains(cfg["content"].(string), "insecure_skip_verify") {
		t.Fatalf("flatten yaml should not set insecure_skip_verify:\n%s", cfg["content"])
	}
}

func TestFlattenConfigFilesProjectsSkipVerifyOnly(t *testing.T) {
	p := &buildPipeline{spec: &configsock.BuildSpec{
		RegistryTLS: &configsock.BuildRegistryTLS{InsecureSkipVerify: true},
	}}
	files, err := p.flattenConfigFiles()
	if err != nil {
		t.Fatalf("flattenConfigFiles: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("got %d files, want 1 (yaml only, no CA)", len(files))
	}
	cfg := flattenFile(t, files, guestFlattenCfg)
	if !strings.Contains(cfg["content"].(string), "insecure_skip_verify: true") {
		t.Fatalf("flatten yaml missing insecure_skip_verify:\n%s", cfg["content"])
	}
	if strings.Contains(cfg["content"].(string), "ca_cert") {
		t.Fatalf("flatten yaml should not set ca_cert:\n%s", cfg["content"])
	}
}

func TestImportYAMLProjectsFlattenConfigFiles(t *testing.T) {
	// Phase A (import) is the only phase that pulls from a registry, so the
	// TLS files must appear here.
	p := &buildPipeline{spec: &configsock.BuildSpec{
		RegistryTLS: &configsock.BuildRegistryTLS{CABundlePEM: testCACertPEM},
		Paths:       configsock.BuildPaths{Kernel: "/k", Runtime: "/r", BuilderDiffTpl: "/d"},
	}}
	doc, err := p.importYAML()
	if err != nil {
		t.Fatalf("importYAML: %v", err)
	}
	files, ok := doc["files"].([]map[string]any)
	if !ok {
		t.Fatalf("files not projected: %#v", doc["files"])
	}
	paths := map[string]bool{}
	for _, f := range files {
		paths[f["path"].(string)] = true
	}
	if !paths[guestCACert] || !paths[guestFlattenCfg] {
		t.Fatalf("TLS config files missing from import projection: %v", paths)
	}
}

func TestStepsYAMLOmitsFlattenConfigFiles(t *testing.T) {
	// Phase B (steps) never touches a registry: TLS config must not be projected.
	p := &buildPipeline{spec: &configsock.BuildSpec{
		RegistryTLS: &configsock.BuildRegistryTLS{CABundlePEM: testCACertPEM},
		Paths:       configsock.BuildPaths{Kernel: "/k", Runtime: "/r", BuilderDiffTpl: "/d"},
	}}
	doc := p.stepsYAML()
	if files, ok := doc["files"]; ok {
		for _, f := range files.([]map[string]any) {
			if f["path"] == guestCACert || f["path"] == guestFlattenCfg {
				t.Fatalf("TLS file leaked into steps YAML: %#v", f)
			}
		}
	}
}

func TestTemplateYAMLOmitsFlattenConfigFiles(t *testing.T) {
	// Phase C never touches a registry, so TLS config must not be projected.
	p := &buildPipeline{spec: &configsock.BuildSpec{
		RegistryTLS: &configsock.BuildRegistryTLS{CABundlePEM: testCACertPEM},
		Paths:       configsock.BuildPaths{Kernel: "/k", Runtime: "/r", OverlayDiffTpl: "/d"},
	}}
	doc, err := p.templateYAML()
	if err != nil {
		t.Fatal(err)
	}
	if files, ok := doc["files"]; ok {
		for _, f := range files.([]map[string]any) {
			if f["path"] == guestCACert || f["path"] == guestFlattenCfg {
				t.Fatalf("TLS file leaked into template YAML: %#v", f)
			}
		}
	}
}

func TestFlattenConfigArgNilWithoutTLSConfig(t *testing.T) {
	p := &buildPipeline{spec: &configsock.BuildSpec{}}
	if got := p.flattenConfigArg(); got != nil {
		t.Fatalf("flattenConfigArg() = %v, want nil", got)
	}
}

func TestFlattenConfigArgForRegistryTLS(t *testing.T) {
	p := &buildPipeline{spec: &configsock.BuildSpec{
		RegistryTLS: &configsock.BuildRegistryTLS{CABundlePEM: testCACertPEM},
	}}
	got := p.flattenConfigArg()
	want := []string{"--config", guestFlattenCfg}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("flattenConfigArg() = %v, want %v", got, want)
	}
}

func TestFlattenConfigArgForSkipVerify(t *testing.T) {
	p := &buildPipeline{spec: &configsock.BuildSpec{
		RegistryTLS: &configsock.BuildRegistryTLS{InsecureSkipVerify: true},
	}}
	got := p.flattenConfigArg()
	want := []string{"--config", guestFlattenCfg}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("flattenConfigArg() = %v, want %v", got, want)
	}
}
