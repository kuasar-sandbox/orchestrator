package builder

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
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

// TestParseTemplateDisk covers the fromTemplate disk extraction: a base
// template's `sandbox-ctl info --json` must yield both the erofs base image
// AND its accumulated overlay diff (the read-only lower a cold-start stacks
// under a fresh overlay), or the template's filesystem is lost.
func TestParseTemplateDisk(t *testing.T) {
	// overlay-mode template: base image + accumulated overlay diff, no chain.
	overlayJSON := `{
		"Metadata": {"e2b.start_cmd": "node server.js", "e2b.ready_cmd": "curl -sf localhost:3000"},
		"Boot": {"Root": {
			"BaseRef": "manifest://aaaabbbb",
			"Overlay": {"Base": "manifest://ccccdddd", "BaseFromRefs": null}
		}}
	}`
	baseRef, overlayBase, overlayChain, meta, err := parseTemplateDisk([]byte(overlayJSON))
	if err != nil {
		t.Fatalf("overlay-mode: unexpected err: %v", err)
	}
	if baseRef != "manifest://aaaabbbb" {
		t.Errorf("baseRef = %q, want manifest://aaaabbbb", baseRef)
	}
	if overlayBase != "manifest://ccccdddd" {
		t.Errorf("overlayBase = %q, want manifest://ccccdddd", overlayBase)
	}
	if len(overlayChain) != 0 {
		t.Errorf("overlayChain = %v, want empty", overlayChain)
	}
	if meta["e2b.start_cmd"] != "node server.js" || meta["e2b.ready_cmd"] != "curl -sf localhost:3000" {
		t.Errorf("meta = %v, want start/ready cmds", meta)
	}

	// overlay omitted (single-disk capture): base only, no overlay lower.
	noOverlay := `{"Boot": {"Root": {"BaseRef": "manifest://eeee"}}}`
	baseRef, overlayBase, overlayChain, _, err = parseTemplateDisk([]byte(noOverlay))
	if err != nil {
		t.Fatalf("no-overlay: unexpected err: %v", err)
	}
	if baseRef != "manifest://eeee" || overlayBase != "" {
		t.Errorf("no-overlay: base=%q overlay=%q, want manifest://eeee + empty", baseRef, overlayBase)
	}

	// A chained overlay remains an explicit top ref plus base_from_refs.
	chained := `{"Boot": {"Root": {
		"BaseRef": "manifest://eeee",
		"Overlay": {"Base": "manifest://ffff", "BaseFromRefs": ["manifest://gggg", "manifest://hhhh"]}
	}}}`
	_, overlayBase, overlayChain, _, err = parseTemplateDisk([]byte(chained))
	if err != nil {
		t.Fatalf("chained overlay: unexpected err: %v", err)
	}
	if overlayBase != "manifest://ffff" || strings.Join(overlayChain, ",") != "manifest://gggg,manifest://hhhh" {
		t.Errorf("chained overlay = %q + %v", overlayBase, overlayChain)
	}

	// missing base image ref is an error.
	if _, _, _, _, err = parseTemplateDisk([]byte(`{"Boot": {"Root": {}}}`)); err == nil {
		t.Error("missing base ref: expected error, got nil")
	}

	// malformed JSON is an error.
	if _, _, _, _, err = parseTemplateDisk([]byte(`not json`)); err == nil {
		t.Error("malformed json: expected error, got nil")
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
