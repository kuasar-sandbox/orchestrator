package builder

import (
	"fmt"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
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
	baseRef, overlayBase, meta, err := parseTemplateDisk([]byte(overlayJSON))
	if err != nil {
		t.Fatalf("overlay-mode: unexpected err: %v", err)
	}
	if baseRef != "manifest://aaaabbbb" {
		t.Errorf("baseRef = %q, want manifest://aaaabbbb", baseRef)
	}
	if overlayBase != "manifest://ccccdddd" {
		t.Errorf("overlayBase = %q, want manifest://ccccdddd", overlayBase)
	}
	if meta["e2b.start_cmd"] != "node server.js" || meta["e2b.ready_cmd"] != "curl -sf localhost:3000" {
		t.Errorf("meta = %v, want start/ready cmds", meta)
	}

	// overlay omitted (single-disk capture): base only, no overlay lower.
	noOverlay := `{"Boot": {"Root": {"BaseRef": "manifest://eeee"}}}`
	baseRef, overlayBase, _, err = parseTemplateDisk([]byte(noOverlay))
	if err != nil {
		t.Fatalf("no-overlay: unexpected err: %v", err)
	}
	if baseRef != "manifest://eeee" || overlayBase != "" {
		t.Errorf("no-overlay: base=%q overlay=%q, want manifest://eeee + empty", baseRef, overlayBase)
	}

	// chained overlay folds into a multi-key overlay.base, top-first
	// ([overlay.base] ++ base_from_refs) — the order restore layers them, never
	// an error and never a dropped layer.
	chained := `{"Boot": {"Root": {
		"BaseRef": "manifest://eeee",
		"Overlay": {"Base": "manifest://ffff", "BaseFromRefs": ["manifest://gggg", "manifest://hhhh"]}
	}}}`
	_, overlayBase, _, err = parseTemplateDisk([]byte(chained))
	if err != nil {
		t.Fatalf("chained overlay: unexpected err: %v", err)
	}
	if overlayBase != "manifest://ffff:gggg:hhhh" {
		t.Errorf("chained overlay fold = %q, want manifest://ffff:gggg:hhhh", overlayBase)
	}

	// missing base image ref is an error.
	if _, _, _, err = parseTemplateDisk([]byte(`{"Boot": {"Root": {}}}`)); err == nil {
		t.Error("missing base ref: expected error, got nil")
	}

	// malformed JSON is an error.
	if _, _, _, err = parseTemplateDisk([]byte(`not json`)); err == nil {
		t.Error("malformed json: expected error, got nil")
	}
}

func TestUploadImageReusesBaseImageKey(t *testing.T) {
	key := strings.Repeat("a", 64)
	p := &buildPipeline{baseImageKey: key}
	got, err := p.uploadImage()
	if err != nil {
		t.Fatalf("uploadImage: %v", err)
	}
	if got != key {
		t.Fatalf("uploadImage = %q, want %q", got, key)
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
