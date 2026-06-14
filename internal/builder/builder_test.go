package builder

import "testing"

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

	// chained overlay (base_from_refs) is refused, never silently dropped.
	chained := `{"Boot": {"Root": {
		"BaseRef": "manifest://eeee",
		"Overlay": {"Base": "manifest://ffff", "BaseFromRefs": ["manifest://gggg"]}
	}}}`
	if _, _, _, err = parseTemplateDisk([]byte(chained)); err == nil {
		t.Error("chained overlay: expected error, got nil")
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
