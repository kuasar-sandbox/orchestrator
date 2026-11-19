package workspacedisk

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestWorkspaceRawSeedAndLayerBoundary(t *testing.T) {
	const seed = "file://blank.ext4@location:seeds"
	o := Options{RefLocationParent: "file:///managed", DefaultTemplate: seed}
	makeDisk := func(base, template string) json.RawMessage {
		raw, err := json.Marshal(rtconfig.DiskV1{Kind: "disk", Version: 1, Base: base, DiffTemplate: template})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for _, input := range []json.RawMessage{[]byte("{}"), makeDisk("", seed)} {
		d, locations, check, err := Plan(input, o)
		if err != nil || d.DiffTemplate != seed || check || !strings.HasPrefix(locations["seeds"], "/managed/") {
			t.Fatal(d, locations, check, err)
		}
	}
	for _, input := range []json.RawMessage{
		makeDisk("", "file:///etc/seed"), makeDisk("", "file://blank.ext4@location:other"), makeDisk("", "file://different.ext4@location:seeds"),
		makeDisk("file:///host/fs", ""), makeDisk("file://disk.raw@digest:"+strings.Repeat("0", 64)+"@location:other", ""),
	} {
		if _, _, _, err := Plan(input, o); !errors.Is(err, ErrUnauthorized) {
			t.Fatal(string(input), err)
		}
	}
	d, _, check, err := Plan(makeDisk("manifest://"+strings.Repeat("0", 64), ""), o)
	if err != nil || !check || d.Base == "" {
		t.Fatal("inline base lost asynchronous authorization", d, check, err)
	}
	o.DefaultTemplate = filepath.Join(t.TempDir(), "blank.ext4")
	d, _, check, err = Plan([]byte("{}"), o)
	if err != nil || d.DiffTemplate != "file://"+o.DefaultTemplate || check {
		t.Fatal(d, check, err)
	}
}

func TestWorkspaceSizeOnlyPrecedesConfiguredTemplate(t *testing.T) {
	input := json.RawMessage(`{"kind":"disk","version":1,"size":16777216}`)
	for _, configured := range []string{"", "file:///default.ext4", "mkfs://ext4?size=32MiB", "mkfs://ext4?size=invalid"} {
		disk, locations, check, err := Plan(input, Options{DefaultTemplate: configured})
		if err != nil || disk.DiffTemplate != "mkfs://ext4?size=16777216" || disk.Size == nil || *disk.Size != 16777216 || len(locations) != 0 || check {
			t.Fatalf("configured=%q: disk=%+v locations=%v check=%t error=%v", configured, disk, locations, check, err)
		}
	}
	for _, raw := range []string{
		`{"size":16777216}`,
		`{"kind":"disk","version":1}`,
		`{"kind":"disk","version":1,"size":0}`,
		`{"kind":"disk","version":1,"size":16777216,"diff_template":"mkfs://ext4?size=bad"}`,
		`{"kind":"disk","version":1,"size":16777216,"base_from_refs":["manifest://` + strings.Repeat("a", 64) + `"]}`,
	} {
		if _, _, _, err := Plan([]byte(raw), Options{DefaultTemplate: "mkfs://ext4?size=32MiB"}); !errors.Is(err, ErrSelection) {
			t.Errorf("invalid explicit request became default: %s: %v", raw, err)
		}
	}
}

func TestWorkspaceDefaultsDoNotInventSources(t *testing.T) {
	if _, _, _, err := Plan([]byte(`{}`), Options{}); !errors.Is(err, ErrDefaultNeedsSlot) {
		t.Fatalf("missing default: %v", err)
	}
	if _, _, _, err := Plan([]byte(`{}`), Options{DefaultTemplate: "mkfs://ext4?size=bad"}); !errors.Is(err, ErrSelection) || errors.Is(err, ErrDefaultNeedsSlot) {
		t.Fatalf("invalid configured template requested fallback: %v", err)
	}
	key := strings.Repeat("b", 64)
	for _, ref := range []string{
		"file:///seed.ext4", "file:///seed.tar@digest:" + key,
		"file:///seed.tar@hmac:" + key, "file:///seed.bundle@manifest:" + key,
		"manifest://" + key, "mkfs://ext4?size=16777216",
	} {
		disk, _, check, err := Plan([]byte(`{}`), Options{DefaultTemplate: ref})
		if err != nil || disk.DiffTemplate != ref || check {
			t.Fatalf("trusted configured template %s: %+v %t %v", ref, disk, check, err)
		}
	}
}

func TestWorkspaceTemplateFormatsKeepExternalAuthorization(t *testing.T) {
	key := strings.Repeat("c", 64)
	for _, ref := range []string{
		"file:///private/disk", "file:///private/tar@digest:" + key,
		"file://tar@hmac:" + key + "@location:seeds",
		"file://bundle@manifest:" + key + "@location:seeds", "manifest://" + key,
	} {
		raw, err := json.Marshal(rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: ref})
		if err != nil {
			t.Fatal(err)
		}
		options := Options{RefLocationParent: "file:///managed"}
		if _, _, _, err := Plan(raw, options); !errors.Is(err, ErrUnauthorized) {
			t.Errorf("external template %q bypassed default allowlist: %v", ref, err)
		}
		options.DefaultTemplate = ref
		_, _, _, err = Plan(raw, options)
		if strings.Contains(ref, "@location:seeds") {
			if err != nil {
				t.Errorf("exact named configured template rejected: %s: %v", ref, err)
			}
		} else if !errors.Is(err, ErrUnauthorized) {
			t.Errorf("unnamed external template allowed: %s: %v", ref, err)
		}
	}
	locations := rtconfig.RefLocations{}
	if err := AddLocation(locations, "invalid-parent", "mkfs://ext4?size=16777216"); err != nil || len(locations) != 0 {
		t.Fatalf("mkfs accessed a location: %v %v", locations, err)
	}
}
