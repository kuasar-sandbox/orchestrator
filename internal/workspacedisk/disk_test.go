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
	o := Options{RefLocationParent: "file:///managed", DefaultDisk: &rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: seed}}
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
	o.DefaultDisk = nil
	o.OverlayDiffTemplate = filepath.Join(t.TempDir(), "blank.ext4")
	d, _, check, err = Plan([]byte("{}"), o)
	if err != nil || d.DiffTemplate != "file://"+o.OverlayDiffTemplate || check {
		t.Fatal(d, check, err)
	}
}
