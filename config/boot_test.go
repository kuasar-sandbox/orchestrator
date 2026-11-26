package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestBootCmdlinePreservesPresenceAndCloneIsolation(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
		want *string
	}{
		{name: "inherit"},
		{name: "clear", yaml: `cmdline: ""`, want: new("")},
		{name: "replace", yaml: `cmdline: 'loglevel=4 example.value="two words"'`, want: new(`loglevel=4 example.value="two words"`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := DecodeConductor(strings.NewReader("sandbox:\n  boot:\n    kernel: /node/vmlinux\n    " + tc.yaml + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg.Sandbox.Boot.Cmdline, tc.want) {
				t.Fatalf("decoded cmdline = %v, want %v", cfg.Sandbox.Boot.Cmdline, tc.want)
			}
			for _, format := range []string{"json", "yaml"} {
				var raw []byte
				var round Conductor
				if format == "json" {
					raw, err = json.Marshal(cfg)
					if err == nil {
						err = json.Unmarshal(raw, &round)
					}
				} else {
					raw, err = yaml.Marshal(cfg)
					if err == nil {
						err = yaml.Unmarshal(raw, &round)
					}
				}
				if err != nil || !reflect.DeepEqual(round.Sandbox.Boot, cfg.Sandbox.Boot) {
					t.Fatalf("%s boot round trip: %+v, %v", format, round.Sandbox.Boot, err)
				}
			}
			clone := cfg.Clone()
			if !reflect.DeepEqual(clone.Sandbox.Boot, cfg.Sandbox.Boot) {
				t.Fatal("clone changed boot configuration")
			}
			if clone.Sandbox.Boot.Cmdline != nil {
				*clone.Sandbox.Boot.Cmdline = "changed=true"
				if !reflect.DeepEqual(cfg.Sandbox.Boot.Cmdline, tc.want) {
					t.Fatal("clone shares cmdline storage with the original")
				}
			}
		})
	}
}
