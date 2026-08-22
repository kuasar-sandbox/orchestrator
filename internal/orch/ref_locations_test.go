package orch

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestAddRefLocationUsesDeterministicURI(t *testing.T) {
	name := "source-1"
	o := &Orchestrator{cfg: &config.Config{Checkpoint: config.CheckpointConfig{
		Remote: config.CheckpointRemoteConfig{RefLocationParent: "file:///mnt/shared/snapshots"},
	}}}
	locations := map[string]string{}
	if err := o.addRefLocation(locations, "file://"+strings.Repeat("a", 64)+".image@location:"+name); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	want := "file:///mnt/shared/snapshots/" + digest[:2] + "/" + digest[2:4] + "/" + name
	if locations[name] != want {
		t.Fatalf("location %q = %q, want %q", name, locations[name], want)
	}
}

func TestAddRefLocationRequiresConfiguredParent(t *testing.T) {
	o := &Orchestrator{cfg: &config.Config{}}
	ref := "file://" + strings.Repeat("a", 64) + ".image@location:source"
	if err := o.addRefLocation(map[string]string{}, ref); err == nil {
		t.Fatal("located ref succeeded without ref_location_parent")
	}
}

func TestAppendRefLocationArgsSortsNames(t *testing.T) {
	got := appendRefLocationArgs([]string{"run"}, map[string]string{
		"z": "file:///z",
		"a": "file:///a",
	})
	want := []string{"run", "--ref-location", "a=file:///a", "--ref-location", "z=file:///z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %v, want %v", got, want)
	}
}
