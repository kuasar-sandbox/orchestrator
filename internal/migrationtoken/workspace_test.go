package migrationtoken

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestWorkspaceMigrationVectorAndPurpose(t *testing.T) {
	size := int64(4096)
	d := rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &size, Base: "manifest://" + strings.Repeat("a", 64), BaseFromRefs: []string{"file://disk.bundle@manifest:" + strings.Repeat("b", 64) + "@location:source"}}
	token, err := sealWorkspace(bytes.NewReader(make([]byte, 12)), testMaterial, d)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(token))); got != "cf8341972b2bd016fd8b0082996f5e4a6ec2646919e9192692daef0cab2bd3ab" {
		t.Fatalf("wire vector %s", got)
	}
	got, err := OpenWorkspace(testMaterial, token)
	if err != nil || !reflect.DeepEqual(d, got) {
		t.Fatal(got, err)
	}
	for _, m := range []KeyMaterial{{testManifestKey, testManifestKey}, {testAPISecret, testAPISecret}} {
		if _, err := OpenWorkspace(m, token); err == nil {
			t.Fatal("accepted different credential")
		}
	}
	for _, wire := range []string{token, strings.Replace(token, "kwt1.", "kmt1.", 1)} {
		if _, err := Open(testMaterial, wire); err == nil {
			t.Fatal("accepted workspace token as whole-machine token")
		}
	}
	whole, err := Seal(testMaterial, validPayload())
	if err != nil {
		t.Fatal(err)
	}
	for _, wire := range []string{whole, strings.Replace(whole, "kmt1.", "kwt1.", 1)} {
		if _, err := OpenWorkspace(testMaterial, wire); err == nil {
			t.Fatal("accepted whole-machine token as workspace")
		}
	}
	for _, ref := range []string{"file:///host/disk.bundle", "file://disk@location:name", "file:///host/raw@digest:" + strings.Repeat("f", 64)} {
		d.Base = ref
		if _, err := SealWorkspace(testMaterial, d); err == nil {
			t.Fatalf("accepted nonportable disk %s", ref)
		}
	}
}

func TestWorkspaceTokenRetainsIdentifiedTarstreamLayers(t *testing.T) {
	size := int64(8192)
	disk := rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &size,
		Base: "file://current.bundle@manifest:" + strings.Repeat("a", 64) + "@location:current",
		BaseFromRefs: []string{
			"file://older.overlay@digest:" + strings.Repeat("b", 64) + "@location:older",
			"file://encrypted.overlay@hmac:" + strings.Repeat("c", 64) + "@location:encrypted",
		},
	}
	token, err := SealWorkspace(testMaterial, disk)
	if err != nil {
		t.Fatal(err)
	}
	got, err := OpenWorkspace(testMaterial, token)
	if err != nil || !reflect.DeepEqual(got, disk) {
		t.Fatalf("mixed portable layers did not round trip: %+v %v", got, err)
	}
	for _, key := range []KeyMaterial{{testManifestKey, testManifestKey}, {testAPISecret, testAPISecret}} {
		if _, err := OpenWorkspace(key, token); err == nil {
			t.Fatal("retained tarstream token accepted other tenant credentials")
		}
	}
}
