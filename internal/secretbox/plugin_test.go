package secretbox

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestPluginKeyRingPreservesRecordsAndBinding(t *testing.T) {
	oldKey, currentKey := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	broker := testsecret.Start(t, map[string][]byte{"current": currentKey, "short": {1, 2}})
	oldBox, err := NewFromColonHex(hex.EncodeToString(oldKey))
	if err != nil {
		t.Fatal(err)
	}
	oldRecord, err := oldBox.EncryptString("stored reference")
	if err != nil {
		t.Fatal(err)
	}
	box, err := NewFromSpec(context.Background(), " : @test/current : "+hex.EncodeToString(oldKey)+" : ")
	if err != nil {
		t.Fatal(err)
	}
	if plain, err := box.DecryptString(oldRecord); err != nil || plain != "stored reference" {
		t.Fatalf("legacy decrypt: %q, %v", plain, err)
	}
	record, err := box.EncryptString("new record")
	if err != nil {
		t.Fatal(err)
	}
	native, err := NewFromColonHex(hex.EncodeToString(currentKey))
	if err != nil {
		t.Fatal(err)
	}
	if plain, err := native.DecryptString(record); err != nil || plain != "new record" {
		t.Fatalf("native key cannot read plugin record: %q, %v", plain, err)
	}
	if _, err := oldBox.DecryptString(record); err == nil {
		t.Fatal("first ring entry was not used for encryption")
	}
	broker.Set("current", bytes.Repeat([]byte{3}, 32))
	if plain, err := box.DecryptString(record); err != nil || plain != "new record" {
		t.Fatalf("bound box changed its key: %q, %v", plain, err)
	}
	if broker.Count("current") != 1 {
		t.Fatal("record I/O resolved the plugin again")
	}
	for _, spec := range []string{"@test/missing:" + hex.EncodeToString(oldKey), "@test/short", "@unknown/key", "@test/"} {
		if _, err := NewFromSpec(context.Background(), spec); err == nil {
			t.Fatalf("invalid ring accepted: %q", spec)
		}
	}
}
