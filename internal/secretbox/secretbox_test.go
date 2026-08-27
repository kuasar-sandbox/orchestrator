package secretbox

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
)

func hexKey(t *testing.T) string {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(k)
}

func TestRoundtrip(t *testing.T) {
	b, err := NewFromColonHex(hexKey(t))
	if err != nil {
		t.Fatal(err)
	}
	plain := "deadbeef" // a manifest key (hex string)
	rec, err := b.EncryptString(plain)
	if err != nil {
		t.Fatal(err)
	}
	if rec == plain {
		t.Fatal("record equals plaintext")
	}
	got, err := b.DecryptString(rec)
	if err != nil {
		t.Fatal(err)
	}
	if got != plain {
		t.Fatalf("decrypt=%q want %q", got, plain)
	}
}

func TestRotation(t *testing.T) {
	oldKey, newKey := hexKey(t), hexKey(t)
	oldBox, _ := NewFromColonHex(oldKey)
	rec, _ := oldBox.EncryptString("secret")

	// New active key + old standby → can still decrypt the old record.
	rotated, err := NewFromColonHex(newKey + ":" + oldKey)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := rotated.DecryptString(rec); err != nil || got != "secret" {
		t.Fatalf("standby decrypt failed: got=%q err=%v", got, err)
	}
	// New records use the new active key.
	rec2, _ := rotated.EncryptString("secret2")
	if got, _ := rotated.DecryptString(rec2); got != "secret2" {
		t.Fatal("active re-encrypt roundtrip failed")
	}
	// A box without the old key cannot read the old record.
	newOnly, _ := NewFromColonHex(newKey)
	if _, err := newOnly.DecryptString(rec); err == nil {
		t.Fatal("decrypt succeeded after rotating the key away")
	}
}

func TestAADBindingAndRotation(t *testing.T) {
	oldKey, newKey := hexKey(t), hexKey(t)
	oldBox, _ := NewFromColonHex(oldKey)
	record, err := oldBox.EncryptAAD([]byte("opaque-value"), []byte("owner-a:revision-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldBox.DecryptAAD(record, []byte("owner-b:revision-1")); err == nil {
		t.Fatal("ciphertext decrypted under substituted AAD")
	}
	rotated, err := NewFromColonHex(newKey + ":" + oldKey)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := rotated.DecryptAAD(record, []byte("owner-a:revision-1"))
	if err != nil || string(plaintext) != "opaque-value" {
		t.Fatalf("rotated AAD decrypt mismatch: err=%v", err)
	}
}

func TestBadKeys(t *testing.T) {
	for _, s := range []string{"", "   ", "abc", hex.EncodeToString(make([]byte, 16)) /*16B*/} {
		if _, err := NewFromColonHex(s); err == nil {
			t.Errorf("NewFromColonHex(%q) = nil err, want error", s)
		}
	}
}

func TestDedupSameKey(t *testing.T) {
	k := hexKey(t)
	b, err := NewFromColonHex(k + ":" + k)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.keys) != 1 {
		t.Fatalf("duplicate key not deduped: %d keys", len(b.keys))
	}
}

func TestNewAcceptsRawOrderedKeySetAndCopiesInput(t *testing.T) {
	active, old := make([]byte, 32), make([]byte, 32)
	active[0], old[0] = 1, 2
	box, err := New([][]byte{active, old})
	if err != nil {
		t.Fatal(err)
	}
	record, err := box.Encrypt([]byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	active[0] = 9
	old[0] = 9
	plaintext, err := box.Decrypt(record)
	if err != nil || string(plaintext) != "value" {
		t.Fatalf("decrypt=%q err=%v", plaintext, err)
	}
	if _, err := New([][]byte{make([]byte, 31)}); err == nil {
		t.Fatal("short raw key accepted")
	}
}
