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

func TestBadKeys(t *testing.T) {
	for _, s := range []string{"", "   ", "abc", hex.EncodeToString(make([]byte, 16)) /*16B*/} {
		if _, err := NewFromColonHex(s); err == nil {
			t.Errorf("NewFromColonHex(%q) = nil err, want error", s)
		}
	}
}

func TestEncryptAADRoundTrip(t *testing.T) {
	b, err := NewFromColonHex(hexKey(t))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := b.EncryptAAD([]byte("plaintext"), []byte("aad-context"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.DecryptAAD(rec, []byte("aad-context"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "plaintext" {
		t.Fatalf("decrypt = %q, want %q", got, "plaintext")
	}
}

func TestDecryptAADRejectsWrongAAD(t *testing.T) {
	b, err := NewFromColonHex(hexKey(t))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := b.EncryptAAD([]byte("plaintext"), []byte("aad-v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.DecryptAAD(rec, []byte("aad-v2")); err == nil {
		t.Fatal("decrypt succeeded with mismatched AAD")
	}
	if _, err := b.DecryptAAD(rec, nil); err == nil {
		t.Fatal("decrypt succeeded with nil AAD against a record encrypted with AAD")
	}
}

func TestDecryptAADRejectsWrongKey(t *testing.T) {
	b1, err := NewFromColonHex(hexKey(t))
	if err != nil {
		t.Fatal(err)
	}
	b2, err := NewFromColonHex(hexKey(t))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := b1.EncryptAAD([]byte("plaintext"), []byte("aad"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b2.DecryptAAD(rec, []byte("aad")); err == nil {
		t.Fatal("decrypt succeeded with a box holding an unrelated key")
	}
}

func TestEncryptDecryptStillWorkWithNilAAD(t *testing.T) {
	// Back-compat: plain Encrypt/Decrypt must remain equivalent to
	// EncryptAAD/DecryptAAD with nil AAD, interchangeably.
	b, err := NewFromColonHex(hexKey(t))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := b.Encrypt([]byte("plaintext"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := b.DecryptAAD(rec, nil); err != nil || string(got) != "plaintext" {
		t.Fatalf("DecryptAAD(nil) on an Encrypt()'d record: got=%q err=%v", got, err)
	}
	rec2, err := b.EncryptAAD([]byte("plaintext2"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := b.Decrypt(rec2); err != nil || string(got) != "plaintext2" {
		t.Fatalf("Decrypt() on an EncryptAAD(nil)'d record: got=%q err=%v", got, err)
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
