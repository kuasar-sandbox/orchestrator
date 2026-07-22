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

func TestEncryptAADRoundtrip(t *testing.T) {
	b, err := NewFromColonHex(hexKey(t))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := b.EncryptAAD([]byte("plaintext"), []byte("sid=abc:name=credentials:rev=0"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.DecryptAAD(rec, []byte("sid=abc:name=credentials:rev=0"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "plaintext" {
		t.Fatalf("decrypt=%q want %q", got, "plaintext")
	}
}

func TestDecryptAADRejectsMismatchedAAD(t *testing.T) {
	b, err := NewFromColonHex(hexKey(t))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := b.EncryptAAD([]byte("plaintext"), []byte("sid=abc:name=credentials:rev=0"))
	if err != nil {
		t.Fatal(err)
	}
	// A record encrypted for one row must not decrypt under another row's AAD —
	// this is what prevents a ciphertext being replayed into a different
	// sandbox/name/backend-type/revision if DB rows were swapped.
	if _, err := b.DecryptAAD(rec, []byte("sid=abc:name=credentials:rev=1")); err == nil {
		t.Fatal("DecryptAAD succeeded with a mismatched revision in the AAD")
	}
	if _, err := b.DecryptAAD(rec, []byte("sid=other:name=credentials:rev=0")); err == nil {
		t.Fatal("DecryptAAD succeeded with a mismatched sandbox id in the AAD")
	}
	if _, err := b.DecryptAAD(rec, nil); err == nil {
		t.Fatal("DecryptAAD succeeded with no AAD against an AAD-bound record")
	}
}

func TestEncryptDecryptDelegateToAADVariantWithNilAAD(t *testing.T) {
	b, err := NewFromColonHex(hexKey(t))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := b.Encrypt([]byte("plaintext"))
	if err != nil {
		t.Fatal(err)
	}
	// A plain Encrypt/Decrypt record is a nil-AAD record: DecryptAAD with an
	// explicit nil must open it (proves Decrypt/DecryptAAD share behavior).
	got, err := b.DecryptAAD(rec, nil)
	if err != nil || string(got) != "plaintext" {
		t.Fatalf("DecryptAAD(nil) on a plain Encrypt record: got=%q err=%v", got, err)
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
