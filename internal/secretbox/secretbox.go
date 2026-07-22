// Package secretbox encrypts small secrets (the per-tenant manifest key) at rest
// with an AES-256-GCM key set. The active key (index 0) encrypts; every key can
// decrypt, so keys rotate by prepending a new active key and keeping the old ones
// as standby until their records are re-encrypted.
//
// Record (hex-encoded in the TEXT column):
//
//	keytag(4) ‖ gcm_nonce(12) ‖ ciphertext+tag
//	keytag = SHA256(rawKey)[:4]   // selects the decrypting key
package secretbox

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const tagLen = 4

type aeadKey struct {
	tag  [tagLen]byte
	aead cipher.AEAD
}

// Box holds the key set. The first key added is the active (encrypting) key.
type Box struct{ keys []aeadKey }

// NewFromColonHex builds a Box from a ":"-separated list of 64-hex AES-256 keys
// (the first is active). Used for the orchestrator's encryption_key config / the
// NODE_CONFIG_ENCRYPTION_KEY env. Empty/blank entries are ignored.
func NewFromColonHex(spec string) (*Box, error) {
	var b Box
	seen := map[[tagLen]byte]bool{}
	for _, p := range strings.Split(spec, ":") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		raw, err := hex.DecodeString(p)
		if err != nil || len(raw) != 32 {
			return nil, errors.New("secretbox: each encryption key must be 64 hex chars (32 bytes)")
		}
		blk, err := aes.NewCipher(raw)
		if err != nil {
			return nil, fmt.Errorf("secretbox: aes: %w", err)
		}
		aead, err := cipher.NewGCM(blk)
		if err != nil {
			return nil, fmt.Errorf("secretbox: gcm: %w", err)
		}
		var tag [tagLen]byte
		sum := sha256.Sum256(raw)
		copy(tag[:], sum[:tagLen])
		if seen[tag] {
			continue // same key listed twice
		}
		seen[tag] = true
		b.keys = append(b.keys, aeadKey{tag: tag, aead: aead})
	}
	if len(b.keys) == 0 {
		return nil, errors.New("secretbox: at least one encryption key required")
	}
	return &b, nil
}

// Encrypt seals plaintext under the active key and returns the hex record.
func (b *Box) Encrypt(plaintext []byte) (string, error) { return b.EncryptAAD(plaintext, nil) }

// Decrypt opens a hex record produced by Encrypt, selecting the key by its tag.
func (b *Box) Decrypt(record string) ([]byte, error) { return b.DecryptAAD(record, nil) }

// EncryptAAD seals plaintext under the active key, additionally authenticating
// (but not encrypting) aad: Decrypt/DecryptAAD only opens the record when given
// the exact same aad it was sealed with. Use this to bind a ciphertext to the
// row/context it belongs to (e.g. record type + owner id + revision) so it
// cannot be replayed into a different row even if the ciphertext were copied.
// A nil aad behaves exactly like Encrypt.
func (b *Box) EncryptAAD(plaintext, aad []byte) (string, error) {
	k := b.keys[0]
	nonce := make([]byte, k.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ct := k.aead.Seal(nil, nonce, plaintext, aad)
	blob := make([]byte, 0, tagLen+len(nonce)+len(ct))
	blob = append(blob, k.tag[:]...)
	blob = append(blob, nonce...)
	blob = append(blob, ct...)
	return hex.EncodeToString(blob), nil
}

// DecryptAAD opens a hex record produced by EncryptAAD/Encrypt, selecting the
// key by its tag and requiring aad to match the value the record was sealed
// with. A nil aad behaves exactly like Decrypt.
func (b *Box) DecryptAAD(record string, aad []byte) ([]byte, error) {
	blob, err := hex.DecodeString(record)
	if err != nil {
		return nil, errors.New("secretbox: record is not hex")
	}
	if len(blob) < tagLen+12 {
		return nil, errors.New("secretbox: record too short")
	}
	tag := blob[:tagLen]
	for _, k := range b.keys {
		if bytes.Equal(k.tag[:], tag) {
			ns := k.aead.NonceSize()
			if len(blob) < tagLen+ns {
				return nil, errors.New("secretbox: record too short for nonce")
			}
			nonce := blob[tagLen : tagLen+ns]
			ct := blob[tagLen+ns:]
			return k.aead.Open(nil, nonce, ct, aad)
		}
	}
	return nil, errors.New("secretbox: no encryption key matches this record (rotated away?)")
}

// EncryptString / DecryptString are string conveniences for hex manifest keys.
func (b *Box) EncryptString(s string) (string, error) { return b.Encrypt([]byte(s)) }
func (b *Box) DecryptString(record string) (string, error) {
	p, err := b.Decrypt(record)
	return string(p), err
}
