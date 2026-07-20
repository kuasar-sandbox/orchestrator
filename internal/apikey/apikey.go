// Package apikey derives and verifies e2b API keys from a tenant's AuthKey.
// AuthKey is independent from the ManifestKey used for content cryptography.
//
// Wire format: the e2b SDK validates the api key against /^e2b_[0-9a-f]+$/ (an
// "e2b_" prefix followed by lowercase hex), so the payload is hex-encoded:
//
//	api_key = "e2b_" + hex( fp(12) ‖ ts(4) ‖ nonce(4) ‖ mac(16) )   // 4 + 72 = 76 chars
//	fp    = SHA256(authKey)[:12]                // fast DB match (non-unique index)
//	ts    = big-endian uint32 unix seconds      // mint time (not expiry-checked)
//	nonce = 4 random bytes                       // distinct keys per mint
//	mac   = HMAC-SHA256(authKey, fp‖ts‖nonce)[:16]       // 128-bit, unforgeable w/o the key
package apikey

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

const (
	prefix     = "e2b_"
	FPLen      = 12 // truncated SHA256(AuthKey)
	tsLen      = 4
	nonceLen   = 4
	macLen     = 16
	payloadLen = FPLen + tsLen + nonceLen + macLen // 36
)

// ErrBadFormat is returned for a structurally invalid api_key.
var ErrBadFormat = errors.New("apikey: malformed api key")

// Fingerprint is SHA256(authKey)[:FPLen], embedded in the API key for indexed
// candidate lookup. Full MAC verification still resolves hash collisions.
func Fingerprint(authKey []byte) []byte {
	h := sha256.Sum256(authKey)
	out := make([]byte, FPLen)
	copy(out, h[:FPLen])
	return out
}

func computeMAC(authKey, fp, ts, nonce []byte) []byte {
	m := hmac.New(sha256.New, authKey)
	m.Write(fp)
	m.Write(ts)
	m.Write(nonce)
	return m.Sum(nil)[:macLen]
}

// Mint derives a fresh API key for authKey (current time + random nonce).
func Mint(authKey []byte) (string, error) {
	fp := Fingerprint(authKey)
	ts := make([]byte, tsLen)
	binary.BigEndian.PutUint32(ts, uint32(time.Now().Unix()))
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	mac := computeMAC(authKey, fp, ts, nonce)

	payload := make([]byte, 0, payloadLen)
	payload = append(payload, fp...)
	payload = append(payload, ts...)
	payload = append(payload, nonce...)
	payload = append(payload, mac...)
	return prefix + hex.EncodeToString(payload), nil
}

// Parsed is a decoded api key (structure validated, MAC not yet checked).
type Parsed struct {
	FP    []byte // 12 bytes
	TS    uint32
	Nonce []byte // 4 bytes
	MAC   []byte // 16 bytes
}

// Parse strips the prefix, hex-decodes, and splits the fixed-width fields.
// It does NOT verify the MAC (that needs the AuthKey; see Verify).
func Parse(apiKey string) (*Parsed, error) {
	s, ok := strings.CutPrefix(apiKey, prefix)
	if !ok {
		return nil, ErrBadFormat
	}
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != payloadLen {
		return nil, ErrBadFormat
	}
	return &Parsed{
		FP:    raw[:FPLen],
		TS:    binary.BigEndian.Uint32(raw[FPLen : FPLen+tsLen]),
		Nonce: raw[FPLen+tsLen : FPLen+tsLen+nonceLen],
		MAC:   raw[FPLen+tsLen+nonceLen:],
	}, nil
}

// Verify reports whether p was genuinely minted from authKey: the embedded
// fingerprint must match, and the MAC must recompute (constant-time).
func Verify(p *Parsed, authKey []byte) bool {
	if !hmac.Equal(p.FP, Fingerprint(authKey)) {
		return false
	}
	ts := make([]byte, tsLen)
	binary.BigEndian.PutUint32(ts, p.TS)
	want := computeMAC(authKey, p.FP, ts, p.Nonce)
	return hmac.Equal(want, p.MAC)
}
