// Package apikey derives and verifies e2b API keys from a tenant's API secret.
// An API key is a bearer MAC token: a holder of the API secret mints it and the
// orchestrator verifies it against the API secret it has stored (encrypted).
// The API secret itself never travels to the SDK.
//
// Wire format: the e2b SDK validates the api key against /^e2b_[0-9a-f]+$/ (an
// "e2b_" prefix followed by lowercase hex), so the payload is hex-encoded:
//
//	api_key = "e2b_" + hex( fp(12) ‖ ts(4) ‖ nonce(4) ‖ mac(16) )   // 4 + 72 = 76 chars
//	fp    = SHA256(apiSecret)[:12]              // fast DB match (non-unique index)
//	ts    = big-endian uint32 unix seconds      // mint time (not expiry-checked)
//	nonce = 4 random bytes                       // distinct keys per mint
//	mac   = HMAC-SHA256(apiSecret, fp‖ts‖nonce)[:16]   // 128-bit, unforgeable w/o the key
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
	prefix                     = "e2b_"
	CandidateFingerprintLen    = 12
	FullFingerprintLen         = sha256.Size
	tsLen                      = 4
	nonceLen                   = 4
	macLen                     = 16
	payloadLen                 = CandidateFingerprintLen + tsLen + nonceLen + macLen // 36
	apiSecretDerivationContext = "kuasar-api-secret-v1"
)

// ErrBadFormat is returned for a structurally invalid api_key.
var ErrBadFormat = errors.New("apikey: malformed api key")

// DeriveAPISecret derives the 32-byte API secret associated with a 32-byte
// manifest key. The context string domain-separates API authentication from
// manifest encryption.
func DeriveAPISecret(manifestKey []byte) []byte {
	m := hmac.New(sha256.New, manifestKey)
	m.Write([]byte(apiSecretDerivationContext))
	return m.Sum(nil)
}

// CandidateFingerprint returns SHA256(apiSecret)[:CandidateFingerprintLen].
// It is embedded in an API key and may be used only to select verification
// candidates; it is not a unique identity for an API secret.
func CandidateFingerprint(apiSecret []byte) []byte {
	full := sha256.Sum256(apiSecret)
	out := make([]byte, CandidateFingerprintLen)
	copy(out, full[:CandidateFingerprintLen])
	return out
}

// FullFingerprint returns the complete SHA256 digest of apiSecret. It is the
// stable identity used when an exact API secret match is required.
func FullFingerprint(apiSecret []byte) []byte {
	full := sha256.Sum256(apiSecret)
	out := make([]byte, FullFingerprintLen)
	copy(out, full[:])
	return out
}

func computeMAC(apiSecret, fp, ts, nonce []byte) []byte {
	m := hmac.New(sha256.New, apiSecret)
	m.Write(fp)
	m.Write(ts)
	m.Write(nonce)
	return m.Sum(nil)[:macLen]
}

// Mint derives a fresh API key for apiSecret (current time + random nonce).
func Mint(apiSecret []byte) (string, error) {
	fp := CandidateFingerprint(apiSecret)
	ts := make([]byte, tsLen)
	binary.BigEndian.PutUint32(ts, uint32(time.Now().Unix()))
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	mac := computeMAC(apiSecret, fp, ts, nonce)

	payload := make([]byte, 0, payloadLen)
	payload = append(payload, fp...)
	payload = append(payload, ts...)
	payload = append(payload, nonce...)
	payload = append(payload, mac...)
	return prefix + hex.EncodeToString(payload), nil
}

// Parsed is a decoded api key (structure validated, MAC not yet checked).
type Parsed struct {
	CandidateFingerprint []byte // 12 bytes
	TS                   uint32
	Nonce                []byte // 4 bytes
	MAC                  []byte // 16 bytes
}

// Parse strips the prefix, hex-decodes, and splits the fixed-width fields.
// It does NOT verify the MAC (that needs the API secret — see Verify).
func Parse(apiKey string) (*Parsed, error) {
	s, ok := strings.CutPrefix(apiKey, prefix)
	if !ok || s != strings.ToLower(s) {
		return nil, ErrBadFormat
	}
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != payloadLen {
		return nil, ErrBadFormat
	}
	return &Parsed{
		CandidateFingerprint: raw[:CandidateFingerprintLen],
		TS: binary.BigEndian.Uint32(
			raw[CandidateFingerprintLen : CandidateFingerprintLen+tsLen],
		),
		Nonce: raw[CandidateFingerprintLen+tsLen : CandidateFingerprintLen+tsLen+nonceLen],
		MAC:   raw[CandidateFingerprintLen+tsLen+nonceLen:],
	}, nil
}

// Verify reports whether p was genuinely minted from apiSecret: the embedded
// fingerprint must match, and the MAC must recompute (constant-time).
func Verify(p *Parsed, apiSecret []byte) bool {
	if !hmac.Equal(p.CandidateFingerprint, CandidateFingerprint(apiSecret)) {
		return false
	}
	ts := make([]byte, tsLen)
	binary.BigEndian.PutUint32(ts, p.TS)
	want := computeMAC(apiSecret, p.CandidateFingerprint, ts, p.Nonce)
	return hmac.Equal(want, p.MAC)
}
