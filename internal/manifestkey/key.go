// Package manifestkey adapts the shared Secret plugin resolver to the
// orchestrator's 32-byte manifest root and its existing fingerprint format.
package manifestkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/secret"
)

func Resolve(ctx context.Context, text string) ([32]byte, error) {
	var key [32]byte
	raw, err := secret.Resolve(ctx, text)
	if err != nil {
		return key, fmt.Errorf("resolve manifest key: %w", err)
	}
	defer clear(raw)
	if len(raw) != len(key) {
		return key, fmt.Errorf("manifest key must be 32 bytes, got %d", len(raw))
	}
	copy(key[:], raw)
	return key, nil
}

func Fingerprint(key [32]byte) string {
	sum := sha256.Sum256(key[:])
	return hex.EncodeToString(sum[:])
}

func Canonical(ctx context.Context, text string) (string, error) {
	key, err := Resolve(ctx, text)
	if err != nil {
		return "", err
	}
	defer clear(key[:])
	return hex.EncodeToString(key[:]), nil
}

// KnownFingerprint is a pure projection of previously validated material.
// A plugin reference needs the fingerprint carried from its resolve boundary
// or the existing database column; this function never calls a plugin.
func KnownFingerprint(text, known string) (string, error) {
	if secret.IsReference(text) {
		if !validFingerprint(known) {
			return "", fmt.Errorf("manifest key reference requires a validated fingerprint")
		}
		return known, nil
	}
	raw, err := hex.DecodeString(text)
	defer clear(raw)
	if err != nil || len(raw) != 32 || strings.ToLower(text) != text {
		return "", fmt.Errorf("manifest key must be 64 lowercase hex characters")
	}
	var key [32]byte
	copy(key[:], raw)
	fp := Fingerprint(key)
	clear(key[:])
	if known != "" && known != fp {
		return "", fmt.Errorf("manifest key fingerprint mismatch")
	}
	return fp, nil
}

func validFingerprint(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
