package proxyapp

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

const maxEffectiveConfigBytes = 4 << 20

// EffectiveConfig is the canonical, immutable declarative snapshot shared by
// one proxy master and all of its worker epochs. Runtime bindings never enter
// this value.
type EffectiveConfig struct {
	config *publicconfig.Proxy
	raw    json.RawMessage
	digest [sha256.Size]byte
}

// FreezeConfig validates, deep-clones, and canonically serializes a final proxy
// configuration. The returned snapshot is detached from every Hook-visible
// value.
func FreezeConfig(cfg *publicconfig.Proxy) (*EffectiveConfig, error) {
	if err := publicconfig.ValidateProxyFinal(cfg); err != nil {
		return nil, err
	}
	frozen := cfg.Clone()
	raw, err := json.Marshal(frozen)
	if err != nil {
		return nil, fmt.Errorf("proxy app: encode effective config: %w", err)
	}
	if len(raw) == 0 || len(raw) > maxEffectiveConfigBytes || raw[0] != '{' {
		return nil, fmt.Errorf("proxy app: effective config exceeds size limit")
	}
	return &EffectiveConfig{config: frozen, raw: raw, digest: sha256.Sum256(raw)}, nil
}

func effectiveConfigFromRaw(raw []byte, digest [sha256.Size]byte) (*EffectiveConfig, error) {
	if len(raw) == 0 || len(raw) > maxEffectiveConfigBytes || raw[0] != '{' {
		return nil, fmt.Errorf("proxy worker bootstrap: effective config is invalid")
	}
	if got := sha256.Sum256(raw); got != digest {
		return nil, fmt.Errorf("proxy worker bootstrap: config digest mismatch")
	}
	if err := strictjson.RejectDuplicateKeys(raw); err != nil {
		return nil, fmt.Errorf("proxy worker bootstrap: effective config: %w", err)
	}
	var cfg publicconfig.Proxy
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("proxy worker bootstrap: decode effective config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("proxy worker bootstrap: trailing config value")
		}
		return nil, fmt.Errorf("proxy worker bootstrap: trailing config data: %w", err)
	}
	if err := publicconfig.ValidateProxyFinal(&cfg); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(&cfg)
	if err != nil {
		return nil, fmt.Errorf("proxy worker bootstrap: encode effective config: %w", err)
	}
	if !bytes.Equal(canonical, raw) {
		return nil, fmt.Errorf("proxy worker bootstrap: effective config is not canonical")
	}
	return &EffectiveConfig{
		config: cfg.Clone(), raw: append(json.RawMessage(nil), raw...), digest: digest,
	}, nil
}

// Config returns a detached declarative snapshot for process-local runtime
// resolution. Mutating it cannot affect the master snapshot or another worker.
func (e *EffectiveConfig) Config() *publicconfig.Proxy {
	if e == nil || e.config == nil {
		return nil
	}
	return e.config.Clone()
}

// Digest returns the SHA-256 digest of the canonical serialized snapshot.
func (e *EffectiveConfig) Digest() [sha256.Size]byte {
	if e == nil {
		return [sha256.Size]byte{}
	}
	return e.digest
}
